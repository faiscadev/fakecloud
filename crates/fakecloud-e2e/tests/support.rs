//! AWS Support (support) control-plane E2E.
//!
//! Exercises the support-case lifecycle, severity levels, and the Trusted
//! Advisor catalogue + refresh state machine against a spawned fakecloud server
//! via the AWS Rust SDK, which speaks the real awsJson1.1 wire format
//! (x-amz-target `AWSSupport_20130415.<Op>`):
//!
//!   CreateCase -> DescribeCases -> AddCommunicationToCase ->
//!     DescribeCommunications -> ResolveCase
//!   DescribeSeverityLevels
//!   DescribeTrustedAdvisorChecks -> RefreshTrustedAdvisorCheck ->
//!     DescribeTrustedAdvisorCheckRefreshStatuses
//!
//! Honest gap: fakecloud runs no Trusted Advisor analysis engine and attaches
//! no live support agent, so `DescribeTrustedAdvisorCheckResult` reports an
//! all-clear account. Cases, communications, severity levels, the check
//! catalogue, and the refresh state machine are real, persisted state.

mod helpers;

use helpers::TestServer;

async fn support_client(server: &TestServer) -> aws_sdk_support::Client {
    aws_sdk_support::Client::new(&server.aws_config().await)
}

#[tokio::test]
async fn case_lifecycle_severity_and_trusted_advisor() {
    let server = TestServer::start().await;
    let sup = support_client(&server).await;

    // CreateCase mints an AWS-shaped case id and opens the case.
    let created = sup
        .create_case()
        .subject("Cannot connect to my EC2 instance")
        .communication_body("SSH to the instance times out after a reboot.")
        .service_code("amazon-elastic-compute-cloud-linux")
        .severity_code("high")
        .category_code("using-aws")
        .send()
        .await
        .expect("create case");
    let case_id = created.case_id().expect("case id").to_string();
    assert!(
        case_id.starts_with("case-"),
        "case id should carry the AWS case- prefix, got {case_id}"
    );

    // A missing required member (communicationBody) is rejected.
    let bad = sup.create_case().subject("no body").send().await;
    assert!(bad.is_err(), "missing communicationBody should be rejected");

    // DescribeCases returns the case with its seeded communication thread.
    let described = sup
        .describe_cases()
        .case_id_list(&case_id)
        .send()
        .await
        .expect("describe cases");
    let cases = described.cases();
    assert_eq!(cases.len(), 1);
    let case = &cases[0];
    assert_eq!(case.case_id(), Some(case_id.as_str()));
    assert_eq!(case.status(), Some("opened"));
    assert!(case.display_id().is_some(), "case should have a displayId");
    let recent = case.recent_communications().expect("recent communications");
    assert_eq!(
        recent.communications().len(),
        1,
        "seeded with the initial communication"
    );

    // Unknown case ids surface CaseIdNotFound.
    let missing = sup
        .describe_cases()
        .case_id_list("case-000000000000-2013-deadbeef")
        .send()
        .await;
    assert!(missing.is_err(), "unknown case id should be rejected");

    // AddCommunicationToCase appends to the thread.
    let added = sup
        .add_communication_to_case()
        .case_id(&case_id)
        .communication_body("Any update on this? Still cannot connect.")
        .send()
        .await
        .expect("add communication");
    assert!(
        added.result(),
        "AddCommunicationToCase returns result: true"
    );

    let comms = sup
        .describe_communications()
        .case_id(&case_id)
        .send()
        .await
        .expect("describe communications");
    assert_eq!(
        comms.communications().len(),
        2,
        "initial + appended communication"
    );

    // ResolveCase flips the case to resolved.
    let resolved = sup
        .resolve_case()
        .case_id(&case_id)
        .send()
        .await
        .expect("resolve case");
    assert_eq!(resolved.initial_case_status(), Some("opened"));
    assert_eq!(resolved.final_case_status(), Some("resolved"));

    // DescribeSeverityLevels returns the five real levels.
    let sev = sup
        .describe_severity_levels()
        .send()
        .await
        .expect("describe severity levels");
    let codes: Vec<&str> = sev
        .severity_levels()
        .iter()
        .filter_map(|s| s.code())
        .collect();
    assert!(codes.contains(&"low"));
    assert!(codes.contains(&"critical"));
    assert_eq!(sev.severity_levels().len(), 5);

    // DescribeTrustedAdvisorChecks returns the catalogue.
    let checks = sup
        .describe_trusted_advisor_checks()
        .language("en")
        .send()
        .await
        .expect("describe TA checks");
    assert!(
        !checks.checks().is_empty(),
        "the TA catalogue should not be empty"
    );
    let check_id = checks.checks()[0].id().to_string();

    // RefreshTrustedAdvisorCheck enqueues a refresh.
    let refreshed = sup
        .refresh_trusted_advisor_check()
        .check_id(&check_id)
        .send()
        .await
        .expect("refresh TA check");
    let rst = refreshed.status().expect("refresh status");
    assert_eq!(rst.check_id(), check_id.as_str());
    assert_eq!(rst.status(), "enqueued");

    // Reading the refresh status advances the state machine.
    let statuses = sup
        .describe_trusted_advisor_check_refresh_statuses()
        .check_ids(Some(check_id.clone()))
        .send()
        .await
        .expect("describe refresh statuses");
    let st = &statuses.statuses()[0];
    assert_eq!(st.check_id(), check_id.as_str());
    assert_eq!(
        st.status(),
        "processing",
        "enqueued -> processing on the first read"
    );

    // A well-formed all-clear result comes back for the check.
    let result = sup
        .describe_trusted_advisor_check_result()
        .check_id(&check_id)
        .send()
        .await
        .expect("describe check result");
    let r = result.result().expect("result");
    assert_eq!(r.check_id(), check_id.as_str());
    assert_eq!(
        r.resources_summary().map(|s| s.resources_flagged()),
        Some(0),
        "all-clear account: no flagged resources"
    );
}
