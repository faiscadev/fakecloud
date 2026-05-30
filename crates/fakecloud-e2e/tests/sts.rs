// bug-audit 2026-05-28, 5.4: AssumeRole targeting a role that does not exist
// must be denied, not silently minted credentials.
#[tokio::test]
async fn sts_assume_role_nonexistent_role_is_denied() {
    let server = TestServer::start().await;
    let endpoint = server.endpoint();

    let config = aws_sdk_sts::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new("us-east-1"))
        .credentials_provider(Credentials::new(
            "AKIAIOSFODNN7EXAMPLE",
            "secret",
            None,
            None,
            "test",
        ))
        .endpoint_url(endpoint)
        .build();
    let sts = aws_sdk_sts::Client::from_conf(config);
    let err = sts
        .assume_role()
        .role_arn("arn:aws:iam::123456789012:role/this-role-does-not-exist-5p4")
        .role_session_name("session")
        .send()
        .await
        .expect_err("assuming a non-existent role must be denied");
    let code = match &err {
        SdkError::ServiceError(e) => e.err().meta().code().map(str::to_string),
        _ => None,
    };
    assert_eq!(code.as_deref(), Some("AccessDenied"), "err: {err:?}");
}
