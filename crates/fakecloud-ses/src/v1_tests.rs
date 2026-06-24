use super::*;
use bytes::Bytes;

use http::HeaderMap;
use parking_lot::RwLock;
use std::sync::Arc;

fn make_state() -> SharedSesState {
    Arc::new(RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new(
            "123456789012",
            "us-east-1",
            "http://localhost:4569",
        ),
    ))
}

fn seed_identity(state: &SharedSesState, name: &str) {
    use crate::state::EmailIdentity;
    let mut accounts = state.write();
    let st = accounts.get_or_create("123456789012");
    let identity_type = if name.contains('@') {
        "EMAIL_ADDRESS"
    } else {
        "DOMAIN"
    };
    st.identities.insert(
        name.to_string(),
        EmailIdentity {
            identity_name: name.to_string(),
            identity_type: identity_type.to_string(),
            verified: true,
            created_at: chrono::Utc::now(),
            dkim_signing_enabled: true,
            dkim_signing_attributes_origin: "AWS_SES".to_string(),
            dkim_domain_signing_private_key: None,
            dkim_domain_signing_selector: None,
            dkim_next_signing_key_length: None,
            email_forwarding_enabled: true,
            mail_from_domain: None,
            mail_from_behavior_on_mx_failure: "USE_DEFAULT_VALUE".to_string(),
            mail_from_domain_status: "NotStarted".to_string(),
            dkim_public_key_b64: None,
            configuration_set_name: None,
            bounce_topic: None,
            complaint_topic: None,
            delivery_topic: None,
        },
    );
}

/// Flip the account out of sandbox so tests focused on send mechanics
/// don't trip the recipient-verification gate.
fn enable_production_access(state: &SharedSesState) {
    let mut accounts = state.write();
    let st = accounts.get_or_create("123456789012");
    st.account_settings.production_access_enabled = true;
}

/// Flip into sandbox mode for tests that explicitly verify the sandbox
/// recipient gate. Default is production access.
fn disable_production_access(state: &SharedSesState) {
    let mut accounts = state.write();
    let st = accounts.get_or_create("123456789012");
    st.account_settings.production_access_enabled = false;
}

fn make_v1_request(action: &str, params: Vec<(&str, &str)>) -> AwsRequest {
    let mut query_params: HashMap<String, String> = HashMap::new();
    query_params.insert("Action".to_string(), action.to_string());
    for (k, v) in params {
        query_params.insert(k.to_string(), v.to_string());
    }
    AwsRequest {
        service: "ses".to_string(),
        action: action.to_string(),
        region: "us-east-1".to_string(),
        account_id: "123456789012".to_string(),
        request_id: "test-request-id".to_string(),
        headers: HeaderMap::new(),
        query_params,
        body: Bytes::new(),
        body_stream: parking_lot::Mutex::new(None),
        path_segments: Vec::new(),
        raw_path: "/".to_string(),
        raw_query: String::new(),
        method: http::Method::POST,
        is_query_protocol: true,
        access_key_id: None,
        principal: None,
    }
}

#[test]
fn test_create_receipt_rule_set() {
    let state = make_state();
    let req = make_v1_request("CreateReceiptRuleSet", vec![("RuleSetName", "my-rules")]);
    let resp = handle_v1_action(&state, &req).unwrap();
    assert_eq!(resp.status, StatusCode::OK);
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(body.contains("CreateReceiptRuleSetResponse"));

    // Duplicate should fail
    let req = make_v1_request("CreateReceiptRuleSet", vec![("RuleSetName", "my-rules")]);
    match handle_v1_action(&state, &req) {
        Err(e) => assert_eq!(e.code(), "AlreadyExistsException"),
        Ok(_) => panic!("expected error"),
    }
}

#[test]
fn test_list_receipt_rule_sets() {
    let state = make_state();
    // Create two rule sets
    handle_v1_action(
        &state,
        &make_v1_request("CreateReceiptRuleSet", vec![("RuleSetName", "set-a")]),
    )
    .unwrap();
    handle_v1_action(
        &state,
        &make_v1_request("CreateReceiptRuleSet", vec![("RuleSetName", "set-b")]),
    )
    .unwrap();

    let req = make_v1_request("ListReceiptRuleSets", vec![]);
    let resp = handle_v1_action(&state, &req).unwrap();
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(body.contains("<Name>set-a</Name>"));
    assert!(body.contains("<Name>set-b</Name>"));
}

#[test]
fn test_delete_receipt_rule_set() {
    let state = make_state();
    handle_v1_action(
        &state,
        &make_v1_request("CreateReceiptRuleSet", vec![("RuleSetName", "to-delete")]),
    )
    .unwrap();
    let req = make_v1_request("DeleteReceiptRuleSet", vec![("RuleSetName", "to-delete")]);
    let resp = handle_v1_action(&state, &req).unwrap();
    assert_eq!(resp.status, StatusCode::OK);

    // Should not exist anymore
    match handle_v1_action(&state, &req) {
        Err(e) => assert_eq!(e.code(), "RuleSetDoesNotExistException"),
        Ok(_) => panic!("expected error"),
    }
}

#[test]
fn test_cannot_delete_active_rule_set() {
    let state = make_state();
    handle_v1_action(
        &state,
        &make_v1_request("CreateReceiptRuleSet", vec![("RuleSetName", "active-set")]),
    )
    .unwrap();
    handle_v1_action(
        &state,
        &make_v1_request(
            "SetActiveReceiptRuleSet",
            vec![("RuleSetName", "active-set")],
        ),
    )
    .unwrap();

    match handle_v1_action(
        &state,
        &make_v1_request("DeleteReceiptRuleSet", vec![("RuleSetName", "active-set")]),
    ) {
        Err(e) => assert_eq!(e.code(), "CannotDeleteException"),
        Ok(_) => panic!("expected error"),
    }
}

#[test]
fn test_clone_receipt_rule_set() {
    let state = make_state();
    // Create source with a rule
    handle_v1_action(
        &state,
        &make_v1_request("CreateReceiptRuleSet", vec![("RuleSetName", "source")]),
    )
    .unwrap();
    handle_v1_action(
        &state,
        &make_v1_request(
            "CreateReceiptRule",
            vec![
                ("RuleSetName", "source"),
                ("Rule.Name", "rule1"),
                ("Rule.Enabled", "true"),
            ],
        ),
    )
    .unwrap();

    // Clone
    let req = make_v1_request(
        "CloneReceiptRuleSet",
        vec![("RuleSetName", "cloned"), ("OriginalRuleSetName", "source")],
    );
    handle_v1_action(&state, &req).unwrap();

    // Verify clone has the rule
    {
        let mas = state.read();
        let st = mas.default_ref();
        let cloned = st.receipt_rule_sets.get("cloned").unwrap();
        assert_eq!(cloned.rules.len(), 1);
        assert_eq!(cloned.rules[0].name, "rule1");
    }
}

#[test]
fn test_set_active_receipt_rule_set() {
    let state = make_state();
    handle_v1_action(
        &state,
        &make_v1_request("CreateReceiptRuleSet", vec![("RuleSetName", "my-set")]),
    )
    .unwrap();

    // Activate
    handle_v1_action(
        &state,
        &make_v1_request("SetActiveReceiptRuleSet", vec![("RuleSetName", "my-set")]),
    )
    .unwrap();
    assert_eq!(
        state
            .read()
            .default_ref()
            .active_receipt_rule_set
            .as_deref(),
        Some("my-set")
    );

    // Deactivate (empty name)
    handle_v1_action(
        &state,
        &make_v1_request("SetActiveReceiptRuleSet", vec![("RuleSetName", "")]),
    )
    .unwrap();
    assert!(state.read().default_ref().active_receipt_rule_set.is_none());
}

#[test]
fn test_create_and_describe_receipt_rule() {
    let state = make_state();
    handle_v1_action(
        &state,
        &make_v1_request("CreateReceiptRuleSet", vec![("RuleSetName", "my-set")]),
    )
    .unwrap();

    // Create rule with S3 action and recipients
    let req = make_v1_request(
        "CreateReceiptRule",
        vec![
            ("RuleSetName", "my-set"),
            ("Rule.Name", "store-email"),
            ("Rule.Enabled", "true"),
            ("Rule.ScanEnabled", "true"),
            ("Rule.TlsPolicy", "Require"),
            ("Rule.Recipients.member.1", "user@example.com"),
            ("Rule.Recipients.member.2", "example.com"),
            ("Rule.Actions.member.1.S3Action.BucketName", "my-bucket"),
            ("Rule.Actions.member.1.S3Action.ObjectKeyPrefix", "emails/"),
        ],
    );
    handle_v1_action(&state, &req).unwrap();

    // Describe the rule
    let req = make_v1_request(
        "DescribeReceiptRule",
        vec![("RuleSetName", "my-set"), ("RuleName", "store-email")],
    );
    let resp = handle_v1_action(&state, &req).unwrap();
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(body.contains("<Name>store-email</Name>"));
    assert!(body.contains("<Enabled>true</Enabled>"));
    assert!(body.contains("<ScanEnabled>true</ScanEnabled>"));
    assert!(body.contains("<TlsPolicy>Require</TlsPolicy>"));
    assert!(body.contains("<BucketName>my-bucket</BucketName>"));
    assert!(body.contains("<ObjectKeyPrefix>emails/</ObjectKeyPrefix>"));
    assert!(body.contains("user@example.com"));
}

#[test]
fn test_update_receipt_rule() {
    let state = make_state();
    handle_v1_action(
        &state,
        &make_v1_request("CreateReceiptRuleSet", vec![("RuleSetName", "my-set")]),
    )
    .unwrap();
    handle_v1_action(
        &state,
        &make_v1_request(
            "CreateReceiptRule",
            vec![
                ("RuleSetName", "my-set"),
                ("Rule.Name", "rule1"),
                ("Rule.Enabled", "true"),
            ],
        ),
    )
    .unwrap();

    // Update: disable the rule and add action
    let req = make_v1_request(
        "UpdateReceiptRule",
        vec![
            ("RuleSetName", "my-set"),
            ("Rule.Name", "rule1"),
            ("Rule.Enabled", "false"),
            (
                "Rule.Actions.member.1.SNSAction.TopicArn",
                "arn:aws:sns:us-east-1:123456789012:my-topic",
            ),
        ],
    );
    handle_v1_action(&state, &req).unwrap();

    {
        let mas = state.read();
        let st = mas.default_ref();
        let rule = &st.receipt_rule_sets.get("my-set").unwrap().rules[0];
        assert!(!rule.enabled);
        assert_eq!(rule.actions.len(), 1);
        assert!(matches!(&rule.actions[0], ReceiptAction::Sns { .. }));
    }
}

#[test]
fn test_delete_receipt_rule() {
    let state = make_state();
    handle_v1_action(
        &state,
        &make_v1_request("CreateReceiptRuleSet", vec![("RuleSetName", "my-set")]),
    )
    .unwrap();
    handle_v1_action(
        &state,
        &make_v1_request(
            "CreateReceiptRule",
            vec![("RuleSetName", "my-set"), ("Rule.Name", "rule1")],
        ),
    )
    .unwrap();

    let req = make_v1_request(
        "DeleteReceiptRule",
        vec![("RuleSetName", "my-set"), ("RuleName", "rule1")],
    );
    handle_v1_action(&state, &req).unwrap();

    {
        let mas = state.read();
        let st = mas.default_ref();
        assert!(st.receipt_rule_sets.get("my-set").unwrap().rules.is_empty());
    }
}

#[test]
fn test_reorder_receipt_rule_set() {
    let state = make_state();
    handle_v1_action(
        &state,
        &make_v1_request("CreateReceiptRuleSet", vec![("RuleSetName", "my-set")]),
    )
    .unwrap();
    for name in &["a", "b", "c"] {
        handle_v1_action(
            &state,
            &make_v1_request(
                "CreateReceiptRule",
                vec![("RuleSetName", "my-set"), ("Rule.Name", name)],
            ),
        )
        .unwrap();
    }

    // Reorder: c, a, b
    let req = make_v1_request(
        "ReorderReceiptRuleSet",
        vec![
            ("RuleSetName", "my-set"),
            ("RuleNames.member.1", "c"),
            ("RuleNames.member.2", "a"),
            ("RuleNames.member.3", "b"),
        ],
    );
    handle_v1_action(&state, &req).unwrap();

    {
        let mas = state.read();
        let st = mas.default_ref();
        let names: Vec<&str> = st
            .receipt_rule_sets
            .get("my-set")
            .unwrap()
            .rules
            .iter()
            .map(|r| r.name.as_str())
            .collect();
        assert_eq!(names, vec!["c", "a", "b"]);
    }
}

#[test]
fn test_receipt_filter_lifecycle() {
    let state = make_state();

    // Create filter
    let req = make_v1_request(
        "CreateReceiptFilter",
        vec![
            ("Filter.Name", "allow-internal"),
            ("Filter.IpFilter.Cidr", "10.0.0.0/8"),
            ("Filter.IpFilter.Policy", "Allow"),
        ],
    );
    handle_v1_action(&state, &req).unwrap();

    // List filters
    let req = make_v1_request("ListReceiptFilters", vec![]);
    let resp = handle_v1_action(&state, &req).unwrap();
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(body.contains("<Name>allow-internal</Name>"));
    assert!(body.contains("<Cidr>10.0.0.0/8</Cidr>"));
    assert!(body.contains("<Policy>Allow</Policy>"));

    // Delete filter
    let req = make_v1_request(
        "DeleteReceiptFilter",
        vec![("FilterName", "allow-internal")],
    );
    handle_v1_action(&state, &req).unwrap();

    // List should be empty
    let req = make_v1_request("ListReceiptFilters", vec![]);
    let resp = handle_v1_action(&state, &req).unwrap();
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(!body.contains("allow-internal"));
}

#[test]
fn test_evaluate_inbound_email_no_active_set() {
    let state = make_state();
    let (msg_id, matched, actions) = evaluate_inbound_email(
        &state,
        "sender@example.com",
        &["recipient@example.com".to_string()],
        "Test",
        "Hello",
    );
    assert!(!msg_id.is_empty());
    assert!(matched.is_empty());
    assert!(actions.is_empty());
}

#[test]
fn test_evaluate_inbound_email_matching_rule() {
    let state = make_state();

    // Setup: create rule set, add rule, activate
    handle_v1_action(
        &state,
        &make_v1_request("CreateReceiptRuleSet", vec![("RuleSetName", "active")]),
    )
    .unwrap();
    handle_v1_action(
        &state,
        &make_v1_request(
            "CreateReceiptRule",
            vec![
                ("RuleSetName", "active"),
                ("Rule.Name", "catch-all"),
                ("Rule.Enabled", "true"),
                ("Rule.Actions.member.1.S3Action.BucketName", "emails-bucket"),
            ],
        ),
    )
    .unwrap();
    handle_v1_action(
        &state,
        &make_v1_request("SetActiveReceiptRuleSet", vec![("RuleSetName", "active")]),
    )
    .unwrap();

    let (_msg_id, matched, actions) = evaluate_inbound_email(
        &state,
        "sender@example.com",
        &["anyone@example.com".to_string()],
        "Hello",
        "Body",
    );
    assert_eq!(matched, vec!["catch-all"]);
    assert_eq!(actions.len(), 1);
    assert!(
        matches!(&actions[0].1, ReceiptAction::S3 { bucket_name, .. } if bucket_name == "emails-bucket")
    );
}

#[test]
fn test_evaluate_inbound_email_recipient_filter() {
    let state = make_state();
    handle_v1_action(
        &state,
        &make_v1_request("CreateReceiptRuleSet", vec![("RuleSetName", "set")]),
    )
    .unwrap();
    handle_v1_action(
        &state,
        &make_v1_request(
            "CreateReceiptRule",
            vec![
                ("RuleSetName", "set"),
                ("Rule.Name", "domain-rule"),
                ("Rule.Enabled", "true"),
                ("Rule.Recipients.member.1", "example.com"),
                (
                    "Rule.Actions.member.1.SNSAction.TopicArn",
                    "arn:aws:sns:us-east-1:123456789012:topic",
                ),
            ],
        ),
    )
    .unwrap();
    handle_v1_action(
        &state,
        &make_v1_request("SetActiveReceiptRuleSet", vec![("RuleSetName", "set")]),
    )
    .unwrap();

    // Should match: recipient@example.com matches domain "example.com"
    let (_msg_id, matched, _actions) = evaluate_inbound_email(
        &state,
        "sender@other.com",
        &["recipient@example.com".to_string()],
        "Test",
        "Body",
    );
    assert_eq!(matched, vec!["domain-rule"]);

    // Should NOT match: recipient@other.com
    let (_msg_id, matched, _actions) = evaluate_inbound_email(
        &state,
        "sender@other.com",
        &["recipient@other.com".to_string()],
        "Test",
        "Body",
    );
    assert!(matched.is_empty());
}

#[test]
fn test_evaluate_inbound_email_stop_action() {
    let state = make_state();
    handle_v1_action(
        &state,
        &make_v1_request("CreateReceiptRuleSet", vec![("RuleSetName", "set")]),
    )
    .unwrap();
    // Rule 1: stop action
    handle_v1_action(
        &state,
        &make_v1_request(
            "CreateReceiptRule",
            vec![
                ("RuleSetName", "set"),
                ("Rule.Name", "stop-rule"),
                ("Rule.Enabled", "true"),
                ("Rule.Actions.member.1.StopAction.Scope", "RuleSet"),
            ],
        ),
    )
    .unwrap();
    // Rule 2: should not be reached
    handle_v1_action(
        &state,
        &make_v1_request(
            "CreateReceiptRule",
            vec![
                ("RuleSetName", "set"),
                ("Rule.Name", "after-stop"),
                ("Rule.Enabled", "true"),
                ("Rule.Actions.member.1.S3Action.BucketName", "bucket"),
            ],
        ),
    )
    .unwrap();
    handle_v1_action(
        &state,
        &make_v1_request("SetActiveReceiptRuleSet", vec![("RuleSetName", "set")]),
    )
    .unwrap();

    let (_msg_id, matched, actions) = evaluate_inbound_email(
        &state,
        "sender@example.com",
        &["anyone@example.com".to_string()],
        "Test",
        "Body",
    );
    // Only stop-rule should match, after-stop should not be evaluated
    assert_eq!(matched, vec!["stop-rule"]);
    assert_eq!(actions.len(), 1);
    assert!(matches!(&actions[0].1, ReceiptAction::Stop { .. }));
}

#[test]
fn test_describe_receipt_rule_set() {
    let state = make_state();
    handle_v1_action(
        &state,
        &make_v1_request("CreateReceiptRuleSet", vec![("RuleSetName", "my-set")]),
    )
    .unwrap();
    handle_v1_action(
        &state,
        &make_v1_request(
            "CreateReceiptRule",
            vec![
                ("RuleSetName", "my-set"),
                ("Rule.Name", "rule1"),
                ("Rule.Enabled", "true"),
            ],
        ),
    )
    .unwrap();

    let req = make_v1_request("DescribeReceiptRuleSet", vec![("RuleSetName", "my-set")]);
    let resp = handle_v1_action(&state, &req).unwrap();
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(body.contains("<Name>my-set</Name>"));
    assert!(body.contains("<Name>rule1</Name>"));
    assert!(body.contains("<Rules>"));
}

#[test]
fn test_all_action_types_parsing() {
    let state = make_state();
    handle_v1_action(
        &state,
        &make_v1_request("CreateReceiptRuleSet", vec![("RuleSetName", "set")]),
    )
    .unwrap();

    let req = make_v1_request(
        "CreateReceiptRule",
        vec![
            ("RuleSetName", "set"),
            ("Rule.Name", "multi-action"),
            ("Rule.Enabled", "true"),
            ("Rule.Actions.member.1.S3Action.BucketName", "bucket"),
            (
                "Rule.Actions.member.2.SNSAction.TopicArn",
                "arn:aws:sns:us-east-1:123:topic",
            ),
            ("Rule.Actions.member.2.SNSAction.Encoding", "UTF-8"),
            (
                "Rule.Actions.member.3.LambdaAction.FunctionArn",
                "arn:aws:lambda:us-east-1:123:function:my-fn",
            ),
            ("Rule.Actions.member.3.LambdaAction.InvocationType", "Event"),
            ("Rule.Actions.member.4.BounceAction.SmtpReplyCode", "550"),
            ("Rule.Actions.member.4.BounceAction.Message", "rejected"),
            (
                "Rule.Actions.member.4.BounceAction.Sender",
                "noreply@example.com",
            ),
            ("Rule.Actions.member.5.AddHeaderAction.HeaderName", "X-Test"),
            ("Rule.Actions.member.5.AddHeaderAction.HeaderValue", "true"),
            ("Rule.Actions.member.6.StopAction.Scope", "RuleSet"),
        ],
    );
    handle_v1_action(&state, &req).unwrap();

    {
        let mas = state.read();
        let st = mas.default_ref();
        let rule = &st.receipt_rule_sets.get("set").unwrap().rules[0];
        assert_eq!(rule.actions.len(), 6);
        assert!(matches!(&rule.actions[0], ReceiptAction::S3 { .. }));
        assert!(matches!(&rule.actions[1], ReceiptAction::Sns { .. }));
        assert!(matches!(&rule.actions[2], ReceiptAction::Lambda { .. }));
        assert!(matches!(&rule.actions[3], ReceiptAction::Bounce { .. }));
        assert!(matches!(&rule.actions[4], ReceiptAction::AddHeader { .. }));
        assert!(matches!(&rule.actions[5], ReceiptAction::Stop { .. }));
    }
}

// ── Identity management tests ──

#[test]
fn test_verify_email_identity() {
    let state = make_state();
    let req = make_v1_request(
        "VerifyEmailIdentity",
        vec![("EmailAddress", "test@example.com")],
    );
    let resp = handle_v1_action(&state, &req).unwrap();
    assert_eq!(resp.status, StatusCode::OK);

    {
        let mas = state.read();
        let st = mas.default_ref();
        let identity = st.identities.get("test@example.com").unwrap();
        assert!(identity.verified);
        assert_eq!(identity.identity_type, "EmailAddress");
    }
}

#[test]
fn test_verify_email_address_legacy_alias() {
    let state = make_state();
    let req = make_v1_request("VerifyEmailAddress", vec![("EmailAddress", "legacy@x.io")]);
    let resp = handle_v1_action(&state, &req).unwrap();
    assert_eq!(resp.status, StatusCode::OK);
    let mas = state.read();
    let st = mas.default_ref();
    let identity = st.identities.get("legacy@x.io").unwrap();
    assert!(identity.verified);
    assert_eq!(identity.identity_type, "EmailAddress");
}

#[test]
fn test_list_verified_email_addresses_returns_only_email_identities() {
    let state = make_state();
    handle_v1_action(
        &state,
        &make_v1_request("VerifyEmailAddress", vec![("EmailAddress", "u1@x.io")]),
    )
    .unwrap();
    handle_v1_action(
        &state,
        &make_v1_request("VerifyEmailAddress", vec![("EmailAddress", "u2@x.io")]),
    )
    .unwrap();
    handle_v1_action(
        &state,
        &make_v1_request("VerifyDomainIdentity", vec![("Domain", "x.io")]),
    )
    .unwrap();

    let resp = handle_v1_action(
        &state,
        &make_v1_request("ListVerifiedEmailAddresses", vec![]),
    )
    .unwrap();
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(body.contains("u1@x.io"));
    assert!(body.contains("u2@x.io"));
    assert!(body.contains("<VerifiedEmailAddresses>"));
    // Domain identities don't appear here.
    assert!(!body.contains("<member>x.io</member>"));
}

#[test]
fn test_delete_verified_email_address_legacy_alias() {
    let state = make_state();
    handle_v1_action(
        &state,
        &make_v1_request("VerifyEmailAddress", vec![("EmailAddress", "drop@x.io")]),
    )
    .unwrap();
    let resp = handle_v1_action(
        &state,
        &make_v1_request(
            "DeleteVerifiedEmailAddress",
            vec![("EmailAddress", "drop@x.io")],
        ),
    )
    .unwrap();
    assert_eq!(resp.status, StatusCode::OK);
    let mas = state.read();
    assert!(!mas.default_ref().identities.contains_key("drop@x.io"));
}

#[test]
fn test_verify_domain_identity() {
    let state = make_state();
    let req = make_v1_request("VerifyDomainIdentity", vec![("Domain", "example.com")]);
    let resp = handle_v1_action(&state, &req).unwrap();
    assert_eq!(resp.status, StatusCode::OK);
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(body.contains("<VerificationToken>"));

    {
        let mas = state.read();
        let st = mas.default_ref();
        let identity = st.identities.get("example.com").unwrap();
        assert!(identity.verified);
        assert_eq!(identity.identity_type, "Domain");
    }
}

#[test]
fn test_verify_domain_dkim() {
    let state = make_state();
    let req = make_v1_request("VerifyDomainDkim", vec![("Domain", "example.com")]);
    let resp = handle_v1_action(&state, &req).unwrap();
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(body.contains("<DkimTokens>"));
    // Should return 3 tokens
    assert_eq!(body.matches("<member>").count(), 3);
}

#[test]
fn test_list_identities() {
    let state = make_state();
    // Create email and domain identities
    handle_v1_action(
        &state,
        &make_v1_request("VerifyEmailIdentity", vec![("EmailAddress", "a@test.com")]),
    )
    .unwrap();
    handle_v1_action(
        &state,
        &make_v1_request("VerifyDomainIdentity", vec![("Domain", "test.com")]),
    )
    .unwrap();

    // List all
    let resp = handle_v1_action(&state, &make_v1_request("ListIdentities", vec![])).unwrap();
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(body.contains("a@test.com"));
    assert!(body.contains("test.com"));

    // List emails only
    let resp = handle_v1_action(
        &state,
        &make_v1_request("ListIdentities", vec![("IdentityType", "EmailAddress")]),
    )
    .unwrap();
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(body.contains("a@test.com"));
    assert!(!body.contains("<member>test.com</member>"));

    // List domains only
    let resp = handle_v1_action(
        &state,
        &make_v1_request("ListIdentities", vec![("IdentityType", "Domain")]),
    )
    .unwrap();
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(!body.contains("a@test.com"));
    assert!(body.contains("test.com"));
}

#[test]
fn test_get_identity_verification_attributes() {
    let state = make_state();
    handle_v1_action(
        &state,
        &make_v1_request("VerifyEmailIdentity", vec![("EmailAddress", "a@test.com")]),
    )
    .unwrap();

    let req = make_v1_request(
        "GetIdentityVerificationAttributes",
        vec![
            ("Identities.member.1", "a@test.com"),
            ("Identities.member.2", "unknown@test.com"),
        ],
    );
    let resp = handle_v1_action(&state, &req).unwrap();
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(body.contains("<VerificationStatus>Success</VerificationStatus>"));
    assert!(body.contains("<VerificationStatus>NotStarted</VerificationStatus>"));
}

#[test]
fn test_delete_identity() {
    let state = make_state();
    handle_v1_action(
        &state,
        &make_v1_request("VerifyEmailIdentity", vec![("EmailAddress", "a@test.com")]),
    )
    .unwrap();

    let req = make_v1_request("DeleteIdentity", vec![("Identity", "a@test.com")]);
    handle_v1_action(&state, &req).unwrap();
    assert!(!state
        .read()
        .default_ref()
        .identities
        .contains_key("a@test.com"));
}

#[test]
fn test_set_identity_dkim_enabled() {
    let state = make_state();
    handle_v1_action(
        &state,
        &make_v1_request("VerifyEmailIdentity", vec![("EmailAddress", "a@test.com")]),
    )
    .unwrap();
    assert!(
        !state
            .read()
            .default_ref()
            .identities
            .get("a@test.com")
            .unwrap()
            .dkim_signing_enabled
    );

    handle_v1_action(
        &state,
        &make_v1_request(
            "SetIdentityDkimEnabled",
            vec![("Identity", "a@test.com"), ("DkimEnabled", "true")],
        ),
    )
    .unwrap();
    assert!(
        state
            .read()
            .default_ref()
            .identities
            .get("a@test.com")
            .unwrap()
            .dkim_signing_enabled
    );
}

#[test]
fn test_get_identity_dkim_attributes() {
    let state = make_state();
    handle_v1_action(
        &state,
        &make_v1_request("VerifyDomainIdentity", vec![("Domain", "example.com")]),
    )
    .unwrap();

    let req = make_v1_request(
        "GetIdentityDkimAttributes",
        vec![("Identities.member.1", "example.com")],
    );
    let resp = handle_v1_action(&state, &req).unwrap();
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(body.contains("<DkimEnabled>"));
    assert!(body.contains("<DkimVerificationStatus>"));
    assert!(body.contains("<DkimTokens>"));
}

// ── Identity attributes tests ──

#[test]
fn test_set_identity_feedback_forwarding() {
    let state = make_state();
    handle_v1_action(
        &state,
        &make_v1_request("VerifyEmailIdentity", vec![("EmailAddress", "a@test.com")]),
    )
    .unwrap();

    handle_v1_action(
        &state,
        &make_v1_request(
            "SetIdentityFeedbackForwardingEnabled",
            vec![("Identity", "a@test.com"), ("ForwardingEnabled", "false")],
        ),
    )
    .unwrap();
    assert!(
        !state
            .read()
            .default_ref()
            .identities
            .get("a@test.com")
            .unwrap()
            .email_forwarding_enabled
    );
}

#[test]
fn test_get_identity_notification_attributes() {
    let state = make_state();
    handle_v1_action(
        &state,
        &make_v1_request("VerifyEmailIdentity", vec![("EmailAddress", "a@test.com")]),
    )
    .unwrap();

    let req = make_v1_request(
        "GetIdentityNotificationAttributes",
        vec![("Identities.member.1", "a@test.com")],
    );
    let resp = handle_v1_action(&state, &req).unwrap();
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(body.contains("<ForwardingEnabled>true</ForwardingEnabled>"));
}

#[test]
fn test_set_identity_mail_from_domain() {
    let state = make_state();
    handle_v1_action(
        &state,
        &make_v1_request("VerifyDomainIdentity", vec![("Domain", "example.com")]),
    )
    .unwrap();

    handle_v1_action(
        &state,
        &make_v1_request(
            "SetIdentityMailFromDomain",
            vec![
                ("Identity", "example.com"),
                ("MailFromDomain", "mail.example.com"),
                ("BehaviorOnMXFailure", "RejectMessage"),
            ],
        ),
    )
    .unwrap();

    {
        let mas = state.read();
        let st = mas.default_ref();
        let id = st.identities.get("example.com").unwrap();
        assert_eq!(id.mail_from_domain.as_deref(), Some("mail.example.com"));
        assert_eq!(id.mail_from_behavior_on_mx_failure, "RejectMessage");
    }
}

#[test]
fn test_get_identity_mail_from_domain_attributes() {
    let state = make_state();
    handle_v1_action(
        &state,
        &make_v1_request("VerifyDomainIdentity", vec![("Domain", "example.com")]),
    )
    .unwrap();

    let req = make_v1_request(
        "GetIdentityMailFromDomainAttributes",
        vec![("Identities.member.1", "example.com")],
    );
    let resp = handle_v1_action(&state, &req).unwrap();
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(body.contains("<BehaviorOnMXFailure>"));
    assert!(body.contains("<MailFromDomainStatus>"));
}

// ── Sending tests ──

#[test]
fn test_send_email_v1() {
    let state = make_state();
    seed_identity(&state, "sender@example.com");
    enable_production_access(&state);
    let req = make_v1_request(
        "SendEmail",
        vec![
            ("Source", "sender@example.com"),
            ("Destination.ToAddresses.member.1", "to@example.com"),
            ("Destination.CcAddresses.member.1", "cc@example.com"),
            ("Message.Subject.Data", "Test Subject"),
            ("Message.Body.Html.Data", "<h1>Hello</h1>"),
            ("Message.Body.Text.Data", "Hello"),
        ],
    );
    let resp = handle_v1_action(&state, &req).unwrap();
    assert_eq!(resp.status, StatusCode::OK);
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(body.contains("<MessageId>"));

    {
        let mas = state.read();
        let st = mas.default_ref();
        assert_eq!(st.sent_emails.len(), 1);
        let sent = &st.sent_emails[0];
        assert_eq!(sent.from, "sender@example.com");
        assert_eq!(sent.to, vec!["to@example.com"]);
        assert_eq!(sent.cc, vec!["cc@example.com"]);
        assert_eq!(sent.subject.as_deref(), Some("Test Subject"));
        assert_eq!(sent.html_body.as_deref(), Some("<h1>Hello</h1>"));
    }
}

#[test]
fn send_email_v1_same_path() {
    // Mirror of `send_email_v2_rejects_unverified_from_in_sandbox` for
    // SES v1: fresh account, no identities verified, expect the v1
    // `MessageRejected` error code.
    let state = make_state();
    let req = make_v1_request(
        "SendEmail",
        vec![
            ("Source", "noreply@example.com"),
            ("Destination.ToAddresses.member.1", "to@example.com"),
            ("Message.Subject.Data", "Hi"),
            ("Message.Body.Text.Data", "Hello"),
        ],
    );
    match handle_v1_action(&state, &req) {
        Err(e) => assert_eq!(e.code(), "MessageRejected"),
        Ok(_) => panic!("expected MessageRejected"),
    }
}

#[test]
fn send_email_v1_rejects_unverified_recipient_in_sandbox() {
    let state = make_state();
    disable_production_access(&state);
    seed_identity(&state, "sender@example.com");
    let req = make_v1_request(
        "SendEmail",
        vec![
            ("Source", "sender@example.com"),
            (
                "Destination.ToAddresses.member.1",
                "unverified@elsewhere.com",
            ),
            ("Message.Subject.Data", "Hi"),
            ("Message.Body.Text.Data", "Hello"),
        ],
    );
    match handle_v1_action(&state, &req) {
        Err(e) => {
            assert_eq!(e.code(), "MessageRejected");
            assert!(e.message().contains("unverified@elsewhere.com"));
        }
        Ok(_) => panic!("expected MessageRejected"),
    }
}

#[test]
fn send_email_v1_skips_recipient_check_in_production() {
    let state = make_state();
    seed_identity(&state, "sender@example.com");
    enable_production_access(&state);
    let req = make_v1_request(
        "SendEmail",
        vec![
            ("Source", "sender@example.com"),
            (
                "Destination.ToAddresses.member.1",
                "unverified@elsewhere.com",
            ),
            ("Message.Subject.Data", "Hi"),
            ("Message.Body.Text.Data", "Hello"),
        ],
    );
    let resp = handle_v1_action(&state, &req).unwrap();
    assert_eq!(resp.status, StatusCode::OK);
}

#[test]
fn send_email_v1_accepts_simulator_recipients_in_sandbox() {
    // Mailbox simulator addresses bypass the gate the same way real SES
    // documents (`simulator.amazonses.com` is implicitly verified).
    let state = make_state();
    disable_production_access(&state);
    seed_identity(&state, "sender@example.com");
    for recipient in [
        "bounce@simulator.amazonses.com",
        "complaint@simulator.amazonses.com",
        "success@simulator.amazonses.com",
        "suppressionlist@simulator.amazonses.com",
    ] {
        let req = make_v1_request(
            "SendEmail",
            vec![
                ("Source", "sender@example.com"),
                ("Destination.ToAddresses.member.1", recipient),
                ("Message.Subject.Data", "Hi"),
                ("Message.Body.Text.Data", "Hello"),
            ],
        );
        let resp = handle_v1_action(&state, &req)
            .unwrap_or_else(|e| panic!("simulator recipient {recipient}: {}", e.message()));
        assert_eq!(resp.status, StatusCode::OK);
    }
}

#[test]
fn send_email_v1_accepts_simulator_sender_without_verified_identity() {
    let state = make_state();
    disable_production_access(&state);
    let req = make_v1_request(
        "SendEmail",
        vec![
            ("Source", "ooto@simulator.amazonses.com"),
            (
                "Destination.ToAddresses.member.1",
                "bounce@simulator.amazonses.com",
            ),
            ("Message.Subject.Data", "Hi"),
            ("Message.Body.Text.Data", "Hello"),
        ],
    );
    let resp = handle_v1_action(&state, &req).unwrap();
    assert_eq!(resp.status, StatusCode::OK);
}

#[test]
fn test_send_raw_email() {
    let state = make_state();
    seed_identity(&state, "sender@example.com");
    enable_production_access(&state);
    let req = make_v1_request(
        "SendRawEmail",
        vec![
            ("Source", "sender@example.com"),
            ("Destinations.member.1", "to@example.com"),
            (
                "RawMessage.Data",
                "From: sender@example.com\r\nTo: to@example.com\r\nSubject: Test\r\n\r\nBody",
            ),
        ],
    );
    let resp = handle_v1_action(&state, &req).unwrap();
    assert_eq!(resp.status, StatusCode::OK);

    {
        let mas = state.read();
        let st = mas.default_ref();
        assert_eq!(st.sent_emails.len(), 1);
        assert!(st.sent_emails[0].raw_data.is_some());
    }
}

#[test]
fn test_send_templated_email() {
    let state = make_state();
    seed_identity(&state, "sender@example.com");
    enable_production_access(&state);
    // Create template first
    handle_v1_action(
        &state,
        &make_v1_request(
            "CreateTemplate",
            vec![
                ("Template.TemplateName", "my-template"),
                ("Template.SubjectPart", "Hello {{name}}"),
                ("Template.HtmlPart", "<p>Hi {{name}}</p>"),
            ],
        ),
    )
    .unwrap();

    let req = make_v1_request(
        "SendTemplatedEmail",
        vec![
            ("Source", "sender@example.com"),
            ("Destination.ToAddresses.member.1", "to@example.com"),
            ("Template", "my-template"),
            ("TemplateData", "{\"name\":\"World\"}"),
        ],
    );
    let resp = handle_v1_action(&state, &req).unwrap();
    assert_eq!(resp.status, StatusCode::OK);

    {
        let mas = state.read();
        let st = mas.default_ref();
        assert_eq!(st.sent_emails.len(), 1);
        assert_eq!(
            st.sent_emails[0].template_name.as_deref(),
            Some("my-template")
        );
    }
}

#[test]
fn test_send_templated_email_missing_template() {
    let state = make_state();
    seed_identity(&state, "sender@example.com");
    let req = make_v1_request(
        "SendTemplatedEmail",
        vec![
            ("Source", "sender@example.com"),
            ("Destination.ToAddresses.member.1", "to@example.com"),
            ("Template", "nonexistent"),
            ("TemplateData", "{}"),
        ],
    );
    match handle_v1_action(&state, &req) {
        Err(e) => assert_eq!(e.code(), "TemplateDoesNotExistException"),
        Ok(_) => panic!("expected error"),
    }
}

#[test]
fn test_send_bulk_templated_email() {
    let state = make_state();
    seed_identity(&state, "sender@example.com");
    enable_production_access(&state);
    handle_v1_action(
        &state,
        &make_v1_request(
            "CreateTemplate",
            vec![
                ("Template.TemplateName", "bulk-tmpl"),
                ("Template.SubjectPart", "Hi"),
            ],
        ),
    )
    .unwrap();

    let req = make_v1_request(
        "SendBulkTemplatedEmail",
        vec![
            ("Source", "sender@example.com"),
            ("Template", "bulk-tmpl"),
            ("DefaultTemplateData", "{\"key\":\"default\"}"),
            (
                "Destinations.member.1.Destination.ToAddresses.member.1",
                "a@example.com",
            ),
            (
                "Destinations.member.2.Destination.ToAddresses.member.1",
                "b@example.com",
            ),
            (
                "Destinations.member.2.ReplacementTemplateData",
                "{\"key\":\"custom\"}",
            ),
        ],
    );
    let resp = handle_v1_action(&state, &req).unwrap();
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(body.contains("<Status>Success</Status>"));

    {
        let mas = state.read();
        let st = mas.default_ref();
        assert_eq!(st.sent_emails.len(), 2);
    }
}

// ── Template tests ──

#[test]
fn test_template_lifecycle() {
    let state = make_state();

    // Create
    handle_v1_action(
        &state,
        &make_v1_request(
            "CreateTemplate",
            vec![
                ("Template.TemplateName", "t1"),
                ("Template.SubjectPart", "Subject"),
                ("Template.HtmlPart", "<p>html</p>"),
                ("Template.TextPart", "text"),
            ],
        ),
    )
    .unwrap();

    // Duplicate should fail
    match handle_v1_action(
        &state,
        &make_v1_request("CreateTemplate", vec![("Template.TemplateName", "t1")]),
    ) {
        Err(e) => assert_eq!(e.code(), "AlreadyExistsException"),
        Ok(_) => panic!("expected error"),
    }

    // Get
    let resp = handle_v1_action(
        &state,
        &make_v1_request("GetTemplate", vec![("TemplateName", "t1")]),
    )
    .unwrap();
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(body.contains("<TemplateName>t1</TemplateName>"));
    assert!(body.contains("<SubjectPart>Subject</SubjectPart>"));

    // List
    let resp = handle_v1_action(&state, &make_v1_request("ListTemplates", vec![])).unwrap();
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(body.contains("<Name>t1</Name>"));

    // Update
    handle_v1_action(
        &state,
        &make_v1_request(
            "UpdateTemplate",
            vec![
                ("Template.TemplateName", "t1"),
                ("Template.SubjectPart", "Updated"),
            ],
        ),
    )
    .unwrap();
    {
        let mas = state.read();
        let st = mas.default_ref();
        assert_eq!(
            st.templates.get("t1").unwrap().subject.as_deref(),
            Some("Updated")
        );
    }

    // Delete
    handle_v1_action(
        &state,
        &make_v1_request("DeleteTemplate", vec![("TemplateName", "t1")]),
    )
    .unwrap();
    assert!(!state.read().default_ref().templates.contains_key("t1"));
}

// ── Configuration Set tests ──

#[test]
fn test_configuration_set_lifecycle() {
    let state = make_state();

    // Create
    handle_v1_action(
        &state,
        &make_v1_request(
            "CreateConfigurationSet",
            vec![("ConfigurationSet.Name", "my-config")],
        ),
    )
    .unwrap();

    // Duplicate
    match handle_v1_action(
        &state,
        &make_v1_request(
            "CreateConfigurationSet",
            vec![("ConfigurationSet.Name", "my-config")],
        ),
    ) {
        Err(e) => assert_eq!(e.code(), "ConfigurationSetAlreadyExistsException"),
        Ok(_) => panic!("expected error"),
    }

    // List
    let resp = handle_v1_action(&state, &make_v1_request("ListConfigurationSets", vec![])).unwrap();
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(body.contains("<Name>my-config</Name>"));

    // Describe
    let resp = handle_v1_action(
        &state,
        &make_v1_request(
            "DescribeConfigurationSet",
            vec![("ConfigurationSetName", "my-config")],
        ),
    )
    .unwrap();
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(body.contains("<Name>my-config</Name>"));

    // Delete
    handle_v1_action(
        &state,
        &make_v1_request(
            "DeleteConfigurationSet",
            vec![("ConfigurationSetName", "my-config")],
        ),
    )
    .unwrap();
    assert!(!state
        .read()
        .default_ref()
        .configuration_sets
        .contains_key("my-config"));
}

#[test]
fn test_configuration_set_event_destination_lifecycle() {
    let state = make_state();
    handle_v1_action(
        &state,
        &make_v1_request(
            "CreateConfigurationSet",
            vec![("ConfigurationSet.Name", "cs")],
        ),
    )
    .unwrap();

    // Create event destination
    handle_v1_action(
        &state,
        &make_v1_request(
            "CreateConfigurationSetEventDestination",
            vec![
                ("ConfigurationSetName", "cs"),
                ("EventDestination.Name", "sns-dest"),
                ("EventDestination.Enabled", "true"),
                ("EventDestination.MatchingEventTypes.member.1", "send"),
                ("EventDestination.MatchingEventTypes.member.2", "bounce"),
                (
                    "EventDestination.SNSDestination.TopicARN",
                    "arn:aws:sns:us-east-1:123456789012:my-topic",
                ),
            ],
        ),
    )
    .unwrap();

    {
        let mas = state.read();
        let st = mas.default_ref();
        let dests = st.event_destinations.get("cs").unwrap();
        assert_eq!(dests.len(), 1);
        assert_eq!(dests[0].name, "sns-dest");
        assert_eq!(dests[0].matching_event_types, vec!["send", "bounce"]);
    }

    // Update
    handle_v1_action(
        &state,
        &make_v1_request(
            "UpdateConfigurationSetEventDestination",
            vec![
                ("ConfigurationSetName", "cs"),
                ("EventDestination.Name", "sns-dest"),
                ("EventDestination.Enabled", "false"),
            ],
        ),
    )
    .unwrap();
    assert!(
        !state
            .read()
            .default_ref()
            .event_destinations
            .get("cs")
            .unwrap()[0]
            .enabled
    );

    // Delete
    handle_v1_action(
        &state,
        &make_v1_request(
            "DeleteConfigurationSetEventDestination",
            vec![
                ("ConfigurationSetName", "cs"),
                ("EventDestinationName", "sns-dest"),
            ],
        ),
    )
    .unwrap();
    assert!(state
        .read()
        .default_ref()
        .event_destinations
        .get("cs")
        .unwrap()
        .is_empty());
}

// ── Account / Quota tests ──

#[test]
fn test_get_send_quota() {
    let state = make_state();
    let resp = handle_v1_action(&state, &make_v1_request("GetSendQuota", vec![])).unwrap();
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(body.contains("<Max24HourSend>50000.0</Max24HourSend>"));
    assert!(body.contains("<MaxSendRate>14.0</MaxSendRate>"));
}

#[test]
fn test_get_send_statistics() {
    let state = make_state();
    seed_identity(&state, "a@b.com");
    enable_production_access(&state);
    // Send an email first
    handle_v1_action(
        &state,
        &make_v1_request(
            "SendEmail",
            vec![
                ("Source", "a@b.com"),
                ("Destination.ToAddresses.member.1", "c@d.com"),
                ("Message.Subject.Data", "Hi"),
                ("Message.Body.Text.Data", "Hello"),
            ],
        ),
    )
    .unwrap();

    let resp = handle_v1_action(&state, &make_v1_request("GetSendStatistics", vec![])).unwrap();
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(body.contains("<DeliveryAttempts>1</DeliveryAttempts>"));
}

#[test]
fn test_get_account_sending_enabled() {
    let state = make_state();
    let resp =
        handle_v1_action(&state, &make_v1_request("GetAccountSendingEnabled", vec![])).unwrap();
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(body.contains("<Enabled>true</Enabled>"));
}

// ── X2: account + config-set sending pause for v1 ──

#[test]
fn send_email_v1_account_pause() {
    let state = make_state();
    seed_identity(&state, "sender@example.com");
    enable_production_access(&state);
    {
        let mut accounts = state.write();
        let st = accounts.get_or_create("123456789012");
        st.account_settings.sending_enabled = false;
    }
    let req = make_v1_request(
        "SendEmail",
        vec![
            ("Source", "sender@example.com"),
            ("Destination.ToAddresses.member.1", "to@example.com"),
            ("Message.Subject.Data", "Hi"),
            ("Message.Body.Text.Data", "Hello"),
        ],
    );
    match handle_v1_action(&state, &req) {
        Err(e) => {
            assert_eq!(e.code(), "MessageRejected");
            assert_eq!(e.message(), "Email sending for the account is paused.");
        }
        Ok(_) => panic!("expected MessageRejected"),
    }
}

#[test]
fn send_email_v1_config_set_pause() {
    use crate::state::ConfigurationSet;
    let state = make_state();
    seed_identity(&state, "sender@example.com");
    enable_production_access(&state);
    {
        let mut accounts = state.write();
        let st = accounts.get_or_create("123456789012");
        st.configuration_sets.insert(
            "my-cs".to_string(),
            ConfigurationSet {
                name: "my-cs".to_string(),
                sending_enabled: false,
                tls_policy: "OPTIONAL".to_string(),
                sending_pool_name: None,
                max_delivery_seconds: None,
                custom_redirect_domain: None,
                https_policy: None,
                suppressed_reasons: Vec::new(),
                reputation_metrics_enabled: false,
                vdm_options: None,
                archive_arn: None,
                archiving_options_present: false,
            },
        );
    }
    let req = make_v1_request(
        "SendEmail",
        vec![
            ("Source", "sender@example.com"),
            ("Destination.ToAddresses.member.1", "to@example.com"),
            ("Message.Subject.Data", "Hi"),
            ("Message.Body.Text.Data", "Hello"),
            ("ConfigurationSetName", "my-cs"),
        ],
    );
    match handle_v1_action(&state, &req) {
        Err(e) => {
            assert_eq!(e.code(), "MessageRejected");
            assert_eq!(
                e.message(),
                "Email sending for the configuration set my-cs is paused."
            );
        }
        Ok(_) => panic!("expected MessageRejected"),
    }
}

#[test]
fn send_email_v1_skips_suppressed_recipient() {
    use crate::state::SuppressedDestination;
    let state = make_state();
    seed_identity(&state, "sender@example.com");
    enable_production_access(&state);
    {
        let mut accounts = state.write();
        let st = accounts.get_or_create("123456789012");
        st.suppressed_destinations.insert(
            "blocked@example.com".to_string(),
            SuppressedDestination {
                email_address: "blocked@example.com".to_string(),
                reason: "BOUNCE".to_string(),
                last_update_time: chrono::Utc::now(),
            },
        );
    }
    let req = make_v1_request(
        "SendEmail",
        vec![
            ("Source", "sender@example.com"),
            ("Destination.ToAddresses.member.1", "blocked@example.com"),
            ("Message.Subject.Data", "Hi"),
            ("Message.Body.Text.Data", "Hello"),
        ],
    );
    match handle_v1_action(&state, &req) {
        Err(e) => {
            assert_eq!(e.code(), "MessageRejected");
            assert_eq!(e.message(), "Address is on the suppression list");
        }
        Ok(_) => panic!("expected MessageRejected"),
    }
}

// ── X9: SendBounce + simulator addresses ──

#[test]
fn test_send_bounce_v1() {
    let state = make_state();
    let req = make_v1_request(
        "SendBounce",
        vec![
            ("BounceSender", "bounce@example.com"),
            ("OriginalMessageId", "msg-123"),
            (
                "BouncedRecipientInfoList.member.1.Recipient",
                "bad@example.com",
            ),
            (
                "BouncedRecipientInfoList.member.2.Recipient",
                "unknown@example.com",
            ),
        ],
    );
    let resp = handle_v1_action(&state, &req).unwrap();
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(body.contains("<MessageId>"));
    assert!(body.contains("<BouncedRecipientInfoList>"));
    assert!(body.contains("<Recipient>bad@example.com</Recipient>"));
    assert!(body.contains("<Recipient>unknown@example.com</Recipient>"));
    assert!(body.contains("<BounceType>ContentRejected</BounceType>"));

    let accounts = state.read();
    let st = accounts.get("123456789012").unwrap();
    assert_eq!(st.bounces.len(), 1);
    assert_eq!(st.bounces[0].original_message_id, "msg-123");
    assert_eq!(st.bounces[0].bounce_sender, "bounce@example.com");
    assert_eq!(st.bounces[0].bounced_recipients.len(), 2);
}

#[test]
fn test_send_bounce_v1_custom_dsn_fields() {
    let state = make_state();
    let req = make_v1_request(
        "SendBounce",
        vec![
            ("BounceSender", "postmaster@example.com"),
            ("OriginalMessageId", "abc-def"),
            (
                "BouncedRecipientInfoList.member.1.Recipient",
                "user@example.com",
            ),
            (
                "BouncedRecipientInfoList.member.1.BounceType",
                "DoesNotExist",
            ),
            (
                "BouncedRecipientInfoList.member.1.RecipientDsnFields.Action",
                "failed",
            ),
            (
                "BouncedRecipientInfoList.member.1.RecipientDsnFields.Status",
                "5.1.1",
            ),
            (
                "BouncedRecipientInfoList.member.1.RecipientDsnFields.DiagnosticCode",
                "No such user",
            ),
        ],
    );
    let resp = handle_v1_action(&state, &req).unwrap();
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(body.contains("<BounceType>DoesNotExist</BounceType>"));
    assert!(body.contains("<StatusCode>5.1.1</StatusCode>"));
    assert!(body.contains("<DiagnosticCode>No such user</DiagnosticCode>"));
}

#[test]
fn test_send_bounce_v1_missing_recipient() {
    let state = make_state();
    let req = make_v1_request(
        "SendBounce",
        vec![
            ("BounceSender", "bounce@example.com"),
            ("OriginalMessageId", "msg-123"),
        ],
    );
    match handle_v1_action(&state, &req) {
        Err(e) => assert_eq!(e.code(), "MissingParameter"),
        Ok(_) => panic!("expected MissingParameter"),
    }
}

#[test]
fn set_and_get_identity_notification_topic() {
    // SetIdentityNotificationTopic was a silent no-op and
    // GetIdentityNotificationAttributes never emitted the topics, so SNS
    // bounce/complaint wiring was lost (bug-audit 2026-06-20, 1.19).
    let state = make_state();
    handle_v1_action(
        &state,
        &make_v1_request("VerifyEmailIdentity", vec![("EmailAddress", "a@test.com")]),
    )
    .unwrap();

    handle_v1_action(
        &state,
        &make_v1_request(
            "SetIdentityNotificationTopic",
            vec![
                ("Identity", "a@test.com"),
                ("NotificationType", "Bounce"),
                ("SnsTopic", "arn:aws:sns:us-east-1:123456789012:bounces"),
            ],
        ),
    )
    .unwrap();

    let resp = handle_v1_action(
        &state,
        &make_v1_request(
            "GetIdentityNotificationAttributes",
            vec![("Identities.member.1", "a@test.com")],
        ),
    )
    .unwrap();
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(
        body.contains("<BounceTopic>arn:aws:sns:us-east-1:123456789012:bounces</BounceTopic>"),
        "{body}"
    );
    // Untouched types are not emitted.
    assert!(!body.contains("<ComplaintTopic>"), "{body}");

    // An empty SnsTopic clears it.
    handle_v1_action(
        &state,
        &make_v1_request(
            "SetIdentityNotificationTopic",
            vec![
                ("Identity", "a@test.com"),
                ("NotificationType", "Bounce"),
                ("SnsTopic", ""),
            ],
        ),
    )
    .unwrap();
    let resp = handle_v1_action(
        &state,
        &make_v1_request(
            "GetIdentityNotificationAttributes",
            vec![("Identities.member.1", "a@test.com")],
        ),
    )
    .unwrap();
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(!body.contains("<BounceTopic>"), "cleared: {body}");
}
