use super::*;

#[test]
fn validate_message_structure_json_rejects_invalid_json() {
    let result = validate_message_structure_json("not valid json");
    assert!(result.is_err());
    let err = result.unwrap_err();
    let msg = format!("{err}");
    assert!(msg.contains("No JSON message body is parseable"), "{msg}");
}

#[test]
fn validate_message_structure_json_rejects_missing_default_key() {
    let result = validate_message_structure_json(r#"{"sqs": "hello"}"#);
    assert!(result.is_err());
    let err = result.unwrap_err();
    let msg = format!("{err}");
    assert!(
        msg.contains("No default entry in JSON message body"),
        "{msg}"
    );
}

#[test]
fn validate_message_structure_json_accepts_valid() {
    let result =
        validate_message_structure_json(r#"{"default": "hello", "sqs": "hello from sqs"}"#);
    assert!(result.is_ok());
}

#[test]
fn build_sns_lambda_event_uses_real_subscription_arn() {
    let now = Utc::now();
    let sub_arn = "arn:aws:sns:us-east-1:123456789012:my-topic:abc-def-123";
    let topic_arn = "arn:aws:sns:us-east-1:123456789012:my-topic";
    let attrs = serde_json::Map::new();

    let payload = build_sns_lambda_event(&SnsLambdaEventInput {
        message_id: "msg-001",
        topic_arn,
        subscription_arn: sub_arn,
        message: "hello",
        subject: Some("test subject"),
        message_attributes: &attrs,
        timestamp: &now,
        endpoint: "http://localhost:4566",
    });

    let parsed: Value = serde_json::from_str(&payload).unwrap();
    let record = &parsed["Records"][0];
    assert_eq!(record["EventSubscriptionArn"], sub_arn);
    assert_eq!(record["EventSource"], "aws:sns");
    assert_eq!(record["Sns"]["TopicArn"], topic_arn);
    assert_eq!(record["Sns"]["Message"], "hello");
    assert_eq!(record["Sns"]["Subject"], "test subject");
    assert_eq!(record["Sns"]["MessageId"], "msg-001");
    // UnsubscribeUrl should use subscription ARN, not topic ARN
    let unsub_url = record["Sns"]["UnsubscribeUrl"].as_str().unwrap();
    assert!(
        unsub_url.contains(sub_arn),
        "UnsubscribeUrl should contain subscription ARN"
    );
}

#[test]
fn build_sns_envelope_uses_configured_endpoint() {
    let endpoint = "http://myhost:5555";
    let topic_arn = "arn:aws:sns:us-east-1:123456789012:my-topic";
    let subscription_arn = "arn:aws:sns:us-east-1:123456789012:my-topic:1a2b3c4d-uuid";
    let attrs = serde_json::Map::new();

    let envelope = build_sns_envelope(
        "msg-002",
        topic_arn,
        subscription_arn,
        &None,
        "test message",
        &attrs,
        endpoint,
    );

    let parsed: Value = serde_json::from_str(&envelope).unwrap();
    let unsub_url = parsed["UnsubscribeURL"].as_str().unwrap();
    assert!(
        unsub_url.starts_with("http://myhost:5555/"),
        "UnsubscribeURL should use the configured endpoint, got: {unsub_url}"
    );
    // AWS's UnsubscribeURL carries the SubscriptionArn, not the TopicArn —
    // calling Unsubscribe with a TopicArn is rejected.
    assert!(
        unsub_url.contains(subscription_arn),
        "UnsubscribeURL should contain the subscription ARN, got: {unsub_url}"
    );
}

#[test]
fn build_sns_lambda_event_uses_configured_endpoint() {
    let now = Utc::now();
    let sub_arn = "arn:aws:sns:us-east-1:123456789012:my-topic:abc-def-123";
    let attrs = serde_json::Map::new();
    let endpoint = "http://custom:9999";

    let payload = build_sns_lambda_event(&SnsLambdaEventInput {
        message_id: "msg-003",
        topic_arn: "arn:aws:sns:us-east-1:123456789012:my-topic",
        subscription_arn: sub_arn,
        message: "hello",
        subject: None,
        message_attributes: &attrs,
        timestamp: &now,
        endpoint,
    });

    let parsed: Value = serde_json::from_str(&payload).unwrap();
    let unsub_url = parsed["Records"][0]["Sns"]["UnsubscribeUrl"]
        .as_str()
        .unwrap();
    assert!(
        unsub_url.starts_with("http://custom:9999/"),
        "UnsubscribeUrl should use configured endpoint, got: {unsub_url}"
    );
}

#[test]
fn add_permission_with_invalid_policy_returns_error_not_panic() {
    use fakecloud_core::delivery::DeliveryBus;
    use fakecloud_core::multi_account::MultiAccountState;
    use parking_lot::RwLock;
    use std::sync::Arc;

    let state = Arc::new(RwLock::new(
        MultiAccountState::<crate::state::SnsState>::new(
            "123456789012",
            "us-east-1",
            "http://localhost:4566",
        ),
    ));
    let delivery = Arc::new(DeliveryBus::new());
    let svc = SnsService::new(state.clone(), delivery);

    // Create a topic first
    let topic_arn = "arn:aws:sns:us-east-1:123456789012:test-topic";
    {
        let mut s = state.write();
        s.default_mut().topics.insert(
            topic_arn.to_string(),
            crate::state::SnsTopic {
                topic_arn: topic_arn.to_string(),
                name: "test-topic".to_string(),
                attributes: {
                    let mut m = std::collections::BTreeMap::new();
                    // Set an intentionally broken JSON policy
                    m.insert("Policy".to_string(), "not valid json {{{".to_string());
                    m
                },
                is_fifo: false,
                tags: vec![],
                created_at: Utc::now(),
                subscriptions_deleted: 0,
                fifo_sequence: 0,
                dedup_cache: std::collections::BTreeMap::new(),
            },
        );
    }

    // Build an AddPermission request
    let body = format!(
        "Action=AddPermission&TopicArn={}&Label=TestLabel&ActionName.member.1=Publish&AWSAccountId.member.1=111111111111",
        topic_arn
    );
    let req = fakecloud_core::service::AwsRequest {
        service: "sns".to_string(),
        action: "AddPermission".to_string(),
        region: "us-east-1".to_string(),
        account_id: "123456789012".to_string(),
        request_id: "test-req".to_string(),
        headers: http::HeaderMap::new(),
        query_params: std::collections::HashMap::new(),
        body: body.into(),
        body_stream: parking_lot::Mutex::new(None),
        path_segments: vec![],
        raw_path: "/".to_string(),
        raw_query: String::new(),
        method: http::Method::POST,
        is_query_protocol: true,
        access_key_id: None,
        principal: None,
    };

    // This should return an error, not panic
    let result = svc.add_permission(&req);
    assert!(
        result.is_err(),
        "Invalid policy JSON should return error, not panic"
    );
}

// --- Helper to build SNS service + state for integration-style unit tests ---

fn make_sns() -> (SnsService, crate::state::SharedSnsState) {
    use fakecloud_core::delivery::DeliveryBus;
    use fakecloud_core::multi_account::MultiAccountState;
    use parking_lot::RwLock;
    use std::sync::Arc;

    let state = Arc::new(RwLock::new(
        MultiAccountState::<crate::state::SnsState>::new(
            "123456789012",
            "us-east-1",
            "http://localhost:4566",
        ),
    ));
    let delivery = Arc::new(DeliveryBus::new());
    let svc = SnsService::new(state.clone(), delivery);
    (svc, state)
}

fn sns_request(action: &str, params: Vec<(&str, &str)>) -> fakecloud_core::service::AwsRequest {
    let mut query_params = std::collections::HashMap::new();
    query_params.insert("Action".to_string(), action.to_string());
    for (k, v) in params {
        query_params.insert(k.to_string(), v.to_string());
    }
    fakecloud_core::service::AwsRequest {
        service: "sns".to_string(),
        action: action.to_string(),
        region: "us-east-1".to_string(),
        account_id: "123456789012".to_string(),
        request_id: "test-req".to_string(),
        headers: http::HeaderMap::new(),
        query_params,
        body: Default::default(),
        body_stream: parking_lot::Mutex::new(None),
        path_segments: vec![],
        raw_path: "/".to_string(),
        raw_query: String::new(),
        method: http::Method::POST,
        is_query_protocol: true,
        access_key_id: None,
        principal: None,
    }
}

fn assert_ok(result: &Result<AwsResponse, AwsServiceError>) {
    assert!(
        result.is_ok(),
        "Expected Ok, got: {:?}",
        result.as_ref().err()
    );
}

fn response_body(result: &Result<AwsResponse, AwsServiceError>) -> String {
    String::from_utf8(result.as_ref().unwrap().body.expect_bytes().to_vec()).unwrap()
}

fn message_id_from(body: &str) -> String {
    let start = body.find("<MessageId>").unwrap() + "<MessageId>".len();
    let end = body[start..].find("</MessageId>").unwrap() + start;
    body[start..end].to_string()
}

#[test]
fn json_structure_delivers_per_protocol_body_to_each_subscriber() {
    let (svc, state) = make_sns();
    let topic_arn = "arn:aws:sns:us-east-1:123456789012:proto";
    assert_ok(&svc.create_topic(&sns_request("CreateTopic", vec![("Name", "proto")])));

    for (proto, endpoint) in [
        ("email", "plain@example.com"),
        ("email-json", "json@example.com"),
        ("sms", "+15551234567"),
    ] {
        assert_ok(&svc.subscribe(&sns_request(
            "Subscribe",
            vec![
                ("TopicArn", topic_arn),
                ("Protocol", proto),
                ("Endpoint", endpoint),
            ],
        )));
    }

    let structured = r#"{"default":"DEFAULT-BODY","email":"EMAIL-BODY","email-json":"EMAILJSON-BODY","sms":"SMS-BODY"}"#;
    assert_ok(&svc.publish(&sns_request(
        "Publish",
        vec![
            ("TopicArn", topic_arn),
            ("Message", structured),
            ("MessageStructure", "json"),
        ],
    )));

    let guard = state.read();
    let s = guard.default_ref();
    let plain = s
        .sent_emails
        .iter()
        .find(|e| e.email_address == "plain@example.com")
        .unwrap();
    assert_eq!(plain.message, "EMAIL-BODY");
    let json_sub = s
        .sent_emails
        .iter()
        .find(|e| e.email_address == "json@example.com")
        .unwrap();
    assert_eq!(json_sub.message, "EMAILJSON-BODY");
    let sms = s
        .sms_messages
        .iter()
        .find(|(num, _)| num == "+15551234567")
        .unwrap();
    assert_eq!(sms.1, "SMS-BODY");
}

#[test]
fn json_structure_falls_back_to_default_when_protocol_key_absent() {
    let (svc, state) = make_sns();
    let topic_arn = "arn:aws:sns:us-east-1:123456789012:fallback";
    assert_ok(&svc.create_topic(&sns_request("CreateTopic", vec![("Name", "fallback")])));
    assert_ok(&svc.subscribe(&sns_request(
        "Subscribe",
        vec![
            ("TopicArn", topic_arn),
            ("Protocol", "email"),
            ("Endpoint", "fb@example.com"),
        ],
    )));

    let structured = r#"{"default":"ONLY-DEFAULT","sms":"SMS-ONLY"}"#;
    assert_ok(&svc.publish(&sns_request(
        "Publish",
        vec![
            ("TopicArn", topic_arn),
            ("Message", structured),
            ("MessageStructure", "json"),
        ],
    )));

    let guard = state.read();
    let s = guard.default_ref();
    let email = s
        .sent_emails
        .iter()
        .find(|e| e.email_address == "fb@example.com")
        .unwrap();
    assert_eq!(email.message, "ONLY-DEFAULT");
}

#[test]
fn fifo_publish_deduplicates_within_window() {
    let (svc, state) = make_sns();
    let topic_arn = "arn:aws:sns:us-east-1:123456789012:dedup.fifo";
    assert_ok(&svc.create_topic(&sns_request(
        "CreateTopic",
        vec![
            ("Name", "dedup.fifo"),
            ("Attributes.entry.1.key", "FifoTopic"),
            ("Attributes.entry.1.value", "true"),
        ],
    )));

    let publish = || {
        svc.publish(&sns_request(
            "Publish",
            vec![
                ("TopicArn", topic_arn),
                ("Message", "hello"),
                ("MessageGroupId", "g1"),
                ("MessageDeduplicationId", "dup-1"),
            ],
        ))
    };

    let first = publish();
    assert_ok(&first);
    let id1 = message_id_from(&response_body(&first));

    let second = publish();
    assert_ok(&second);
    let id2 = message_id_from(&response_body(&second));

    assert_eq!(
        id1, id2,
        "duplicate publish should replay original MessageId"
    );

    let guard = state.read();
    let s = guard.default_ref();
    let count = s
        .published
        .iter()
        .filter(|m| m.topic_arn == topic_arn)
        .count();
    assert_eq!(count, 1, "duplicate should not be re-published");
}

#[test]
fn fifo_content_based_dedup_suppresses_identical_body() {
    let (svc, state) = make_sns();
    let topic_arn = "arn:aws:sns:us-east-1:123456789012:cbd.fifo";
    assert_ok(&svc.create_topic(&sns_request(
        "CreateTopic",
        vec![
            ("Name", "cbd.fifo"),
            ("Attributes.entry.1.key", "FifoTopic"),
            ("Attributes.entry.1.value", "true"),
            ("Attributes.entry.2.key", "ContentBasedDeduplication"),
            ("Attributes.entry.2.value", "true"),
        ],
    )));

    let publish = || {
        svc.publish(&sns_request(
            "Publish",
            vec![
                ("TopicArn", topic_arn),
                ("Message", "same-body"),
                ("MessageGroupId", "g1"),
            ],
        ))
    };

    let id1 = message_id_from(&response_body(&publish()));
    let id2 = message_id_from(&response_body(&publish()));
    assert_eq!(id1, id2);

    let guard = state.read();
    let s = guard.default_ref();
    let count = s
        .published
        .iter()
        .filter(|m| m.topic_arn == topic_arn)
        .count();
    assert_eq!(count, 1);
}

// --- Subscribe / Unsubscribe / ListSubscriptions / ListSubscriptionsByTopic ---

#[test]
fn iam_condition_keys_for_subscribe_populates_protocol_and_endpoint() {
    let (svc, _state) = make_sns();
    let req = sns_request(
        "Subscribe",
        vec![
            ("TopicArn", "arn:aws:sns:us-east-1:123456789012:t"),
            ("Protocol", "https"),
            ("Endpoint", "https://example.com/hook"),
        ],
    );
    let action = fakecloud_core::auth::IamAction {
        service: "sns",
        action: "Subscribe",
        resource: "arn:aws:sns:us-east-1:123456789012:t".to_string(),
    };
    let keys = svc.iam_condition_keys_for(&req, &action);
    assert_eq!(keys.get("sns:protocol"), Some(&vec!["https".to_string()]));
    assert_eq!(
        keys.get("sns:endpoint"),
        Some(&vec!["https://example.com/hook".to_string()])
    );
}

#[test]
fn iam_condition_keys_for_subscribe_omits_missing_fields() {
    let (svc, _state) = make_sns();
    let req = sns_request(
        "Subscribe",
        vec![("TopicArn", "arn:aws:sns:us-east-1:123456789012:t")],
    );
    let action = fakecloud_core::auth::IamAction {
        service: "sns",
        action: "Subscribe",
        resource: "arn:aws:sns:us-east-1:123456789012:t".to_string(),
    };
    assert!(svc.iam_condition_keys_for(&req, &action).is_empty());
}

#[test]
fn iam_condition_keys_for_non_subscribe_is_empty() {
    let (svc, _state) = make_sns();
    let req = sns_request(
        "Publish",
        vec![
            ("TopicArn", "arn:aws:sns:us-east-1:123456789012:t"),
            ("Protocol", "https"),
        ],
    );
    let action = fakecloud_core::auth::IamAction {
        service: "sns",
        action: "Publish",
        resource: "arn:aws:sns:us-east-1:123456789012:t".to_string(),
    };
    assert!(svc.iam_condition_keys_for(&req, &action).is_empty());
}

#[test]
fn subscribe_creates_subscription() {
    let (svc, _state) = make_sns();
    // Create topic first
    let req = sns_request("CreateTopic", vec![("Name", "my-topic")]);
    assert_ok(&svc.create_topic(&req));

    let topic_arn = "arn:aws:sns:us-east-1:123456789012:my-topic";
    let req = sns_request(
        "Subscribe",
        vec![
            ("TopicArn", topic_arn),
            ("Protocol", "email"),
            ("Endpoint", "user@example.com"),
        ],
    );
    let result = svc.subscribe(&req);
    assert_ok(&result);
    let body = response_body(&result);
    assert!(
        body.contains("<SubscriptionArn>"),
        "Response should contain SubscriptionArn"
    );
    assert!(
        body.contains(topic_arn),
        "SubscriptionArn should contain topic ARN"
    );
}

#[test]
fn subscribe_duplicate_returns_existing_arn() {
    let (svc, _state) = make_sns();
    let req = sns_request("CreateTopic", vec![("Name", "dup-topic")]);
    assert_ok(&svc.create_topic(&req));

    let topic_arn = "arn:aws:sns:us-east-1:123456789012:dup-topic";
    let params = vec![
        ("TopicArn", topic_arn),
        ("Protocol", "email"),
        ("Endpoint", "user@example.com"),
    ];
    let r1 = svc.subscribe(&sns_request("Subscribe", params.clone()));
    assert_ok(&r1);
    let body1 = response_body(&r1);

    let r2 = svc.subscribe(&sns_request("Subscribe", params));
    assert_ok(&r2);
    let body2 = response_body(&r2);

    // Both should return same subscription ARN
    assert_eq!(body1, body2, "Duplicate subscribe should return same ARN");
}

#[test]
fn unsubscribe_removes_subscription() {
    let (svc, state) = make_sns();
    let req = sns_request("CreateTopic", vec![("Name", "unsub-topic")]);
    assert_ok(&svc.create_topic(&req));

    let topic_arn = "arn:aws:sns:us-east-1:123456789012:unsub-topic";
    let req = sns_request(
        "Subscribe",
        vec![
            ("TopicArn", topic_arn),
            ("Protocol", "email"),
            ("Endpoint", "user@example.com"),
        ],
    );
    assert_ok(&svc.subscribe(&req));

    // Get subscription ARN from state
    let sub_arn = {
        let s = state.read();
        s.default_ref().subscriptions.keys().next().unwrap().clone()
    };

    let req = sns_request("Unsubscribe", vec![("SubscriptionArn", &sub_arn)]);
    assert_ok(&svc.unsubscribe(&req));

    let s = state.read();
    assert!(
        s.default_ref().subscriptions.is_empty(),
        "Subscription should be removed"
    );
    // SubscriptionsDeleted on the parent topic must reflect the
    // unsubscribe so GetTopicAttributes returns a real cumulative count.
    assert_eq!(
        s.default_ref()
            .topics
            .get(topic_arn)
            .map(|t| t.subscriptions_deleted),
        Some(1)
    );
}

#[test]
fn get_topic_attributes_emits_subscriptions_deleted() {
    let (svc, _state) = make_sns();
    let req = sns_request("CreateTopic", vec![("Name", "sd-topic")]);
    assert_ok(&svc.create_topic(&req));

    let topic_arn = "arn:aws:sns:us-east-1:123456789012:sd-topic";
    // Two subscriptions, then unsubscribe both.
    for ep in ["a@example.com", "b@example.com"] {
        let req = sns_request(
            "Subscribe",
            vec![
                ("TopicArn", topic_arn),
                ("Protocol", "email"),
                ("Endpoint", ep),
            ],
        );
        assert_ok(&svc.subscribe(&req));
    }
    let sub_arns: Vec<String> = svc
        .state
        .read()
        .default_ref()
        .subscriptions
        .keys()
        .cloned()
        .collect();
    for arn in sub_arns {
        let req = sns_request("Unsubscribe", vec![("SubscriptionArn", &arn)]);
        assert_ok(&svc.unsubscribe(&req));
    }

    let req = sns_request("GetTopicAttributes", vec![("TopicArn", topic_arn)]);
    let resp = svc.get_topic_attributes(&req).unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(
        body.contains("<key>SubscriptionsDeleted</key><value>2</value>"),
        "expected SubscriptionsDeleted=2 in response, got: {body}"
    );
}

#[test]
fn list_subscriptions_returns_all() {
    let (svc, _state) = make_sns();
    let req = sns_request("CreateTopic", vec![("Name", "list-topic")]);
    assert_ok(&svc.create_topic(&req));

    let topic_arn = "arn:aws:sns:us-east-1:123456789012:list-topic";
    for i in 0..3 {
        let email = format!("user{}@example.com", i);
        let req = sns_request(
            "Subscribe",
            vec![
                ("TopicArn", topic_arn),
                ("Protocol", "email"),
                ("Endpoint", &email),
            ],
        );
        assert_ok(&svc.subscribe(&req));
    }

    let req = sns_request("ListSubscriptions", vec![]);
    let result = svc.list_subscriptions(&req);
    assert_ok(&result);
    let body = response_body(&result);
    // Should contain all 3 subscriptions
    let count = body.matches("<member>").count();
    assert_eq!(count, 3, "Should list 3 subscriptions, found {}", count);
}

#[test]
fn list_subscriptions_by_topic_filters_correctly() {
    let (svc, _state) = make_sns();
    // Create two topics
    assert_ok(&svc.create_topic(&sns_request("CreateTopic", vec![("Name", "topicA")])));
    assert_ok(&svc.create_topic(&sns_request("CreateTopic", vec![("Name", "topicB")])));

    let arn_a = "arn:aws:sns:us-east-1:123456789012:topicA";
    let arn_b = "arn:aws:sns:us-east-1:123456789012:topicB";

    // Subscribe 2 to A, 1 to B
    assert_ok(&svc.subscribe(&sns_request(
        "Subscribe",
        vec![
            ("TopicArn", arn_a),
            ("Protocol", "email"),
            ("Endpoint", "a1@example.com"),
        ],
    )));
    assert_ok(&svc.subscribe(&sns_request(
        "Subscribe",
        vec![
            ("TopicArn", arn_a),
            ("Protocol", "email"),
            ("Endpoint", "a2@example.com"),
        ],
    )));
    assert_ok(&svc.subscribe(&sns_request(
        "Subscribe",
        vec![
            ("TopicArn", arn_b),
            ("Protocol", "email"),
            ("Endpoint", "b1@example.com"),
        ],
    )));

    let req = sns_request("ListSubscriptionsByTopic", vec![("TopicArn", arn_a)]);
    let result = svc.list_subscriptions_by_topic(&req);
    assert_ok(&result);
    let body = response_body(&result);
    let count = body.matches("<member>").count();
    assert_eq!(
        count, 2,
        "Topic A should have 2 subscriptions, found {}",
        count
    );
}

// --- Publish / PublishBatch ---

#[test]
fn publish_to_topic_stores_message() {
    let (svc, state) = make_sns();
    assert_ok(&svc.create_topic(&sns_request("CreateTopic", vec![("Name", "pub-topic")])));

    let topic_arn = "arn:aws:sns:us-east-1:123456789012:pub-topic";
    let req = sns_request(
        "Publish",
        vec![
            ("TopicArn", topic_arn),
            ("Message", "Hello world"),
            ("Subject", "Test subject"),
        ],
    );
    let result = svc.publish(&req);
    assert_ok(&result);
    let body = response_body(&result);
    assert!(
        body.contains("<MessageId>"),
        "Response should contain MessageId"
    );

    let s = state.read();
    assert_eq!(s.default_ref().published.len(), 1);
    assert_eq!(s.default_ref().published[0].message, "Hello world");
    assert_eq!(
        s.default_ref().published[0].subject.as_deref(),
        Some("Test subject")
    );
}

#[test]
fn publish_to_nonexistent_topic_returns_error() {
    let (svc, _state) = make_sns();
    let req = sns_request(
        "Publish",
        vec![
            ("TopicArn", "arn:aws:sns:us-east-1:123456789012:nope"),
            ("Message", "Hello"),
        ],
    );
    let result = svc.publish(&req);
    assert!(result.is_err(), "Publish to nonexistent topic should error");
}

#[test]
fn publish_without_topic_or_phone_returns_error() {
    let (svc, _state) = make_sns();
    let req = sns_request("Publish", vec![("Message", "Hello")]);
    let result = svc.publish(&req);
    assert!(result.is_err(), "Publish without target should error");
}

#[test]
fn publish_validates_subject_length() {
    let (svc, _state) = make_sns();
    assert_ok(&svc.create_topic(&sns_request("CreateTopic", vec![("Name", "subj-topic")])));

    let topic_arn = "arn:aws:sns:us-east-1:123456789012:subj-topic";
    let long_subject = "x".repeat(101);
    let req = sns_request(
        "Publish",
        vec![
            ("TopicArn", topic_arn),
            ("Message", "Hello"),
            ("Subject", &long_subject),
        ],
    );
    let result = svc.publish(&req);
    assert!(result.is_err(), "Subject > 100 chars should error");
}

#[test]
fn publish_to_sms_phone_number() {
    let (svc, state) = make_sns();
    let req = sns_request(
        "Publish",
        vec![("PhoneNumber", "+15551234567"), ("Message", "SMS test")],
    );
    let result = svc.publish(&req);
    assert_ok(&result);

    let s = state.read();
    assert_eq!(s.default_ref().sms_messages.len(), 1);
    assert_eq!(s.default_ref().sms_messages[0].0, "+15551234567");
    assert_eq!(s.default_ref().sms_messages[0].1, "SMS test");
}

#[test]
fn publish_to_invalid_phone_returns_error() {
    let (svc, _state) = make_sns();
    let req = sns_request(
        "Publish",
        vec![("PhoneNumber", "not-a-phone"), ("Message", "SMS test")],
    );
    let result = svc.publish(&req);
    assert!(result.is_err(), "Invalid phone should error");
}

#[test]
fn publish_batch_stores_multiple_messages() {
    let (svc, state) = make_sns();
    assert_ok(&svc.create_topic(&sns_request("CreateTopic", vec![("Name", "batch-topic")])));

    let topic_arn = "arn:aws:sns:us-east-1:123456789012:batch-topic";
    let req = sns_request(
        "PublishBatch",
        vec![
            ("TopicArn", topic_arn),
            ("PublishBatchRequestEntries.member.1.Id", "msg1"),
            ("PublishBatchRequestEntries.member.1.Message", "Hello 1"),
            ("PublishBatchRequestEntries.member.2.Id", "msg2"),
            ("PublishBatchRequestEntries.member.2.Message", "Hello 2"),
        ],
    );
    let result = svc.publish_batch(&req);
    assert_ok(&result);
    let body = response_body(&result);
    assert!(
        body.contains("<Successful>"),
        "Response should contain Successful element"
    );

    let s = state.read();
    assert_eq!(s.default_ref().published.len(), 2);
}

#[test]
fn publish_batch_rejects_duplicate_ids() {
    let (svc, _state) = make_sns();
    assert_ok(&svc.create_topic(&sns_request("CreateTopic", vec![("Name", "batch-dup")])));

    let topic_arn = "arn:aws:sns:us-east-1:123456789012:batch-dup";
    let req = sns_request(
        "PublishBatch",
        vec![
            ("TopicArn", topic_arn),
            ("PublishBatchRequestEntries.member.1.Id", "same"),
            ("PublishBatchRequestEntries.member.1.Message", "Hello 1"),
            ("PublishBatchRequestEntries.member.2.Id", "same"),
            ("PublishBatchRequestEntries.member.2.Message", "Hello 2"),
        ],
    );
    let result = svc.publish_batch(&req);
    assert!(result.is_err(), "Duplicate batch IDs should error");
}

/// 1.21: PublishBatch with zero entries must return EmptyBatchRequest.
#[test]
fn publish_batch_empty_returns_empty_batch_request() {
    let (svc, _state) = make_sns();
    assert_ok(&svc.create_topic(&sns_request("CreateTopic", vec![("Name", "empty-batch")])));
    let req = sns_request(
        "PublishBatch",
        vec![("TopicArn", "arn:aws:sns:us-east-1:123456789012:empty-batch")],
    );
    let err = svc.publish_batch(&req).err().expect("empty batch errors");
    assert!(err.to_string().contains("EmptyBatchRequest"), "got {err}");
}

/// 1.21: PublishBatch on a FIFO topic without ContentBasedDeduplication
/// must require MessageDeduplicationId on every entry (matches single
/// Publish).
#[test]
fn publish_batch_fifo_requires_dedup_id() {
    let (svc, _state) = make_sns();
    let mut req = sns_request("CreateTopic", vec![("Name", "batch.fifo")]);
    req.query_params.insert(
        "Attributes.entry.1.key".to_string(),
        "FifoTopic".to_string(),
    );
    req.query_params
        .insert("Attributes.entry.1.value".to_string(), "true".to_string());
    assert_ok(&svc.create_topic(&req));

    let topic_arn = "arn:aws:sns:us-east-1:123456789012:batch.fifo";
    // MessageGroupId present but no MessageDeduplicationId -> error.
    let req = sns_request(
        "PublishBatch",
        vec![
            ("TopicArn", topic_arn),
            ("PublishBatchRequestEntries.member.1.Id", "m1"),
            ("PublishBatchRequestEntries.member.1.Message", "hi"),
            ("PublishBatchRequestEntries.member.1.MessageGroupId", "g1"),
        ],
    );
    let err = svc
        .publish_batch(&req)
        .err()
        .expect("FIFO batch without dedup id errors");
    assert!(
        err.to_string().contains("ContentBasedDeduplication"),
        "got {err}"
    );

    // With a MessageDeduplicationId it succeeds.
    let req = sns_request(
        "PublishBatch",
        vec![
            ("TopicArn", topic_arn),
            ("PublishBatchRequestEntries.member.1.Id", "m1"),
            ("PublishBatchRequestEntries.member.1.Message", "hi"),
            ("PublishBatchRequestEntries.member.1.MessageGroupId", "g1"),
            (
                "PublishBatchRequestEntries.member.1.MessageDeduplicationId",
                "d1",
            ),
        ],
    );
    assert_ok(&svc.publish_batch(&req));
}

// --- SetSubscriptionAttributes / GetSubscriptionAttributes ---

#[test]
fn get_subscription_attributes_returns_defaults() {
    let (svc, state) = make_sns();
    assert_ok(&svc.create_topic(&sns_request("CreateTopic", vec![("Name", "attr-topic")])));

    let topic_arn = "arn:aws:sns:us-east-1:123456789012:attr-topic";
    assert_ok(&svc.subscribe(&sns_request(
        "Subscribe",
        vec![
            ("TopicArn", topic_arn),
            ("Protocol", "email"),
            ("Endpoint", "u@example.com"),
        ],
    )));

    let sub_arn = {
        let s = state.read();
        s.default_ref().subscriptions.keys().next().unwrap().clone()
    };

    let req = sns_request(
        "GetSubscriptionAttributes",
        vec![("SubscriptionArn", &sub_arn)],
    );
    let result = svc.get_subscription_attributes(&req);
    assert_ok(&result);
    let body = response_body(&result);
    assert!(
        body.contains("<key>Protocol</key>"),
        "Should contain Protocol attribute"
    );
    assert!(
        body.contains("<value>email</value>"),
        "Protocol should be email"
    );
    assert!(
        body.contains("<key>Endpoint</key>"),
        "Should contain Endpoint attribute"
    );
    assert!(
        body.contains("<key>RawMessageDelivery</key>"),
        "Should contain RawMessageDelivery"
    );
}

#[test]
fn get_subscription_attributes_reflects_pending_confirmation() {
    // PendingConfirmation was hardcoded "false" (bug-audit 2026-06-20, 1.24):
    // an unconfirmed http subscription must report "true".
    let (svc, state) = make_sns();
    assert_ok(&svc.create_topic(&sns_request("CreateTopic", vec![("Name", "pc-topic")])));
    let topic_arn = "arn:aws:sns:us-east-1:123456789012:pc-topic";

    // http subscriptions stay unconfirmed until the token is confirmed.
    assert_ok(&svc.subscribe(&sns_request(
        "Subscribe",
        vec![
            ("TopicArn", topic_arn),
            ("Protocol", "http"),
            ("Endpoint", "http://example.com/hook"),
        ],
    )));
    // email auto-confirms.
    assert_ok(&svc.subscribe(&sns_request(
        "Subscribe",
        vec![
            ("TopicArn", topic_arn),
            ("Protocol", "email"),
            ("Endpoint", "u@example.com"),
        ],
    )));

    let (http_arn, email_arn) = {
        let s = state.read();
        let subs = &s.default_ref().subscriptions;
        let http = subs
            .values()
            .find(|sub| sub.protocol == "http")
            .unwrap()
            .subscription_arn
            .clone();
        let email = subs
            .values()
            .find(|sub| sub.protocol == "email")
            .unwrap()
            .subscription_arn
            .clone();
        (http, email)
    };

    let http_body = response_body(&svc.get_subscription_attributes(&sns_request(
        "GetSubscriptionAttributes",
        vec![("SubscriptionArn", &http_arn)],
    )));
    assert!(
        http_body.contains("<key>PendingConfirmation</key>")
            && http_body.contains("<value>true</value>"),
        "http subscription must report PendingConfirmation=true: {http_body}"
    );

    let email_body = response_body(&svc.get_subscription_attributes(&sns_request(
        "GetSubscriptionAttributes",
        vec![("SubscriptionArn", &email_arn)],
    )));
    assert!(
        email_body.contains("<key>PendingConfirmation</key>"),
        "{email_body}"
    );
    // The PendingConfirmation entry for a confirmed sub is false.
    let pc_idx = email_body.find("PendingConfirmation").unwrap();
    assert!(
        email_body[pc_idx..].contains("<value>false</value>"),
        "email subscription must report PendingConfirmation=false: {email_body}"
    );
}

#[test]
fn set_subscription_attributes_updates_value() {
    let (svc, state) = make_sns();
    assert_ok(&svc.create_topic(&sns_request("CreateTopic", vec![("Name", "setattr-topic")])));

    let topic_arn = "arn:aws:sns:us-east-1:123456789012:setattr-topic";
    assert_ok(&svc.subscribe(&sns_request(
        "Subscribe",
        vec![
            ("TopicArn", topic_arn),
            ("Protocol", "email"),
            ("Endpoint", "u@example.com"),
        ],
    )));

    let sub_arn = {
        let s = state.read();
        s.default_ref().subscriptions.keys().next().unwrap().clone()
    };

    // Set RawMessageDelivery to true
    let req = sns_request(
        "SetSubscriptionAttributes",
        vec![
            ("SubscriptionArn", &sub_arn),
            ("AttributeName", "RawMessageDelivery"),
            ("AttributeValue", "true"),
        ],
    );
    assert_ok(&svc.set_subscription_attributes(&req));

    // Verify in state
    let s = state.read();
    let sub = s.default_ref().subscriptions.get(&sub_arn).unwrap();
    assert_eq!(sub.attributes.get("RawMessageDelivery").unwrap(), "true");
}

#[test]
fn set_subscription_attributes_rejects_invalid_attr() {
    let (svc, state) = make_sns();
    assert_ok(&svc.create_topic(&sns_request("CreateTopic", vec![("Name", "inv-attr")])));

    let topic_arn = "arn:aws:sns:us-east-1:123456789012:inv-attr";
    assert_ok(&svc.subscribe(&sns_request(
        "Subscribe",
        vec![
            ("TopicArn", topic_arn),
            ("Protocol", "email"),
            ("Endpoint", "u@example.com"),
        ],
    )));

    let sub_arn = {
        let s = state.read();
        s.default_ref().subscriptions.keys().next().unwrap().clone()
    };

    let req = sns_request(
        "SetSubscriptionAttributes",
        vec![
            ("SubscriptionArn", &sub_arn),
            ("AttributeName", "BogusAttribute"),
            ("AttributeValue", "whatever"),
        ],
    );
    let result = svc.set_subscription_attributes(&req);
    assert!(result.is_err(), "Invalid attribute name should error");
}

#[test]
fn get_subscription_attributes_nonexistent_returns_error() {
    let (svc, _state) = make_sns();
    let req = sns_request(
        "GetSubscriptionAttributes",
        vec![(
            "SubscriptionArn",
            "arn:aws:sns:us-east-1:123456789012:nope:fake",
        )],
    );
    let result = svc.get_subscription_attributes(&req);
    assert!(result.is_err(), "Nonexistent subscription should error");
}

// --- TagResource / UntagResource / ListTagsForResource ---

#[test]
fn tag_untag_list_tags_lifecycle() {
    let (svc, _state) = make_sns();
    assert_ok(&svc.create_topic(&sns_request("CreateTopic", vec![("Name", "tag-topic")])));

    let topic_arn = "arn:aws:sns:us-east-1:123456789012:tag-topic";

    // Tag the resource
    let req = sns_request(
        "TagResource",
        vec![
            ("ResourceArn", topic_arn),
            ("Tags.member.1.Key", "env"),
            ("Tags.member.1.Value", "prod"),
            ("Tags.member.2.Key", "team"),
            ("Tags.member.2.Value", "platform"),
        ],
    );
    assert_ok(&svc.tag_resource(&req));

    // List tags
    let req = sns_request("ListTagsForResource", vec![("ResourceArn", topic_arn)]);
    let result = svc.list_tags_for_resource(&req);
    assert_ok(&result);
    let body = response_body(&result);
    assert!(
        body.contains("<Key>env</Key>"),
        "Should contain env tag key"
    );
    assert!(
        body.contains("<Value>prod</Value>"),
        "Should contain prod tag value"
    );
    assert!(
        body.contains("<Key>team</Key>"),
        "Should contain team tag key"
    );

    // Untag one key
    let req = sns_request(
        "UntagResource",
        vec![("ResourceArn", topic_arn), ("TagKeys.member.1", "env")],
    );
    assert_ok(&svc.untag_resource(&req));

    // Verify only team remains
    let req = sns_request("ListTagsForResource", vec![("ResourceArn", topic_arn)]);
    let result = svc.list_tags_for_resource(&req);
    assert_ok(&result);
    let body = response_body(&result);
    assert!(
        !body.contains("<Key>env</Key>"),
        "env tag should be removed"
    );
    assert!(body.contains("<Key>team</Key>"), "team tag should remain");
}

#[test]
fn tag_resource_nonexistent_returns_error() {
    let (svc, _state) = make_sns();
    let req = sns_request(
        "TagResource",
        vec![
            ("ResourceArn", "arn:aws:sns:us-east-1:123456789012:nope"),
            ("Tags.member.1.Key", "k"),
            ("Tags.member.1.Value", "v"),
        ],
    );
    let result = svc.tag_resource(&req);
    assert!(result.is_err(), "Tagging nonexistent resource should error");
}

#[test]
fn tag_resource_overwrites_existing_key() {
    let (svc, _state) = make_sns();
    assert_ok(&svc.create_topic(&sns_request("CreateTopic", vec![("Name", "tag-overwrite")])));

    let topic_arn = "arn:aws:sns:us-east-1:123456789012:tag-overwrite";

    // Add tag
    let req = sns_request(
        "TagResource",
        vec![
            ("ResourceArn", topic_arn),
            ("Tags.member.1.Key", "env"),
            ("Tags.member.1.Value", "dev"),
        ],
    );
    assert_ok(&svc.tag_resource(&req));

    // Overwrite tag
    let req = sns_request(
        "TagResource",
        vec![
            ("ResourceArn", topic_arn),
            ("Tags.member.1.Key", "env"),
            ("Tags.member.1.Value", "prod"),
        ],
    );
    assert_ok(&svc.tag_resource(&req));

    // Verify overwritten
    let req = sns_request("ListTagsForResource", vec![("ResourceArn", topic_arn)]);
    let body = response_body(&svc.list_tags_for_resource(&req));
    assert!(
        body.contains("<Value>prod</Value>"),
        "Tag value should be overwritten to prod"
    );
    // Should only have 1 member
    assert_eq!(
        body.matches("<member>").count(),
        1,
        "Should have exactly 1 tag"
    );
}

// --- SetTopicAttributes / GetTopicAttributes ---

#[test]
fn set_and_get_topic_attributes() {
    let (svc, _state) = make_sns();
    assert_ok(&svc.create_topic(&sns_request("CreateTopic", vec![("Name", "attr-topic2")])));

    let topic_arn = "arn:aws:sns:us-east-1:123456789012:attr-topic2";

    // Set DisplayName
    let req = sns_request(
        "SetTopicAttributes",
        vec![
            ("TopicArn", topic_arn),
            ("AttributeName", "DisplayName"),
            ("AttributeValue", "My Nice Topic"),
        ],
    );
    assert_ok(&svc.set_topic_attributes(&req));

    // Get attributes
    let req = sns_request("GetTopicAttributes", vec![("TopicArn", topic_arn)]);
    let result = svc.get_topic_attributes(&req);
    assert_ok(&result);
    let body = response_body(&result);
    assert!(
        body.contains("<value>My Nice Topic</value>"),
        "DisplayName should be set"
    );
    assert!(
        body.contains("<key>TopicArn</key>"),
        "Should contain TopicArn"
    );
    assert!(body.contains("<key>Owner</key>"), "Should contain Owner");
}

#[test]
fn get_topic_attributes_nonexistent_returns_error() {
    let (svc, _state) = make_sns();
    let req = sns_request(
        "GetTopicAttributes",
        vec![("TopicArn", "arn:aws:sns:us-east-1:123456789012:nope")],
    );
    let result = svc.get_topic_attributes(&req);
    assert!(result.is_err(), "Nonexistent topic should error");
}

#[test]
fn get_topic_attributes_wrong_region_returns_error() {
    let (svc, _state) = make_sns();
    assert_ok(&svc.create_topic(&sns_request("CreateTopic", vec![("Name", "region-topic")])));

    // The topic was created in us-east-1, but try to get it with a different region in the ARN
    let req = sns_request(
        "GetTopicAttributes",
        vec![(
            "TopicArn",
            "arn:aws:sns:eu-west-1:123456789012:region-topic",
        )],
    );
    let result = svc.get_topic_attributes(&req);
    assert!(result.is_err(), "Topic in wrong region should error");
}

// --- ConfirmSubscription ---

#[test]
fn confirm_subscription_returns_arn() {
    let (svc, state) = make_sns();
    assert_ok(&svc.create_topic(&sns_request("CreateTopic", vec![("Name", "confirm-topic")])));

    let topic_arn = "arn:aws:sns:us-east-1:123456789012:confirm-topic";

    // Subscribe an HTTP endpoint (starts as pending)
    assert_ok(&svc.subscribe(&sns_request(
        "Subscribe",
        vec![
            ("TopicArn", topic_arn),
            ("Protocol", "http"),
            ("Endpoint", "http://example.com/hook"),
        ],
    )));

    // Get the token from the pending subscription
    let token = {
        let s = state.read();
        s.default_ref()
            .subscriptions
            .values()
            .find(|sub| sub.topic_arn == topic_arn && !sub.confirmed)
            .expect("should have a pending subscription")
            .confirmation_token
            .clone()
            .expect("pending subscription should have a token")
    };

    let req = sns_request(
        "ConfirmSubscription",
        vec![("TopicArn", topic_arn), ("Token", &token)],
    );
    let result = svc.confirm_subscription(&req);
    assert_ok(&result);
    let body = response_body(&result);
    assert!(
        body.contains("<SubscriptionArn>"),
        "Should return a SubscriptionArn"
    );

    // Verify the subscription is now confirmed
    let s = state.read();
    let sub = s
        .default_ref()
        .subscriptions
        .values()
        .find(|sub| sub.topic_arn == topic_arn)
        .expect("subscription should exist");
    assert!(sub.confirmed, "subscription should be confirmed");
}

#[test]
fn confirm_subscription_rejects_invalid_token() {
    let (svc, _state) = make_sns();
    assert_ok(&svc.create_topic(&sns_request("CreateTopic", vec![("Name", "confirm-topic")])));

    let topic_arn = "arn:aws:sns:us-east-1:123456789012:confirm-topic";

    // Subscribe an HTTP endpoint (starts as pending)
    assert_ok(&svc.subscribe(&sns_request(
        "Subscribe",
        vec![
            ("TopicArn", topic_arn),
            ("Protocol", "http"),
            ("Endpoint", "http://example.com/hook"),
        ],
    )));

    // Try to confirm with wrong token
    let req = sns_request(
        "ConfirmSubscription",
        vec![("TopicArn", topic_arn), ("Token", "wrong-token")],
    );
    let result = svc.confirm_subscription(&req);
    assert!(result.is_err(), "Should reject invalid token");
}

#[test]
fn confirm_subscription_matches_correct_pending_sub() {
    let (svc, state) = make_sns();
    assert_ok(&svc.create_topic(&sns_request("CreateTopic", vec![("Name", "multi-topic")])));

    let topic_arn = "arn:aws:sns:us-east-1:123456789012:multi-topic";

    // Subscribe two HTTP endpoints (both start as pending)
    assert_ok(&svc.subscribe(&sns_request(
        "Subscribe",
        vec![
            ("TopicArn", topic_arn),
            ("Protocol", "http"),
            ("Endpoint", "http://first.example.com/hook"),
        ],
    )));
    assert_ok(&svc.subscribe(&sns_request(
        "Subscribe",
        vec![
            ("TopicArn", topic_arn),
            ("Protocol", "http"),
            ("Endpoint", "http://second.example.com/hook"),
        ],
    )));

    // Get the token for the second subscription
    let (second_arn, second_token) = {
        let s = state.read();
        let sub = s
            .default_ref()
            .subscriptions
            .values()
            .find(|sub| sub.endpoint == "http://second.example.com/hook")
            .expect("should have second subscription");
        (
            sub.subscription_arn.clone(),
            sub.confirmation_token.clone().unwrap(),
        )
    };

    // Confirm using the second subscription's token
    let req = sns_request(
        "ConfirmSubscription",
        vec![("TopicArn", topic_arn), ("Token", &second_token)],
    );
    let result = svc.confirm_subscription(&req);
    assert_ok(&result);
    let body = response_body(&result);
    assert!(
        body.contains(&second_arn),
        "Should return the second subscription's ARN"
    );

    // Verify only the second subscription is confirmed
    let s = state.read();
    for sub in s.default_ref().subscriptions.values() {
        if sub.endpoint == "http://second.example.com/hook" {
            assert!(sub.confirmed, "second subscription should be confirmed");
        } else {
            assert!(!sub.confirmed, "first subscription should still be pending");
        }
    }
}

#[test]
fn confirm_subscription_accepts_sub_arn_as_token() {
    let (svc, state) = make_sns();
    assert_ok(&svc.create_topic(&sns_request("CreateTopic", vec![("Name", "arn-token")])));

    let topic_arn = "arn:aws:sns:us-east-1:123456789012:arn-token";

    // Subscribe an HTTP endpoint (starts as pending)
    assert_ok(&svc.subscribe(&sns_request(
        "Subscribe",
        vec![
            ("TopicArn", topic_arn),
            ("Protocol", "http"),
            ("Endpoint", "http://example.com/hook"),
        ],
    )));

    // Get the subscription ARN
    let sub_arn = {
        let s = state.read();
        s.default_ref()
            .subscriptions
            .values()
            .find(|sub| sub.topic_arn == topic_arn)
            .expect("should have a subscription")
            .subscription_arn
            .clone()
    };

    // Confirm using the subscription ARN as the token (AWS-compatible behavior)
    let req = sns_request(
        "ConfirmSubscription",
        vec![("TopicArn", topic_arn), ("Token", &sub_arn)],
    );
    let result = svc.confirm_subscription(&req);
    assert_ok(&result);

    // Verify the subscription is now confirmed
    let s = state.read();
    let sub = s
        .default_ref()
        .subscriptions
        .values()
        .find(|sub| sub.topic_arn == topic_arn)
        .expect("subscription should exist");
    assert!(sub.confirmed, "subscription should be confirmed");
}

// --- CreateTopic / DeleteTopic / ListTopics ---

#[test]
fn create_delete_list_topics_lifecycle() {
    let (svc, _state) = make_sns();
    // Create two topics
    assert_ok(&svc.create_topic(&sns_request("CreateTopic", vec![("Name", "topic1")])));
    assert_ok(&svc.create_topic(&sns_request("CreateTopic", vec![("Name", "topic2")])));

    // List
    let req = sns_request("ListTopics", vec![]);
    let body = response_body(&svc.list_topics(&req));
    assert_eq!(body.matches("<TopicArn>").count(), 2);

    // Delete one
    let req = sns_request(
        "DeleteTopic",
        vec![("TopicArn", "arn:aws:sns:us-east-1:123456789012:topic1")],
    );
    assert_ok(&svc.delete_topic(&req));

    // List again
    let req = sns_request("ListTopics", vec![]);
    let body = response_body(&svc.list_topics(&req));
    assert_eq!(body.matches("<TopicArn>").count(), 1);
    assert!(body.contains("topic2"), "topic2 should remain");
}

#[test]
fn create_topic_idempotent() {
    let (svc, _state) = make_sns();
    let r1 = svc.create_topic(&sns_request("CreateTopic", vec![("Name", "idem-topic")]));
    assert_ok(&r1);
    let r2 = svc.create_topic(&sns_request("CreateTopic", vec![("Name", "idem-topic")]));
    assert_ok(&r2);
    let body1 = response_body(&r1);
    let body2 = response_body(&r2);
    assert_eq!(
        body1, body2,
        "Creating same topic twice should be idempotent"
    );
}

// --- AddPermission / RemovePermission ---

#[test]
fn add_and_remove_permission() {
    let (svc, state) = make_sns();
    assert_ok(&svc.create_topic(&sns_request("CreateTopic", vec![("Name", "perm-topic")])));

    let topic_arn = "arn:aws:sns:us-east-1:123456789012:perm-topic";
    let req = sns_request(
        "AddPermission",
        vec![
            ("TopicArn", topic_arn),
            ("Label", "MyPermission"),
            ("AWSAccountId.member.1", "111111111111"),
            ("ActionName.member.1", "Publish"),
        ],
    );
    assert_ok(&svc.add_permission(&req));

    // Verify policy has the new statement
    {
        let s = state.read();
        let policy_str = s
            .default_ref()
            .topics
            .get(topic_arn)
            .unwrap()
            .attributes
            .get("Policy")
            .unwrap();
        let policy: Value = serde_json::from_str(policy_str).unwrap();
        let stmts = policy["Statement"].as_array().unwrap();
        assert!(
            stmts
                .iter()
                .any(|s| s["Sid"].as_str() == Some("MyPermission")),
            "Policy should contain MyPermission statement"
        );
    }

    // Remove permission
    let req = sns_request(
        "RemovePermission",
        vec![("TopicArn", topic_arn), ("Label", "MyPermission")],
    );
    assert_ok(&svc.remove_permission(&req));

    // Verify removed
    {
        let s = state.read();
        let policy_str = s
            .default_ref()
            .topics
            .get(topic_arn)
            .unwrap()
            .attributes
            .get("Policy")
            .unwrap();
        let policy: Value = serde_json::from_str(policy_str).unwrap();
        let stmts = policy["Statement"].as_array().unwrap();
        assert!(
            !stmts
                .iter()
                .any(|s| s["Sid"].as_str() == Some("MyPermission")),
            "MyPermission should be removed"
        );
    }
}

// --- FIFO topic ---

#[test]
fn publish_to_fifo_topic_requires_group_id() {
    let (svc, _state) = make_sns();
    let mut req = sns_request("CreateTopic", vec![("Name", "fifo-topic.fifo")]);
    req.query_params.insert(
        "Attributes.entry.1.key".to_string(),
        "FifoTopic".to_string(),
    );
    req.query_params
        .insert("Attributes.entry.1.value".to_string(), "true".to_string());
    assert_ok(&svc.create_topic(&req));

    let topic_arn = "arn:aws:sns:us-east-1:123456789012:fifo-topic.fifo";
    // Publish without MessageGroupId — should fail
    let req = sns_request(
        "Publish",
        vec![("TopicArn", topic_arn), ("Message", "Hello")],
    );
    let result = svc.publish(&req);
    assert!(
        result.is_err(),
        "FIFO publish without MessageGroupId should error"
    );
}

// --- SMS attributes ---

#[test]
fn set_and_get_sms_attributes() {
    let (svc, _state) = make_sns();

    let mut req = sns_request("SetSMSAttributes", vec![]);
    req.query_params.insert(
        "attributes.entry.1.key".to_string(),
        "DefaultSMSType".to_string(),
    );
    req.query_params.insert(
        "attributes.entry.1.value".to_string(),
        "Transactional".to_string(),
    );
    assert_ok(&svc.set_sms_attributes(&req));

    let req = sns_request("GetSMSAttributes", vec![]);
    let result = svc.get_sms_attributes(&req);
    assert_ok(&result);
    let body = response_body(&result);
    assert!(
        body.contains("DefaultSMSType"),
        "Should contain set SMS attribute"
    );
}

// --- Phone opt-out ---

#[test]
fn check_phone_opted_out() {
    let (svc, state) = make_sns();
    state.write().default_mut().seed_default_opted_out();

    let req = sns_request(
        "CheckIfPhoneNumberIsOptedOut",
        vec![("phoneNumber", "+15005550099")],
    );
    let result = svc.check_if_phone_number_is_opted_out(&req);
    assert_ok(&result);
    let body = response_body(&result);
    assert!(
        body.contains("<isOptedOut>true</isOptedOut>"),
        "Seeded number should be opted out"
    );
}

#[test]
fn list_phone_numbers_opted_out() {
    let (svc, state) = make_sns();
    state.write().default_mut().seed_default_opted_out();

    let req = sns_request("ListPhoneNumbersOptedOut", vec![]);
    let result = svc.list_phone_numbers_opted_out(&req);
    assert_ok(&result);
    let body = response_body(&result);
    assert!(
        body.contains("+15005550099"),
        "Should list seeded opted-out number"
    );
}

#[test]
fn opt_in_phone_number() {
    let (svc, state) = make_sns();
    state.write().default_mut().seed_default_opted_out();

    let req = sns_request("OptInPhoneNumber", vec![("phoneNumber", "+15005550099")]);
    assert_ok(&svc.opt_in_phone_number(&req));

    // Verify removed from opted-out list
    let s = state.read();
    assert!(
        !s.default_ref()
            .opted_out_numbers
            .contains(&"+15005550099".to_string()),
        "Phone should no longer be opted out"
    );
}

// --- Delete topic also removes subscriptions ---

#[test]
fn delete_topic_removes_subscriptions() {
    let (svc, state) = make_sns();
    assert_ok(&svc.create_topic(&sns_request("CreateTopic", vec![("Name", "del-sub-topic")])));
    let topic_arn = "arn:aws:sns:us-east-1:123456789012:del-sub-topic";
    assert_ok(&svc.subscribe(&sns_request(
        "Subscribe",
        vec![
            ("TopicArn", topic_arn),
            ("Protocol", "email"),
            ("Endpoint", "u@example.com"),
        ],
    )));

    assert_eq!(state.read().default_ref().subscriptions.len(), 1);

    assert_ok(&svc.delete_topic(&sns_request("DeleteTopic", vec![("TopicArn", topic_arn)])));
    assert_eq!(
        state.read().default_ref().subscriptions.len(),
        0,
        "Subscriptions should be removed with topic"
    );
}

#[test]
fn malformed_filter_policy_does_not_match() {
    let sub = SnsSubscription {
        subscription_arn: "arn:aws:sns:us-east-1:123456789012:t:sub-1".to_string(),
        topic_arn: "arn:aws:sns:us-east-1:123456789012:t".to_string(),
        protocol: "sqs".to_string(),
        endpoint: "arn:aws:sqs:us-east-1:123456789012:q".to_string(),
        owner: "123456789012".to_string(),
        attributes: BTreeMap::from([(
            "FilterPolicy".to_string(),
            "not valid json {{[".to_string(),
        )]),
        confirmed: true,
        confirmation_token: None,
    };
    let attrs = BTreeMap::new();
    assert!(
        !matches_filter_policy(&sub, &attrs, "hello"),
        "malformed FilterPolicy JSON must not match (fail closed)"
    );
}

// ── Platform applications and endpoints ─────────────────────────

fn create_app(svc: &SnsService, name: &str, platform: &str) -> String {
    let req = sns_request(
        "CreatePlatformApplication",
        vec![
            ("Name", name),
            ("Platform", platform),
            ("Attributes.entry.1.key", "PlatformPrincipal"),
            ("Attributes.entry.1.value", "principal"),
            ("Attributes.entry.2.key", "PlatformCredential"),
            ("Attributes.entry.2.value", "secret"),
        ],
    );
    let resp = svc.create_platform_application(&req).unwrap();
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    let start = body.find("<PlatformApplicationArn>").unwrap() + "<PlatformApplicationArn>".len();
    let end = body.find("</PlatformApplicationArn>").unwrap();
    body[start..end].to_string()
}

#[test]
fn create_platform_application_persists_arn_and_attrs() {
    let (svc, state) = make_sns();
    let arn = create_app(&svc, "MyApp", "GCM");
    let s = state.read();
    let app = s.default_ref().platform_applications.get(&arn).unwrap();
    assert_eq!(app.name, "MyApp");
    assert_eq!(app.platform, "GCM");
    assert_eq!(
        app.attributes.get("PlatformPrincipal").map(String::as_str),
        Some("principal")
    );
}

#[test]
fn list_platform_applications_returns_created_app() {
    let (svc, _) = make_sns();
    let arn = create_app(&svc, "MyApp", "APNS");
    let req = sns_request("ListPlatformApplications", vec![]);
    let resp = svc.list_platform_applications(&req).unwrap();
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(body.contains(&arn));
}

#[test]
fn get_platform_application_attributes_unknown_errors() {
    let (svc, _) = make_sns();
    let req = sns_request(
        "GetPlatformApplicationAttributes",
        vec![(
            "PlatformApplicationArn",
            "arn:aws:sns:us-east-1:123456789012:app/GCM/Ghost",
        )],
    );
    let result = svc.get_platform_application_attributes(&req);
    assert!(result.is_err());
}

#[test]
fn set_platform_application_attributes_updates_attrs() {
    let (svc, state) = make_sns();
    let arn = create_app(&svc, "MyApp", "GCM");
    let req = sns_request(
        "SetPlatformApplicationAttributes",
        vec![
            ("PlatformApplicationArn", arn.as_str()),
            ("Attributes.entry.1.key", "Enabled"),
            ("Attributes.entry.1.value", "false"),
        ],
    );
    svc.set_platform_application_attributes(&req).unwrap();
    let s = state.read();
    assert_eq!(
        s.default_ref()
            .platform_applications
            .get(&arn)
            .unwrap()
            .attributes
            .get("Enabled")
            .map(String::as_str),
        Some("false")
    );
}

#[test]
fn delete_platform_application_removes_entry() {
    let (svc, state) = make_sns();
    let arn = create_app(&svc, "MyApp", "GCM");
    let req = sns_request(
        "DeletePlatformApplication",
        vec![("PlatformApplicationArn", arn.as_str())],
    );
    svc.delete_platform_application(&req).unwrap();
    assert!(state.read().default_ref().platform_applications.is_empty());
}

fn create_endpoint(svc: &SnsService, app_arn: &str, token: &str) -> String {
    let req = sns_request(
        "CreatePlatformEndpoint",
        vec![("PlatformApplicationArn", app_arn), ("Token", token)],
    );
    let resp = svc.create_platform_endpoint(&req).unwrap();
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    let start = body.find("<EndpointArn>").unwrap() + "<EndpointArn>".len();
    let end = body.find("</EndpointArn>").unwrap();
    body[start..end].to_string()
}

#[test]
fn create_platform_endpoint_unknown_app_errors() {
    let (svc, _) = make_sns();
    let req = sns_request(
        "CreatePlatformEndpoint",
        vec![
            (
                "PlatformApplicationArn",
                "arn:aws:sns:us-east-1:123456789012:app/GCM/Ghost",
            ),
            ("Token", "token-1"),
        ],
    );
    assert!(svc.create_platform_endpoint(&req).is_err());
}

#[test]
fn create_platform_endpoint_idempotent_on_same_token() {
    let (svc, _) = make_sns();
    let app_arn = create_app(&svc, "MyApp", "GCM");
    let arn1 = create_endpoint(&svc, &app_arn, "token-1");
    let arn2 = create_endpoint(&svc, &app_arn, "token-1");
    assert_eq!(arn1, arn2, "duplicate Token should return same EndpointArn");
}

#[test]
fn create_platform_endpoint_same_token_different_attrs_errors() {
    let (svc, _) = make_sns();
    let app_arn = create_app(&svc, "MyApp", "GCM");
    let _ = create_endpoint(&svc, &app_arn, "token-1");
    let req = sns_request(
        "CreatePlatformEndpoint",
        vec![
            ("PlatformApplicationArn", app_arn.as_str()),
            ("Token", "token-1"),
            ("Attributes.entry.1.key", "Enabled"),
            ("Attributes.entry.1.value", "false"),
        ],
    );
    let result = svc.create_platform_endpoint(&req);
    assert!(result.is_err());
}

#[test]
fn get_set_endpoint_attributes_round_trip() {
    let (svc, _) = make_sns();
    let app_arn = create_app(&svc, "MyApp", "GCM");
    let endpoint_arn = create_endpoint(&svc, &app_arn, "token-1");

    let set_req = sns_request(
        "SetEndpointAttributes",
        vec![
            ("EndpointArn", endpoint_arn.as_str()),
            ("Attributes.entry.1.key", "CustomUserData"),
            ("Attributes.entry.1.value", "user-1"),
        ],
    );
    svc.set_endpoint_attributes(&set_req).unwrap();

    let get_req = sns_request(
        "GetEndpointAttributes",
        vec![("EndpointArn", endpoint_arn.as_str())],
    );
    let resp = svc.get_endpoint_attributes(&get_req).unwrap();
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(body.contains("<key>CustomUserData</key>"));
    assert!(body.contains("<value>user-1</value>"));
}

#[test]
fn delete_endpoint_removes_endpoint() {
    let (svc, state) = make_sns();
    let app_arn = create_app(&svc, "MyApp", "GCM");
    let endpoint_arn = create_endpoint(&svc, &app_arn, "token-1");
    let del = sns_request(
        "DeleteEndpoint",
        vec![("EndpointArn", endpoint_arn.as_str())],
    );
    svc.delete_endpoint(&del).unwrap();
    let s = state.read();
    let app = s.default_ref().platform_applications.get(&app_arn).unwrap();
    assert!(app.endpoints.is_empty());
}

#[test]
fn get_endpoint_attributes_unknown_errors() {
    let (svc, _) = make_sns();
    let req = sns_request(
        "GetEndpointAttributes",
        vec![(
            "EndpointArn",
            "arn:aws:sns:us-east-1:123456789012:endpoint/GCM/MyApp/ghost",
        )],
    );
    assert!(svc.get_endpoint_attributes(&req).is_err());
}

// ── Error branch tests ──

#[test]
fn get_topic_attributes_not_found() {
    let (svc, _) = make_sns();
    let req = sns_request(
        "GetTopicAttributes",
        vec![("TopicArn", "arn:aws:sns:us-east-1:123456789012:nonexistent")],
    );
    assert!(svc.get_topic_attributes(&req).is_err());
}

#[test]
fn delete_topic_not_found() {
    let (svc, _) = make_sns();
    let req = sns_request(
        "DeleteTopic",
        vec![("TopicArn", "arn:aws:sns:us-east-1:123456789012:nonexistent")],
    );
    // DeleteTopic returns success even for nonexistent topics (AWS behavior)
    assert!(svc.delete_topic(&req).is_ok());
}

#[test]
fn subscribe_to_nonexistent_topic() {
    let (svc, _) = make_sns();
    let req = sns_request(
        "Subscribe",
        vec![
            ("TopicArn", "arn:aws:sns:us-east-1:123456789012:nope"),
            ("Protocol", "email"),
            ("Endpoint", "test@example.com"),
        ],
    );
    assert!(svc.subscribe(&req).is_err());
}

#[test]
fn unsubscribe_nonexistent_is_noop() {
    let (svc, _) = make_sns();
    let req = sns_request(
        "Unsubscribe",
        vec![(
            "SubscriptionArn",
            "arn:aws:sns:us-east-1:123456789012:topic:nonexistent-sub",
        )],
    );
    // AWS returns success for nonexistent subscriptions
    assert!(svc.unsubscribe(&req).is_ok());
}

#[test]
fn set_topic_attributes_not_found() {
    let (svc, _) = make_sns();
    let req = sns_request(
        "SetTopicAttributes",
        vec![
            ("TopicArn", "arn:aws:sns:us-east-1:123456789012:nope"),
            ("AttributeName", "DisplayName"),
            ("AttributeValue", "My Topic"),
        ],
    );
    assert!(svc.set_topic_attributes(&req).is_err());
}

#[test]
fn publish_to_nonexistent_topic() {
    let (svc, _) = make_sns();
    let req = sns_request(
        "Publish",
        vec![
            ("TopicArn", "arn:aws:sns:us-east-1:123456789012:nope"),
            ("Message", "hello"),
        ],
    );
    assert!(svc.publish(&req).is_err());
}

#[test]
fn get_subscription_attributes_not_found() {
    let (svc, _) = make_sns();
    let req = sns_request(
        "GetSubscriptionAttributes",
        vec![(
            "SubscriptionArn",
            "arn:aws:sns:us-east-1:123456789012:topic:bad-sub",
        )],
    );
    assert!(svc.get_subscription_attributes(&req).is_err());
}

#[test]
fn set_subscription_attributes_not_found() {
    let (svc, _) = make_sns();
    let req = sns_request(
        "SetSubscriptionAttributes",
        vec![
            (
                "SubscriptionArn",
                "arn:aws:sns:us-east-1:123456789012:topic:bad-sub",
            ),
            ("AttributeName", "FilterPolicy"),
            ("AttributeValue", "{}"),
        ],
    );
    assert!(svc.set_subscription_attributes(&req).is_err());
}

#[test]
fn tag_resource_nonexistent_topic() {
    let (svc, _) = make_sns();
    let req = sns_request(
        "TagResource",
        vec![
            ("ResourceArn", "arn:aws:sns:us-east-1:123456789012:nope"),
            ("Tags.member.1.Key", "env"),
            ("Tags.member.1.Value", "prod"),
        ],
    );
    assert!(svc.tag_resource(&req).is_err());
}

#[test]
fn untag_resource_nonexistent_topic() {
    let (svc, _) = make_sns();
    let req = sns_request(
        "UntagResource",
        vec![
            ("ResourceArn", "arn:aws:sns:us-east-1:123456789012:nope"),
            ("TagKeys.member.1", "env"),
        ],
    );
    assert!(svc.untag_resource(&req).is_err());
}

#[test]
fn list_tags_nonexistent_topic() {
    let (svc, _) = make_sns();
    let req = sns_request(
        "ListTagsForResource",
        vec![("ResourceArn", "arn:aws:sns:us-east-1:123456789012:nope")],
    );
    assert!(svc.list_tags_for_resource(&req).is_err());
}

#[test]
fn create_topic_duplicate_returns_existing_arn() {
    let (svc, _) = make_sns();
    let req = sns_request("CreateTopic", vec![("Name", "dup-topic")]);
    let resp1 = svc.create_topic(&req).unwrap();

    let req = sns_request("CreateTopic", vec![("Name", "dup-topic")]);
    let resp2 = svc.create_topic(&req).unwrap();

    // Should return same ARN (idempotent)
    let body1 = std::str::from_utf8(resp1.body.expect_bytes()).unwrap();
    let body2 = std::str::from_utf8(resp2.body.expect_bytes()).unwrap();
    assert_eq!(body1, body2);
}

#[test]
fn confirm_subscription_not_found() {
    let (svc, _) = make_sns();
    let req = sns_request(
        "ConfirmSubscription",
        vec![
            ("TopicArn", "arn:aws:sns:us-east-1:123456789012:nope"),
            ("Token", "fake-token"),
        ],
    );
    assert!(svc.confirm_subscription(&req).is_err());
}

#[test]
fn get_platform_application_attributes_not_found() {
    let (svc, _) = make_sns();
    let req = sns_request(
        "GetPlatformApplicationAttributes",
        vec![(
            "PlatformApplicationArn",
            "arn:aws:sns:us-east-1:123456789012:app/GCM/ghost",
        )],
    );
    assert!(svc.get_platform_application_attributes(&req).is_err());
}

#[test]
fn create_platform_endpoint_app_not_found() {
    let (svc, _) = make_sns();
    let req = sns_request(
        "CreatePlatformEndpoint",
        vec![
            (
                "PlatformApplicationArn",
                "arn:aws:sns:us-east-1:123456789012:app/GCM/ghost",
            ),
            ("Token", "device-token"),
        ],
    );
    assert!(svc.create_platform_endpoint(&req).is_err());
}

// ── Phone number opt-out check ──

#[test]
fn check_if_phone_number_is_opted_out() {
    let (svc, _) = make_sns();
    let req = sns_request(
        "CheckIfPhoneNumberIsOptedOut",
        vec![("phoneNumber", "+15551234567")],
    );
    let resp = svc.check_if_phone_number_is_opted_out(&req).unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(body.contains("isOptedOut"));
}

// ── Publish batch ──

#[test]
fn publish_batch() {
    let (svc, _) = make_sns();
    let req = sns_request("CreateTopic", vec![("Name", "batch-topic")]);
    let resp = svc.create_topic(&req).unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    let arn_start = body.find("<TopicArn>").unwrap() + 10;
    let arn_end = body.find("</TopicArn>").unwrap();
    let arn = body[arn_start..arn_end].to_string();

    let req = sns_request(
        "PublishBatch",
        vec![
            ("TopicArn", &arn),
            ("PublishBatchRequestEntries.member.1.Id", "1"),
            ("PublishBatchRequestEntries.member.1.Message", "msg1"),
            ("PublishBatchRequestEntries.member.2.Id", "2"),
            ("PublishBatchRequestEntries.member.2.Message", "msg2"),
        ],
    );
    let resp = svc.publish_batch(&req).unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(body.contains("Successful"));
}

#[test]
fn publish_batch_to_nonexistent_topic() {
    let (svc, _) = make_sns();
    let req = sns_request(
        "PublishBatch",
        vec![
            ("TopicArn", "arn:aws:sns:us-east-1:123456789012:ghost"),
            ("PublishBatchRequestEntries.member.1.Id", "1"),
            ("PublishBatchRequestEntries.member.1.Message", "msg"),
        ],
    );
    assert!(svc.publish_batch(&req).is_err());
}

// ── Subscriptions list ──

#[test]
fn list_subscriptions_by_topic() {
    let (svc, _) = make_sns();
    let req = sns_request("CreateTopic", vec![("Name", "sub-topic")]);
    let resp = svc.create_topic(&req).unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    let arn_start = body.find("<TopicArn>").unwrap() + 10;
    let arn_end = body.find("</TopicArn>").unwrap();
    let arn = body[arn_start..arn_end].to_string();

    let req = sns_request(
        "Subscribe",
        vec![
            ("TopicArn", &arn),
            ("Protocol", "email"),
            ("Endpoint", "test@example.com"),
            ("ReturnSubscriptionArn", "true"),
        ],
    );
    svc.subscribe(&req).unwrap();

    let req = sns_request("ListSubscriptionsByTopic", vec![("TopicArn", &arn)]);
    let resp = svc.list_subscriptions_by_topic(&req).unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(body.contains("Subscriptions"));
}

#[test]
fn list_subscriptions() {
    let (svc, _) = make_sns();
    let req = sns_request("ListSubscriptions", vec![]);
    svc.list_subscriptions(&req).unwrap();
}

// ── Topic policy attribute ──

#[test]
fn set_topic_policy_attribute() {
    let (svc, _) = make_sns();
    let req = sns_request("CreateTopic", vec![("Name", "policy-t")]);
    let resp = svc.create_topic(&req).unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    let arn_start = body.find("<TopicArn>").unwrap() + 10;
    let arn_end = body.find("</TopicArn>").unwrap();
    let arn = body[arn_start..arn_end].to_string();

    let policy = r#"{"Version":"2012-10-17","Statement":[]}"#;
    let req = sns_request(
        "SetTopicAttributes",
        vec![
            ("TopicArn", &arn),
            ("AttributeName", "Policy"),
            ("AttributeValue", policy),
        ],
    );
    svc.set_topic_attributes(&req).unwrap();
}

// ── Platform application full lifecycle ──

#[test]
fn platform_application_create_list_delete() {
    let (svc, _) = make_sns();

    let req = sns_request(
        "CreatePlatformApplication",
        vec![
            ("Name", "MyApp"),
            ("Platform", "GCM"),
            ("Attributes.entry.1.key", "PlatformCredential"),
            ("Attributes.entry.1.value", "api-key-value"),
        ],
    );
    let resp = svc.create_platform_application(&req).unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    let arn_start = body.find("<PlatformApplicationArn>").unwrap() + 24;
    let arn_end = body.find("</PlatformApplicationArn>").unwrap();
    let arn = body[arn_start..arn_end].to_string();

    // List
    let req = sns_request("ListPlatformApplications", vec![]);
    let resp = svc.list_platform_applications(&req).unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(body.contains("MyApp"));

    // GetPlatformApplicationAttributes
    let req = sns_request(
        "GetPlatformApplicationAttributes",
        vec![("PlatformApplicationArn", &arn)],
    );
    svc.get_platform_application_attributes(&req).unwrap();

    // Delete
    let req = sns_request(
        "DeletePlatformApplication",
        vec![("PlatformApplicationArn", &arn)],
    );
    svc.delete_platform_application(&req).unwrap();
}

// ── Subscription filter policy ──

#[test]
fn set_subscription_filter_policy() {
    let (svc, _) = make_sns();
    let req = sns_request("CreateTopic", vec![("Name", "filter-t")]);
    let resp = svc.create_topic(&req).unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    let arn_start = body.find("<TopicArn>").unwrap() + 10;
    let arn_end = body.find("</TopicArn>").unwrap();
    let arn = body[arn_start..arn_end].to_string();

    let req = sns_request(
        "Subscribe",
        vec![
            ("TopicArn", &arn),
            ("Protocol", "sqs"),
            ("Endpoint", "arn:aws:sqs:us-east-1:123456789012:q"),
            ("ReturnSubscriptionArn", "true"),
        ],
    );
    let resp = svc.subscribe(&req).unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    let sub_arn_start = body.find("<SubscriptionArn>").unwrap() + 17;
    let sub_arn_end = body.find("</SubscriptionArn>").unwrap();
    let sub_arn = body[sub_arn_start..sub_arn_end].to_string();

    let req = sns_request(
        "SetSubscriptionAttributes",
        vec![
            ("SubscriptionArn", &sub_arn),
            ("AttributeName", "FilterPolicy"),
            ("AttributeValue", r#"{"color":["blue"]}"#),
        ],
    );
    svc.set_subscription_attributes(&req).unwrap();
}

// ── publish error branches ──

#[test]
fn publish_missing_message_errors() {
    let (svc, _) = make_sns();
    svc.create_topic(&sns_request("CreateTopic", vec![("Name", "t")]))
        .unwrap();
    let req = sns_request(
        "Publish",
        vec![("TopicArn", "arn:aws:sns:us-east-1:123456789012:t")],
    );
    assert!(svc.publish(&req).is_err());
}

#[test]
fn publish_message_too_long_errors() {
    let (svc, _) = make_sns();
    svc.create_topic(&sns_request("CreateTopic", vec![("Name", "t")]))
        .unwrap();
    let big = "x".repeat(262145);
    let req = sns_request(
        "Publish",
        vec![
            ("TopicArn", "arn:aws:sns:us-east-1:123456789012:t"),
            ("Message", &big),
        ],
    );
    assert!(svc.publish(&req).is_err());
}

#[test]
fn publish_message_structure_invalid_json_errors() {
    let (svc, _) = make_sns();
    svc.create_topic(&sns_request("CreateTopic", vec![("Name", "t")]))
        .unwrap();
    let req = sns_request(
        "Publish",
        vec![
            ("TopicArn", "arn:aws:sns:us-east-1:123456789012:t"),
            ("Message", "not json"),
            ("MessageStructure", "json"),
        ],
    );
    assert!(svc.publish(&req).is_err());
}

#[test]
fn publish_message_structure_json_missing_default_errors() {
    let (svc, _) = make_sns();
    svc.create_topic(&sns_request("CreateTopic", vec![("Name", "t")]))
        .unwrap();
    let req = sns_request(
        "Publish",
        vec![
            ("TopicArn", "arn:aws:sns:us-east-1:123456789012:t"),
            ("Message", r#"{"email":"only"}"#),
            ("MessageStructure", "json"),
        ],
    );
    assert!(svc.publish(&req).is_err());
}

#[test]
fn publish_message_structure_json_uses_protocol_specific() {
    let (svc, _) = make_sns();
    let r = svc
        .create_topic(&sns_request("CreateTopic", vec![("Name", "struc")]))
        .unwrap();
    let body = String::from_utf8(r.body.expect_bytes().to_vec()).unwrap();
    let arn_start = body.find("<TopicArn>").unwrap() + 10;
    let arn_end = body.find("</TopicArn>").unwrap();
    let arn = body[arn_start..arn_end].to_string();

    let req = sns_request(
        "Publish",
        vec![
            ("TopicArn", &arn),
            ("Message", r#"{"default":"hi","sqs":"for sqs"}"#),
            ("MessageStructure", "json"),
        ],
    );
    svc.publish(&req).unwrap();
}

#[test]
fn publish_non_fifo_with_dedup_id_errors() {
    let (svc, _) = make_sns();
    svc.create_topic(&sns_request("CreateTopic", vec![("Name", "s")]))
        .unwrap();
    let req = sns_request(
        "Publish",
        vec![
            ("TopicArn", "arn:aws:sns:us-east-1:123456789012:s"),
            ("Message", "hi"),
            ("MessageDeduplicationId", "d1"),
        ],
    );
    assert!(svc.publish(&req).is_err());
}

#[test]
fn publish_fifo_without_dedup_errors() {
    let (svc, _) = make_sns();
    svc.create_topic(&sns_request(
        "CreateTopic",
        vec![
            ("Name", "ff.fifo"),
            ("Attributes.entry.1.key", "FifoTopic"),
            ("Attributes.entry.1.value", "true"),
        ],
    ))
    .unwrap();
    let req = sns_request(
        "Publish",
        vec![
            ("TopicArn", "arn:aws:sns:us-east-1:123456789012:ff.fifo"),
            ("Message", "m"),
            ("MessageGroupId", "g1"),
        ],
    );
    assert!(svc.publish(&req).is_err());
}

#[test]
fn publish_fifo_with_content_based_dedup_works() {
    let (svc, _) = make_sns();
    svc.create_topic(&sns_request(
        "CreateTopic",
        vec![
            ("Name", "cb.fifo"),
            ("Attributes.entry.1.key", "FifoTopic"),
            ("Attributes.entry.1.value", "true"),
            ("Attributes.entry.2.key", "ContentBasedDeduplication"),
            ("Attributes.entry.2.value", "true"),
        ],
    ))
    .unwrap();
    let req = sns_request(
        "Publish",
        vec![
            ("TopicArn", "arn:aws:sns:us-east-1:123456789012:cb.fifo"),
            ("Message", "m"),
            ("MessageGroupId", "g1"),
        ],
    );
    svc.publish(&req).unwrap();
}

// ── platform endpoint publish/delete ──

#[test]
fn publish_to_unknown_platform_endpoint_errors() {
    let (svc, _) = make_sns();
    let req = sns_request(
        "Publish",
        vec![
            (
                "TargetArn",
                "arn:aws:sns:us-east-1:123456789012:endpoint/GCM/app/abc",
            ),
            ("Message", "hi"),
        ],
    );
    assert!(svc.publish(&req).is_err());
}

#[test]
fn delete_endpoint_unknown_is_idempotent() {
    let (svc, _) = make_sns();
    let req = sns_request(
        "DeleteEndpoint",
        vec![(
            "EndpointArn",
            "arn:aws:sns:us-east-1:123456789012:endpoint/GCM/app/ghost",
        )],
    );
    svc.delete_endpoint(&req).unwrap();
}

#[test]
fn delete_platform_application_unknown_is_ok() {
    let (svc, _) = make_sns();
    let req = sns_request(
        "DeletePlatformApplication",
        vec![(
            "PlatformApplicationArn",
            "arn:aws:sns:us-east-1:123456789012:app/GCM/ghost",
        )],
    );
    svc.delete_platform_application(&req).unwrap();
}

#[test]
fn set_endpoint_attributes_unknown_errors() {
    let (svc, _) = make_sns();
    let req = sns_request(
        "SetEndpointAttributes",
        vec![
            (
                "EndpointArn",
                "arn:aws:sns:us-east-1:123456789012:endpoint/GCM/app/missing",
            ),
            ("Attributes.entry.1.key", "Enabled"),
            ("Attributes.entry.1.value", "false"),
        ],
    );
    assert!(svc.set_endpoint_attributes(&req).is_err());
}

// ── SMS attributes ──

#[test]
fn set_sms_attributes_stores_value() {
    let (svc, _) = make_sns();
    let req = sns_request(
        "SetSMSAttributes",
        vec![
            ("attributes.entry.1.key", "DefaultSenderID"),
            ("attributes.entry.1.value", "MyCorp"),
        ],
    );
    svc.set_sms_attributes(&req).unwrap();
    let req = sns_request("GetSMSAttributes", vec![]);
    let resp = svc.get_sms_attributes(&req).unwrap();
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(body.contains("MyCorp"));
}

#[test]
fn opt_in_phone_number_and_check() {
    let (svc, _) = make_sns();
    let _ = svc.check_if_phone_number_is_opted_out(&sns_request(
        "CheckIfPhoneNumberIsOptedOut",
        vec![("phoneNumber", "+15555555555")],
    ));
    let _ = svc.opt_in_phone_number(&sns_request(
        "OptInPhoneNumber",
        vec![("phoneNumber", "+15555555555")],
    ));
    svc.list_phone_numbers_opted_out(&sns_request("ListPhoneNumbersOptedOut", vec![]))
        .unwrap();
}

// ── subscription attribute filter policy validation ──

#[test]
fn set_subscription_attributes_raw_message_delivery() {
    let (svc, _) = make_sns();
    let r = svc
        .create_topic(&sns_request("CreateTopic", vec![("Name", "rmd")]))
        .unwrap();
    let body = String::from_utf8(r.body.expect_bytes().to_vec()).unwrap();
    let arn_start = body.find("<TopicArn>").unwrap() + 10;
    let arn_end = body.find("</TopicArn>").unwrap();
    let arn = body[arn_start..arn_end].to_string();

    let req = sns_request(
        "Subscribe",
        vec![
            ("TopicArn", &arn),
            ("Protocol", "sqs"),
            ("Endpoint", "arn:aws:sqs:us-east-1:123456789012:q"),
            ("ReturnSubscriptionArn", "true"),
        ],
    );
    let r = svc.subscribe(&req).unwrap();
    let body = String::from_utf8(r.body.expect_bytes().to_vec()).unwrap();
    let sub_arn = body
        [body.find("<SubscriptionArn>").unwrap() + 17..body.find("</SubscriptionArn>").unwrap()]
        .to_string();

    let req = sns_request(
        "SetSubscriptionAttributes",
        vec![
            ("SubscriptionArn", &sub_arn),
            ("AttributeName", "RawMessageDelivery"),
            ("AttributeValue", "true"),
        ],
    );
    svc.set_subscription_attributes(&req).unwrap();
}

// ── list topics pagination ──

#[test]
fn list_topics_pagination_token() {
    let (svc, _) = make_sns();
    for i in 0..120 {
        let name = format!("t{i}");
        svc.create_topic(&sns_request("CreateTopic", vec![("Name", &name)]))
            .unwrap();
    }
    let req = sns_request("ListTopics", vec![]);
    let resp = svc.list_topics(&req).unwrap();
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(body.contains("<NextToken>"));
}

// ── invalid topic name branches ──

#[test]
fn create_topic_empty_name_errors() {
    let (svc, _) = make_sns();
    assert!(svc
        .create_topic(&sns_request("CreateTopic", vec![("Name", "")]))
        .is_err());
}

#[test]
fn create_topic_too_long_name_errors() {
    let (svc, _) = make_sns();
    let name = "x".repeat(257);
    assert!(svc
        .create_topic(&sns_request("CreateTopic", vec![("Name", &name)]))
        .is_err());
}

#[test]
fn create_topic_fifo_without_suffix_errors() {
    let (svc, _) = make_sns();
    assert!(svc
        .create_topic(&sns_request(
            "CreateTopic",
            vec![
                ("Name", "plain"),
                ("Attributes.entry.1.key", "FifoTopic"),
                ("Attributes.entry.1.value", "true"),
            ]
        ))
        .is_err());
}

#[test]
fn create_topic_non_fifo_with_fifo_suffix_errors() {
    let (svc, _) = make_sns();
    assert!(svc
        .create_topic(&sns_request("CreateTopic", vec![("Name", "bad.fifo")]))
        .is_err());
}

// ── subscribe protocol validation ──

#[test]
fn subscribe_missing_protocol_errors() {
    let (svc, _) = make_sns();
    svc.create_topic(&sns_request("CreateTopic", vec![("Name", "t")]))
        .unwrap();
    let req = sns_request(
        "Subscribe",
        vec![("TopicArn", "arn:aws:sns:us-east-1:123456789012:t")],
    );
    assert!(svc.subscribe(&req).is_err());
}

// ── get_topic_attributes wrong region ──

#[test]
fn get_topic_attributes_returns_policy() {
    let (svc, _) = make_sns();
    svc.create_topic(&sns_request("CreateTopic", vec![("Name", "pol")]))
        .unwrap();
    let req = sns_request(
        "GetTopicAttributes",
        vec![("TopicArn", "arn:aws:sns:us-east-1:123456789012:pol")],
    );
    let resp = svc.get_topic_attributes(&req).unwrap();
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(body.contains("Policy"));
}

// ── PublishBatch error paths ──

#[test]
fn publish_batch_missing_topic_errors() {
    let (svc, _) = make_sns();
    let req = sns_request(
        "PublishBatch",
        vec![
            ("PublishBatchRequestEntries.member.1.Id", "e1"),
            ("PublishBatchRequestEntries.member.1.Message", "hi"),
        ],
    );
    assert!(svc.publish_batch(&req).is_err());
}

#[test]
fn subscribe_missing_topic_arn_errors() {
    let (svc, _) = make_sns();
    let req = sns_request("Subscribe", vec![("Protocol", "sqs")]);
    assert!(svc.subscribe(&req).is_err());
}

#[test]
fn unsubscribe_missing_arn_errors() {
    let (svc, _) = make_sns();
    let req = sns_request("Unsubscribe", vec![]);
    assert!(svc.unsubscribe(&req).is_err());
}

#[test]
fn get_subscription_attributes_missing_arn_errors() {
    let (svc, _) = make_sns();
    let req = sns_request("GetSubscriptionAttributes", vec![]);
    assert!(svc.get_subscription_attributes(&req).is_err());
}

#[test]
fn set_subscription_attributes_missing_arn_errors() {
    let (svc, _) = make_sns();
    let req = sns_request(
        "SetSubscriptionAttributes",
        vec![("AttributeName", "x"), ("AttributeValue", "y")],
    );
    assert!(svc.set_subscription_attributes(&req).is_err());
}

#[test]
fn set_topic_attributes_missing_arn_errors() {
    let (svc, _) = make_sns();
    let req = sns_request(
        "SetTopicAttributes",
        vec![("AttributeName", "DisplayName"), ("AttributeValue", "x")],
    );
    assert!(svc.set_topic_attributes(&req).is_err());
}

#[test]
fn list_tags_missing_resource_arn_errors() {
    let (svc, _) = make_sns();
    let req = sns_request("ListTagsForResource", vec![]);
    assert!(svc.list_tags_for_resource(&req).is_err());
}

#[test]
fn tag_resource_missing_arn_errors() {
    let (svc, _) = make_sns();
    let req = sns_request(
        "TagResource",
        vec![("Tags.member.1.Key", "k"), ("Tags.member.1.Value", "v")],
    );
    assert!(svc.tag_resource(&req).is_err());
}

#[test]
fn untag_resource_missing_arn_errors() {
    let (svc, _) = make_sns();
    let req = sns_request("UntagResource", vec![("TagKeys.member.1", "k")]);
    assert!(svc.untag_resource(&req).is_err());
}

#[test]
fn add_permission_missing_topic_arn_errors() {
    let (svc, _) = make_sns();
    let req = sns_request(
        "AddPermission",
        vec![("Label", "l"), ("AWSAccountId.member.1", "123")],
    );
    assert!(svc.add_permission(&req).is_err());
}

#[test]
fn remove_permission_missing_topic_errors() {
    let (svc, _) = make_sns();
    let req = sns_request("RemovePermission", vec![("Label", "l")]);
    assert!(svc.remove_permission(&req).is_err());
}

#[test]
fn create_platform_endpoint_missing_app_arn_errors() {
    let (svc, _) = make_sns();
    let req = sns_request("CreatePlatformEndpoint", vec![("Token", "t")]);
    assert!(svc.create_platform_endpoint(&req).is_err());
}

#[test]
fn set_platform_application_attributes_unknown_app_errors() {
    let (svc, _) = make_sns();
    let req = sns_request(
        "SetPlatformApplicationAttributes",
        vec![
            (
                "PlatformApplicationArn",
                "arn:aws:sns:us-east-1:123456789012:app/GCM/ghost",
            ),
            ("Attributes.entry.1.key", "PlatformCredential"),
            ("Attributes.entry.1.value", "x"),
        ],
    );
    assert!(svc.set_platform_application_attributes(&req).is_err());
}

#[test]
fn delete_topic_missing_arn_errors() {
    let (svc, _) = make_sns();
    let req = sns_request("DeleteTopic", vec![]);
    assert!(svc.delete_topic(&req).is_err());
}

#[test]
fn get_topic_attributes_missing_arn_errors() {
    let (svc, _) = make_sns();
    let req = sns_request("GetTopicAttributes", vec![]);
    assert!(svc.get_topic_attributes(&req).is_err());
}

#[test]
fn publish_message_with_subject() {
    let (svc, _) = make_sns();
    svc.create_topic(&sns_request("CreateTopic", vec![("Name", "subj")]))
        .unwrap();
    let req = sns_request(
        "Publish",
        vec![
            ("TopicArn", "arn:aws:sns:us-east-1:123456789012:subj"),
            ("Message", "hello"),
            ("Subject", "Greeting"),
        ],
    );
    svc.publish(&req).unwrap();
}

#[test]
fn confirm_subscription_missing_token_errors() {
    let (svc, _) = make_sns();
    let req = sns_request(
        "ConfirmSubscription",
        vec![("TopicArn", "arn:aws:sns:us-east-1:123456789012:t")],
    );
    assert!(svc.confirm_subscription(&req).is_err());
}

#[test]
fn confirm_subscription_missing_topic_errors() {
    let (svc, _) = make_sns();
    let req = sns_request("ConfirmSubscription", vec![("Token", "tok")]);
    assert!(svc.confirm_subscription(&req).is_err());
}

#[test]
fn list_subscriptions_by_topic_missing_arn_errors() {
    let (svc, _) = make_sns();
    let req = sns_request("ListSubscriptionsByTopic", vec![]);
    assert!(svc.list_subscriptions_by_topic(&req).is_err());
}

#[test]
fn create_platform_application_missing_name_errors() {
    let (svc, _) = make_sns();
    let req = sns_request(
        "CreatePlatformApplication",
        vec![
            ("Platform", "GCM"),
            ("Attributes.entry.1.key", "PlatformCredential"),
            ("Attributes.entry.1.value", "creds"),
        ],
    );
    assert!(svc.create_platform_application(&req).is_err());
}

#[test]
fn create_platform_application_missing_platform_errors() {
    let (svc, _) = make_sns();
    let req = sns_request("CreatePlatformApplication", vec![("Name", "a")]);
    assert!(svc.create_platform_application(&req).is_err());
}

#[test]
fn delete_endpoint_missing_arn_errors() {
    let (svc, _) = make_sns();
    let req = sns_request("DeleteEndpoint", vec![]);
    assert!(svc.delete_endpoint(&req).is_err());
}

#[test]
fn get_endpoint_attributes_missing_arn_errors() {
    let (svc, _) = make_sns();
    let req = sns_request("GetEndpointAttributes", vec![]);
    assert!(svc.get_endpoint_attributes(&req).is_err());
}

#[test]
fn list_endpoints_by_app_missing_arn_errors() {
    let (svc, _) = make_sns();
    let req = sns_request("ListEndpointsByPlatformApplication", vec![]);
    assert!(svc.list_endpoints_by_platform_application(&req).is_err());
}

#[test]
fn check_phone_opted_out_missing_number_errors() {
    let (svc, _) = make_sns();
    let req = sns_request("CheckIfPhoneNumberIsOptedOut", vec![]);
    assert!(svc.check_if_phone_number_is_opted_out(&req).is_err());
}

#[test]
fn opt_in_phone_missing_number_errors() {
    let (svc, _) = make_sns();
    let req = sns_request("OptInPhoneNumber", vec![]);
    assert!(svc.opt_in_phone_number(&req).is_err());
}

// ── SMS sandbox / data protection coverage ──

#[test]
fn validate_e164_rejects_short_numbers() {
    assert!(validate_e164("+1").is_err());
    assert!(validate_e164("+12").is_err());
    assert!(validate_e164("+123").is_err());
    assert!(validate_e164("+1234").is_ok());
}

#[test]
fn validate_e164_rejects_too_long_or_non_digit() {
    assert!(validate_e164("+1234567890123456").is_err()); // 16 digits
    assert!(validate_e164("+1abc4567").is_err());
    assert!(validate_e164("12345").is_err()); // missing +
}

#[test]
fn is_valid_sandbox_language_round_trip() {
    for code in SUPPORTED_SANDBOX_LANGUAGES {
        assert!(is_valid_sandbox_language(code), "{code}");
    }
    assert!(!is_valid_sandbox_language("xx-XX"));
    assert!(!is_valid_sandbox_language(""));
}

#[test]
fn create_sms_sandbox_phone_number_rejects_invalid_phone() {
    let (svc, _) = make_sns();
    let req = sns_request("CreateSMSSandboxPhoneNumber", vec![("PhoneNumber", "+1")]);
    let err = match svc.create_sms_sandbox_phone_number(&req) {
        Err(e) => e,
        Ok(_) => panic!("expected err"),
    };
    assert_eq!(err.code(), "InvalidParameter");
}

#[test]
fn create_sms_sandbox_phone_number_rejects_invalid_language() {
    let (svc, _) = make_sns();
    let req = sns_request(
        "CreateSMSSandboxPhoneNumber",
        vec![("PhoneNumber", "+15551234567"), ("LanguageCode", "xx-YY")],
    );
    let err = match svc.create_sms_sandbox_phone_number(&req) {
        Err(e) => e,
        Ok(_) => panic!("expected err"),
    };
    assert_eq!(err.code(), "InvalidParameter");
}

#[test]
fn create_sms_sandbox_phone_number_rejects_duplicate() {
    let (svc, _) = make_sns();
    let req = sns_request(
        "CreateSMSSandboxPhoneNumber",
        vec![("PhoneNumber", "+15551234500")],
    );
    svc.create_sms_sandbox_phone_number(&req).unwrap();
    let err = match svc.create_sms_sandbox_phone_number(&req) {
        Err(e) => e,
        Ok(_) => panic!("expected err"),
    };
    assert_eq!(err.code(), "OptedOutException");
}

#[test]
fn delete_sms_sandbox_phone_number_unknown_errors() {
    let (svc, _) = make_sns();
    let req = sns_request(
        "DeleteSMSSandboxPhoneNumber",
        vec![("PhoneNumber", "+15551234599")],
    );
    let err = match svc.delete_sms_sandbox_phone_number(&req) {
        Err(e) => e,
        Ok(_) => panic!("expected err"),
    };
    assert_eq!(err.code(), "ResourceNotFound");
}

#[test]
fn verify_sms_sandbox_phone_number_wrong_otp_errors() {
    let (svc, _) = make_sns();
    let create = sns_request(
        "CreateSMSSandboxPhoneNumber",
        vec![("PhoneNumber", "+15551234511")],
    );
    svc.create_sms_sandbox_phone_number(&create).unwrap();
    let verify = sns_request(
        "VerifySMSSandboxPhoneNumber",
        vec![
            ("PhoneNumber", "+15551234511"),
            ("OneTimePassword", "999999"),
        ],
    );
    let err = match svc.verify_sms_sandbox_phone_number(&verify) {
        Err(e) => e,
        Ok(_) => panic!("expected err"),
    };
    assert_eq!(err.code(), "VerificationException");
}

#[test]
fn verify_sms_sandbox_phone_number_unknown_errors() {
    let (svc, _) = make_sns();
    let req = sns_request(
        "VerifySMSSandboxPhoneNumber",
        vec![
            ("PhoneNumber", "+15550009999"),
            ("OneTimePassword", "000000"),
        ],
    );
    let err = match svc.verify_sms_sandbox_phone_number(&req) {
        Err(e) => e,
        Ok(_) => panic!("expected err"),
    };
    assert_eq!(err.code(), "ResourceNotFound");
}

#[test]
fn list_sms_sandbox_phone_numbers_returns_entries() {
    let (svc, _) = make_sns();
    let create = sns_request(
        "CreateSMSSandboxPhoneNumber",
        vec![("PhoneNumber", "+15551239876")],
    );
    svc.create_sms_sandbox_phone_number(&create).unwrap();
    let list = sns_request("ListSMSSandboxPhoneNumbers", vec![]);
    let resp = svc.list_sms_sandbox_phone_numbers(&list).unwrap();
    assert!(resp.status.is_success());
}

#[test]
fn get_sms_sandbox_account_status_starts_in_sandbox() {
    let (svc, _) = make_sns();
    let req = sns_request("GetSMSSandboxAccountStatus", vec![]);
    let resp = svc.get_sms_sandbox_account_status(&req).unwrap();
    // raw XML body — eyeball IsInSandbox=true.
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(body.contains("<IsInSandbox>true</IsInSandbox>"), "{body}");
}

#[test]
fn list_origination_numbers_seeds_default() {
    let (svc, _) = make_sns();
    let req = sns_request("ListOriginationNumbers", vec![]);
    let resp = svc.list_origination_numbers(&req).unwrap();
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(body.contains("+18005550100"), "{body}");
}

#[test]
fn list_origination_numbers_rejects_max_results_below_min() {
    let (svc, _) = make_sns();
    let req = sns_request("ListOriginationNumbers", vec![("MaxResults", "0")]);
    let err = match svc.list_origination_numbers(&req) {
        Err(e) => e,
        Ok(_) => panic!("expected MaxResults validation error"),
    };
    assert_eq!(err.code(), "InvalidParameter");
}

#[test]
fn list_origination_numbers_rejects_max_results_above_max() {
    let (svc, _) = make_sns();
    // Smithy @range max for ListOriginationNumbers is 30.
    let req = sns_request("ListOriginationNumbers", vec![("MaxResults", "31")]);
    let err = match svc.list_origination_numbers(&req) {
        Err(e) => e,
        Ok(_) => panic!("expected MaxResults validation error"),
    };
    assert_eq!(err.code(), "InvalidParameter");
}

#[test]
fn list_sms_sandbox_phone_numbers_rejects_max_results_above_max() {
    let (svc, _) = make_sns();
    // Smithy @range max for MaxItems is 100.
    let req = sns_request("ListSMSSandboxPhoneNumbers", vec![("MaxResults", "101")]);
    let err = match svc.list_sms_sandbox_phone_numbers(&req) {
        Err(e) => e,
        Ok(_) => panic!("expected MaxResults validation error"),
    };
    assert_eq!(err.code(), "InvalidParameter");
}

#[test]
fn put_data_protection_policy_requires_topic() {
    let (svc, _) = make_sns();
    let req = sns_request(
        "PutDataProtectionPolicy",
        vec![
            ("ResourceArn", "arn:aws:sns:us-east-1:123456789012:no-topic"),
            ("DataProtectionPolicy", "{}"),
        ],
    );
    let err = match svc.put_data_protection_policy(&req) {
        Err(e) => e,
        Ok(_) => panic!("expected err"),
    };
    assert_eq!(err.code(), "NotFound");
}

#[test]
fn put_data_protection_policy_rejects_invalid_json() {
    let (svc, _) = make_sns();
    // First create a topic.
    let create = sns_request("CreateTopic", vec![("Name", "dpp-topic")]);
    svc.create_topic(&create).unwrap();
    let arn = "arn:aws:sns:us-east-1:123456789012:dpp-topic";
    let req = sns_request(
        "PutDataProtectionPolicy",
        vec![("ResourceArn", arn), ("DataProtectionPolicy", "not-json")],
    );
    let err = match svc.put_data_protection_policy(&req) {
        Err(e) => e,
        Ok(_) => panic!("expected err"),
    };
    assert_eq!(err.code(), "InvalidParameter");
}

#[test]
fn put_then_get_data_protection_policy_round_trips() {
    let (svc, _) = make_sns();
    let create = sns_request("CreateTopic", vec![("Name", "dpp-topic2")]);
    svc.create_topic(&create).unwrap();
    let arn = "arn:aws:sns:us-east-1:123456789012:dpp-topic2";
    let req = sns_request(
        "PutDataProtectionPolicy",
        vec![
            ("ResourceArn", arn),
            ("DataProtectionPolicy", r#"{"Name":"p"}"#),
        ],
    );
    svc.put_data_protection_policy(&req).unwrap();
    let get = sns_request("GetDataProtectionPolicy", vec![("ResourceArn", arn)]);
    let resp = svc.get_data_protection_policy(&get).unwrap();
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(body.contains("&quot;Name&quot;:&quot;p&quot;"), "{body}");
}

#[test]
fn put_empty_data_protection_policy_clears_it() {
    // AWS accepts an empty DataProtectionPolicy to remove the policy; the
    // Terraform resource sends "" on delete. fakecloud must not reject it as
    // invalid JSON, and the policy must actually be cleared.
    let (svc, _) = make_sns();
    svc.create_topic(&sns_request("CreateTopic", vec![("Name", "dpp-clear")]))
        .unwrap();
    let arn = "arn:aws:sns:us-east-1:123456789012:dpp-clear";
    svc.put_data_protection_policy(&sns_request(
        "PutDataProtectionPolicy",
        vec![
            ("ResourceArn", arn),
            ("DataProtectionPolicy", r#"{"Name":"p"}"#),
        ],
    ))
    .unwrap();
    // Empty body must succeed and clear.
    svc.put_data_protection_policy(&sns_request(
        "PutDataProtectionPolicy",
        vec![("ResourceArn", arn), ("DataProtectionPolicy", "")],
    ))
    .unwrap();
    let resp = svc
        .get_data_protection_policy(&sns_request(
            "GetDataProtectionPolicy",
            vec![("ResourceArn", arn)],
        ))
        .unwrap();
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(
        !body.contains("&quot;Name&quot;"),
        "policy not cleared: {body}"
    );
}

#[test]
fn get_data_protection_policy_unknown_topic_errors() {
    let (svc, _) = make_sns();
    let req = sns_request(
        "GetDataProtectionPolicy",
        vec![("ResourceArn", "arn:aws:sns:us-east-1:123456789012:nope")],
    );
    let err = match svc.get_data_protection_policy(&req) {
        Err(e) => e,
        Ok(_) => panic!("expected err"),
    };
    assert_eq!(err.code(), "NotFound");
}

/// No snapshot store (memory mode) -> no persist hook for the CFN provisioner.
#[test]
fn snapshot_hook_is_none_without_store() {
    let (svc, _state) = make_sns();
    assert!(svc.snapshot_hook().is_none());
}

/// With a store, the hook is present and invoking it runs the whole-state
/// persist path the CloudFormation provisioner uses after mutating SNS
/// state directly.
#[tokio::test]
async fn snapshot_hook_fires_with_store() {
    use std::sync::Arc;
    let (svc, _state) = make_sns();
    let store: Arc<dyn fakecloud_persistence::SnapshotStore> =
        Arc::new(fakecloud_persistence::MemorySnapshotStore::new());
    let svc = svc.with_snapshot_store(store);
    let hook = svc
        .snapshot_hook()
        .expect("hook present when a store is set");
    hook().await;
}
