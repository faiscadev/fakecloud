mod helpers;
use helpers::TestServer;

#[tokio::test]
async fn test_receipt_rule_set_lifecycle() {
    let server = TestServer::start().await;
    let client = server.ses_client().await;

    // Create a rule set
    client
        .create_receipt_rule_set()
        .rule_set_name("my-rules")
        .send()
        .await
        .expect("create rule set");

    // List rule sets
    let list = client
        .list_receipt_rule_sets()
        .send()
        .await
        .expect("list rule sets");
    let sets = list.rule_sets();
    assert_eq!(sets.len(), 1);
    assert_eq!(sets[0].name(), Some("my-rules"));

    // Describe rule set
    let desc = client
        .describe_receipt_rule_set()
        .rule_set_name("my-rules")
        .send()
        .await
        .expect("describe rule set");
    assert!(desc.metadata().is_some());
    let metadata = desc.metadata().unwrap();
    assert_eq!(metadata.name(), Some("my-rules"));
    assert!(desc.rules().is_empty());

    // Delete rule set
    client
        .delete_receipt_rule_set()
        .rule_set_name("my-rules")
        .send()
        .await
        .expect("delete rule set");

    // Verify deleted
    let list = client
        .list_receipt_rule_sets()
        .send()
        .await
        .expect("list rule sets");
    assert!(list.rule_sets().is_empty());
}

#[tokio::test]
async fn test_receipt_rule_crud() {
    let server = TestServer::start().await;
    let client = server.ses_client().await;

    // Create a rule set
    client
        .create_receipt_rule_set()
        .rule_set_name("test-set")
        .send()
        .await
        .unwrap();

    // Create a rule with S3 action
    use aws_sdk_ses::types::{ReceiptAction, ReceiptRule, S3Action};

    let s3_action = S3Action::builder()
        .bucket_name("my-bucket")
        .object_key_prefix("emails/")
        .build()
        .unwrap();

    let rule = ReceiptRule::builder()
        .name("store-emails")
        .enabled(true)
        .scan_enabled(true)
        .tls_policy(aws_sdk_ses::types::TlsPolicy::Require)
        .recipients("user@example.com")
        .recipients("example.com")
        .actions(ReceiptAction::builder().s3_action(s3_action).build())
        .build()
        .unwrap();

    client
        .create_receipt_rule()
        .rule_set_name("test-set")
        .rule(rule)
        .send()
        .await
        .expect("create receipt rule");

    // Describe the rule
    let desc = client
        .describe_receipt_rule()
        .rule_set_name("test-set")
        .rule_name("store-emails")
        .send()
        .await
        .expect("describe receipt rule");
    let rule = desc.rule().unwrap();
    assert_eq!(rule.name(), "store-emails");
    assert!(rule.enabled());
    assert!(rule.scan_enabled());
    assert_eq!(
        rule.tls_policy(),
        Some(&aws_sdk_ses::types::TlsPolicy::Require)
    );
    assert_eq!(rule.recipients().len(), 2);
    assert_eq!(rule.actions().len(), 1);

    // Describe rule set should show the rule
    let desc = client
        .describe_receipt_rule_set()
        .rule_set_name("test-set")
        .send()
        .await
        .unwrap();
    assert_eq!(desc.rules().len(), 1);

    // Delete the rule
    client
        .delete_receipt_rule()
        .rule_set_name("test-set")
        .rule_name("store-emails")
        .send()
        .await
        .expect("delete receipt rule");

    // Verify deleted
    let desc = client
        .describe_receipt_rule_set()
        .rule_set_name("test-set")
        .send()
        .await
        .unwrap();
    assert!(desc.rules().is_empty());
}

#[tokio::test]
async fn test_set_active_receipt_rule_set() {
    let server = TestServer::start().await;
    let client = server.ses_client().await;

    client
        .create_receipt_rule_set()
        .rule_set_name("active-set")
        .send()
        .await
        .unwrap();

    // Activate
    client
        .set_active_receipt_rule_set()
        .rule_set_name("active-set")
        .send()
        .await
        .expect("set active receipt rule set");

    // Deactivate (no rule set name)
    client
        .set_active_receipt_rule_set()
        .send()
        .await
        .expect("deactivate receipt rule set");
}

#[tokio::test]
async fn test_clone_receipt_rule_set() {
    let server = TestServer::start().await;
    let client = server.ses_client().await;

    // Create source with a rule
    client
        .create_receipt_rule_set()
        .rule_set_name("source")
        .send()
        .await
        .unwrap();

    let rule = aws_sdk_ses::types::ReceiptRule::builder()
        .name("rule1")
        .enabled(true)
        .build()
        .unwrap();
    client
        .create_receipt_rule()
        .rule_set_name("source")
        .rule(rule)
        .send()
        .await
        .unwrap();

    // Clone
    client
        .clone_receipt_rule_set()
        .rule_set_name("cloned")
        .original_rule_set_name("source")
        .send()
        .await
        .expect("clone rule set");

    // Verify clone has the rule
    let desc = client
        .describe_receipt_rule_set()
        .rule_set_name("cloned")
        .send()
        .await
        .unwrap();
    assert_eq!(desc.rules().len(), 1);
    assert_eq!(desc.rules()[0].name(), "rule1");
}

#[tokio::test]
async fn test_reorder_receipt_rule_set() {
    let server = TestServer::start().await;
    let client = server.ses_client().await;

    client
        .create_receipt_rule_set()
        .rule_set_name("order-set")
        .send()
        .await
        .unwrap();

    for name in &["alpha", "beta", "gamma"] {
        let rule = aws_sdk_ses::types::ReceiptRule::builder()
            .name(*name)
            .enabled(true)
            .build()
            .unwrap();
        client
            .create_receipt_rule()
            .rule_set_name("order-set")
            .rule(rule)
            .send()
            .await
            .unwrap();
    }

    // Reorder: gamma, alpha, beta
    client
        .reorder_receipt_rule_set()
        .rule_set_name("order-set")
        .rule_names("gamma")
        .rule_names("alpha")
        .rule_names("beta")
        .send()
        .await
        .expect("reorder rule set");

    let desc = client
        .describe_receipt_rule_set()
        .rule_set_name("order-set")
        .send()
        .await
        .unwrap();
    let names: Vec<&str> = desc.rules().iter().map(|r| r.name()).collect();
    assert_eq!(names, vec!["gamma", "alpha", "beta"]);
}

#[tokio::test]
async fn test_receipt_filter_lifecycle() {
    let server = TestServer::start().await;
    let client = server.ses_client().await;

    use aws_sdk_ses::types::{ReceiptFilter, ReceiptIpFilter};

    let filter = ReceiptFilter::builder()
        .name("allow-internal")
        .ip_filter(
            ReceiptIpFilter::builder()
                .cidr("10.0.0.0/8")
                .policy(aws_sdk_ses::types::ReceiptFilterPolicy::Allow)
                .build()
                .unwrap(),
        )
        .build()
        .unwrap();

    client
        .create_receipt_filter()
        .filter(filter)
        .send()
        .await
        .expect("create receipt filter");

    // List
    let list = client
        .list_receipt_filters()
        .send()
        .await
        .expect("list receipt filters");
    let filters = list.filters();
    assert_eq!(filters.len(), 1);
    assert_eq!(filters[0].name(), "allow-internal");

    // Delete
    client
        .delete_receipt_filter()
        .filter_name("allow-internal")
        .send()
        .await
        .expect("delete receipt filter");

    let list = client.list_receipt_filters().send().await.unwrap();
    assert!(list.filters().is_empty());
}

#[tokio::test]
async fn test_inbound_email_introspection() {
    let server = TestServer::start().await;
    let client = server.ses_client().await;
    let http_client = reqwest::Client::new();

    // Set up rule set with a catch-all rule
    client
        .create_receipt_rule_set()
        .rule_set_name("inbound")
        .send()
        .await
        .unwrap();

    let rule = aws_sdk_ses::types::ReceiptRule::builder()
        .name("catch-all")
        .enabled(true)
        .actions(
            aws_sdk_ses::types::ReceiptAction::builder()
                .s3_action(
                    aws_sdk_ses::types::S3Action::builder()
                        .bucket_name("emails")
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .build()
        .unwrap();
    client
        .create_receipt_rule()
        .rule_set_name("inbound")
        .rule(rule)
        .send()
        .await
        .unwrap();

    client
        .set_active_receipt_rule_set()
        .rule_set_name("inbound")
        .send()
        .await
        .unwrap();

    // POST inbound email
    let resp = http_client
        .post(format!("{}/_fakecloud/ses/inbound", server.endpoint()))
        .json(&serde_json::json!({
            "from": "sender@example.com",
            "to": ["recipient@example.com"],
            "subject": "Test Subject",
            "body": "Hello, world!"
        }))
        .send()
        .await
        .expect("post inbound email");

    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(!body["messageId"].as_str().unwrap().is_empty());
    assert_eq!(body["matchedRules"].as_array().unwrap().len(), 1);
    assert_eq!(body["matchedRules"][0], "catch-all");
    assert_eq!(body["actionsExecuted"].as_array().unwrap().len(), 1);
    assert_eq!(body["actionsExecuted"][0]["actionType"], "S3");
    assert_eq!(body["actionsExecuted"][0]["rule"], "catch-all");
}

#[tokio::test]
async fn test_inbound_email_no_match() {
    let server = TestServer::start().await;
    let client = server.ses_client().await;
    let http_client = reqwest::Client::new();

    // Set up rule set with a domain-specific rule
    client
        .create_receipt_rule_set()
        .rule_set_name("selective")
        .send()
        .await
        .unwrap();

    let rule = aws_sdk_ses::types::ReceiptRule::builder()
        .name("specific-domain")
        .enabled(true)
        .recipients("specific.com")
        .actions(
            aws_sdk_ses::types::ReceiptAction::builder()
                .s3_action(
                    aws_sdk_ses::types::S3Action::builder()
                        .bucket_name("emails")
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .build()
        .unwrap();
    client
        .create_receipt_rule()
        .rule_set_name("selective")
        .rule(rule)
        .send()
        .await
        .unwrap();

    client
        .set_active_receipt_rule_set()
        .rule_set_name("selective")
        .send()
        .await
        .unwrap();

    // POST email to non-matching domain
    let resp = http_client
        .post(format!("{}/_fakecloud/ses/inbound", server.endpoint()))
        .json(&serde_json::json!({
            "from": "sender@example.com",
            "to": ["user@other.com"],
            "subject": "No match",
            "body": "Should not match"
        }))
        .send()
        .await
        .unwrap();

    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(body["matchedRules"].as_array().unwrap().is_empty());
    assert!(body["actionsExecuted"].as_array().unwrap().is_empty());

    // POST email to matching domain
    let resp = http_client
        .post(format!("{}/_fakecloud/ses/inbound", server.endpoint()))
        .json(&serde_json::json!({
            "from": "sender@example.com",
            "to": ["user@specific.com"],
            "subject": "Match",
            "body": "Should match"
        }))
        .send()
        .await
        .unwrap();

    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["matchedRules"][0], "specific-domain");
}

#[tokio::test]
async fn test_update_receipt_rule() {
    let server = TestServer::start().await;
    let client = server.ses_client().await;

    client
        .create_receipt_rule_set()
        .rule_set_name("update-set")
        .send()
        .await
        .unwrap();

    let rule = aws_sdk_ses::types::ReceiptRule::builder()
        .name("myrule")
        .enabled(true)
        .build()
        .unwrap();
    client
        .create_receipt_rule()
        .rule_set_name("update-set")
        .rule(rule)
        .send()
        .await
        .unwrap();

    // Update the rule: disable it and add an SNS action
    let updated_rule = aws_sdk_ses::types::ReceiptRule::builder()
        .name("myrule")
        .enabled(false)
        .actions(
            aws_sdk_ses::types::ReceiptAction::builder()
                .sns_action(
                    aws_sdk_ses::types::SnsAction::builder()
                        .topic_arn("arn:aws:sns:us-east-1:123456789012:my-topic")
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .build()
        .unwrap();
    client
        .update_receipt_rule()
        .rule_set_name("update-set")
        .rule(updated_rule)
        .send()
        .await
        .expect("update receipt rule");

    // Verify the update
    let desc = client
        .describe_receipt_rule()
        .rule_set_name("update-set")
        .rule_name("myrule")
        .send()
        .await
        .unwrap();
    let rule = desc.rule().unwrap();
    assert!(!rule.enabled());
    assert_eq!(rule.actions().len(), 1);
}

#[tokio::test]
async fn test_inbound_email_s3_action_stores_object() {
    let server = TestServer::start().await;
    let ses_client = server.ses_client().await;
    let s3_client = server.s3_client().await;
    let http_client = reqwest::Client::new();

    // Create the S3 bucket first
    s3_client
        .create_bucket()
        .bucket("inbound-emails")
        .send()
        .await
        .expect("create bucket");

    // Set up receipt rule with S3 action
    ses_client
        .create_receipt_rule_set()
        .rule_set_name("s3-test")
        .send()
        .await
        .unwrap();

    let rule = aws_sdk_ses::types::ReceiptRule::builder()
        .name("store-to-s3")
        .enabled(true)
        .actions(
            aws_sdk_ses::types::ReceiptAction::builder()
                .s3_action(
                    aws_sdk_ses::types::S3Action::builder()
                        .bucket_name("inbound-emails")
                        .object_key_prefix("inbox/")
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .build()
        .unwrap();
    ses_client
        .create_receipt_rule()
        .rule_set_name("s3-test")
        .rule(rule)
        .send()
        .await
        .unwrap();

    ses_client
        .set_active_receipt_rule_set()
        .rule_set_name("s3-test")
        .send()
        .await
        .unwrap();

    // Send inbound email
    let resp = http_client
        .post(format!("{}/_fakecloud/ses/inbound", server.endpoint()))
        .json(&serde_json::json!({
            "from": "sender@example.com",
            "to": ["recipient@example.com"],
            "subject": "Test S3 Action",
            "body": "Email body for S3 storage"
        }))
        .send()
        .await
        .expect("post inbound email");

    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    let message_id = body["messageId"].as_str().unwrap();

    // Verify object was stored in S3
    let key = format!("inbox/{message_id}");
    let obj = s3_client
        .get_object()
        .bucket("inbound-emails")
        .key(&key)
        .send()
        .await
        .expect("get object from S3");

    let data = obj.body.collect().await.unwrap().into_bytes();
    assert_eq!(
        std::str::from_utf8(&data).unwrap(),
        "Email body for S3 storage"
    );
}

#[tokio::test]
async fn test_inbound_email_sns_action_publishes_message() {
    let server = TestServer::start().await;
    let ses_client = server.ses_client().await;
    let sns_client = server.sns_client().await;
    let sqs_client = server.sqs_client().await;
    let http_client = reqwest::Client::new();

    // Create SNS topic and SQS queue, subscribe queue to topic
    let topic = sns_client
        .create_topic()
        .name("ses-notifications")
        .send()
        .await
        .expect("create topic");
    let topic_arn = topic.topic_arn().unwrap();

    let queue = sqs_client
        .create_queue()
        .queue_name("ses-queue")
        .send()
        .await
        .expect("create queue");
    let queue_url = queue.queue_url().unwrap();

    // Get queue ARN
    let attrs = sqs_client
        .get_queue_attributes()
        .queue_url(queue_url)
        .attribute_names(aws_sdk_sqs::types::QueueAttributeName::QueueArn)
        .send()
        .await
        .unwrap();
    let queue_arn = attrs
        .attributes()
        .unwrap()
        .get(&aws_sdk_sqs::types::QueueAttributeName::QueueArn)
        .unwrap();

    sns_client
        .subscribe()
        .topic_arn(topic_arn)
        .protocol("sqs")
        .endpoint(queue_arn)
        .send()
        .await
        .expect("subscribe");

    // Set up receipt rule with SNS action
    ses_client
        .create_receipt_rule_set()
        .rule_set_name("sns-test")
        .send()
        .await
        .unwrap();

    let rule = aws_sdk_ses::types::ReceiptRule::builder()
        .name("notify-sns")
        .enabled(true)
        .actions(
            aws_sdk_ses::types::ReceiptAction::builder()
                .sns_action(
                    aws_sdk_ses::types::SnsAction::builder()
                        .topic_arn(topic_arn)
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .build()
        .unwrap();
    ses_client
        .create_receipt_rule()
        .rule_set_name("sns-test")
        .rule(rule)
        .send()
        .await
        .unwrap();

    ses_client
        .set_active_receipt_rule_set()
        .rule_set_name("sns-test")
        .send()
        .await
        .unwrap();

    // First verify direct SNS->SQS fanout works
    sns_client
        .publish()
        .topic_arn(topic_arn)
        .message("direct-test")
        .send()
        .await
        .expect("direct publish");
    let direct = sqs_client
        .receive_message()
        .queue_url(queue_url)
        .max_number_of_messages(1)
        .send()
        .await
        .unwrap();
    assert_eq!(
        direct.messages().len(),
        1,
        "direct SNS->SQS fanout should work"
    );

    // Purge queue for the real test
    sqs_client
        .purge_queue()
        .queue_url(queue_url)
        .send()
        .await
        .unwrap();

    // Send inbound email
    let resp = http_client
        .post(format!("{}/_fakecloud/ses/inbound", server.endpoint()))
        .json(&serde_json::json!({
            "from": "sender@example.com",
            "to": ["recipient@example.com"],
            "subject": "Test SNS Action",
            "body": "Email body for SNS"
        }))
        .send()
        .await
        .expect("post inbound email");

    assert_eq!(resp.status(), 200);
    let inbound_body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(
        inbound_body["actionsExecuted"][0]["actionType"], "SNS",
        "SNS action should be in executed list"
    );

    // Check that the SNS message was published via introspection
    let sns_msgs_resp = http_client
        .get(format!("{}/_fakecloud/sns/messages", server.endpoint()))
        .send()
        .await
        .unwrap();
    let sns_msgs: serde_json::Value = sns_msgs_resp.json().await.unwrap();
    let msg_count = sns_msgs["messages"]
        .as_array()
        .map(|a| a.len())
        .unwrap_or(0);
    // Should be 2: 1 from direct publish + 1 from inbound
    assert!(
        msg_count >= 2,
        "expected at least 2 SNS messages (direct + inbound), got {msg_count}"
    );

    // Check that a message was delivered to SQS via SNS
    let messages = sqs_client
        .receive_message()
        .queue_url(queue_url)
        .max_number_of_messages(1)
        .send()
        .await
        .expect("receive message");

    let msgs = messages.messages();
    assert_eq!(msgs.len(), 1, "expected 1 message in SQS from SNS fanout");
    let msg_body = msgs[0].body().unwrap();
    // The message body is an SNS notification envelope containing the SES notification
    let sns_envelope: serde_json::Value = serde_json::from_str(msg_body).unwrap();
    let ses_notification: serde_json::Value =
        serde_json::from_str(sns_envelope["Message"].as_str().unwrap()).unwrap();
    assert_eq!(ses_notification["notificationType"], "Received");
    assert_eq!(ses_notification["mail"]["source"], "sender@example.com");
}
