mod helpers;

use aws_sdk_cognitoidentityprovider::types::{AttributeType, ExplicitAuthFlowsType};
use aws_sdk_dynamodb::types::{
    AttributeDefinition, AttributeValue, KeySchemaElement, KeyType, ScalarAttributeType,
    TimeToLiveSpecification,
};
use aws_sdk_eventbridge::types::PutEventsRequestEntry;
use aws_sdk_sesv2::types::{Body, Content, Destination, EmailContent, Message};
use fakecloud_sdk::FakeCloud;
use helpers::TestServer;

// ── Health ──────────────────────────────────────────────────────────

#[tokio::test]
async fn sdk_health() {
    let server = TestServer::start().await;
    let fc = FakeCloud::new(server.endpoint());

    let resp = fc.health().await.expect("health");
    assert_eq!(resp.status, "ok");
    assert!(!resp.services.is_empty(), "should list services");
    assert!(
        resp.services.contains(&"sqs".to_string()),
        "should contain sqs"
    );
    assert!(
        resp.services.contains(&"kinesis".to_string()),
        "should contain kinesis"
    );
}

// ── Reset (global) ─────────────────────────────────────────────────

#[tokio::test]
async fn sdk_reset_clears_state() {
    let server = TestServer::start().await;
    let fc = FakeCloud::new(server.endpoint());
    let sqs = server.sqs_client().await;

    // Create queue and send a message
    let create = sqs
        .create_queue()
        .queue_name("reset-queue")
        .send()
        .await
        .unwrap();
    let queue_url = create.queue_url().unwrap();

    sqs.send_message()
        .queue_url(queue_url)
        .message_body("before reset")
        .send()
        .await
        .unwrap();

    // Verify message exists via SDK
    let msgs = fc.sqs().get_messages().await.expect("get messages");
    assert!(!msgs.queues.is_empty(), "should have queues before reset");

    // Reset all state
    let reset = fc.reset().await.expect("reset");
    assert_eq!(reset.status, "ok");

    // After reset, SQS introspection should show no queues
    let msgs = fc
        .sqs()
        .get_messages()
        .await
        .expect("get messages after reset");
    assert!(msgs.queues.is_empty(), "queues should be empty after reset");
}

// ── Reset service ──────────────────────────────────────────────────

#[tokio::test]
async fn sdk_reset_service_sqs() {
    let server = TestServer::start().await;
    let fc = FakeCloud::new(server.endpoint());
    let sqs = server.sqs_client().await;

    // Create queue and send a message
    let create = sqs
        .create_queue()
        .queue_name("svc-reset-queue")
        .send()
        .await
        .unwrap();
    let queue_url = create.queue_url().unwrap();

    sqs.send_message()
        .queue_url(queue_url)
        .message_body("before svc reset")
        .send()
        .await
        .unwrap();

    // Reset only SQS
    let resp = fc.reset_service("sqs").await.expect("reset sqs");
    assert_eq!(resp.reset, "sqs");

    // SQS should be empty
    let msgs = fc.sqs().get_messages().await.expect("get messages");
    assert!(
        msgs.queues.is_empty(),
        "SQS should be empty after reset_service"
    );
}

#[tokio::test]
async fn sdk_reset_service_kinesis() {
    let server = TestServer::start().await;
    let fc = FakeCloud::new(server.endpoint());

    let resp = fc.reset_service("kinesis").await.expect("reset kinesis");
    assert_eq!(resp.reset, "kinesis");
}

#[tokio::test]
async fn sdk_rds_get_instances() {
    let server = TestServer::start().await;
    let fc = FakeCloud::new(server.endpoint());
    let rds = server.rds_client().await;

    rds.create_db_instance()
        .db_instance_identifier("sdk-rds-db")
        .allocated_storage(20)
        .db_instance_class("db.t3.micro")
        .engine("postgres")
        .engine_version("16.3")
        .master_username("admin")
        .master_user_password("secret123")
        .db_name("appdb")
        .send()
        .await
        .unwrap();

    let instances = fc.rds().get_instances().await.expect("get rds instances");
    let instance = instances
        .instances
        .iter()
        .find(|instance| instance.db_instance_identifier == "sdk-rds-db")
        .expect("sdk-rds-db instance");
    assert_eq!(instance.engine, "postgres");
    assert_eq!(instance.db_name.as_deref(), Some("appdb"));
    assert!(!instance.container_id.is_empty());
    assert!(instance.host_port > 0);
}

// ── ElastiCache ────────────────────────────────────────────────────

#[tokio::test]
async fn sdk_elasticache_get_replication_groups() {
    let server = TestServer::start().await;
    let fc = FakeCloud::new(server.endpoint());
    let ec = server.elasticache_client().await;

    ec.create_replication_group()
        .replication_group_id("sdk-ec-rg")
        .replication_group_description("SDK test replication group")
        .cache_node_type("cache.t3.micro")
        .engine("redis")
        .engine_version("7.1")
        .num_cache_clusters(2)
        .send()
        .await
        .unwrap();

    let groups = fc
        .elasticache()
        .get_replication_groups()
        .await
        .expect("get replication groups");
    let group = groups
        .replication_groups
        .iter()
        .find(|g| g.replication_group_id == "sdk-ec-rg")
        .expect("sdk-ec-rg replication group");
    assert_eq!(group.engine, "redis");
    assert_eq!(group.num_cache_clusters, 2);
}

#[tokio::test]
async fn sdk_elasticache_get_clusters() {
    let server = TestServer::start().await;
    let fc = FakeCloud::new(server.endpoint());
    let ec = server.elasticache_client().await;

    ec.create_cache_cluster()
        .cache_cluster_id("sdk-ec-cluster")
        .cache_node_type("cache.t3.micro")
        .engine("redis")
        .engine_version("7.1")
        .num_cache_nodes(1)
        .send()
        .await
        .unwrap();

    let clusters = fc
        .elasticache()
        .get_clusters()
        .await
        .expect("get clusters");
    let cluster = clusters
        .clusters
        .iter()
        .find(|c| c.cache_cluster_id == "sdk-ec-cluster")
        .expect("sdk-ec-cluster");
    assert_eq!(cluster.engine, "redis");
    assert_eq!(cluster.num_cache_nodes, 1);
    assert!(cluster.container_id.is_some());
    assert!(cluster.host_port.is_some());
}

#[tokio::test]
async fn sdk_elasticache_get_serverless_caches() {
    let server = TestServer::start().await;
    let fc = FakeCloud::new(server.endpoint());
    let ec = server.elasticache_client().await;

    ec.create_serverless_cache()
        .serverless_cache_name("sdk-ec-serverless")
        .engine("redis")
        .major_engine_version("7.1")
        .send()
        .await
        .unwrap();

    let caches = fc
        .elasticache()
        .get_serverless_caches()
        .await
        .expect("get serverless caches");
    let cache = caches
        .serverless_caches
        .iter()
        .find(|c| c.serverless_cache_name == "sdk-ec-serverless")
        .expect("sdk-ec-serverless");
    assert_eq!(cache.engine, "redis");
    assert_eq!(cache.status, "available");
}

// ── SQS ────────────────────────────────────────────────────────────

#[tokio::test]
async fn sdk_sqs_get_messages() {
    let server = TestServer::start().await;
    let fc = FakeCloud::new(server.endpoint());
    let sqs = server.sqs_client().await;

    let create = sqs
        .create_queue()
        .queue_name("sdk-sqs-queue")
        .send()
        .await
        .unwrap();
    let queue_url = create.queue_url().unwrap();

    sqs.send_message()
        .queue_url(queue_url)
        .message_body("hello from sdk test")
        .send()
        .await
        .unwrap();

    let resp = fc.sqs().get_messages().await.expect("get sqs messages");
    assert_eq!(resp.queues.len(), 1);
    assert_eq!(resp.queues[0].queue_name, "sdk-sqs-queue");
    assert_eq!(resp.queues[0].messages.len(), 1);
    assert_eq!(resp.queues[0].messages[0].body, "hello from sdk test");
}

// ── SNS ────────────────────────────────────────────────────────────

#[tokio::test]
async fn sdk_sns_get_messages() {
    let server = TestServer::start().await;
    let fc = FakeCloud::new(server.endpoint());
    let sns = server.sns_client().await;

    let topic = sns
        .create_topic()
        .name("sdk-sns-topic")
        .send()
        .await
        .unwrap();
    let topic_arn = topic.topic_arn().unwrap();

    sns.publish()
        .topic_arn(topic_arn)
        .subject("Test Subject")
        .message("hello from sns")
        .send()
        .await
        .unwrap();

    let resp = fc.sns().get_messages().await.expect("get sns messages");
    assert!(
        !resp.messages.is_empty(),
        "should have at least one message"
    );

    let msg = resp
        .messages
        .iter()
        .find(|m| m.message == "hello from sns")
        .expect("should find published message");
    assert_eq!(msg.topic_arn, topic_arn);
    assert_eq!(msg.subject.as_deref(), Some("Test Subject"));
}

// ── SES ────────────────────────────────────────────────────────────

#[tokio::test]
async fn sdk_ses_get_emails() {
    let server = TestServer::start().await;
    let fc = FakeCloud::new(server.endpoint());
    let ses = server.sesv2_client().await;

    // Create identity first
    ses.create_email_identity()
        .email_identity("sdk-sender@example.com")
        .send()
        .await
        .unwrap();

    // Send email
    ses.send_email()
        .from_email_address("sdk-sender@example.com")
        .destination(
            Destination::builder()
                .to_addresses("recipient@example.com")
                .build(),
        )
        .content(
            EmailContent::builder()
                .simple(
                    Message::builder()
                        .subject(Content::builder().data("SDK Test").build().unwrap())
                        .body(
                            Body::builder()
                                .text(
                                    Content::builder()
                                        .data("Hello from SDK test")
                                        .build()
                                        .unwrap(),
                                )
                                .build(),
                        )
                        .build(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();

    let resp = fc.ses().get_emails().await.expect("get emails");
    assert!(!resp.emails.is_empty(), "should have at least one email");

    let email = resp
        .emails
        .iter()
        .find(|e| e.subject.as_deref() == Some("SDK Test"))
        .expect("should find sent email");
    assert_eq!(email.from, "sdk-sender@example.com");
    assert!(email.to.contains(&"recipient@example.com".to_string()));
}

// ── S3 ─────────────────────────────────────────────────────────────

#[tokio::test]
async fn sdk_s3_get_notifications() {
    let server = TestServer::start().await;
    let fc = FakeCloud::new(server.endpoint());
    let s3 = server.s3_client().await;

    s3.create_bucket()
        .bucket("sdk-test-bucket")
        .send()
        .await
        .unwrap();

    s3.put_object()
        .bucket("sdk-test-bucket")
        .key("test-key.txt")
        .body(aws_sdk_s3::primitives::ByteStream::from_static(b"data"))
        .send()
        .await
        .unwrap();

    // Notifications endpoint works (may be empty without notification config)
    let resp = fc
        .s3()
        .get_notifications()
        .await
        .expect("get notifications");
    // Just verify the endpoint responds correctly with the expected shape
    let _ = resp.notifications;
}

// ── DynamoDB ───────────────────────────────────────────────────────

#[tokio::test]
async fn sdk_dynamodb_tick_ttl() {
    let server = TestServer::start().await;
    let fc = FakeCloud::new(server.endpoint());
    let ddb = server.dynamodb_client().await;

    // Create table
    ddb.create_table()
        .table_name("sdk-ttl-table")
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("pk")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .billing_mode(aws_sdk_dynamodb::types::BillingMode::PayPerRequest)
        .send()
        .await
        .unwrap();

    // Enable TTL
    ddb.update_time_to_live()
        .table_name("sdk-ttl-table")
        .time_to_live_specification(
            TimeToLiveSpecification::builder()
                .enabled(true)
                .attribute_name("ttl")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    // Put item with expired TTL (timestamp in the past)
    ddb.put_item()
        .table_name("sdk-ttl-table")
        .item("pk", AttributeValue::S("item1".into()))
        .item("ttl", AttributeValue::N("0".into()))
        .send()
        .await
        .unwrap();

    // Tick TTL processor
    let resp = fc.dynamodb().tick_ttl().await.expect("tick ttl");
    assert_eq!(resp.expired_items, 1, "should expire 1 item");

    // Verify item was deleted
    let scan = ddb.scan().table_name("sdk-ttl-table").send().await.unwrap();
    assert_eq!(scan.count(), 0, "table should be empty after TTL tick");
}

// ── Cognito ────────────────────────────────────────────────────────

#[tokio::test]
async fn sdk_cognito_get_confirmation_codes() {
    let server = TestServer::start().await;
    let fc = FakeCloud::new(server.endpoint());
    let cognito = server.cognito_client().await;

    // Create pool
    let pool = cognito
        .create_user_pool()
        .pool_name("sdk-codes-pool")
        .send()
        .await
        .unwrap();
    let pool_id = pool.user_pool().unwrap().id().unwrap().to_string();

    // Create client
    let client_result = cognito
        .create_user_pool_client()
        .user_pool_id(&pool_id)
        .client_name("sdk-client")
        .explicit_auth_flows(ExplicitAuthFlowsType::AllowUserPasswordAuth)
        .explicit_auth_flows(ExplicitAuthFlowsType::AllowRefreshTokenAuth)
        .send()
        .await
        .unwrap();
    let client_id = client_result
        .user_pool_client()
        .unwrap()
        .client_id()
        .unwrap()
        .to_string();

    // Sign up user
    cognito
        .sign_up()
        .client_id(&client_id)
        .username("sdkuser")
        .password("Password1!")
        .user_attributes(
            AttributeType::builder()
                .name("email")
                .value("sdkuser@example.com")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    // Resend confirmation code (sign_up doesn't generate one, resend does)
    cognito
        .resend_confirmation_code()
        .client_id(&client_id)
        .username("sdkuser")
        .send()
        .await
        .unwrap();

    // Get all confirmation codes via SDK
    let codes = fc
        .cognito()
        .get_confirmation_codes()
        .await
        .expect("get confirmation codes");
    assert!(
        !codes.codes.is_empty(),
        "should have at least one confirmation code"
    );

    let code = codes
        .codes
        .iter()
        .find(|c| c.username == "sdkuser")
        .expect("should find code for sdkuser");
    assert_eq!(code.pool_id, pool_id);

    // Get codes for specific user
    let user_codes = fc
        .cognito()
        .get_user_codes(&pool_id, "sdkuser")
        .await
        .expect("get user codes");
    assert!(
        user_codes.confirmation_code.is_some(),
        "should have a confirmation code"
    );
}

// ── EventBridge ────────────────────────────────────────────────────

#[tokio::test]
async fn sdk_eventbridge_get_history() {
    let server = TestServer::start().await;
    let fc = FakeCloud::new(server.endpoint());
    let eb = server.eventbridge_client().await;

    // Put events
    eb.put_events()
        .entries(
            PutEventsRequestEntry::builder()
                .source("sdk.test")
                .detail_type("TestEvent")
                .detail(r#"{"key": "value"}"#)
                .build(),
        )
        .send()
        .await
        .unwrap();

    let resp = fc.events().get_history().await.expect("get history");
    assert!(!resp.events.is_empty(), "should have at least one event");

    let event = resp
        .events
        .iter()
        .find(|e| e.source == "sdk.test")
        .expect("should find sdk.test event");
    assert_eq!(event.detail_type, "TestEvent");
}

// ── SecretsManager ─────────────────────────────────────────────────

#[tokio::test]
async fn sdk_secretsmanager_tick_rotation() {
    let server = TestServer::start().await;
    let fc = FakeCloud::new(server.endpoint());

    // tick_rotation should succeed even with no secrets
    let resp = fc
        .secretsmanager()
        .tick_rotation()
        .await
        .expect("tick rotation");
    // Just verify it returns successfully with expected shape
    let _ = resp.rotated_secrets;
}
