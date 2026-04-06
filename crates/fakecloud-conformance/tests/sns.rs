mod helpers;

use aws_sdk_sns::types::{PublishBatchRequestEntry, Tag};
use fakecloud_conformance_macros::test_action;
use helpers::TestServer;

// ---------------------------------------------------------------------------
// Topic lifecycle
// ---------------------------------------------------------------------------

#[test_action("sns", "CreateTopic", checksum = "134daf7a")]
#[tokio::test]
async fn sns_create_topic() {
    let server = TestServer::start().await;
    let client = server.sns_client().await;

    let resp = client.create_topic().name("ct-topic").send().await.unwrap();
    assert!(resp.topic_arn().unwrap().contains("ct-topic"));
}

#[test_action("sns", "ListTopics", checksum = "3231aa30")]
#[tokio::test]
async fn sns_list_topics() {
    let server = TestServer::start().await;
    let client = server.sns_client().await;

    client.create_topic().name("lt-topic").send().await.unwrap();
    let resp = client.list_topics().send().await.unwrap();
    assert!(!resp.topics().is_empty());
}

#[test_action("sns", "DeleteTopic", checksum = "4e5aad6b")]
#[tokio::test]
async fn sns_delete_topic() {
    let server = TestServer::start().await;
    let client = server.sns_client().await;

    let topic = client.create_topic().name("dt-topic").send().await.unwrap();
    let arn = topic.topic_arn().unwrap();

    client.delete_topic().topic_arn(arn).send().await.unwrap();

    let resp = client.list_topics().send().await.unwrap();
    assert!(resp.topics().is_empty());
}

#[test_action("sns", "GetTopicAttributes", checksum = "31909d65")]
#[tokio::test]
async fn sns_get_topic_attributes() {
    let server = TestServer::start().await;
    let client = server.sns_client().await;

    let topic = client
        .create_topic()
        .name("gta-topic")
        .send()
        .await
        .unwrap();
    let arn = topic.topic_arn().unwrap();

    let resp = client
        .get_topic_attributes()
        .topic_arn(arn)
        .send()
        .await
        .unwrap();
    let attrs = resp.attributes().unwrap();
    assert_eq!(attrs.get("TopicArn").unwrap(), arn);
}

#[test_action("sns", "SetTopicAttributes", checksum = "5d93e648")]
#[tokio::test]
async fn sns_set_topic_attributes() {
    let server = TestServer::start().await;
    let client = server.sns_client().await;

    let topic = client
        .create_topic()
        .name("sta-topic")
        .send()
        .await
        .unwrap();
    let arn = topic.topic_arn().unwrap();

    client
        .set_topic_attributes()
        .topic_arn(arn)
        .attribute_name("DisplayName")
        .attribute_value("My Topic")
        .send()
        .await
        .unwrap();

    let resp = client
        .get_topic_attributes()
        .topic_arn(arn)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.attributes().unwrap().get("DisplayName").unwrap(),
        "My Topic"
    );
}

// ---------------------------------------------------------------------------
// Subscriptions
// ---------------------------------------------------------------------------

#[test_action("sns", "Subscribe", checksum = "523a0c34")]
#[tokio::test]
async fn sns_subscribe() {
    let server = TestServer::start().await;
    let client = server.sns_client().await;

    let topic = client
        .create_topic()
        .name("sub-topic")
        .send()
        .await
        .unwrap();
    let arn = topic.topic_arn().unwrap();

    let resp = client
        .subscribe()
        .topic_arn(arn)
        .protocol("sqs")
        .endpoint("arn:aws:sqs:us-east-1:123456789012:q")
        .send()
        .await
        .unwrap();
    assert!(resp.subscription_arn().is_some());
}

#[test_action("sns", "ConfirmSubscription", checksum = "9096ab08")]
#[tokio::test]
async fn sns_confirm_subscription() {
    let server = TestServer::start().await;
    let client = server.sns_client().await;

    let topic = client
        .create_topic()
        .name("confirm-topic")
        .send()
        .await
        .unwrap();
    let arn = topic.topic_arn().unwrap();

    let resp = client
        .confirm_subscription()
        .topic_arn(arn)
        .token("fake-token")
        .send()
        .await
        .unwrap();
    assert!(resp.subscription_arn().is_some());
}

#[test_action("sns", "Unsubscribe", checksum = "59a84ca0")]
#[tokio::test]
async fn sns_unsubscribe() {
    let server = TestServer::start().await;
    let client = server.sns_client().await;

    let topic = client
        .create_topic()
        .name("unsub-topic")
        .send()
        .await
        .unwrap();
    let arn = topic.topic_arn().unwrap();

    let sub = client
        .subscribe()
        .topic_arn(arn)
        .protocol("sqs")
        .endpoint("arn:aws:sqs:us-east-1:123456789012:q")
        .send()
        .await
        .unwrap();
    let sub_arn = sub.subscription_arn().unwrap();

    client
        .unsubscribe()
        .subscription_arn(sub_arn)
        .send()
        .await
        .unwrap();

    let resp = client.list_subscriptions().send().await.unwrap();
    assert!(resp.subscriptions().is_empty());
}

#[test_action("sns", "ListSubscriptions", checksum = "7d8ed2d6")]
#[tokio::test]
async fn sns_list_subscriptions() {
    let server = TestServer::start().await;
    let client = server.sns_client().await;

    let topic = client.create_topic().name("ls-topic").send().await.unwrap();
    let arn = topic.topic_arn().unwrap();

    client
        .subscribe()
        .topic_arn(arn)
        .protocol("sqs")
        .endpoint("arn:aws:sqs:us-east-1:123456789012:q")
        .send()
        .await
        .unwrap();

    let resp = client.list_subscriptions().send().await.unwrap();
    assert_eq!(resp.subscriptions().len(), 1);
}

#[test_action("sns", "ListSubscriptionsByTopic", checksum = "e2bbd283")]
#[tokio::test]
async fn sns_list_subscriptions_by_topic() {
    let server = TestServer::start().await;
    let client = server.sns_client().await;

    let topic = client
        .create_topic()
        .name("lsbt-topic")
        .send()
        .await
        .unwrap();
    let arn = topic.topic_arn().unwrap();

    client
        .subscribe()
        .topic_arn(arn)
        .protocol("sqs")
        .endpoint("arn:aws:sqs:us-east-1:123456789012:q")
        .send()
        .await
        .unwrap();

    let resp = client
        .list_subscriptions_by_topic()
        .topic_arn(arn)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.subscriptions().len(), 1);
}

#[test_action("sns", "GetSubscriptionAttributes", checksum = "2022e312")]
#[tokio::test]
async fn sns_get_subscription_attributes() {
    let server = TestServer::start().await;
    let client = server.sns_client().await;

    let topic = client
        .create_topic()
        .name("gsa-topic")
        .send()
        .await
        .unwrap();
    let arn = topic.topic_arn().unwrap();

    let sub = client
        .subscribe()
        .topic_arn(arn)
        .protocol("sqs")
        .endpoint("arn:aws:sqs:us-east-1:123456789012:q")
        .send()
        .await
        .unwrap();
    let sub_arn = sub.subscription_arn().unwrap();

    let resp = client
        .get_subscription_attributes()
        .subscription_arn(sub_arn)
        .send()
        .await
        .unwrap();
    let attrs = resp.attributes().unwrap();
    assert_eq!(attrs.get("Protocol").unwrap(), "sqs");
}

#[test_action("sns", "SetSubscriptionAttributes", checksum = "d02509bc")]
#[tokio::test]
async fn sns_set_subscription_attributes() {
    let server = TestServer::start().await;
    let client = server.sns_client().await;

    let topic = client
        .create_topic()
        .name("ssa-topic")
        .send()
        .await
        .unwrap();
    let arn = topic.topic_arn().unwrap();

    let sub = client
        .subscribe()
        .topic_arn(arn)
        .protocol("sqs")
        .endpoint("arn:aws:sqs:us-east-1:123456789012:q")
        .send()
        .await
        .unwrap();
    let sub_arn = sub.subscription_arn().unwrap();

    client
        .set_subscription_attributes()
        .subscription_arn(sub_arn)
        .attribute_name("RawMessageDelivery")
        .attribute_value("true")
        .send()
        .await
        .unwrap();

    let resp = client
        .get_subscription_attributes()
        .subscription_arn(sub_arn)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.attributes()
            .unwrap()
            .get("RawMessageDelivery")
            .unwrap(),
        "true"
    );
}

// ---------------------------------------------------------------------------
// Publish
// ---------------------------------------------------------------------------

#[test_action("sns", "Publish", checksum = "bccbca72")]
#[tokio::test]
async fn sns_publish() {
    let server = TestServer::start().await;
    let client = server.sns_client().await;

    let topic = client
        .create_topic()
        .name("pub-topic")
        .send()
        .await
        .unwrap();
    let arn = topic.topic_arn().unwrap();

    let resp = client
        .publish()
        .topic_arn(arn)
        .message("hello")
        .send()
        .await
        .unwrap();
    assert!(resp.message_id().is_some());
}

#[test_action("sns", "PublishBatch", checksum = "e0faf03a")]
#[tokio::test]
async fn sns_publish_batch() {
    let server = TestServer::start().await;
    let client = server.sns_client().await;

    let topic = client.create_topic().name("pb-topic").send().await.unwrap();
    let arn = topic.topic_arn().unwrap();

    let entries = vec![
        PublishBatchRequestEntry::builder()
            .id("1")
            .message("msg1")
            .build()
            .unwrap(),
        PublishBatchRequestEntry::builder()
            .id("2")
            .message("msg2")
            .build()
            .unwrap(),
    ];

    let resp = client
        .publish_batch()
        .topic_arn(arn)
        .set_publish_batch_request_entries(Some(entries))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.successful().len(), 2);
    assert!(resp.failed().is_empty());
}

// ---------------------------------------------------------------------------
// Tags
// ---------------------------------------------------------------------------

#[test_action("sns", "TagResource", checksum = "7c7539fe")]
#[tokio::test]
async fn sns_tag_resource() {
    let server = TestServer::start().await;
    let client = server.sns_client().await;

    let topic = client
        .create_topic()
        .name("tag-topic")
        .send()
        .await
        .unwrap();
    let arn = topic.topic_arn().unwrap();

    client
        .tag_resource()
        .resource_arn(arn)
        .tags(Tag::builder().key("env").value("test").build().unwrap())
        .send()
        .await
        .unwrap();

    let tags = client
        .list_tags_for_resource()
        .resource_arn(arn)
        .send()
        .await
        .unwrap();
    assert_eq!(tags.tags().len(), 1);
    assert_eq!(tags.tags()[0].key(), "env");
}

#[test_action("sns", "UntagResource", checksum = "0f91c85a")]
#[tokio::test]
async fn sns_untag_resource() {
    let server = TestServer::start().await;
    let client = server.sns_client().await;

    let topic = client
        .create_topic()
        .name("untag-topic")
        .send()
        .await
        .unwrap();
    let arn = topic.topic_arn().unwrap();

    client
        .tag_resource()
        .resource_arn(arn)
        .tags(Tag::builder().key("env").value("test").build().unwrap())
        .send()
        .await
        .unwrap();

    client
        .untag_resource()
        .resource_arn(arn)
        .tag_keys("env")
        .send()
        .await
        .unwrap();

    let tags = client
        .list_tags_for_resource()
        .resource_arn(arn)
        .send()
        .await
        .unwrap();
    assert!(tags.tags().is_empty());
}

#[test_action("sns", "ListTagsForResource", checksum = "6281ac11")]
#[tokio::test]
async fn sns_list_tags_for_resource() {
    let server = TestServer::start().await;
    let client = server.sns_client().await;

    let topic = client
        .create_topic()
        .name("listtags-topic")
        .send()
        .await
        .unwrap();
    let arn = topic.topic_arn().unwrap();

    let tags = client
        .list_tags_for_resource()
        .resource_arn(arn)
        .send()
        .await
        .unwrap();
    assert!(tags.tags().is_empty());
}

// ---------------------------------------------------------------------------
// Permissions
// ---------------------------------------------------------------------------

#[test_action("sns", "AddPermission", checksum = "6c110218")]
#[tokio::test]
async fn sns_add_permission() {
    let server = TestServer::start().await;
    let client = server.sns_client().await;

    let topic = client
        .create_topic()
        .name("perm-topic")
        .send()
        .await
        .unwrap();
    let arn = topic.topic_arn().unwrap();

    client
        .add_permission()
        .topic_arn(arn)
        .label("my-permission")
        .aws_account_id("123456789012")
        .action_name("Publish")
        .send()
        .await
        .unwrap();
}

#[test_action("sns", "RemovePermission", checksum = "497a29ab")]
#[tokio::test]
async fn sns_remove_permission() {
    let server = TestServer::start().await;
    let client = server.sns_client().await;

    let topic = client
        .create_topic()
        .name("rmperm-topic")
        .send()
        .await
        .unwrap();
    let arn = topic.topic_arn().unwrap();

    client
        .add_permission()
        .topic_arn(arn)
        .label("my-perm")
        .aws_account_id("123456789012")
        .action_name("Publish")
        .send()
        .await
        .unwrap();

    client
        .remove_permission()
        .topic_arn(arn)
        .label("my-perm")
        .send()
        .await
        .unwrap();
}

// ---------------------------------------------------------------------------
// Platform applications and endpoints
// ---------------------------------------------------------------------------

#[test_action("sns", "CreatePlatformApplication", checksum = "d20a7824")]
#[tokio::test]
async fn sns_create_platform_application() {
    let server = TestServer::start().await;
    let client = server.sns_client().await;

    let resp = client
        .create_platform_application()
        .name("my-app")
        .platform("GCM")
        .attributes("PlatformCredential", "fake-key")
        .send()
        .await
        .unwrap();
    assert!(resp.platform_application_arn().is_some());
}

#[test_action("sns", "ListPlatformApplications", checksum = "66b7868d")]
#[tokio::test]
async fn sns_list_platform_applications() {
    let server = TestServer::start().await;
    let client = server.sns_client().await;

    client
        .create_platform_application()
        .name("list-app")
        .platform("GCM")
        .attributes("PlatformCredential", "fake-key")
        .send()
        .await
        .unwrap();

    let resp = client.list_platform_applications().send().await.unwrap();
    assert_eq!(resp.platform_applications().len(), 1);
}

#[test_action("sns", "GetPlatformApplicationAttributes", checksum = "056c95b3")]
#[tokio::test]
async fn sns_get_platform_application_attributes() {
    let server = TestServer::start().await;
    let client = server.sns_client().await;

    let app = client
        .create_platform_application()
        .name("get-app")
        .platform("GCM")
        .attributes("PlatformCredential", "fake-key")
        .send()
        .await
        .unwrap();
    let app_arn = app.platform_application_arn().unwrap();

    let resp = client
        .get_platform_application_attributes()
        .platform_application_arn(app_arn)
        .send()
        .await
        .unwrap();
    assert!(resp.attributes().is_some());
}

#[test_action("sns", "SetPlatformApplicationAttributes", checksum = "7c224318")]
#[tokio::test]
async fn sns_set_platform_application_attributes() {
    let server = TestServer::start().await;
    let client = server.sns_client().await;

    let app = client
        .create_platform_application()
        .name("set-app")
        .platform("GCM")
        .attributes("PlatformCredential", "fake-key")
        .send()
        .await
        .unwrap();
    let app_arn = app.platform_application_arn().unwrap();

    client
        .set_platform_application_attributes()
        .platform_application_arn(app_arn)
        .attributes("PlatformCredential", "new-key")
        .send()
        .await
        .unwrap();
}

#[test_action("sns", "DeletePlatformApplication", checksum = "d87bbec2")]
#[tokio::test]
async fn sns_delete_platform_application() {
    let server = TestServer::start().await;
    let client = server.sns_client().await;

    let app = client
        .create_platform_application()
        .name("del-app")
        .platform("GCM")
        .attributes("PlatformCredential", "fake-key")
        .send()
        .await
        .unwrap();
    let app_arn = app.platform_application_arn().unwrap();

    client
        .delete_platform_application()
        .platform_application_arn(app_arn)
        .send()
        .await
        .unwrap();

    let resp = client.list_platform_applications().send().await.unwrap();
    assert!(resp.platform_applications().is_empty());
}

#[test_action("sns", "CreatePlatformEndpoint", checksum = "796d9493")]
#[tokio::test]
async fn sns_create_platform_endpoint() {
    let server = TestServer::start().await;
    let client = server.sns_client().await;

    let app = client
        .create_platform_application()
        .name("ep-app")
        .platform("GCM")
        .attributes("PlatformCredential", "fake-key")
        .send()
        .await
        .unwrap();
    let app_arn = app.platform_application_arn().unwrap();

    let resp = client
        .create_platform_endpoint()
        .platform_application_arn(app_arn)
        .token("device-token-abc")
        .send()
        .await
        .unwrap();
    assert!(resp.endpoint_arn().is_some());
}

#[test_action("sns", "ListEndpointsByPlatformApplication", checksum = "d8223b27")]
#[tokio::test]
async fn sns_list_endpoints_by_platform_application() {
    let server = TestServer::start().await;
    let client = server.sns_client().await;

    let app = client
        .create_platform_application()
        .name("lep-app")
        .platform("GCM")
        .attributes("PlatformCredential", "fake-key")
        .send()
        .await
        .unwrap();
    let app_arn = app.platform_application_arn().unwrap();

    client
        .create_platform_endpoint()
        .platform_application_arn(app_arn)
        .token("device-token")
        .send()
        .await
        .unwrap();

    let resp = client
        .list_endpoints_by_platform_application()
        .platform_application_arn(app_arn)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.endpoints().len(), 1);
}

#[test_action("sns", "GetEndpointAttributes", checksum = "c379158b")]
#[tokio::test]
async fn sns_get_endpoint_attributes() {
    let server = TestServer::start().await;
    let client = server.sns_client().await;

    let app = client
        .create_platform_application()
        .name("gea-app")
        .platform("GCM")
        .attributes("PlatformCredential", "fake-key")
        .send()
        .await
        .unwrap();
    let app_arn = app.platform_application_arn().unwrap();

    let ep = client
        .create_platform_endpoint()
        .platform_application_arn(app_arn)
        .token("device-token")
        .send()
        .await
        .unwrap();
    let ep_arn = ep.endpoint_arn().unwrap();

    let resp = client
        .get_endpoint_attributes()
        .endpoint_arn(ep_arn)
        .send()
        .await
        .unwrap();
    assert!(resp.attributes().is_some());
}

#[test_action("sns", "SetEndpointAttributes", checksum = "d9119f0f")]
#[tokio::test]
async fn sns_set_endpoint_attributes() {
    let server = TestServer::start().await;
    let client = server.sns_client().await;

    let app = client
        .create_platform_application()
        .name("sea-app")
        .platform("GCM")
        .attributes("PlatformCredential", "fake-key")
        .send()
        .await
        .unwrap();
    let app_arn = app.platform_application_arn().unwrap();

    let ep = client
        .create_platform_endpoint()
        .platform_application_arn(app_arn)
        .token("device-token")
        .send()
        .await
        .unwrap();
    let ep_arn = ep.endpoint_arn().unwrap();

    client
        .set_endpoint_attributes()
        .endpoint_arn(ep_arn)
        .attributes("Enabled", "false")
        .send()
        .await
        .unwrap();
}

#[test_action("sns", "DeleteEndpoint", checksum = "1a351918")]
#[tokio::test]
async fn sns_delete_endpoint() {
    let server = TestServer::start().await;
    let client = server.sns_client().await;

    let app = client
        .create_platform_application()
        .name("dep-app")
        .platform("GCM")
        .attributes("PlatformCredential", "fake-key")
        .send()
        .await
        .unwrap();
    let app_arn = app.platform_application_arn().unwrap();

    let ep = client
        .create_platform_endpoint()
        .platform_application_arn(app_arn)
        .token("device-token")
        .send()
        .await
        .unwrap();
    let ep_arn = ep.endpoint_arn().unwrap();

    client
        .delete_endpoint()
        .endpoint_arn(ep_arn)
        .send()
        .await
        .unwrap();

    let resp = client
        .list_endpoints_by_platform_application()
        .platform_application_arn(app_arn)
        .send()
        .await
        .unwrap();
    assert!(resp.endpoints().is_empty());
}

// ---------------------------------------------------------------------------
// SMS
// ---------------------------------------------------------------------------

#[test_action("sns", "SetSMSAttributes", checksum = "4d87387c")]
#[tokio::test]
async fn sns_set_sms_attributes() {
    let server = TestServer::start().await;
    let client = server.sns_client().await;

    client
        .set_sms_attributes()
        .attributes("DefaultSMSType", "Transactional")
        .send()
        .await
        .unwrap();
}

#[test_action("sns", "GetSMSAttributes", checksum = "ea9059f6")]
#[tokio::test]
async fn sns_get_sms_attributes() {
    let server = TestServer::start().await;
    let client = server.sns_client().await;

    let resp = client.get_sms_attributes().send().await.unwrap();
    // Should return attributes (possibly empty map)
    let _ = resp.attributes();
}

#[test_action("sns", "CheckIfPhoneNumberIsOptedOut", checksum = "1791f458")]
#[tokio::test]
async fn sns_check_if_phone_number_is_opted_out() {
    let server = TestServer::start().await;
    let client = server.sns_client().await;

    let resp = client
        .check_if_phone_number_is_opted_out()
        .phone_number("+15551234567")
        .send()
        .await
        .unwrap();
    // Default: not opted out
    assert!(!resp.is_opted_out());
}

#[test_action("sns", "ListPhoneNumbersOptedOut", checksum = "13088dd7")]
#[tokio::test]
async fn sns_list_phone_numbers_opted_out() {
    let server = TestServer::start().await;
    let client = server.sns_client().await;

    let resp = client.list_phone_numbers_opted_out().send().await.unwrap();
    assert!(resp.phone_numbers().is_empty());
}

#[test_action("sns", "OptInPhoneNumber", checksum = "794da5bf")]
#[tokio::test]
async fn sns_opt_in_phone_number() {
    let server = TestServer::start().await;
    let client = server.sns_client().await;

    client
        .opt_in_phone_number()
        .phone_number("+15551234567")
        .send()
        .await
        .unwrap();
}
