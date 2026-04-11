mod helpers;

use aws_sdk_bedrock::types::Tag;
use fakecloud_conformance_macros::test_action;
use helpers::TestServer;

// ---------------------------------------------------------------------------
// Foundation Models
// ---------------------------------------------------------------------------

#[test_action("bedrock", "ListFoundationModels", checksum = "e6dacdd3")]
#[tokio::test]
async fn bedrock_list_foundation_models() {
    let server = TestServer::start().await;
    let client = server.bedrock_client().await;

    let resp = client.list_foundation_models().send().await.unwrap();
    let models = resp.model_summaries();
    assert!(!models.is_empty());

    let model = &models[0];
    assert!(model.model_id().is_some());
    assert!(model.provider_name().is_some());
}

#[test_action("bedrock", "GetFoundationModel", checksum = "b7a7c9e1")]
#[tokio::test]
async fn bedrock_get_foundation_model() {
    let server = TestServer::start().await;
    let client = server.bedrock_client().await;

    let resp = client
        .get_foundation_model()
        .model_identifier("amazon.titan-text-express-v1")
        .send()
        .await
        .unwrap();
    let details = resp.model_details().unwrap();
    assert_eq!(details.model_id(), Some("amazon.titan-text-express-v1"));
    assert!(details.model_arn().unwrap().contains("foundation-model/"));
}

// ---------------------------------------------------------------------------
// Tags
// ---------------------------------------------------------------------------

#[test_action("bedrock", "TagResource", checksum = "c06b4b31")]
#[test_action("bedrock", "ListTagsForResource", checksum = "8506dfb0")]
#[tokio::test]
async fn bedrock_tag_and_list_tags() {
    let server = TestServer::start().await;
    let client = server.bedrock_client().await;

    let arn =
        "arn:aws:bedrock:us-east-1::foundation-model/anthropic.claude-3-5-sonnet-20241022-v2:0";

    client
        .tag_resource()
        .resource_arn(arn)
        .tags(
            Tag::builder()
                .key("env")
                .value("conformance")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    let resp = client
        .list_tags_for_resource()
        .resource_arn(arn)
        .send()
        .await
        .unwrap();
    let tags = resp.tags();
    assert!(tags
        .iter()
        .any(|t| t.key() == "env" && t.value() == "conformance"));
}

#[test_action("bedrock", "UntagResource", checksum = "57b96caf")]
#[tokio::test]
async fn bedrock_untag_resource() {
    let server = TestServer::start().await;
    let client = server.bedrock_client().await;

    let arn =
        "arn:aws:bedrock:us-east-1::foundation-model/anthropic.claude-3-5-sonnet-20241022-v2:0";

    client
        .tag_resource()
        .resource_arn(arn)
        .tags(Tag::builder().key("k1").value("v1").build().unwrap())
        .tags(Tag::builder().key("k2").value("v2").build().unwrap())
        .send()
        .await
        .unwrap();

    client
        .untag_resource()
        .resource_arn(arn)
        .tag_keys("k1")
        .send()
        .await
        .unwrap();

    let resp = client
        .list_tags_for_resource()
        .resource_arn(arn)
        .send()
        .await
        .unwrap();
    let tags = resp.tags();
    assert!(!tags.iter().any(|t| t.key() == "k1"));
    assert!(tags.iter().any(|t| t.key() == "k2" && t.value() == "v2"));
}
