mod helpers;

use aws_sdk_bedrock::types::Tag;
use helpers::TestServer;

#[tokio::test]
async fn bedrock_list_foundation_models() {
    let server = TestServer::start().await;
    let client = server.bedrock_client().await;

    let resp = client.list_foundation_models().send().await.unwrap();
    let models = resp.model_summaries();
    assert!(!models.is_empty(), "should return foundation models");

    // Verify a known model exists
    let claude = models
        .iter()
        .find(|m| m.model_id().contains("anthropic.claude"))
        .expect("should have a Claude model");
    assert_eq!(claude.provider_name(), Some("Anthropic"));
}

#[tokio::test]
async fn bedrock_list_foundation_models_filter_by_provider() {
    let server = TestServer::start().await;
    let client = server.bedrock_client().await;

    let resp = client
        .list_foundation_models()
        .by_provider("Amazon")
        .send()
        .await
        .unwrap();
    let models = resp.model_summaries();
    assert!(!models.is_empty());
    for model in models {
        assert_eq!(model.provider_name(), Some("Amazon"));
    }
}

#[tokio::test]
async fn bedrock_get_foundation_model() {
    let server = TestServer::start().await;
    let client = server.bedrock_client().await;

    let resp = client
        .get_foundation_model()
        .model_identifier("anthropic.claude-3-5-sonnet-20241022-v2:0")
        .send()
        .await
        .unwrap();
    let details = resp.model_details().expect("should have model details");
    assert_eq!(
        details.model_id(),
        "anthropic.claude-3-5-sonnet-20241022-v2:0"
    );
    assert_eq!(details.provider_name(), Some("Anthropic"));
    assert!(details
        .model_arn()
        .contains("foundation-model/anthropic.claude-3-5-sonnet-20241022-v2:0"));
}

#[tokio::test]
async fn bedrock_get_foundation_model_not_found() {
    let server = TestServer::start().await;
    let client = server.bedrock_client().await;

    let err = client
        .get_foundation_model()
        .model_identifier("nonexistent.model-v1")
        .send()
        .await
        .unwrap_err();

    let service_err = err.into_service_error();
    assert!(service_err.is_resource_not_found_exception());
}

#[tokio::test]
async fn bedrock_tag_untag_list_tags() {
    let server = TestServer::start().await;
    let client = server.bedrock_client().await;

    let resource_arn =
        "arn:aws:bedrock:us-east-1::foundation-model/anthropic.claude-3-5-sonnet-20241022-v2:0";

    // Tag the resource
    client
        .tag_resource()
        .resource_arn(resource_arn)
        .tags(Tag::builder().key("env").value("test").build().unwrap())
        .tags(
            Tag::builder()
                .key("team")
                .value("platform")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    // List tags
    let resp = client
        .list_tags_for_resource()
        .resource_arn(resource_arn)
        .send()
        .await
        .unwrap();
    let tags = resp.tags();
    assert_eq!(tags.len(), 2);
    assert!(tags.iter().any(|t| t.key() == "env" && t.value() == "test"));
    assert!(tags
        .iter()
        .any(|t| t.key() == "team" && t.value() == "platform"));

    // Untag one key
    client
        .untag_resource()
        .resource_arn(resource_arn)
        .tag_keys("team")
        .send()
        .await
        .unwrap();

    // Verify only one tag remains
    let resp = client
        .list_tags_for_resource()
        .resource_arn(resource_arn)
        .send()
        .await
        .unwrap();
    let tags = resp.tags();
    assert_eq!(tags.len(), 1);
    assert!(tags.iter().any(|t| t.key() == "env" && t.value() == "test"));
    assert!(!tags.iter().any(|t| t.key() == "team"));
}
