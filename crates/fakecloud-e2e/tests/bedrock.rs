mod helpers;

use aws_sdk_bedrock::types::{
    GuardrailPiiEntityConfig, GuardrailPiiEntityType, GuardrailSensitiveInformationAction,
    GuardrailSensitiveInformationPolicyConfig, GuardrailWordConfig, GuardrailWordPolicyConfig, Tag,
};
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

// ---------------------------------------------------------------------------
// Guardrails
// ---------------------------------------------------------------------------

#[tokio::test]
async fn bedrock_guardrail_crud() {
    let server = TestServer::start().await;
    let client = server.bedrock_client().await;

    // Create guardrail
    let resp = client
        .create_guardrail()
        .name("test-guardrail")
        .description("A test guardrail")
        .blocked_input_messaging("Input blocked")
        .blocked_outputs_messaging("Output blocked")
        .send()
        .await
        .unwrap();

    let guardrail_id = resp.guardrail_id();
    assert!(!guardrail_id.is_empty());
    assert!(resp.guardrail_arn().contains("guardrail/"));
    assert_eq!(resp.version(), "DRAFT");

    // Get guardrail
    let resp = client
        .get_guardrail()
        .guardrail_identifier(guardrail_id)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.name(), "test-guardrail");
    assert_eq!(resp.description(), Some("A test guardrail"));
    assert_eq!(resp.status().as_str(), "READY");

    // List guardrails
    let resp = client.list_guardrails().send().await.unwrap();
    assert!(resp.guardrails().iter().any(|g| g.id() == guardrail_id));

    // Update guardrail
    let resp = client
        .update_guardrail()
        .guardrail_identifier(guardrail_id)
        .name("updated-guardrail")
        .blocked_input_messaging("Input blocked")
        .blocked_outputs_messaging("Output blocked")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.guardrail_id(), guardrail_id);

    // Verify update
    let resp = client
        .get_guardrail()
        .guardrail_identifier(guardrail_id)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.name(), "updated-guardrail");

    // Delete guardrail
    client
        .delete_guardrail()
        .guardrail_identifier(guardrail_id)
        .send()
        .await
        .unwrap();

    // Verify deleted
    let err = client
        .get_guardrail()
        .guardrail_identifier(guardrail_id)
        .send()
        .await
        .unwrap_err();
    let service_err = err.into_service_error();
    assert!(service_err.is_resource_not_found_exception());
}

#[tokio::test]
async fn bedrock_guardrail_versioning() {
    let server = TestServer::start().await;
    let client = server.bedrock_client().await;

    // Create guardrail
    let resp = client
        .create_guardrail()
        .name("versioned-guardrail")
        .blocked_input_messaging("blocked")
        .blocked_outputs_messaging("blocked")
        .send()
        .await
        .unwrap();
    let guardrail_id = resp.guardrail_id().to_string();

    // Create version 1
    let v1 = client
        .create_guardrail_version()
        .guardrail_identifier(&guardrail_id)
        .send()
        .await
        .unwrap();
    assert_eq!(v1.guardrail_id(), guardrail_id);
    assert_eq!(v1.version(), "1");

    // Create version 2
    let v2 = client
        .create_guardrail_version()
        .guardrail_identifier(&guardrail_id)
        .send()
        .await
        .unwrap();
    assert_eq!(v2.version(), "2");

    // Get specific version
    let resp = client
        .get_guardrail()
        .guardrail_identifier(&guardrail_id)
        .guardrail_version("1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.version(), "1");
}

#[tokio::test]
async fn bedrock_guardrail_with_word_policy() {
    let server = TestServer::start().await;
    let client = server.bedrock_client().await;

    let word_policy = GuardrailWordPolicyConfig::builder()
        .words_config(
            GuardrailWordConfig::builder()
                .text("forbidden")
                .build()
                .unwrap(),
        )
        .build();

    let resp = client
        .create_guardrail()
        .name("word-filter-guardrail")
        .blocked_input_messaging("blocked")
        .blocked_outputs_messaging("blocked")
        .word_policy_config(word_policy)
        .send()
        .await
        .unwrap();

    let guardrail_id = resp.guardrail_id();

    // Verify word policy is stored
    let resp = client
        .get_guardrail()
        .guardrail_identifier(guardrail_id)
        .send()
        .await
        .unwrap();
    assert!(resp.word_policy().is_some());
}

#[tokio::test]
async fn bedrock_guardrail_with_pii_detection() {
    let server = TestServer::start().await;
    let client = server.bedrock_client().await;

    let pii_policy = GuardrailSensitiveInformationPolicyConfig::builder()
        .pii_entities_config(
            GuardrailPiiEntityConfig::builder()
                .r#type(GuardrailPiiEntityType::Email)
                .action(GuardrailSensitiveInformationAction::Block)
                .build()
                .unwrap(),
        )
        .build();

    let resp = client
        .create_guardrail()
        .name("pii-guardrail")
        .blocked_input_messaging("blocked")
        .blocked_outputs_messaging("blocked")
        .sensitive_information_policy_config(pii_policy)
        .send()
        .await
        .unwrap();

    let guardrail_id = resp.guardrail_id();

    let resp = client
        .get_guardrail()
        .guardrail_identifier(guardrail_id)
        .send()
        .await
        .unwrap();
    assert!(resp.sensitive_information_policy().is_some());
}
