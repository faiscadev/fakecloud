mod helpers;

use aws_sdk_bedrock::types::{
    GuardrailPiiEntityConfig, GuardrailPiiEntityType, GuardrailSensitiveInformationAction,
    GuardrailSensitiveInformationPolicyConfig, GuardrailWordConfig, GuardrailWordPolicyConfig, Tag,
};
use aws_sdk_bedrockruntime::primitives::Blob;
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

// ---------------------------------------------------------------------------
// Model Customization Jobs
// ---------------------------------------------------------------------------

#[tokio::test]
async fn bedrock_model_customization_job_lifecycle() {
    let server = TestServer::start().await;
    let client = server.bedrock_client().await;

    // Create job
    let resp = client
        .create_model_customization_job()
        .job_name("test-job")
        .custom_model_name("my-custom-model")
        .base_model_identifier("amazon.titan-text-express-v1")
        .role_arn("arn:aws:iam::123456789012:role/test-role")
        .training_data_config(
            aws_sdk_bedrock::types::TrainingDataConfig::builder()
                .s3_uri("s3://my-bucket/training-data/")
                .build(),
        )
        .output_data_config(
            aws_sdk_bedrock::types::OutputDataConfig::builder()
                .s3_uri("s3://my-bucket/output/")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    let job_arn = resp.job_arn();
    assert!(job_arn.contains("model-customization-job/"));

    // Get job
    let resp = client
        .get_model_customization_job()
        .job_identifier(job_arn)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.job_name(), "test-job");

    // List jobs
    let resp = client.list_model_customization_jobs().send().await.unwrap();
    assert!(!resp.model_customization_job_summaries().is_empty());

    // Stop job
    client
        .stop_model_customization_job()
        .job_identifier(job_arn)
        .send()
        .await
        .unwrap();

    // Verify stopped
    let resp = client
        .get_model_customization_job()
        .job_identifier(job_arn)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        Some(&aws_sdk_bedrock::types::ModelCustomizationJobStatus::Stopped)
    );
}

// ---------------------------------------------------------------------------
// Provisioned Model Throughput
// ---------------------------------------------------------------------------

#[tokio::test]
async fn bedrock_provisioned_throughput_crud() {
    let server = TestServer::start().await;
    let client = server.bedrock_client().await;

    // Create
    let resp = client
        .create_provisioned_model_throughput()
        .provisioned_model_name("my-provisioned")
        .model_id("anthropic.claude-3-5-sonnet-20241022-v2:0")
        .model_units(1)
        .send()
        .await
        .unwrap();
    let arn = resp.provisioned_model_arn();
    assert!(arn.contains("provisioned-model/"));

    // Get
    let resp = client
        .get_provisioned_model_throughput()
        .provisioned_model_id(arn)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.provisioned_model_name(), "my-provisioned");
    assert_eq!(resp.model_units(), 1);

    // List
    let resp = client
        .list_provisioned_model_throughputs()
        .send()
        .await
        .unwrap();
    assert!(!resp.provisioned_model_summaries().is_empty());

    // Update name
    client
        .update_provisioned_model_throughput()
        .provisioned_model_id(arn)
        .desired_provisioned_model_name("renamed-provisioned")
        .send()
        .await
        .unwrap();

    // Delete
    client
        .delete_provisioned_model_throughput()
        .provisioned_model_id(arn)
        .send()
        .await
        .unwrap();

    // Verify deleted
    let err = client
        .get_provisioned_model_throughput()
        .provisioned_model_id(arn)
        .send()
        .await
        .unwrap_err();
    let service_err = err.into_service_error();
    assert!(service_err.is_resource_not_found_exception());
}

// ---------------------------------------------------------------------------
// Model Invocation Logging
// ---------------------------------------------------------------------------

#[tokio::test]
async fn bedrock_logging_configuration() {
    let server = TestServer::start().await;
    let client = server.bedrock_client().await;

    // Put logging config
    client
        .put_model_invocation_logging_configuration()
        .logging_config(
            aws_sdk_bedrock::types::LoggingConfig::builder()
                .text_data_delivery_enabled(true)
                .image_data_delivery_enabled(false)
                .embedding_data_delivery_enabled(true)
                .s3_config(
                    aws_sdk_bedrock::types::S3Config::builder()
                        .bucket_name("my-logging-bucket")
                        .key_prefix("bedrock-logs/")
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();

    // Get logging config
    let resp = client
        .get_model_invocation_logging_configuration()
        .send()
        .await
        .unwrap();
    let config = resp.logging_config().expect("should have logging config");
    assert_eq!(config.text_data_delivery_enabled(), Some(true));
    assert_eq!(config.image_data_delivery_enabled(), Some(false));
    assert!(config.s3_config().is_some());

    // Delete logging config
    client
        .delete_model_invocation_logging_configuration()
        .send()
        .await
        .unwrap();

    // Verify deleted
    let resp = client
        .get_model_invocation_logging_configuration()
        .send()
        .await
        .unwrap();
    assert!(resp.logging_config().is_none());
}

// ---------------------------------------------------------------------------
// InvokeModel (Runtime)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn bedrock_invoke_model_anthropic() {
    let server = TestServer::start().await;
    let client = server.bedrock_runtime_client().await;

    let body = serde_json::to_vec(&serde_json::json!({
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 100,
        "messages": [{"role": "user", "content": "Hello"}]
    }))
    .unwrap();

    let resp = client
        .invoke_model()
        .model_id("anthropic.claude-3-5-sonnet-20241022-v2:0")
        .content_type("application/json")
        .accept("application/json")
        .body(Blob::new(body))
        .send()
        .await
        .unwrap();

    let response_body: serde_json::Value = serde_json::from_slice(resp.body().as_ref()).unwrap();
    assert_eq!(response_body["type"], "message");
    assert_eq!(response_body["stop_reason"], "end_turn");
    assert!(response_body["content"][0]["text"].as_str().is_some());
    assert!(response_body["usage"]["input_tokens"].as_i64().is_some());
}

#[tokio::test]
async fn bedrock_invoke_model_titan() {
    let server = TestServer::start().await;
    let client = server.bedrock_runtime_client().await;

    let body = serde_json::to_vec(&serde_json::json!({
        "inputText": "Hello, how are you?",
        "textGenerationConfig": {
            "maxTokenCount": 100,
            "temperature": 0.7
        }
    }))
    .unwrap();

    let resp = client
        .invoke_model()
        .model_id("amazon.titan-text-express-v1")
        .content_type("application/json")
        .accept("application/json")
        .body(Blob::new(body))
        .send()
        .await
        .unwrap();

    let response_body: serde_json::Value = serde_json::from_slice(resp.body().as_ref()).unwrap();
    assert!(response_body["results"][0]["outputText"].as_str().is_some());
    assert_eq!(response_body["results"][0]["completionReason"], "FINISH");
}

// ---------------------------------------------------------------------------
// Converse (Runtime)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn bedrock_converse() {
    let server = TestServer::start().await;
    let client = server.bedrock_runtime_client().await;

    let resp = client
        .converse()
        .model_id("anthropic.claude-3-5-sonnet-20241022-v2:0")
        .messages(
            aws_sdk_bedrockruntime::types::Message::builder()
                .role(aws_sdk_bedrockruntime::types::ConversationRole::User)
                .content(aws_sdk_bedrockruntime::types::ContentBlock::Text(
                    "Hello!".to_string(),
                ))
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    assert_eq!(resp.stop_reason().as_str(), "end_turn");
    let output = resp.output().expect("should have output");
    if let aws_sdk_bedrockruntime::types::ConverseOutput::Message(msg) = output {
        assert!(!msg.content().is_empty());
    } else {
        panic!("expected message output");
    }
}

// ---------------------------------------------------------------------------
// Introspection & Simulation
// ---------------------------------------------------------------------------

#[tokio::test]
async fn bedrock_introspection_invocations() {
    let server = TestServer::start().await;
    let runtime_client = server.bedrock_runtime_client().await;

    // Invoke a model first
    let body = serde_json::to_vec(&serde_json::json!({
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 10,
        "messages": [{"role": "user", "content": "Test"}]
    }))
    .unwrap();

    runtime_client
        .invoke_model()
        .model_id("anthropic.claude-3-5-sonnet-20241022-v2:0")
        .content_type("application/json")
        .accept("application/json")
        .body(Blob::new(body))
        .send()
        .await
        .unwrap();

    // Check introspection endpoint
    let resp: serde_json::Value = reqwest::get(format!(
        "{}/_fakecloud/bedrock/invocations",
        server.endpoint()
    ))
    .await
    .unwrap()
    .json()
    .await
    .unwrap();

    let invocations = resp["invocations"].as_array().unwrap();
    assert!(!invocations.is_empty());
    assert_eq!(
        invocations[0]["modelId"],
        "anthropic.claude-3-5-sonnet-20241022-v2:0"
    );
    assert!(invocations[0]["timestamp"].as_str().is_some());
}

#[tokio::test]
async fn bedrock_simulation_custom_response() {
    let server = TestServer::start().await;
    let runtime_client = server.bedrock_runtime_client().await;
    let http_client = reqwest::Client::new();

    // Configure custom response
    let custom_response = serde_json::json!({
        "id": "msg_custom",
        "type": "message",
        "role": "assistant",
        "content": [{"type": "text", "text": "Custom test response!"}],
        "model": "anthropic.claude-3-5-sonnet-20241022-v2:0",
        "stop_reason": "end_turn",
        "usage": {"input_tokens": 5, "output_tokens": 10}
    });

    http_client
        .post(format!(
            "{}/_fakecloud/bedrock/models/anthropic.claude-3-5-sonnet-20241022-v2:0/response",
            server.endpoint()
        ))
        .body(serde_json::to_string(&custom_response).unwrap())
        .send()
        .await
        .unwrap();

    // Invoke model — should get custom response
    let body = serde_json::to_vec(&serde_json::json!({
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 10,
        "messages": [{"role": "user", "content": "Hi"}]
    }))
    .unwrap();

    let resp = runtime_client
        .invoke_model()
        .model_id("anthropic.claude-3-5-sonnet-20241022-v2:0")
        .content_type("application/json")
        .accept("application/json")
        .body(Blob::new(body))
        .send()
        .await
        .unwrap();

    let response_body: serde_json::Value = serde_json::from_slice(resp.body().as_ref()).unwrap();
    assert_eq!(response_body["content"][0]["text"], "Custom test response!");
}
