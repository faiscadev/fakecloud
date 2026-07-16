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

// Guardrails

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

// Model Customization Jobs

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

// Provisioned Model Throughput

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

// Model Invocation Logging

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

// InvokeModel (Runtime)

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

// Converse (Runtime)

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

// Introspection & Simulation

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

// Fault injection

/// Build a Bedrock runtime client that never retries, so fault-injection
/// tests see the first-attempt error instead of an SDK-automatic retry.
async fn bedrock_runtime_no_retry(server: &TestServer) -> aws_sdk_bedrockruntime::Client {
    let base = server.aws_config().await;
    let cfg = aws_sdk_bedrockruntime::config::Builder::from(&base)
        .retry_config(aws_sdk_bedrockruntime::config::retry::RetryConfig::disabled())
        .build();
    aws_sdk_bedrockruntime::Client::from_conf(cfg)
}

async fn queue_fault(endpoint: &str, http: &reqwest::Client, body: serde_json::Value) {
    let resp = http
        .post(format!("{endpoint}/_fakecloud/bedrock/faults"))
        .json(&body)
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());
}

async fn invoke_simple(
    client: &aws_sdk_bedrockruntime::Client,
    model_id: &str,
) -> Result<
    aws_sdk_bedrockruntime::operation::invoke_model::InvokeModelOutput,
    aws_sdk_bedrockruntime::error::SdkError<
        aws_sdk_bedrockruntime::operation::invoke_model::InvokeModelError,
    >,
> {
    let body = serde_json::to_vec(&serde_json::json!({
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 10,
        "messages": [{"role": "user", "content": "hi"}]
    }))
    .unwrap();
    client
        .invoke_model()
        .model_id(model_id)
        .content_type("application/json")
        .accept("application/json")
        .body(Blob::new(body))
        .send()
        .await
}

#[tokio::test]
async fn bedrock_fault_injection_throttling() {
    let server = TestServer::start().await;
    let runtime = bedrock_runtime_no_retry(&server).await;
    let http = reqwest::Client::new();
    let model_id = "anthropic.claude-3-5-sonnet-20241022-v2:0";

    queue_fault(
        server.endpoint(),
        &http,
        serde_json::json!({
            "errorType": "ThrottlingException",
            "message": "Rate exceeded",
            "httpStatus": 429,
            "count": 1
        }),
    )
    .await;

    let err = invoke_simple(&runtime, model_id).await.unwrap_err();
    let service_err = err.into_service_error();
    assert!(
        service_err
            .meta()
            .code()
            .unwrap_or("")
            .contains("ThrottlingException"),
        "expected ThrottlingException, got: {service_err:?}"
    );

    // Second call should succeed now that the rule's count is exhausted.
    invoke_simple(&runtime, model_id).await.unwrap();
}

#[tokio::test]
async fn bedrock_fault_injection_n_calls() {
    let server = TestServer::start().await;
    let runtime = bedrock_runtime_no_retry(&server).await;
    let http = reqwest::Client::new();
    let model_id = "anthropic.claude-3-5-sonnet-20241022-v2:0";

    queue_fault(
        server.endpoint(),
        &http,
        serde_json::json!({
            "errorType": "ServiceUnavailableException",
            "message": "service unavailable",
            "httpStatus": 503,
            "count": 3
        }),
    )
    .await;

    for _ in 0..3 {
        assert!(invoke_simple(&runtime, model_id).await.is_err());
    }
    invoke_simple(&runtime, model_id).await.unwrap();
}

#[tokio::test]
async fn bedrock_fault_injection_filter_by_model() {
    let server = TestServer::start().await;
    let runtime = bedrock_runtime_no_retry(&server).await;
    let http = reqwest::Client::new();
    let model_a = "anthropic.claude-3-5-sonnet-20241022-v2:0";
    let model_b = "amazon.titan-text-express-v1";

    queue_fault(
        server.endpoint(),
        &http,
        serde_json::json!({
            "errorType": "ValidationException",
            "message": "bad model",
            "httpStatus": 400,
            "count": 5,
            "modelId": model_a
        }),
    )
    .await;

    // model B is untouched
    invoke_simple(&runtime, model_b).await.unwrap();
    // model A faults
    assert!(invoke_simple(&runtime, model_a).await.is_err());
}

#[tokio::test]
async fn bedrock_fault_injection_filter_by_operation() {
    let server = TestServer::start().await;
    let runtime = bedrock_runtime_no_retry(&server).await;
    let http = reqwest::Client::new();
    let model_id = "anthropic.claude-3-5-sonnet-20241022-v2:0";

    queue_fault(
        server.endpoint(),
        &http,
        serde_json::json!({
            "errorType": "ThrottlingException",
            "message": "throttled",
            "httpStatus": 429,
            "count": 10,
            "operation": "Converse"
        }),
    )
    .await;

    // InvokeModel still works
    invoke_simple(&runtime, model_id).await.unwrap();
    // Converse faults
    let converse_err = runtime
        .converse()
        .model_id(model_id)
        .messages(
            aws_sdk_bedrockruntime::types::Message::builder()
                .role(aws_sdk_bedrockruntime::types::ConversationRole::User)
                .content(aws_sdk_bedrockruntime::types::ContentBlock::Text(
                    "hi".to_string(),
                ))
                .build()
                .unwrap(),
        )
        .send()
        .await;
    assert!(converse_err.is_err(), "converse should have faulted");
}

#[tokio::test]
async fn bedrock_fault_injection_history_records_errors() {
    let server = TestServer::start().await;
    let runtime = bedrock_runtime_no_retry(&server).await;
    let http = reqwest::Client::new();
    let model_id = "anthropic.claude-3-5-sonnet-20241022-v2:0";

    queue_fault(
        server.endpoint(),
        &http,
        serde_json::json!({
            "errorType": "ModelTimeoutException",
            "message": "timeout",
            "httpStatus": 408,
            "count": 1
        }),
    )
    .await;

    assert!(invoke_simple(&runtime, model_id).await.is_err());

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
    assert_eq!(invocations.len(), 1);
    assert_eq!(invocations[0]["modelId"], model_id);
    let error = invocations[0]["error"]
        .as_str()
        .expect("error field should be populated for faulted call");
    assert!(error.contains("ModelTimeoutException"));
    assert!(error.contains("timeout"));
}

#[tokio::test]
async fn bedrock_fault_injection_converse_stream() {
    let server = TestServer::start().await;
    let http = reqwest::Client::new();
    let model_id = "anthropic.claude-3-5-sonnet-20241022-v2:0";

    queue_fault(
        server.endpoint(),
        &http,
        serde_json::json!({
            "errorType": "ModelStreamErrorException",
            "message": "stream failed",
            "httpStatus": 500,
            "count": 1,
            "operation": "ConverseStream"
        }),
    )
    .await;

    let body = serde_json::json!({
        "modelId": model_id,
        "messages": [{"role": "user", "content": [{"text": "hi"}]}]
    });
    let resp = http
        .post(format!(
            "{}/model/{}/converse-stream",
            server.endpoint(),
            model_id
        ))
        .header("content-type", "application/json")
        .header(
            "authorization",
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20260411/us-east-1/bedrock/aws4_request, SignedHeaders=host, Signature=fake",
        )
        .body(serde_json::to_string(&body).unwrap())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 500);
    let text = resp.text().await.unwrap();
    assert!(
        text.contains("ModelStreamErrorException"),
        "expected error in body, got: {text}"
    );
}

// ApplyGuardrail (Runtime)

#[tokio::test]
async fn bedrock_apply_guardrail() {
    let server = TestServer::start().await;
    let bedrock_client = server.bedrock_client().await;
    let runtime_client = server.bedrock_runtime_client().await;

    // Create a guardrail with a word policy
    let word_policy = GuardrailWordPolicyConfig::builder()
        .words_config(
            GuardrailWordConfig::builder()
                .text("forbidden")
                .build()
                .unwrap(),
        )
        .build();

    let resp = bedrock_client
        .create_guardrail()
        .name("apply-test-guardrail")
        .blocked_input_messaging("Input blocked")
        .blocked_outputs_messaging("Output blocked")
        .word_policy_config(word_policy)
        .send()
        .await
        .unwrap();
    let guardrail_id = resp.guardrail_id().to_string();

    // Create a version
    let version_resp = bedrock_client
        .create_guardrail_version()
        .guardrail_identifier(&guardrail_id)
        .send()
        .await
        .unwrap();
    let version = version_resp.version().to_string();

    // Apply guardrail with safe content — should pass
    let safe_resp = runtime_client
        .apply_guardrail()
        .guardrail_identifier(&guardrail_id)
        .guardrail_version(&version)
        .source(aws_sdk_bedrockruntime::types::GuardrailContentSource::Input)
        .content(aws_sdk_bedrockruntime::types::GuardrailContentBlock::Text(
            aws_sdk_bedrockruntime::types::GuardrailTextBlock::builder()
                .text("Hello, this is safe content")
                .build()
                .unwrap(),
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(safe_resp.action().as_str(), "NONE");

    // Apply guardrail with forbidden word — should block
    let blocked_resp = runtime_client
        .apply_guardrail()
        .guardrail_identifier(&guardrail_id)
        .guardrail_version(&version)
        .source(aws_sdk_bedrockruntime::types::GuardrailContentSource::Input)
        .content(aws_sdk_bedrockruntime::types::GuardrailContentBlock::Text(
            aws_sdk_bedrockruntime::types::GuardrailTextBlock::builder()
                .text("This contains the forbidden word")
                .build()
                .unwrap(),
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(blocked_resp.action().as_str(), "GUARDRAIL_INTERVENED");
    assert!(!blocked_resp.assessments().is_empty());
}

// Converse with inferenceConfig and toolConfig

#[tokio::test]
async fn bedrock_converse_with_system_and_inference_config() {
    let server = TestServer::start().await;
    let client = server.bedrock_runtime_client().await;

    let resp = client
        .converse()
        .model_id("anthropic.claude-3-5-sonnet-20241022-v2:0")
        .system(aws_sdk_bedrockruntime::types::SystemContentBlock::Text(
            "You are a helpful assistant.".to_string(),
        ))
        .messages(
            aws_sdk_bedrockruntime::types::Message::builder()
                .role(aws_sdk_bedrockruntime::types::ConversationRole::User)
                .content(aws_sdk_bedrockruntime::types::ContentBlock::Text(
                    "Hello!".to_string(),
                ))
                .build()
                .unwrap(),
        )
        .inference_config(
            aws_sdk_bedrockruntime::types::InferenceConfiguration::builder()
                .max_tokens(50)
                .temperature(0.7_f32)
                .build(),
        )
        .send()
        .await
        .unwrap();

    assert_eq!(resp.stop_reason().as_str(), "end_turn");
    let usage = resp.usage().expect("should have usage");
    assert!(usage.input_tokens() > 0);
    assert!(usage.output_tokens() > 0);
}

#[tokio::test]
async fn bedrock_converse_with_tool_config() {
    let server = TestServer::start().await;
    let http_client = reqwest::Client::new();

    // A bare toolConfig (no toolChoice / implicit "auto") lets the model
    // decide whether to call a tool. Offline we reply with plain text and
    // must NOT fabricate an empty-arg toolUse — that used to derail agent /
    // LangChain tool-loops.
    let body = serde_json::json!({
        "modelId": "anthropic.claude-3-5-sonnet-20241022-v2:0",
        "messages": [
            {"role": "user", "content": [{"text": "What's the weather?"}]}
        ],
        "toolConfig": {
            "tools": [
                {
                    "toolSpec": {
                        "name": "get_weather",
                        "description": "Get weather for a location",
                        "inputSchema": {
                            "json": {"type": "object", "properties": {}}
                        }
                    }
                }
            ]
        }
    });

    let resp = http_client
        .post(format!(
            "{}/model/anthropic.claude-3-5-sonnet-20241022-v2:0/converse",
            server.endpoint()
        ))
        .header("content-type", "application/json")
        .header(
            "authorization",
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20260411/us-east-1/bedrock/aws4_request, SignedHeaders=host, Signature=fake",
        )
        .body(serde_json::to_string(&body).unwrap())
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    let result: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(result["stopReason"], "end_turn");

    let content = result["output"]["message"]["content"].as_array().unwrap();
    assert!(
        content.iter().all(|c| c.get("toolUse").is_none()),
        "bare toolConfig must not emit a toolUse block"
    );
}

#[tokio::test]
async fn bedrock_converse_with_forced_tool_choice() {
    let server = TestServer::start().await;
    let http_client = reqwest::Client::new();

    // toolChoice `any` forces the model to call a tool.
    let body = serde_json::json!({
        "modelId": "anthropic.claude-3-5-sonnet-20241022-v2:0",
        "messages": [
            {"role": "user", "content": [{"text": "What's the weather?"}]}
        ],
        "toolConfig": {
            "tools": [
                {
                    "toolSpec": {
                        "name": "get_weather",
                        "description": "Get weather for a location",
                        "inputSchema": {
                            "json": {"type": "object", "properties": {}}
                        }
                    }
                }
            ],
            "toolChoice": {"any": {}}
        }
    });

    let resp = http_client
        .post(format!(
            "{}/model/anthropic.claude-3-5-sonnet-20241022-v2:0/converse",
            server.endpoint()
        ))
        .header("content-type", "application/json")
        .header(
            "authorization",
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20260411/us-east-1/bedrock/aws4_request, SignedHeaders=host, Signature=fake",
        )
        .body(serde_json::to_string(&body).unwrap())
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    let result: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(result["stopReason"], "tool_use");

    let content = result["output"]["message"]["content"].as_array().unwrap();
    assert!(content.len() >= 2, "should have text and tool_use blocks");
    let tool_use = content
        .iter()
        .find_map(|c| c.get("toolUse"))
        .expect("should have a toolUse block");
    assert_eq!(tool_use["name"], "get_weather");
}

// CountTokens (Runtime)

#[tokio::test]
async fn bedrock_count_tokens_raw() {
    let server = TestServer::start().await;
    let http_client = reqwest::Client::new();

    let body = serde_json::json!({
        "input": {
            "converse": {
                "messages": [
                    {"role": "user", "content": [{"text": "Hello world how are you today"}]}
                ]
            }
        }
    });

    let resp = http_client
        .post(format!(
            "{}/model/anthropic.claude-3-5-sonnet-20241022-v2:0/count-tokens",
            server.endpoint()
        ))
        .header("content-type", "application/json")
        .header(
            "authorization",
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20260411/us-east-1/bedrock/aws4_request, SignedHeaders=host, Signature=fake",
        )
        .body(serde_json::to_string(&body).unwrap())
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    let result: serde_json::Value = resp.json().await.unwrap();
    let token_count = result["inputTokens"].as_i64().unwrap();
    assert!(token_count > 0, "should count some tokens");
}

// Async Invoke (Runtime)

#[tokio::test]
async fn bedrock_async_invoke_lifecycle_raw() {
    let server = TestServer::start().await;
    let http_client = reqwest::Client::new();

    // Start async invoke
    let body = serde_json::json!({
        "modelId": "anthropic.claude-3-5-sonnet-20241022-v2:0",
        "modelInput": {"messages": [{"role": "user", "content": "Hello"}]},
        "outputDataConfig": {
            "s3OutputDataConfig": {
                "s3Uri": "s3://my-bucket/output/"
            }
        }
    });

    let resp = http_client
        .post(format!("{}/async-invoke", server.endpoint()))
        .header("content-type", "application/json")
        .header(
            "authorization",
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20260411/us-east-1/bedrock/aws4_request, SignedHeaders=host, Signature=fake",
        )
        .body(serde_json::to_string(&body).unwrap())
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    let result: serde_json::Value = resp.json().await.unwrap();
    let invocation_arn = result["invocationArn"].as_str().unwrap();
    assert!(invocation_arn.contains("async-invoke/"));

    // Get async invoke
    let resp = http_client
        .get(format!(
            "{}/async-invoke/{}",
            server.endpoint(),
            invocation_arn.rsplit('/').next().unwrap()
        ))
        .header(
            "authorization",
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20260411/us-east-1/bedrock/aws4_request, SignedHeaders=host, Signature=fake",
        )
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    let result: serde_json::Value = resp.json().await.unwrap();
    // AsyncInvokeStatus is serialized in upstream UPPERCASE form
    // (COMPLETED/FAILED/IN_PROGRESS) to match the Smithy enum.
    assert_eq!(result["status"], "COMPLETED");
    assert_eq!(result["invocationArn"], invocation_arn);

    // List async invokes
    let resp = http_client
        .get(format!("{}/async-invoke", server.endpoint()))
        .header(
            "authorization",
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20260411/us-east-1/bedrock/aws4_request, SignedHeaders=host, Signature=fake",
        )
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    let result: serde_json::Value = resp.json().await.unwrap();
    let summaries = result["asyncInvokeSummaries"].as_array().unwrap();
    assert!(!summaries.is_empty());
}

// InvokeModelWithBidirectionalStream (via raw HTTP)

#[tokio::test]
async fn bedrock_invoke_model_with_bidirectional_stream_raw() {
    let server = TestServer::start().await;
    let http_client = reqwest::Client::new();

    let body = serde_json::json!({
        "messages": [{"role": "user", "content": "Hello"}]
    });

    let resp = http_client
        .post(format!(
            "{}/model/amazon.nova-sonic-v1:0/invoke-with-bidirectional-stream",
            server.endpoint()
        ))
        .header("content-type", "application/json")
        .header(
            "authorization",
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20260411/us-east-1/bedrock/aws4_request, SignedHeaders=host, Signature=fake",
        )
        .body(serde_json::to_string(&body).unwrap())
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    let content_type = resp
        .headers()
        .get("content-type")
        .unwrap()
        .to_str()
        .unwrap();
    assert_eq!(content_type, "application/vnd.amazon.eventstream");
    let body_bytes = resp.bytes().await.unwrap();
    assert!(body_bytes.len() > 16, "should have event stream data");
}

// InvokeModel response headers

#[tokio::test]
async fn bedrock_invoke_model_response_headers() {
    let server = TestServer::start().await;
    let http_client = reqwest::Client::new();

    let body = serde_json::json!({
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 10,
        "messages": [{"role": "user", "content": "Hi"}]
    });

    let resp = http_client
        .post(format!(
            "{}/model/anthropic.claude-3-5-sonnet-20241022-v2:0/invoke",
            server.endpoint()
        ))
        .header("content-type", "application/json")
        .header(
            "authorization",
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20260411/us-east-1/bedrock/aws4_request, SignedHeaders=host, Signature=fake",
        )
        .body(serde_json::to_string(&body).unwrap())
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    assert!(resp
        .headers()
        .contains_key("x-amzn-bedrock-input-token-count"));
    assert!(resp
        .headers()
        .contains_key("x-amzn-bedrock-output-token-count"));
    assert!(resp
        .headers()
        .contains_key("x-amzn-bedrock-performanceconfig-latency"));
    assert_eq!(
        resp.headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap(),
        "application/json"
    );
}

// FF1: echo mode + dynamic token counts
//
// `FAKECLOUD_BEDROCK_ECHO=1` reflects the user's prompt back as the
// assistant text, and the response token counts scale with the actual
// input length instead of the historical 10/20 placeholder.

#[tokio::test]
async fn bedrock_invoke_model_echo_mode_reflects_prompt() {
    let server = TestServer::start_with_env(&[("FAKECLOUD_BEDROCK_ECHO", "1")]).await;
    let client = server.bedrock_runtime_client().await;

    let body = serde_json::to_vec(&serde_json::json!({
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 100,
        "messages": [{"role": "user", "content": "hello"}]
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
    let text = response_body["content"][0]["text"].as_str().unwrap();
    assert!(
        text.contains("hello"),
        "echo response missing prompt: {text}"
    );
}

#[tokio::test]
async fn bedrock_invoke_model_default_no_echo_when_var_unset() {
    let server = TestServer::start().await;
    let client = server.bedrock_runtime_client().await;

    let body = serde_json::to_vec(&serde_json::json!({
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 100,
        "messages": [{"role": "user", "content": "hello"}]
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
    let text = response_body["content"][0]["text"].as_str().unwrap();
    // Default canned phrase, not the prompt.
    assert!(text.contains("test response from the emulated model"));
}

#[tokio::test]
async fn bedrock_invoke_model_token_counts_scale_with_input() {
    let server = TestServer::start().await;
    let http_client = reqwest::Client::new();

    let do_call = |prompt: &'static str| {
        let endpoint = server.endpoint().to_string();
        let client = http_client.clone();
        async move {
            let body = serde_json::json!({
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": 100,
                "messages": [{"role": "user", "content": prompt}]
            });
            let resp = client
                .post(format!(
                    "{endpoint}/model/anthropic.claude-3-5-sonnet-20241022-v2:0/invoke"
                ))
                .header("content-type", "application/json")
                .header(
                    "authorization",
                    "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20260411/us-east-1/bedrock/aws4_request, SignedHeaders=host, Signature=fake",
                )
                .body(serde_json::to_string(&body).unwrap())
                .send()
                .await
                .unwrap();
            assert_eq!(resp.status(), 200);
            resp.headers()
                .get("x-amzn-bedrock-input-token-count")
                .unwrap()
                .to_str()
                .unwrap()
                .parse::<u64>()
                .unwrap()
        }
    };

    let short_count = do_call("hi").await;
    let long_count =
        do_call("please count many words across this longer message body for tokens").await;
    assert!(
        long_count > short_count,
        "token count should scale with input length (short={short_count}, long={long_count})"
    );
}

// Streaming (via raw HTTP — AWS SDK event stream parsing is complex)

#[tokio::test]
async fn bedrock_invoke_model_with_response_stream_raw() {
    let server = TestServer::start().await;
    let http_client = reqwest::Client::new();

    let body = serde_json::json!({
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 100,
        "messages": [{"role": "user", "content": "Hello"}]
    });

    let resp = http_client
        .post(format!(
            "{}/model/anthropic.claude-3-5-sonnet-20241022-v2:0/invoke-with-response-stream",
            server.endpoint()
        ))
        .header("content-type", "application/json")
        .header(
            "authorization",
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20260411/us-east-1/bedrock/aws4_request, SignedHeaders=host, Signature=fake",
        )
        .body(serde_json::to_string(&body).unwrap())
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    let content_type = resp
        .headers()
        .get("content-type")
        .unwrap()
        .to_str()
        .unwrap();
    assert_eq!(content_type, "application/vnd.amazon.eventstream");

    let body_bytes = resp.bytes().await.unwrap();
    // Event stream should have some data (at minimum one event frame)
    assert!(
        body_bytes.len() > 16,
        "event stream body should not be empty"
    );
}

#[tokio::test]
async fn bedrock_converse_stream_raw() {
    let server = TestServer::start().await;
    let http_client = reqwest::Client::new();

    let body = serde_json::json!({
        "modelId": "anthropic.claude-3-5-sonnet-20241022-v2:0",
        "messages": [
            {"role": "user", "content": [{"text": "Hello"}]}
        ]
    });

    let resp = http_client
        .post(format!(
            "{}/model/anthropic.claude-3-5-sonnet-20241022-v2:0/converse-stream",
            server.endpoint()
        ))
        .header("content-type", "application/json")
        .header(
            "authorization",
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20260411/us-east-1/bedrock/aws4_request, SignedHeaders=host, Signature=fake",
        )
        .body(serde_json::to_string(&body).unwrap())
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    let content_type = resp
        .headers()
        .get("content-type")
        .unwrap()
        .to_str()
        .unwrap();
    assert_eq!(content_type, "application/vnd.amazon.eventstream");

    let body_bytes = resp.bytes().await.unwrap();
    // Should have multiple events (messageStart, contentBlockStart, delta, stop, metadata)
    assert!(
        body_bytes.len() > 100,
        "converse stream should have multiple events"
    );
}

// Custom Models

#[tokio::test]
async fn bedrock_custom_model_crud() {
    let server = TestServer::start().await;
    let http_client = reqwest::Client::new();
    let auth = "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20260411/us-east-1/bedrock/aws4_request, SignedHeaders=host, Signature=fake";

    // Create custom model
    let body = serde_json::json!({
        "modelName": "my-custom-model",
        "modelSourceConfig": {"s3DataSource": {"s3Uri": "s3://bucket/model/"}}
    });
    let resp = http_client
        .post(format!(
            "{}/custom-models/create-custom-model",
            server.endpoint()
        ))
        .header("content-type", "application/json")
        .header("authorization", auth)
        .body(serde_json::to_string(&body).unwrap())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);
    let result: serde_json::Value = resp.json().await.unwrap();
    let model_arn = result["modelArn"].as_str().unwrap().to_string();
    assert!(model_arn.contains("custom-model/"));

    // Get custom model
    let model_id = model_arn.rsplit('/').next().unwrap();
    let resp = http_client
        .get(format!("{}/custom-models/{}", server.endpoint(), model_id))
        .header("authorization", auth)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let result: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(result["modelName"], "my-custom-model");
    assert_eq!(result["modelStatus"], "Active");

    // List custom models
    let resp = http_client
        .get(format!("{}/custom-models", server.endpoint()))
        .header("authorization", auth)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let result: serde_json::Value = resp.json().await.unwrap();
    assert!(!result["modelSummaries"].as_array().unwrap().is_empty());

    // Delete custom model
    let resp = http_client
        .delete(format!("{}/custom-models/{}", server.endpoint(), model_id))
        .header("authorization", auth)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // Verify deleted
    let resp = http_client
        .get(format!("{}/custom-models/{}", server.endpoint(), model_id))
        .header("authorization", auth)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
}

// Custom Model Deployments

#[tokio::test]
async fn bedrock_custom_model_deployment_crud() {
    let server = TestServer::start().await;
    let http_client = reqwest::Client::new();
    let auth = "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20260411/us-east-1/bedrock/aws4_request, SignedHeaders=host, Signature=fake";

    // Create deployment
    let body = serde_json::json!({
        "modelDeploymentName": "my-deployment",
        "modelArn": "arn:aws:bedrock:us-east-1:123456789012:custom-model/test-model",
        "description": "Test deployment"
    });
    let resp = http_client
        .post(format!(
            "{}/model-customization/custom-model-deployments",
            server.endpoint()
        ))
        .header("content-type", "application/json")
        .header("authorization", auth)
        .body(serde_json::to_string(&body).unwrap())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);
    let result: serde_json::Value = resp.json().await.unwrap();
    let deployment_arn = result["customModelDeploymentArn"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(deployment_arn.contains("custom-model-deployment/"));

    // Get deployment
    let deployment_id = deployment_arn.rsplit('/').next().unwrap();
    let resp = http_client
        .get(format!(
            "{}/model-customization/custom-model-deployments/{}",
            server.endpoint(),
            deployment_id
        ))
        .header("authorization", auth)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let result: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(result["modelDeploymentName"], "my-deployment");
    assert_eq!(result["status"], "Active");
    assert_eq!(result["description"], "Test deployment");

    // List deployments
    let resp = http_client
        .get(format!(
            "{}/model-customization/custom-model-deployments",
            server.endpoint()
        ))
        .header("authorization", auth)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let result: serde_json::Value = resp.json().await.unwrap();
    assert!(!result["modelDeploymentSummaries"]
        .as_array()
        .unwrap()
        .is_empty());

    // Update deployment
    let body = serde_json::json!({
        "modelArn": "arn:aws:bedrock:us-east-1:123456789012:custom-model/updated-model"
    });
    let resp = http_client
        .patch(format!(
            "{}/model-customization/custom-model-deployments/{}",
            server.endpoint(),
            deployment_id
        ))
        .header("content-type", "application/json")
        .header("authorization", auth)
        .body(serde_json::to_string(&body).unwrap())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // Delete deployment
    let resp = http_client
        .delete(format!(
            "{}/model-customization/custom-model-deployments/{}",
            server.endpoint(),
            deployment_id
        ))
        .header("authorization", auth)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // Verify deleted
    let resp = http_client
        .get(format!(
            "{}/model-customization/custom-model-deployments/{}",
            server.endpoint(),
            deployment_id
        ))
        .header("authorization", auth)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
}

// Model Import Jobs + Imported Models

#[tokio::test]
async fn bedrock_model_import_lifecycle() {
    let server = TestServer::start().await;
    let http_client = reqwest::Client::new();
    let auth = "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20260411/us-east-1/bedrock/aws4_request, SignedHeaders=host, Signature=fake";

    let body = serde_json::json!({
        "jobName": "my-import-job",
        "importedModelName": "my-imported-model",
        "roleArn": "arn:aws:iam::123456789012:role/test",
        "modelDataSource": {"s3DataSource": {"s3Uri": "s3://bucket/model/"}}
    });
    let resp = http_client
        .post(format!("{}/model-import-jobs", server.endpoint()))
        .header("content-type", "application/json")
        .header("authorization", auth)
        .body(serde_json::to_string(&body).unwrap())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);
    let result: serde_json::Value = resp.json().await.unwrap();
    let job_arn = result["jobArn"].as_str().unwrap().to_string();
    let job_id = job_arn.rsplit('/').next().unwrap();

    let resp = http_client
        .get(format!(
            "{}/model-import-jobs/{}",
            server.endpoint(),
            job_id
        ))
        .header("authorization", auth)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let result: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(result["jobName"], "my-import-job");
    assert_eq!(result["status"], "Completed");

    let resp = http_client
        .get(format!(
            "{}/imported-models/my-imported-model",
            server.endpoint()
        ))
        .header("authorization", auth)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let resp = http_client
        .get(format!("{}/imported-models", server.endpoint()))
        .header("authorization", auth)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let resp = http_client
        .delete(format!(
            "{}/imported-models/my-imported-model",
            server.endpoint()
        ))
        .header("authorization", auth)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
}

// Model Copy Jobs

#[tokio::test]
async fn bedrock_model_copy_lifecycle() {
    let server = TestServer::start().await;
    let http_client = reqwest::Client::new();
    let auth = "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20260411/us-east-1/bedrock/aws4_request, SignedHeaders=host, Signature=fake";

    let body = serde_json::json!({
        "sourceModelArn": "arn:aws:bedrock:us-east-1:123456789012:custom-model/source-model",
        "targetModelName": "my-copy-target"
    });
    let resp = http_client
        .post(format!("{}/model-copy-jobs", server.endpoint()))
        .header("content-type", "application/json")
        .header("authorization", auth)
        .body(serde_json::to_string(&body).unwrap())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);
    let result: serde_json::Value = resp.json().await.unwrap();
    let job_arn = result["jobArn"].as_str().unwrap().to_string();
    let job_id = job_arn.rsplit('/').next().unwrap();

    let resp = http_client
        .get(format!("{}/model-copy-jobs/{}", server.endpoint(), job_id))
        .header("authorization", auth)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let result: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(result["status"], "Completed");

    let resp = http_client
        .get(format!("{}/model-copy-jobs", server.endpoint()))
        .header("authorization", auth)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let result: serde_json::Value = resp.json().await.unwrap();
    assert!(!result["modelCopyJobSummaries"]
        .as_array()
        .unwrap()
        .is_empty());
}

// Model Invocation Jobs

#[tokio::test]
async fn bedrock_model_invocation_job_lifecycle() {
    let server = TestServer::start().await;
    let http_client = reqwest::Client::new();
    let auth = "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20260411/us-east-1/bedrock/aws4_request, SignedHeaders=host, Signature=fake";

    let body = serde_json::json!({
        "jobName": "my-batch-job",
        "modelId": "anthropic.claude-3-5-sonnet-20241022-v2:0",
        "roleArn": "arn:aws:iam::123456789012:role/test",
        "inputDataConfig": {"s3InputDataConfig": {"s3Uri": "s3://bucket/input/"}},
        "outputDataConfig": {"s3OutputDataConfig": {"s3Uri": "s3://bucket/output/"}}
    });
    let resp = http_client
        .post(format!("{}/model-invocation-job", server.endpoint()))
        .header("content-type", "application/json")
        .header("authorization", auth)
        .body(serde_json::to_string(&body).unwrap())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);
    let result: serde_json::Value = resp.json().await.unwrap();
    let job_arn = result["jobArn"].as_str().unwrap().to_string();
    let job_id = job_arn.rsplit('/').next().unwrap();

    let resp = http_client
        .get(format!(
            "{}/model-invocation-job/{}",
            server.endpoint(),
            job_id
        ))
        .header("authorization", auth)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let result: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(result["jobName"], "my-batch-job");
    assert_eq!(result["status"], "InProgress");

    let resp = http_client
        .get(format!("{}/model-invocation-jobs", server.endpoint()))
        .header("authorization", auth)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let result: serde_json::Value = resp.json().await.unwrap();
    assert!(!result["invocationJobSummaries"]
        .as_array()
        .unwrap()
        .is_empty());

    // Stop job
    let resp = http_client
        .post(format!(
            "{}/model-invocation-job/{}/stop",
            server.endpoint(),
            job_id
        ))
        .header("authorization", auth)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // Verify stopped
    let resp = http_client
        .get(format!(
            "{}/model-invocation-job/{}",
            server.endpoint(),
            job_id
        ))
        .header("authorization", auth)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let result: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(result["status"], "Stopped");
}

// Evaluation Jobs

#[tokio::test]
async fn bedrock_evaluation_job_lifecycle() {
    let server = TestServer::start().await;
    let http_client = reqwest::Client::new();
    let auth = "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20260411/us-east-1/bedrock/aws4_request, SignedHeaders=host, Signature=fake";

    let body = serde_json::json!({
        "jobName": "my-eval-job",
        "jobDescription": "Test evaluation",
        "roleArn": "arn:aws:iam::123456789012:role/test",
        "evaluationConfig": {"automated": {"datasetMetricConfigs": []}},
        "inferenceConfig": {"models": []},
        "outputDataConfig": {"s3Uri": "s3://bucket/output/"}
    });
    let resp = http_client
        .post(format!("{}/evaluation-jobs", server.endpoint()))
        .header("content-type", "application/json")
        .header("authorization", auth)
        .body(serde_json::to_string(&body).unwrap())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);
    let result: serde_json::Value = resp.json().await.unwrap();
    let job_arn = result["jobArn"].as_str().unwrap().to_string();
    let job_id = job_arn.rsplit('/').next().unwrap();

    let resp = http_client
        .get(format!("{}/evaluation-jobs/{}", server.endpoint(), job_id))
        .header("authorization", auth)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let result: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(result["jobName"], "my-eval-job");
    assert_eq!(result["status"], "InProgress");
    assert_eq!(result["jobType"], "Automated");

    let resp = http_client
        .get(format!("{}/evaluation-jobs", server.endpoint()))
        .header("authorization", auth)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // Stop
    let resp = http_client
        .post(format!(
            "{}/evaluation-job/{}/stop",
            server.endpoint(),
            job_id
        ))
        .header("authorization", auth)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // Batch delete
    let body = serde_json::json!({"jobIdentifiers": [job_arn]});
    let resp = http_client
        .post(format!(
            "{}/evaluation-jobs/batch-delete",
            server.endpoint()
        ))
        .header("content-type", "application/json")
        .header("authorization", auth)
        .body(serde_json::to_string(&body).unwrap())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let result: serde_json::Value = resp.json().await.unwrap();
    assert!(result["errors"].as_array().unwrap().is_empty());

    // Verify deleted
    let resp = http_client
        .get(format!("{}/evaluation-jobs/{}", server.endpoint(), job_id))
        .header("authorization", auth)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
}

// Inference Profiles

#[tokio::test]
async fn bedrock_inference_profile_crud() {
    let server = TestServer::start().await;
    let h = reqwest::Client::new();
    let a = "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20260411/us-east-1/bedrock/aws4_request, SignedHeaders=host, Signature=fake";

    let b = serde_json::json!({"inferenceProfileName": "my-profile", "description": "Test", "modelSource": {"copyFrom": "arn:aws:bedrock:us-east-1::foundation-model/anthropic.claude-3-5-sonnet-20241022-v2:0"}});
    let r = h
        .post(format!("{}/inference-profiles", server.endpoint()))
        .header("content-type", "application/json")
        .header("authorization", a)
        .body(serde_json::to_string(&b).unwrap())
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 201);
    let v: serde_json::Value = r.json().await.unwrap();
    let id = v["inferenceProfileArn"]
        .as_str()
        .unwrap()
        .rsplit('/')
        .next()
        .unwrap()
        .to_string();

    let r = h
        .get(format!("{}/inference-profiles/{}", server.endpoint(), id))
        .header("authorization", a)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let v: serde_json::Value = r.json().await.unwrap();
    assert_eq!(v["inferenceProfileName"], "my-profile");

    let r = h
        .get(format!("{}/inference-profiles", server.endpoint()))
        .header("authorization", a)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);

    let r = h
        .delete(format!("{}/inference-profiles/{}", server.endpoint(), id))
        .header("authorization", a)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);

    let r = h
        .get(format!("{}/inference-profiles/{}", server.endpoint(), id))
        .header("authorization", a)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 404);
}

// Prompt Routers

#[tokio::test]
async fn bedrock_prompt_router_crud() {
    let server = TestServer::start().await;
    let h = reqwest::Client::new();
    let a = "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20260411/us-east-1/bedrock/aws4_request, SignedHeaders=host, Signature=fake";

    // Real AWS rejects empty `models` / `routingCriteria` for CreatePromptRouter
    // with a ValidationException; supply minimally-valid placeholders so
    // the round-trip exercises the happy path instead of the validation gate.
    let b = serde_json::json!({
        "promptRouterName": "my-router",
        "models": [{"modelArn": "arn:aws:bedrock:us-east-1::foundation-model/anthropic.claude-3-haiku-20240307-v1:0"}],
        "routingCriteria": {"responseQualityDifference": 0.1},
        "fallbackModel": {"modelArn": "arn:aws:bedrock:us-east-1::foundation-model/anthropic.claude-3-haiku-20240307-v1:0"}
    });
    let r = h
        .post(format!("{}/prompt-routers", server.endpoint()))
        .header("content-type", "application/json")
        .header("authorization", a)
        .body(serde_json::to_string(&b).unwrap())
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 201);
    let v: serde_json::Value = r.json().await.unwrap();
    let id = v["promptRouterArn"]
        .as_str()
        .unwrap()
        .rsplit('/')
        .next()
        .unwrap()
        .to_string();

    let r = h
        .get(format!("{}/prompt-routers/{}", server.endpoint(), id))
        .header("authorization", a)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);

    let r = h
        .get(format!("{}/prompt-routers", server.endpoint()))
        .header("authorization", a)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);

    let r = h
        .delete(format!("{}/prompt-routers/{}", server.endpoint(), id))
        .header("authorization", a)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
}

// Resource Policies

#[tokio::test]
async fn bedrock_resource_policy_crud() {
    let server = TestServer::start().await;
    let h = reqwest::Client::new();
    let a = "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20260411/us-east-1/bedrock/aws4_request, SignedHeaders=host, Signature=fake";
    let arn = "arn:aws:bedrock:us-east-1:123456789012:custom-model/test-res-policy";

    let b =
        serde_json::json!({"resourceArn": arn, "resourcePolicy": "{\"Version\":\"2012-10-17\"}"});
    let r = h
        .post(format!("{}/resource-policy", server.endpoint()))
        .header("content-type", "application/json")
        .header("authorization", a)
        .body(serde_json::to_string(&b).unwrap())
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);

    let encoded_arn = arn.replace(':', "%3A").replace('/', "%2F");
    let r = h
        .get(format!(
            "{}/resource-policy/{}",
            server.endpoint(),
            encoded_arn
        ))
        .header("authorization", a)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let v: serde_json::Value = r.json().await.unwrap();
    assert!(v["resourcePolicy"].as_str().is_some());

    let r = h
        .delete(format!(
            "{}/resource-policy/{}",
            server.endpoint(),
            encoded_arn
        ))
        .header("authorization", a)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);

    let r = h
        .get(format!(
            "{}/resource-policy/{}",
            server.endpoint(),
            encoded_arn
        ))
        .header("authorization", a)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 404);
}

#[tokio::test]
async fn bedrock_guardrail_content_policy_read_shape_and_arn_identifier() {
    use aws_sdk_bedrock::types::{
        GuardrailContentFilterConfig, GuardrailContentFilterType, GuardrailContentPolicyConfig,
        GuardrailFilterStrength,
    };
    let server = TestServer::start().await;
    let client = server.bedrock_client().await;

    let content_policy = GuardrailContentPolicyConfig::builder()
        .filters_config(
            GuardrailContentFilterConfig::builder()
                .r#type(GuardrailContentFilterType::Hate)
                .input_strength(GuardrailFilterStrength::Medium)
                .output_strength(GuardrailFilterStrength::Medium)
                .build()
                .unwrap(),
        )
        .filters_config(
            GuardrailContentFilterConfig::builder()
                .r#type(GuardrailContentFilterType::Violence)
                .input_strength(GuardrailFilterStrength::High)
                .output_strength(GuardrailFilterStrength::High)
                .build()
                .unwrap(),
        )
        .build()
        .unwrap();

    let created = client
        .create_guardrail()
        .name("content-guardrail")
        .blocked_input_messaging("blocked")
        .blocked_outputs_messaging("blocked")
        .content_policy_config(content_policy)
        .send()
        .await
        .unwrap();
    let arn = created.guardrail_arn().to_string();

    // GetGuardrail must render the stored content policy in its read shape, so
    // the SDK can parse `content_policy.filters` (not the `*Config` wrapper).
    let got = client
        .get_guardrail()
        .guardrail_identifier(&arn) // resolve by ARN, not just the short id
        .send()
        .await
        .unwrap();
    let filters = got.content_policy().expect("content policy").filters();
    assert_eq!(filters.len(), 2);
    assert_eq!(filters[0].r#type(), &GuardrailContentFilterType::Hate);

    // CreateGuardrailVersion is called by the provider with the ARN identifier.
    let version = client
        .create_guardrail_version()
        .guardrail_identifier(&arn)
        .send()
        .await
        .unwrap();
    assert!(!version.version().is_empty());
}

#[tokio::test]
async fn bedrock_inference_profiles_catalogue_and_application_arn() {
    use aws_sdk_bedrock::types::{InferenceProfileModelSource, InferenceProfileType};
    let server = TestServer::start().await;
    let client = server.bedrock_client().await;

    // The AWS-managed SYSTEM_DEFINED catalogue is listed out of the box.
    let listed = client.list_inference_profiles().send().await.unwrap();
    let summaries = listed.inference_profile_summaries();
    assert!(!summaries.is_empty());
    let system = summaries
        .iter()
        .find(|s| s.r#type() == &InferenceProfileType::SystemDefined)
        .expect("a system-defined profile");
    assert!(system.description().is_some());

    // It resolves via GetInferenceProfile by id.
    let got = client
        .get_inference_profile()
        .inference_profile_identifier(system.inference_profile_id())
        .send()
        .await
        .unwrap();
    assert_eq!(got.r#type(), &InferenceProfileType::SystemDefined);
    assert!(!got.models().is_empty());

    // An APPLICATION profile gets the application-inference-profile ARN.
    let created = client
        .create_inference_profile()
        .inference_profile_name("my-app-profile")
        .model_source(InferenceProfileModelSource::CopyFrom(
            "arn:aws:bedrock:us-east-1::foundation-model/amazon.titan-text-express-v1".to_string(),
        ))
        .send()
        .await
        .unwrap();
    assert!(created
        .inference_profile_arn()
        .contains("application-inference-profile/"));
}

#[tokio::test]
async fn bedrock_list_inference_profiles_paginates_without_skip_or_dup() {
    let server = TestServer::start().await;
    let client = server.bedrock_client().await;

    // Full list in one shot as the source of truth.
    let all = client.list_inference_profiles().send().await.unwrap();
    let expected: Vec<String> = all
        .inference_profile_summaries()
        .iter()
        .map(|s| s.inference_profile_id().to_string())
        .collect();
    assert!(
        expected.len() >= 2,
        "need at least 2 profiles to exercise pagination"
    );

    // Page through with maxResults=1; the cursor must neither skip nor
    // duplicate an entry (regression: token was an Id but resume compared ARNs
    // and offset indexed the wrong list).
    let mut collected: Vec<String> = Vec::new();
    let mut token: Option<String> = None;
    loop {
        let mut req = client.list_inference_profiles().max_results(1);
        if let Some(t) = &token {
            req = req.next_token(t.clone());
        }
        let page = req.send().await.unwrap();
        for s in page.inference_profile_summaries() {
            collected.push(s.inference_profile_id().to_string());
        }
        match page.next_token() {
            Some(t) => token = Some(t.to_string()),
            None => break,
        }
    }

    let mut got = collected.clone();
    got.sort();
    got.dedup();
    assert_eq!(
        got.len(),
        collected.len(),
        "pagination duplicated an entry: {collected:?}"
    );
    let mut exp_sorted = expected.clone();
    exp_sorted.sort();
    assert_eq!(got, exp_sorted, "pagination skipped or added entries");
}
