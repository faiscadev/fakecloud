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
    assert!(!model.model_id().is_empty());
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
    assert_eq!(details.model_id(), "amazon.titan-text-express-v1");
    assert!(details.model_arn().contains("foundation-model/"));
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

// ---------------------------------------------------------------------------
// Guardrails
// ---------------------------------------------------------------------------

#[test_action("bedrock", "CreateGuardrail", checksum = "22fd9f5c")]
#[test_action("bedrock", "GetGuardrail", checksum = "f64e7901")]
#[test_action("bedrock", "ListGuardrails", checksum = "69e9f011")]
#[tokio::test]
async fn bedrock_create_get_list_guardrail() {
    let server = TestServer::start().await;
    let client = server.bedrock_client().await;

    let resp = client
        .create_guardrail()
        .name("conf-guardrail")
        .blocked_input_messaging("blocked")
        .blocked_outputs_messaging("blocked")
        .send()
        .await
        .unwrap();
    let guardrail_id = resp.guardrail_id();
    assert!(!guardrail_id.is_empty());

    let resp = client
        .get_guardrail()
        .guardrail_identifier(guardrail_id)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.name(), "conf-guardrail");

    let resp = client.list_guardrails().send().await.unwrap();
    assert!(!resp.guardrails().is_empty());
}

#[test_action("bedrock", "UpdateGuardrail", checksum = "e1a06efa")]
#[tokio::test]
async fn bedrock_update_guardrail() {
    let server = TestServer::start().await;
    let client = server.bedrock_client().await;

    let resp = client
        .create_guardrail()
        .name("update-me")
        .blocked_input_messaging("blocked")
        .blocked_outputs_messaging("blocked")
        .send()
        .await
        .unwrap();
    let guardrail_id = resp.guardrail_id().to_string();

    client
        .update_guardrail()
        .guardrail_identifier(&guardrail_id)
        .name("updated-name")
        .blocked_input_messaging("blocked")
        .blocked_outputs_messaging("blocked")
        .send()
        .await
        .unwrap();

    let resp = client
        .get_guardrail()
        .guardrail_identifier(&guardrail_id)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.name(), "updated-name");
}

#[test_action("bedrock", "DeleteGuardrail", checksum = "0e05ca0d")]
#[tokio::test]
async fn bedrock_delete_guardrail() {
    let server = TestServer::start().await;
    let client = server.bedrock_client().await;

    let resp = client
        .create_guardrail()
        .name("delete-me")
        .blocked_input_messaging("blocked")
        .blocked_outputs_messaging("blocked")
        .send()
        .await
        .unwrap();
    let guardrail_id = resp.guardrail_id().to_string();

    client
        .delete_guardrail()
        .guardrail_identifier(&guardrail_id)
        .send()
        .await
        .unwrap();

    let err = client
        .get_guardrail()
        .guardrail_identifier(&guardrail_id)
        .send()
        .await
        .unwrap_err();
    assert!(err.into_service_error().is_resource_not_found_exception());
}

#[test_action("bedrock", "CreateGuardrailVersion", checksum = "02f01128")]
#[tokio::test]
async fn bedrock_create_guardrail_version() {
    let server = TestServer::start().await;
    let client = server.bedrock_client().await;

    let resp = client
        .create_guardrail()
        .name("version-test")
        .blocked_input_messaging("blocked")
        .blocked_outputs_messaging("blocked")
        .send()
        .await
        .unwrap();
    let guardrail_id = resp.guardrail_id().to_string();

    let v1 = client
        .create_guardrail_version()
        .guardrail_identifier(&guardrail_id)
        .send()
        .await
        .unwrap();
    assert_eq!(v1.version(), "1");
}

// ---------------------------------------------------------------------------
// Model Customization Jobs
// ---------------------------------------------------------------------------

#[test_action("bedrock", "CreateModelCustomizationJob", checksum = "79f8b20b")]
#[test_action("bedrock", "GetModelCustomizationJob", checksum = "877cb4c1")]
#[test_action("bedrock", "ListModelCustomizationJobs", checksum = "395bacc9")]
#[tokio::test]
async fn bedrock_model_customization_job_crud() {
    let server = TestServer::start().await;
    let client = server.bedrock_client().await;

    let resp = client
        .create_model_customization_job()
        .job_name("conf-job")
        .custom_model_name("conf-model")
        .base_model_identifier("amazon.titan-text-express-v1")
        .role_arn("arn:aws:iam::123456789012:role/test")
        .training_data_config(
            aws_sdk_bedrock::types::TrainingDataConfig::builder()
                .s3_uri("s3://bucket/train/")
                .build(),
        )
        .output_data_config(
            aws_sdk_bedrock::types::OutputDataConfig::builder()
                .s3_uri("s3://bucket/out/")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    let job_arn = resp.job_arn();
    assert!(job_arn.contains("model-customization-job/"));

    let resp = client
        .get_model_customization_job()
        .job_identifier(job_arn)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.job_name(), "conf-job");

    let resp = client.list_model_customization_jobs().send().await.unwrap();
    assert!(!resp.model_customization_job_summaries().is_empty());
}

#[test_action("bedrock", "StopModelCustomizationJob", checksum = "cd450c9d")]
#[tokio::test]
async fn bedrock_stop_model_customization_job() {
    let server = TestServer::start().await;
    let client = server.bedrock_client().await;

    let resp = client
        .create_model_customization_job()
        .job_name("stop-me")
        .custom_model_name("stop-model")
        .base_model_identifier("amazon.titan-text-express-v1")
        .role_arn("arn:aws:iam::123456789012:role/test")
        .training_data_config(
            aws_sdk_bedrock::types::TrainingDataConfig::builder()
                .s3_uri("s3://bucket/train/")
                .build(),
        )
        .output_data_config(
            aws_sdk_bedrock::types::OutputDataConfig::builder()
                .s3_uri("s3://bucket/out/")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    client
        .stop_model_customization_job()
        .job_identifier(resp.job_arn())
        .send()
        .await
        .unwrap();
}

// ---------------------------------------------------------------------------
// Provisioned Model Throughput
// ---------------------------------------------------------------------------

#[test_action("bedrock", "CreateProvisionedModelThroughput", checksum = "30e21d40")]
#[test_action("bedrock", "GetProvisionedModelThroughput", checksum = "fa4bf01f")]
#[test_action("bedrock", "ListProvisionedModelThroughputs", checksum = "5b0816af")]
#[tokio::test]
async fn bedrock_provisioned_throughput_crud() {
    let server = TestServer::start().await;
    let client = server.bedrock_client().await;

    let resp = client
        .create_provisioned_model_throughput()
        .provisioned_model_name("conf-prov")
        .model_id("amazon.titan-text-express-v1")
        .model_units(1)
        .send()
        .await
        .unwrap();
    let arn = resp.provisioned_model_arn();
    assert!(arn.contains("provisioned-model/"));

    let resp = client
        .get_provisioned_model_throughput()
        .provisioned_model_id(arn)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.provisioned_model_name(), "conf-prov");

    let resp = client
        .list_provisioned_model_throughputs()
        .send()
        .await
        .unwrap();
    assert!(!resp.provisioned_model_summaries().is_empty());
}

#[test_action("bedrock", "UpdateProvisionedModelThroughput", checksum = "d2371270")]
#[test_action("bedrock", "DeleteProvisionedModelThroughput", checksum = "df6801df")]
#[tokio::test]
async fn bedrock_update_delete_provisioned_throughput() {
    let server = TestServer::start().await;
    let client = server.bedrock_client().await;

    let resp = client
        .create_provisioned_model_throughput()
        .provisioned_model_name("del-prov")
        .model_id("amazon.titan-text-express-v1")
        .model_units(1)
        .send()
        .await
        .unwrap();
    let arn = resp.provisioned_model_arn();

    client
        .update_provisioned_model_throughput()
        .provisioned_model_id(arn)
        .desired_provisioned_model_name("renamed")
        .send()
        .await
        .unwrap();

    client
        .delete_provisioned_model_throughput()
        .provisioned_model_id(arn)
        .send()
        .await
        .unwrap();
}

// ---------------------------------------------------------------------------
// Logging Configuration
// ---------------------------------------------------------------------------

#[test_action(
    "bedrock",
    "PutModelInvocationLoggingConfiguration",
    checksum = "aea620de"
)]
#[test_action(
    "bedrock",
    "GetModelInvocationLoggingConfiguration",
    checksum = "97689194"
)]
#[test_action(
    "bedrock",
    "DeleteModelInvocationLoggingConfiguration",
    checksum = "3dab2c6b"
)]
#[tokio::test]
async fn bedrock_logging_configuration() {
    let server = TestServer::start().await;
    let client = server.bedrock_client().await;

    client
        .put_model_invocation_logging_configuration()
        .logging_config(
            aws_sdk_bedrock::types::LoggingConfig::builder()
                .text_data_delivery_enabled(true)
                .build(),
        )
        .send()
        .await
        .unwrap();

    let resp = client
        .get_model_invocation_logging_configuration()
        .send()
        .await
        .unwrap();
    assert!(resp.logging_config().is_some());

    client
        .delete_model_invocation_logging_configuration()
        .send()
        .await
        .unwrap();

    let resp = client
        .get_model_invocation_logging_configuration()
        .send()
        .await
        .unwrap();
    assert!(resp.logging_config().is_none());
}

// ---------------------------------------------------------------------------
// Bedrock Runtime — InvokeModel & Converse
// ---------------------------------------------------------------------------

#[test_action("bedrock-runtime", "InvokeModel", checksum = "a289714a")]
#[tokio::test]
async fn bedrock_invoke_model() {
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
        .body(aws_sdk_bedrockruntime::primitives::Blob::new(body))
        .send()
        .await
        .unwrap();

    let response_body: serde_json::Value = serde_json::from_slice(resp.body().as_ref()).unwrap();
    assert_eq!(response_body["type"], "message");
    assert!(response_body["content"][0]["text"].as_str().is_some());
}

#[test_action("bedrock-runtime", "Converse", checksum = "813a7054")]
#[tokio::test]
async fn bedrock_converse_conformance() {
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
    assert!(resp.output().is_some());
}

// ---------------------------------------------------------------------------
// ApplyGuardrail (Runtime)
// ---------------------------------------------------------------------------

#[test_action("bedrock-runtime", "ApplyGuardrail", checksum = "ab609d3b")]
#[tokio::test]
async fn bedrock_apply_guardrail_conformance() {
    let server = TestServer::start().await;
    let bedrock_client = server.bedrock_client().await;
    let runtime_client = server.bedrock_runtime_client().await;

    let resp = bedrock_client
        .create_guardrail()
        .name("apply-conf")
        .blocked_input_messaging("blocked")
        .blocked_outputs_messaging("blocked")
        .word_policy_config(
            aws_sdk_bedrock::types::GuardrailWordPolicyConfig::builder()
                .words_config(
                    aws_sdk_bedrock::types::GuardrailWordConfig::builder()
                        .text("badword")
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();
    let guardrail_id = resp.guardrail_id().to_string();

    let version_resp = bedrock_client
        .create_guardrail_version()
        .guardrail_identifier(&guardrail_id)
        .send()
        .await
        .unwrap();

    let result = runtime_client
        .apply_guardrail()
        .guardrail_identifier(&guardrail_id)
        .guardrail_version(version_resp.version())
        .source(aws_sdk_bedrockruntime::types::GuardrailContentSource::Input)
        .content(aws_sdk_bedrockruntime::types::GuardrailContentBlock::Text(
            aws_sdk_bedrockruntime::types::GuardrailTextBlock::builder()
                .text("this has badword in it")
                .build()
                .unwrap(),
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(result.action().as_str(), "GUARDRAIL_INTERVENED");
}

#[test_action("bedrock-runtime", "CountTokens", checksum = "6f28bb5c")]
#[tokio::test]
async fn bedrock_count_tokens_conformance() {
    let server = TestServer::start().await;
    let http_client = reqwest::Client::new();

    let body = serde_json::json!({
        "input": {
            "converse": {
                "messages": [
                    {"role": "user", "content": [{"text": "Hello world"}]}
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
    assert!(result["inputTokens"].as_i64().unwrap() > 0);
}

#[test_action("bedrock-runtime", "StartAsyncInvoke", checksum = "bee22eb2")]
#[test_action("bedrock-runtime", "GetAsyncInvoke", checksum = "fa2624ed")]
#[test_action("bedrock-runtime", "ListAsyncInvokes", checksum = "b76e6e1c")]
#[tokio::test]
async fn bedrock_async_invoke_conformance() {
    let server = TestServer::start().await;
    let http_client = reqwest::Client::new();

    let body = serde_json::json!({
        "modelId": "anthropic.claude-3-5-sonnet-20241022-v2:0",
        "modelInput": {"messages": [{"role": "user", "content": "Hello"}]},
        "outputDataConfig": {"s3OutputDataConfig": {"s3Uri": "s3://bucket/out/"}}
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
    let arn = result["invocationArn"].as_str().unwrap();
    assert!(arn.contains("async-invoke/"));

    // Get
    let resp = http_client
        .get(format!(
            "{}/async-invoke/{}",
            server.endpoint(),
            arn.rsplit('/').next().unwrap()
        ))
        .header(
            "authorization",
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20260411/us-east-1/bedrock/aws4_request, SignedHeaders=host, Signature=fake",
        )
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // List
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
    assert!(!result["asyncInvokeSummaries"]
        .as_array()
        .unwrap()
        .is_empty());
}

#[test_action(
    "bedrock-runtime",
    "InvokeModelWithBidirectionalStream",
    checksum = "6b0e9775"
)]
#[tokio::test]
async fn bedrock_bidirectional_stream_conformance() {
    let server = TestServer::start().await;
    let http_client = reqwest::Client::new();

    let body = serde_json::json!({"messages": [{"role": "user", "content": "Hello"}]});

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
    assert_eq!(
        resp.headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap(),
        "application/vnd.amazon.eventstream"
    );
}

// ---------------------------------------------------------------------------
// Automated Reasoning Policies — Core
// ---------------------------------------------------------------------------

#[test_action("bedrock", "CreateAutomatedReasoningPolicy", checksum = "f393af04")]
#[test_action("bedrock", "GetAutomatedReasoningPolicy", checksum = "72f1e179")]
#[test_action("bedrock", "ListAutomatedReasoningPolicies", checksum = "8f74b1fa")]
#[test_action("bedrock", "UpdateAutomatedReasoningPolicy", checksum = "152863e5")]
#[test_action("bedrock", "DeleteAutomatedReasoningPolicy", checksum = "395df130")]
#[test_action(
    "bedrock",
    "CreateAutomatedReasoningPolicyVersion",
    checksum = "0d12d2b2"
)]
#[test_action(
    "bedrock",
    "ExportAutomatedReasoningPolicyVersion",
    checksum = "07d318c1"
)]
#[tokio::test]
async fn bedrock_automated_reasoning_policy_lifecycle() {
    let server = TestServer::start().await;
    let h = reqwest::Client::new();
    let a = "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20260411/us-east-1/bedrock/aws4_request, SignedHeaders=host, Signature=fake";
    let b = serde_json::json!({"policyName": "conf-policy", "policyDocument": {"rules": []}});
    let r = h
        .post(format!(
            "{}/automated-reasoning-policies",
            server.endpoint()
        ))
        .header("content-type", "application/json")
        .header("authorization", a)
        .body(serde_json::to_string(&b).unwrap())
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 201);
    let v: serde_json::Value = r.json().await.unwrap();
    let arn = v["policyArn"].as_str().unwrap().to_string();
    let id = arn.rsplit('/').next().unwrap();
    assert_eq!(
        h.get(format!(
            "{}/automated-reasoning-policies/{}",
            server.endpoint(),
            id
        ))
        .header("authorization", a)
        .send()
        .await
        .unwrap()
        .status(),
        200
    );
    assert_eq!(
        h.get(format!(
            "{}/automated-reasoning-policies",
            server.endpoint()
        ))
        .header("authorization", a)
        .send()
        .await
        .unwrap()
        .status(),
        200
    );
    let b = serde_json::json!({"description": "updated"});
    assert_eq!(
        h.patch(format!(
            "{}/automated-reasoning-policies/{}",
            server.endpoint(),
            id
        ))
        .header("content-type", "application/json")
        .header("authorization", a)
        .body(serde_json::to_string(&b).unwrap())
        .send()
        .await
        .unwrap()
        .status(),
        200
    );
    assert_eq!(
        h.post(format!(
            "{}/automated-reasoning-policies/{}/versions",
            server.endpoint(),
            id
        ))
        .header("content-type", "application/json")
        .header("authorization", a)
        .body("{}")
        .send()
        .await
        .unwrap()
        .status(),
        201
    );
    assert_eq!(
        h.get(format!(
            "{}/automated-reasoning-policies/{}/export",
            server.endpoint(),
            id
        ))
        .header("authorization", a)
        .send()
        .await
        .unwrap()
        .status(),
        200
    );
    assert_eq!(
        h.delete(format!(
            "{}/automated-reasoning-policies/{}",
            server.endpoint(),
            id
        ))
        .header("authorization", a)
        .send()
        .await
        .unwrap()
        .status(),
        200
    );
}

#[test_action(
    "bedrock",
    "CreateAutomatedReasoningPolicyTestCase",
    checksum = "dbbd6c0c"
)]
#[test_action(
    "bedrock",
    "GetAutomatedReasoningPolicyTestCase",
    checksum = "8847960f"
)]
#[test_action(
    "bedrock",
    "ListAutomatedReasoningPolicyTestCases",
    checksum = "61acc65a"
)]
#[test_action(
    "bedrock",
    "UpdateAutomatedReasoningPolicyTestCase",
    checksum = "ee06d0d0"
)]
#[test_action(
    "bedrock",
    "DeleteAutomatedReasoningPolicyTestCase",
    checksum = "ec4f201a"
)]
#[tokio::test]
async fn bedrock_automated_reasoning_test_case_lifecycle() {
    let server = TestServer::start().await;
    let h = reqwest::Client::new();
    let a = "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20260411/us-east-1/bedrock/aws4_request, SignedHeaders=host, Signature=fake";
    // Create a policy first
    let b = serde_json::json!({"policyName": "tc-policy", "policyDocument": {}});
    let r = h
        .post(format!(
            "{}/automated-reasoning-policies",
            server.endpoint()
        ))
        .header("content-type", "application/json")
        .header("authorization", a)
        .body(serde_json::to_string(&b).unwrap())
        .send()
        .await
        .unwrap();
    let v: serde_json::Value = r.json().await.unwrap();
    let policy_id = v["policyArn"]
        .as_str()
        .unwrap()
        .rsplit('/')
        .next()
        .unwrap()
        .to_string();
    // Create test case
    let b = serde_json::json!({"testCaseName": "conf-tc", "input": {"query": "test"}, "expectedOutput": {"result": true}});
    let r = h
        .post(format!(
            "{}/automated-reasoning-policies/{}/test-cases",
            server.endpoint(),
            policy_id
        ))
        .header("content-type", "application/json")
        .header("authorization", a)
        .body(serde_json::to_string(&b).unwrap())
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 201);
    let v: serde_json::Value = r.json().await.unwrap();
    let tc_id = v["testCaseId"].as_str().unwrap();
    assert_eq!(
        h.get(format!(
            "{}/automated-reasoning-policies/{}/test-cases/{}",
            server.endpoint(),
            policy_id,
            tc_id
        ))
        .header("authorization", a)
        .send()
        .await
        .unwrap()
        .status(),
        200
    );
    assert_eq!(
        h.get(format!(
            "{}/automated-reasoning-policies/{}/test-cases",
            server.endpoint(),
            policy_id
        ))
        .header("authorization", a)
        .send()
        .await
        .unwrap()
        .status(),
        200
    );
    let b = serde_json::json!({"description": "updated tc"});
    assert_eq!(
        h.patch(format!(
            "{}/automated-reasoning-policies/{}/test-cases/{}",
            server.endpoint(),
            policy_id,
            tc_id
        ))
        .header("content-type", "application/json")
        .header("authorization", a)
        .body(serde_json::to_string(&b).unwrap())
        .send()
        .await
        .unwrap()
        .status(),
        200
    );
    assert_eq!(
        h.delete(format!(
            "{}/automated-reasoning-policies/{}/test-cases/{}",
            server.endpoint(),
            policy_id,
            tc_id
        ))
        .header("authorization", a)
        .send()
        .await
        .unwrap()
        .status(),
        200
    );
}

// ---------------------------------------------------------------------------
// Marketplace + Agreements + Enforced Guardrails + Misc
// ---------------------------------------------------------------------------

#[test_action("bedrock", "CreateMarketplaceModelEndpoint", checksum = "04141127")]
#[test_action("bedrock", "GetMarketplaceModelEndpoint", checksum = "2c3c9e3a")]
#[test_action("bedrock", "ListMarketplaceModelEndpoints", checksum = "38615071")]
#[test_action("bedrock", "UpdateMarketplaceModelEndpoint", checksum = "f27ced48")]
#[test_action("bedrock", "DeleteMarketplaceModelEndpoint", checksum = "f9674669")]
#[test_action("bedrock", "RegisterMarketplaceModelEndpoint", checksum = "12b80572")]
#[test_action("bedrock", "DeregisterMarketplaceModelEndpoint", checksum = "3abacdc6")]
#[tokio::test]
async fn bedrock_marketplace_endpoint_lifecycle() {
    let server = TestServer::start().await;
    let h = reqwest::Client::new();
    let a = "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20260411/us-east-1/bedrock/aws4_request, SignedHeaders=host, Signature=fake";
    let b = serde_json::json!({"endpointName": "conf-ep", "modelSourceIdentifier": "model-1"});
    let r = h
        .post(format!("{}/marketplace-model/endpoints", server.endpoint()))
        .header("content-type", "application/json")
        .header("authorization", a)
        .body(serde_json::to_string(&b).unwrap())
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 201);
    let v: serde_json::Value = r.json().await.unwrap();
    let arn = v["marketplaceModelEndpointArn"]
        .as_str()
        .unwrap()
        .to_string();
    let id = arn.rsplit('/').next().unwrap();
    assert_eq!(
        h.get(format!(
            "{}/marketplace-model/endpoints/{}",
            server.endpoint(),
            id
        ))
        .header("authorization", a)
        .send()
        .await
        .unwrap()
        .status(),
        200
    );
    assert_eq!(
        h.get(format!("{}/marketplace-model/endpoints", server.endpoint()))
            .header("authorization", a)
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    let b = serde_json::json!({"endpointConfig": {"sageMakerEndpoint": {}}});
    assert_eq!(
        h.patch(format!(
            "{}/marketplace-model/endpoints/{}",
            server.endpoint(),
            id
        ))
        .header("content-type", "application/json")
        .header("authorization", a)
        .body(serde_json::to_string(&b).unwrap())
        .send()
        .await
        .unwrap()
        .status(),
        200
    );
    assert_eq!(
        h.post(format!(
            "{}/marketplace-model/endpoints/{}/registration",
            server.endpoint(),
            id
        ))
        .header("authorization", a)
        .send()
        .await
        .unwrap()
        .status(),
        200
    );
    assert_eq!(
        h.delete(format!(
            "{}/marketplace-model/endpoints/{}/registration",
            server.endpoint(),
            id
        ))
        .header("authorization", a)
        .send()
        .await
        .unwrap()
        .status(),
        200
    );
    assert_eq!(
        h.delete(format!(
            "{}/marketplace-model/endpoints/{}",
            server.endpoint(),
            id
        ))
        .header("authorization", a)
        .send()
        .await
        .unwrap()
        .status(),
        200
    );
}

#[test_action("bedrock", "CreateFoundationModelAgreement", checksum = "73bee202")]
#[test_action("bedrock", "DeleteFoundationModelAgreement", checksum = "4e58b5fc")]
#[test_action("bedrock", "ListFoundationModelAgreementOffers", checksum = "452337b9")]
#[test_action("bedrock", "GetFoundationModelAvailability", checksum = "f22978bc")]
#[test_action("bedrock", "GetUseCaseForModelAccess", checksum = "4bb00bb9")]
#[test_action("bedrock", "PutUseCaseForModelAccess", checksum = "4c020e02")]
#[tokio::test]
async fn bedrock_agreements_and_misc() {
    let server = TestServer::start().await;
    let h = reqwest::Client::new();
    let a = "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20260411/us-east-1/bedrock/aws4_request, SignedHeaders=host, Signature=fake";
    let b = serde_json::json!({"modelId": "anthropic.claude-3-5-sonnet-20241022-v2:0"});
    assert_eq!(
        h.post(format!(
            "{}/create-foundation-model-agreement",
            server.endpoint()
        ))
        .header("content-type", "application/json")
        .header("authorization", a)
        .body(serde_json::to_string(&b).unwrap())
        .send()
        .await
        .unwrap()
        .status(),
        200
    );
    assert_eq!(
        h.post(format!(
            "{}/delete-foundation-model-agreement",
            server.endpoint()
        ))
        .header("content-type", "application/json")
        .header("authorization", a)
        .body(serde_json::to_string(&b).unwrap())
        .send()
        .await
        .unwrap()
        .status(),
        200
    );
    assert_eq!(
        h.get(format!(
            "{}/list-foundation-model-agreement-offers/anthropic.claude-3-5-sonnet-20241022-v2:0",
            server.endpoint()
        ))
        .header("authorization", a)
        .send()
        .await
        .unwrap()
        .status(),
        200
    );
    assert_eq!(
        h.get(format!(
            "{}/foundation-model-availability/anthropic.claude-3-5-sonnet-20241022-v2:0",
            server.endpoint()
        ))
        .header("authorization", a)
        .send()
        .await
        .unwrap()
        .status(),
        200
    );
    let b = serde_json::json!({"useCase": "testing"});
    assert_eq!(
        h.post(format!("{}/use-case-for-model-access", server.endpoint()))
            .header("content-type", "application/json")
            .header("authorization", a)
            .body(serde_json::to_string(&b).unwrap())
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    assert_eq!(
        h.get(format!("{}/use-case-for-model-access", server.endpoint()))
            .header("authorization", a)
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
}

#[test_action("bedrock", "PutEnforcedGuardrailConfiguration", checksum = "c1a65cc8")]
#[test_action(
    "bedrock",
    "ListEnforcedGuardrailsConfiguration",
    checksum = "897cb129"
)]
#[test_action(
    "bedrock",
    "DeleteEnforcedGuardrailConfiguration",
    checksum = "28e115e4"
)]
#[tokio::test]
async fn bedrock_enforced_guardrails() {
    let server = TestServer::start().await;
    let h = reqwest::Client::new();
    let a = "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20260411/us-east-1/bedrock/aws4_request, SignedHeaders=host, Signature=fake";
    let b = serde_json::json!({"guardrailIdentifier": "test-guard", "guardrailVersion": "1"});
    let r = h
        .put(format!(
            "{}/enforcedGuardrailsConfiguration",
            server.endpoint()
        ))
        .header("content-type", "application/json")
        .header("authorization", a)
        .body(serde_json::to_string(&b).unwrap())
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let v: serde_json::Value = r.json().await.unwrap();
    let config_id = v["configId"].as_str().unwrap();
    assert_eq!(
        h.get(format!(
            "{}/enforcedGuardrailsConfiguration",
            server.endpoint()
        ))
        .header("authorization", a)
        .send()
        .await
        .unwrap()
        .status(),
        200
    );
    assert_eq!(
        h.delete(format!(
            "{}/enforcedGuardrailsConfiguration/{}",
            server.endpoint(),
            config_id
        ))
        .header("authorization", a)
        .send()
        .await
        .unwrap()
        .status(),
        200
    );
}

// ---------------------------------------------------------------------------
// Inference Profiles + Prompt Routers + Resource Policies
// ---------------------------------------------------------------------------

#[test_action("bedrock", "CreateInferenceProfile", checksum = "388f64f4")]
#[test_action("bedrock", "GetInferenceProfile", checksum = "fe429d75")]
#[test_action("bedrock", "ListInferenceProfiles", checksum = "bfad176c")]
#[test_action("bedrock", "DeleteInferenceProfile", checksum = "ce7ec24d")]
#[tokio::test]
async fn bedrock_inference_profile_crud() {
    let server = TestServer::start().await;
    let h = reqwest::Client::new();
    let a = "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20260411/us-east-1/bedrock/aws4_request, SignedHeaders=host, Signature=fake";
    let b = serde_json::json!({"inferenceProfileName": "conf-profile", "modelSource": {"copyFrom": "arn:aws:bedrock:us-east-1::foundation-model/anthropic.claude-3-5-sonnet-20241022-v2:0"}});
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
    let arn = v["inferenceProfileArn"].as_str().unwrap().to_string();
    let id = arn.rsplit('/').next().unwrap();
    let r = h
        .get(format!("{}/inference-profiles/{}", server.endpoint(), id))
        .header("authorization", a)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
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
}

#[test_action("bedrock", "CreatePromptRouter", checksum = "fafd4f61")]
#[test_action("bedrock", "GetPromptRouter", checksum = "2dd3ec96")]
#[test_action("bedrock", "ListPromptRouters", checksum = "a5214114")]
#[test_action("bedrock", "DeletePromptRouter", checksum = "c3558acb")]
#[tokio::test]
async fn bedrock_prompt_router_crud() {
    let server = TestServer::start().await;
    let h = reqwest::Client::new();
    let a = "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20260411/us-east-1/bedrock/aws4_request, SignedHeaders=host, Signature=fake";
    let b = serde_json::json!({"promptRouterName": "conf-router", "models": [{"modelIdentifier": "anthropic.claude-3-5-sonnet-20241022-v2:0"}], "routingCriteria": {"responseQualityDifference": 0.5}, "fallbackModel": {"modelIdentifier": "anthropic.claude-3-haiku-20240307-v1:0"}});
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
    let arn = v["promptRouterArn"].as_str().unwrap().to_string();
    let id = arn.rsplit('/').next().unwrap();
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

#[test_action("bedrock", "PutResourcePolicy", checksum = "63f426a5")]
#[test_action("bedrock", "GetResourcePolicy", checksum = "fbded8d2")]
#[test_action("bedrock", "DeleteResourcePolicy", checksum = "83805f9a")]
#[tokio::test]
async fn bedrock_resource_policy_crud() {
    let server = TestServer::start().await;
    let h = reqwest::Client::new();
    let a = "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20260411/us-east-1/bedrock/aws4_request, SignedHeaders=host, Signature=fake";
    let arn = "arn:aws:bedrock:us-east-1:123456789012:custom-model/test";
    let b = serde_json::json!({"resourceArn": arn, "resourcePolicy": "{\"Version\":\"2012-10-17\",\"Statement\":[]}"});
    let r = h
        .post(format!("{}/resource-policy", server.endpoint()))
        .header("content-type", "application/json")
        .header("authorization", a)
        .body(serde_json::to_string(&b).unwrap())
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let r = h
        .get(format!(
            "{}/resource-policy/{}",
            server.endpoint(),
            arn.replace(':', "%3A").replace('/', "%2F")
        ))
        .header("authorization", a)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let r = h
        .delete(format!(
            "{}/resource-policy/{}",
            server.endpoint(),
            arn.replace(':', "%3A").replace('/', "%2F")
        ))
        .header("authorization", a)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
}

// ---------------------------------------------------------------------------
// Model Invocation Jobs + Evaluation Jobs
// ---------------------------------------------------------------------------

#[test_action("bedrock", "CreateModelInvocationJob", checksum = "5880c108")]
#[test_action("bedrock", "GetModelInvocationJob", checksum = "480ec99e")]
#[test_action("bedrock", "ListModelInvocationJobs", checksum = "b0e2ea2f")]
#[test_action("bedrock", "StopModelInvocationJob", checksum = "88af9f13")]
#[tokio::test]
async fn bedrock_invocation_job_lifecycle() {
    let server = TestServer::start().await;
    let http_client = reqwest::Client::new();
    let auth = "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20260411/us-east-1/bedrock/aws4_request, SignedHeaders=host, Signature=fake";

    let body = serde_json::json!({
        "jobName": "conf-inv-job",
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

    let resp = http_client
        .get(format!("{}/model-invocation-jobs", server.endpoint()))
        .header("authorization", auth)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

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
}

#[test_action("bedrock", "CreateEvaluationJob", checksum = "381c6747")]
#[test_action("bedrock", "GetEvaluationJob", checksum = "78e37cc5")]
#[test_action("bedrock", "ListEvaluationJobs", checksum = "90cf9a61")]
#[test_action("bedrock", "StopEvaluationJob", checksum = "3b4c4189")]
#[test_action("bedrock", "BatchDeleteEvaluationJob", checksum = "f6a3ed26")]
#[tokio::test]
async fn bedrock_evaluation_job_lifecycle() {
    let server = TestServer::start().await;
    let http_client = reqwest::Client::new();
    let auth = "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20260411/us-east-1/bedrock/aws4_request, SignedHeaders=host, Signature=fake";

    let body = serde_json::json!({
        "jobName": "conf-eval-job",
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

    let resp = http_client
        .get(format!("{}/evaluation-jobs", server.endpoint()))
        .header("authorization", auth)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

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
}

// ---------------------------------------------------------------------------
// Model Import
// ---------------------------------------------------------------------------

#[test_action("bedrock", "CreateModelImportJob", checksum = "aa03dfa3")]
#[test_action("bedrock", "GetModelImportJob", checksum = "49328baf")]
#[test_action("bedrock", "ListModelImportJobs", checksum = "77700f04")]
#[test_action("bedrock", "GetImportedModel", checksum = "4043fd91")]
#[test_action("bedrock", "ListImportedModels", checksum = "7ece5cb8")]
#[test_action("bedrock", "DeleteImportedModel", checksum = "c62da780")]
#[tokio::test]
async fn bedrock_model_import_lifecycle() {
    let server = TestServer::start().await;
    let http_client = reqwest::Client::new();
    let auth = "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20260411/us-east-1/bedrock/aws4_request, SignedHeaders=host, Signature=fake";

    let body = serde_json::json!({
        "jobName": "conf-import",
        "importedModelName": "conf-imported",
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

    // Get job
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

    // List jobs
    let resp = http_client
        .get(format!("{}/model-import-jobs", server.endpoint()))
        .header("authorization", auth)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // Get imported model
    let resp = http_client
        .get(format!(
            "{}/imported-models/conf-imported",
            server.endpoint()
        ))
        .header("authorization", auth)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // List imported models
    let resp = http_client
        .get(format!("{}/imported-models", server.endpoint()))
        .header("authorization", auth)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // Delete imported model
    let resp = http_client
        .delete(format!(
            "{}/imported-models/conf-imported",
            server.endpoint()
        ))
        .header("authorization", auth)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
}

#[test_action("bedrock", "CreateModelCopyJob", checksum = "75268069")]
#[test_action("bedrock", "GetModelCopyJob", checksum = "b0dec5ba")]
#[test_action("bedrock", "ListModelCopyJobs", checksum = "454a776c")]
#[tokio::test]
async fn bedrock_model_copy_lifecycle() {
    let server = TestServer::start().await;
    let http_client = reqwest::Client::new();
    let auth = "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20260411/us-east-1/bedrock/aws4_request, SignedHeaders=host, Signature=fake";

    let body = serde_json::json!({
        "sourceModelArn": "arn:aws:bedrock:us-east-1:123456789012:custom-model/source",
        "targetModelName": "conf-copy-target"
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

// ---------------------------------------------------------------------------
// Custom Models
// ---------------------------------------------------------------------------

#[test_action("bedrock", "CreateCustomModel", checksum = "4887448e")]
#[test_action("bedrock", "GetCustomModel", checksum = "93aaf6da")]
#[test_action("bedrock", "ListCustomModels", checksum = "0966941c")]
#[test_action("bedrock", "DeleteCustomModel", checksum = "287a1b06")]
#[tokio::test]
async fn bedrock_custom_model_crud() {
    let server = TestServer::start().await;
    let http_client = reqwest::Client::new();
    let auth = "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20260411/us-east-1/bedrock/aws4_request, SignedHeaders=host, Signature=fake";

    let body = serde_json::json!({"modelName": "conf-model", "modelSourceConfig": {}});
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
    let arn = result["modelArn"].as_str().unwrap().to_string();
    let id = arn.rsplit('/').next().unwrap();

    let resp = http_client
        .get(format!("{}/custom-models/{}", server.endpoint(), id))
        .header("authorization", auth)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let resp = http_client
        .get(format!("{}/custom-models", server.endpoint()))
        .header("authorization", auth)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let resp = http_client
        .delete(format!("{}/custom-models/{}", server.endpoint(), id))
        .header("authorization", auth)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
}

#[test_action("bedrock", "CreateCustomModelDeployment", checksum = "798775b9")]
#[test_action("bedrock", "GetCustomModelDeployment", checksum = "452bbfdb")]
#[test_action("bedrock", "ListCustomModelDeployments", checksum = "8b36e796")]
#[test_action("bedrock", "UpdateCustomModelDeployment", checksum = "860d20f9")]
#[test_action("bedrock", "DeleteCustomModelDeployment", checksum = "74d8fc2e")]
#[tokio::test]
async fn bedrock_custom_model_deployment_crud() {
    let server = TestServer::start().await;
    let http_client = reqwest::Client::new();
    let auth = "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20260411/us-east-1/bedrock/aws4_request, SignedHeaders=host, Signature=fake";

    let body = serde_json::json!({
        "modelDeploymentName": "conf-deployment",
        "modelArn": "arn:aws:bedrock:us-east-1:123456789012:custom-model/test"
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
    let arn = result["customModelDeploymentArn"]
        .as_str()
        .unwrap()
        .to_string();
    let id = arn.rsplit('/').next().unwrap();

    let resp = http_client
        .get(format!(
            "{}/model-customization/custom-model-deployments/{}",
            server.endpoint(),
            id
        ))
        .header("authorization", auth)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

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

    let body = serde_json::json!({"modelArn": "arn:aws:bedrock:us-east-1:123456789012:custom-model/updated"});
    let resp = http_client
        .patch(format!(
            "{}/model-customization/custom-model-deployments/{}",
            server.endpoint(),
            id
        ))
        .header("content-type", "application/json")
        .header("authorization", auth)
        .body(serde_json::to_string(&body).unwrap())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let resp = http_client
        .delete(format!(
            "{}/model-customization/custom-model-deployments/{}",
            server.endpoint(),
            id
        ))
        .header("authorization", auth)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
}

// ---------------------------------------------------------------------------
// Streaming (raw HTTP since SDK event stream parsing is complex)
// ---------------------------------------------------------------------------

#[test_action(
    "bedrock-runtime",
    "InvokeModelWithResponseStream",
    checksum = "b594a2e9"
)]
#[tokio::test]
async fn bedrock_invoke_model_with_response_stream() {
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
    let body_bytes = resp.bytes().await.unwrap();
    assert!(body_bytes.len() > 16);
}

#[test_action("bedrock-runtime", "ConverseStream", checksum = "94a08bea")]
#[tokio::test]
async fn bedrock_converse_stream() {
    let server = TestServer::start().await;
    let http_client = reqwest::Client::new();

    let body = serde_json::json!({
        "modelId": "anthropic.claude-3-5-sonnet-20241022-v2:0",
        "messages": [{"role": "user", "content": [{"text": "Hello"}]}]
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
    let body_bytes = resp.bytes().await.unwrap();
    assert!(body_bytes.len() > 100);
}
