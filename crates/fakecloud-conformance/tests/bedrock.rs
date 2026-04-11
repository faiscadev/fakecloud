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
