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
    assert!(resp.guardrail_arn().contains("guardrail/"));

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
