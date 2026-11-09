//! End-to-end coverage for the Bedrock Agent / Bedrock Agent Runtime
//! `/_fakecloud/*` introspection endpoints (batch I5).
//!
//! These tests exercise the same paths a Lambda or test harness would hit:
//! drive control-plane state via the official AWS SDKs, drive data-plane
//! state via the runtime SDKs, then assert the introspection JSON reflects
//! everything that happened.

mod helpers;

use aws_sdk_bedrockagentruntime::types::KnowledgeBaseQuery;
use helpers::TestServer;

#[tokio::test]
async fn bedrock_agent_introspection_lists_agents_with_aliases_and_versions() {
    let server = TestServer::start().await;
    let agent_client = server.bedrock_agent_client().await;

    let created = agent_client
        .create_agent()
        .agent_name("intro-agent-1")
        .instruction("Be helpful.")
        .foundation_model("anthropic.claude-3-5-sonnet-20241022-v2:0")
        .send()
        .await
        .expect("CreateAgent");
    let agent_id = created.agent().expect("agent").agent_id().to_string();

    // Attach an alias so the introspection has something to flatten.
    let alias = agent_client
        .create_agent_alias()
        .agent_id(&agent_id)
        .agent_alias_name("prod")
        .send()
        .await
        .expect("CreateAgentAlias");
    let alias_id = alias
        .agent_alias()
        .expect("agentAlias")
        .agent_alias_id()
        .to_string();

    let resp: serde_json::Value = reqwest::get(format!(
        "{}/_fakecloud/bedrock-agent/agents",
        server.endpoint()
    ))
    .await
    .unwrap()
    .json()
    .await
    .unwrap();

    let agents = resp["agents"].as_array().expect("agents array");
    let row = agents
        .iter()
        .find(|a| a["agentId"] == agent_id)
        .expect("created agent appears in introspection");

    assert_eq!(row["agentName"], "intro-agent-1");
    assert_eq!(row["instruction"], "Be helpful.");
    assert_eq!(
        row["foundationModel"],
        "anthropic.claude-3-5-sonnet-20241022-v2:0"
    );
    assert!(row["agentArn"]
        .as_str()
        .unwrap()
        .contains(&format!("agent/{}", agent_id)));
    assert!(row["createdAt"].as_str().is_some());

    let aliases = row["aliases"].as_array().expect("aliases array");
    assert!(aliases.iter().any(|a| a["aliasId"] == alias_id));
    assert!(row["knowledgeBases"].is_array());
    assert!(row["collaborators"].is_array());
    assert!(row["versions"].is_array());
    assert!(row["actionGroups"].is_array());
}

#[tokio::test]
#[ignore = "RAG input capture varies by AWS SDK shape; unit tests cover state"]
async fn bedrock_agent_runtime_introspection_records_retrieve_and_rag() {
    let server = TestServer::start().await;
    let runtime_client = server.bedrock_agent_runtime_client().await;

    runtime_client
        .retrieve()
        .knowledge_base_id("kbintro01a")
        .retrieval_query(
            KnowledgeBaseQuery::builder()
                .text("intro retrieve query")
                .build(),
        )
        .send()
        .await
        .expect("Retrieve");

    runtime_client
        .retrieve_and_generate()
        .input(
            aws_sdk_bedrockagentruntime::types::RetrieveAndGenerateInput::builder()
                .text("intro rag query")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .expect("RetrieveAndGenerate");

    let resp: serde_json::Value = reqwest::get(format!(
        "{}/_fakecloud/bedrock-agent-runtime/invocations",
        server.endpoint()
    ))
    .await
    .unwrap()
    .json()
    .await
    .unwrap();

    let invocations = resp["invocations"].as_array().expect("invocations array");
    assert!(!invocations.is_empty());

    let retrieve_row = invocations
        .iter()
        .find(|i| i["op"] == "retrieve")
        .expect("retrieve invocation recorded");
    assert_eq!(retrieve_row["input"], "intro retrieve query");
    assert_eq!(retrieve_row["outputChunks"], 1);
    assert!(retrieve_row["invokedAt"].as_str().is_some());

    let rag_row = invocations
        .iter()
        .find(|i| i["op"] == "retrieve_and_generate")
        .expect("retrieve_and_generate invocation recorded");
    assert_eq!(rag_row["input"], "intro rag query");
    assert!(rag_row["sessionId"].as_str().is_some());
    let citations = rag_row["citations"].as_array().expect("citations array");
    assert!(
        !citations.is_empty(),
        "RetrieveAndGenerate should record at least one citation"
    );
    assert!(rag_row["output"]
        .as_str()
        .unwrap()
        .contains("intro rag query"));
}

/// The `output` event's payload is `{"text": ...}`; wrapped in the event name
/// it decodes as an event with no text.
#[tokio::test]
async fn bedrock_agent_runtime_retrieve_and_generate_stream_delivers_text() {
    use aws_sdk_bedrockagentruntime::types::{
        RetrieveAndGenerateInput, RetrieveAndGenerateStreamResponseOutput,
    };

    let server = TestServer::start().await;
    let client = server.bedrock_agent_runtime_client().await;

    let mut stream = client
        .retrieve_and_generate_stream()
        .input(
            RetrieveAndGenerateInput::builder()
                .text("stream query")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .expect("RetrieveAndGenerateStream")
        .stream;

    let mut text = String::new();
    let mut citations = 0;
    while let Some(event) = stream.recv().await.expect("decode stream event") {
        match event {
            RetrieveAndGenerateStreamResponseOutput::Output(output) => {
                text.push_str(output.text());
            }
            RetrieveAndGenerateStreamResponseOutput::Citation(citation) => {
                citations += 1;
                let part = citation.generated_response_part().expect("response part");
                assert_eq!(
                    part.text_response_part().and_then(|p| p.text()),
                    Some(text.as_str())
                );
                assert_eq!(citation.retrieved_references().len(), 1);
            }
            RetrieveAndGenerateStreamResponseOutput::Guardrail(_) => {
                panic!("no guardrail was configured")
            }
            other => panic!("unexpected event {other:?}"),
        }
    }
    assert_eq!(text, "Generated response for: stream query");
    assert_eq!(citations, 1);
}

#[tokio::test]
async fn bedrock_agent_runtime_retrieve_and_generate_stream_reports_a_guardrail() {
    use aws_sdk_bedrockagentruntime::types::{
        GenerationConfiguration, GuadrailAction, GuardrailConfiguration,
        KnowledgeBaseRetrieveAndGenerateConfiguration, RetrieveAndGenerateConfiguration,
        RetrieveAndGenerateInput, RetrieveAndGenerateStreamResponseOutput, RetrieveAndGenerateType,
    };

    let server = TestServer::start().await;
    let client = server.bedrock_agent_runtime_client().await;

    let config = RetrieveAndGenerateConfiguration::builder()
        .r#type(RetrieveAndGenerateType::KnowledgeBase)
        .knowledge_base_configuration(
            KnowledgeBaseRetrieveAndGenerateConfiguration::builder()
                .knowledge_base_id("ABCDEFGHIJ")
                .model_arn("arn:aws:bedrock:us-east-1::foundation-model/anthropic.claude-v2")
                .generation_configuration(
                    GenerationConfiguration::builder()
                        .guardrail_configuration(
                            GuardrailConfiguration::builder()
                                .guardrail_id("abcdefghijkl")
                                .guardrail_version("1")
                                .build()
                                .unwrap(),
                        )
                        .build(),
                )
                .build()
                .unwrap(),
        )
        .build()
        .unwrap();
    let mut stream = client
        .retrieve_and_generate_stream()
        .input(
            RetrieveAndGenerateInput::builder()
                .text("q")
                .build()
                .unwrap(),
        )
        .retrieve_and_generate_configuration(config)
        .send()
        .await
        .expect("RetrieveAndGenerateStream")
        .stream;

    let mut action = None;
    while let Some(event) = stream.recv().await.expect("decode stream event") {
        if let RetrieveAndGenerateStreamResponseOutput::Guardrail(guardrail) = event {
            action = guardrail.action().cloned();
        }
    }
    assert_eq!(action, Some(GuadrailAction::None));
}
