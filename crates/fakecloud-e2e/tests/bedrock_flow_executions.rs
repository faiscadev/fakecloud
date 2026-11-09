//! Bedrock Agents Runtime flow executions run real Bedrock Agents flows: an
//! execution targets an existing flow alias, runs the version that alias routes
//! to, and captures that version's definition and role in its snapshot.
//! Unknown flows, aliases and executions are `ResourceNotFoundException`.

mod helpers;

use aws_sdk_bedrockagent::types::{
    FlowAliasRoutingConfigurationListItem, FlowDefinition, FlowNode, FlowNodeConfiguration,
    FlowNodeType, InputFlowNodeConfiguration,
};
use aws_sdk_bedrockagentruntime::error::ProvideErrorMetadata;
use aws_sdk_bedrockagentruntime::types::FlowExecutionStatus;
use helpers::TestServer;

const ROLE: &str = "arn:aws:iam::123456789012:role/service-role/flow-exec-role";

fn definition(node: &str) -> FlowDefinition {
    FlowDefinition::builder()
        .nodes(
            FlowNode::builder()
                .name(node)
                .r#type(FlowNodeType::Input)
                .configuration(FlowNodeConfiguration::Input(
                    InputFlowNodeConfiguration::builder().build(),
                ))
                .build()
                .expect("flow node"),
        )
        .build()
}

fn is_not_found<E>(err: &aws_sdk_bedrockagentruntime::error::SdkError<E>) -> bool
where
    E: std::fmt::Debug + aws_sdk_bedrockagentruntime::error::ProvideErrorMetadata,
{
    err.as_service_error().and_then(|e| e.code()) == Some("ResourceNotFoundException")
}

#[tokio::test]
async fn flow_executions_run_the_aliased_version_and_capture_it() {
    let server = TestServer::start().await;
    let agent = server.bedrock_agent_client().await;
    let runtime = server.bedrock_agent_runtime_client().await;

    // A flow whose version 1 differs from the later-edited draft.
    let flow_id = agent
        .create_flow()
        .name("e2e-flow")
        .execution_role_arn(ROLE)
        .definition(definition("VersionOneInput"))
        .send()
        .await
        .expect("create flow")
        .id;
    agent
        .prepare_flow()
        .flow_identifier(&flow_id)
        .send()
        .await
        .expect("prepare flow");
    let version = agent
        .create_flow_version()
        .flow_identifier(&flow_id)
        .send()
        .await
        .expect("create flow version")
        .version;
    assert_eq!(version, "1");
    let alias_id = agent
        .create_flow_alias()
        .flow_identifier(&flow_id)
        .name("live")
        .routing_configuration(
            FlowAliasRoutingConfigurationListItem::builder()
                .flow_version("1")
                .build(),
        )
        .send()
        .await
        .expect("create flow alias")
        .id;
    agent
        .update_flow()
        .flow_identifier(&flow_id)
        .name("e2e-flow")
        .execution_role_arn(ROLE)
        .definition(definition("DraftInput"))
        .send()
        .await
        .expect("update draft");

    // The alias runs version 1, not the edited draft.
    let exec_arn = runtime
        .start_flow_execution()
        .flow_identifier(&flow_id)
        .flow_alias_identifier(&alias_id)
        .set_inputs(Some(Vec::new()))
        .send()
        .await
        .expect("start flow execution")
        .execution_arn
        .expect("execution arn");
    assert!(
        exec_arn.contains(&format!(":flow/{flow_id}/alias/{alias_id}/execution/")),
        "{exec_arn}"
    );
    let snapshot = runtime
        .get_execution_flow_snapshot()
        .flow_identifier(&flow_id)
        .flow_alias_identifier(&alias_id)
        .execution_identifier(&exec_arn)
        .send()
        .await
        .expect("get snapshot");
    assert_eq!(snapshot.flow_version(), "1");
    assert_eq!(snapshot.execution_role_arn(), ROLE);
    assert!(
        snapshot.definition().contains("VersionOneInput"),
        "{}",
        snapshot.definition()
    );
    assert!(!snapshot.definition().contains("DraftInput"));

    // The edited draft must be prepared again before the test alias runs it.
    let err = runtime
        .start_flow_execution()
        .flow_identifier(&flow_id)
        .flow_alias_identifier("TSTALIASID")
        .set_inputs(Some(Vec::new()))
        .send()
        .await
        .expect_err("unprepared draft");
    assert_eq!(
        err.as_service_error().and_then(|e| e.code()),
        Some("ValidationException"),
        "{err:?}"
    );
    agent
        .prepare_flow()
        .flow_identifier(&flow_id)
        .send()
        .await
        .expect("prepare edited draft");

    // The test alias runs the draft.
    let draft_exec = runtime
        .start_flow_execution()
        .flow_identifier(&flow_id)
        .flow_alias_identifier("TSTALIASID")
        .set_inputs(Some(Vec::new()))
        .send()
        .await
        .expect("start draft execution")
        .execution_arn
        .expect("execution arn");
    let draft = runtime
        .get_execution_flow_snapshot()
        .flow_identifier(&flow_id)
        .flow_alias_identifier("TSTALIASID")
        .execution_identifier(&draft_exec)
        .send()
        .await
        .expect("get draft snapshot");
    assert_eq!(draft.flow_version(), "DRAFT");
    assert!(draft.definition().contains("DraftInput"));

    // Stop aborts the running execution.
    let stopped = runtime
        .stop_flow_execution()
        .flow_identifier(&flow_id)
        .flow_alias_identifier(&alias_id)
        .execution_identifier(&exec_arn)
        .send()
        .await
        .expect("stop execution");
    assert_eq!(stopped.status(), &FlowExecutionStatus::Aborted);
    let got = runtime
        .get_flow_execution()
        .flow_identifier(&flow_id)
        .flow_alias_identifier(&alias_id)
        .execution_identifier(&exec_arn)
        .send()
        .await
        .expect("get execution");
    assert_eq!(got.status(), &FlowExecutionStatus::Aborted);
    assert_eq!(got.flow_version(), "1");
    assert!(got.ended_at().is_some());

    // An execution belongs to its alias: it is not found under another one.
    let err = runtime
        .get_flow_execution()
        .flow_identifier(&flow_id)
        .flow_alias_identifier("TSTALIASID")
        .execution_identifier(&exec_arn)
        .send()
        .await
        .expect_err("execution under another alias");
    assert!(is_not_found(&err), "{err:?}");

    // A named execution uses its name as its id.
    let named = runtime
        .start_flow_execution()
        .flow_identifier(&flow_id)
        .flow_alias_identifier(&alias_id)
        .flow_execution_name("nightly-run")
        .set_inputs(Some(Vec::new()))
        .send()
        .await
        .expect("start named execution")
        .execution_arn
        .expect("execution arn");
    assert!(named.ends_with("/execution/nightly-run"), "{named}");
}

/// Runtime operations that share path prefixes with the control plane reach
/// the runtime through a real SDK: InvokeInlineAgent (POST /agents/{session})
/// and tagging a runtime session (/tags/{sessionArn}).
#[tokio::test]
async fn runtime_inline_agent_and_session_tags_route_to_the_runtime() {
    let server = TestServer::start().await;
    let runtime = server.bedrock_agent_runtime_client().await;

    let session_arn = runtime
        .create_session()
        .send()
        .await
        .expect("create session")
        .session_arn;
    runtime
        .tag_resource()
        .resource_arn(&session_arn)
        .tags("team", "flows")
        .send()
        .await
        .expect("tag session");
    let tags = runtime
        .list_tags_for_resource()
        .resource_arn(&session_arn)
        .send()
        .await
        .expect("list session tags");
    assert_eq!(
        tags.tags().and_then(|t| t.get("team")).map(String::as_str),
        Some("flows")
    );
    runtime
        .untag_resource()
        .resource_arn(&session_arn)
        .tag_keys("team")
        .send()
        .await
        .expect("untag session");

    let mut stream = runtime
        .invoke_inline_agent()
        .session_id("inline-session-1")
        .foundation_model("anthropic.claude-3-haiku-20240307-v1:0")
        .instruction("You are a helpful assistant for routing tests.")
        .input_text("hello")
        .send()
        .await
        .expect("invoke inline agent");
    let mut got_event = false;
    while let Some(_event) = stream.completion.recv().await.expect("inline agent stream") {
        got_event = true;
    }
    assert!(got_event, "inline agent returned no events");
}

#[tokio::test]
async fn unknown_flows_and_executions_are_not_found() {
    let server = TestServer::start().await;
    let agent = server.bedrock_agent_client().await;
    let runtime = server.bedrock_agent_runtime_client().await;

    let err = runtime
        .start_flow_execution()
        .flow_identifier("NOSUCHFLOW")
        .flow_alias_identifier("TSTALIASID")
        .set_inputs(Some(Vec::new()))
        .send()
        .await
        .expect_err("unknown flow");
    assert!(is_not_found(&err), "{err:?}");

    let flow_id = agent
        .create_flow()
        .name("e2e-flow-2")
        .execution_role_arn(ROLE)
        .definition(definition("In"))
        .send()
        .await
        .expect("create flow")
        .id;
    let err = runtime
        .start_flow_execution()
        .flow_identifier(&flow_id)
        .flow_alias_identifier("NOSUCHALIA")
        .set_inputs(Some(Vec::new()))
        .send()
        .await
        .expect_err("unknown alias");
    assert!(is_not_found(&err), "{err:?}");

    let err = runtime
        .get_execution_flow_snapshot()
        .flow_identifier(&flow_id)
        .flow_alias_identifier("TSTALIASID")
        .execution_identifier("no-such-execution")
        .send()
        .await
        .expect_err("unknown execution snapshot");
    assert!(is_not_found(&err), "{err:?}");

    let err = runtime
        .stop_flow_execution()
        .flow_identifier(&flow_id)
        .flow_alias_identifier("TSTALIASID")
        .execution_identifier("no-such-execution")
        .send()
        .await
        .expect_err("unknown execution stop");
    assert!(is_not_found(&err), "{err:?}");
}

/// `InvokeFlow` streams the flow's output, then its completion, through the SDK.
#[tokio::test]
async fn invoke_flow_streams_output_then_completion() {
    use aws_sdk_bedrockagentruntime::types::{
        FlowCompletionReason, FlowInput, FlowInputContent, FlowOutputContent, FlowResponseStream,
    };

    let server = TestServer::start().await;
    let agent = server.bedrock_agent_client().await;
    let runtime = server.bedrock_agent_runtime_client().await;

    let flow_id = agent
        .create_flow()
        .name("stream-flow")
        .execution_role_arn(ROLE)
        .definition(definition("Input"))
        .send()
        .await
        .expect("create flow")
        .id;
    agent
        .prepare_flow()
        .flow_identifier(&flow_id)
        .send()
        .await
        .expect("prepare flow");
    agent
        .create_flow_version()
        .flow_identifier(&flow_id)
        .send()
        .await
        .expect("create flow version");
    let alias_id = agent
        .create_flow_alias()
        .flow_identifier(&flow_id)
        .name("live")
        .routing_configuration(
            FlowAliasRoutingConfigurationListItem::builder()
                .flow_version("1")
                .build(),
        )
        .send()
        .await
        .expect("create flow alias")
        .id;

    let mut stream = runtime
        .invoke_flow()
        .flow_identifier(&flow_id)
        .flow_alias_identifier(&alias_id)
        .inputs(
            FlowInput::builder()
                .node_name("Input")
                .node_output_name("document")
                .content(FlowInputContent::Document(
                    aws_smithy_types::Document::String("hi".to_string()),
                ))
                .build()
                .unwrap(),
        )
        .send()
        .await
        .expect("invoke flow")
        .response_stream;

    let mut events = Vec::new();
    while let Some(event) = stream.recv().await.expect("decode stream event") {
        events.push(event);
    }
    assert_eq!(events.len(), 2, "{events:?}");
    let FlowResponseStream::FlowOutputEvent(output) = &events[0] else {
        panic!("expected flowOutputEvent first: {events:?}");
    };
    assert!(!output.node_name().is_empty());
    assert!(matches!(
        output.content(),
        Some(FlowOutputContent::Document(_))
    ));
    let FlowResponseStream::FlowCompletionEvent(completion) = &events[1] else {
        panic!("expected flowCompletionEvent last: {events:?}");
    };
    assert_eq!(
        completion.completion_reason(),
        &FlowCompletionReason::Success
    );
}

/// The recorded `enableTrace` stream of a Start -> End flow: the input node's
/// output trace, the output node's input trace, the output, then completion.
#[tokio::test]
async fn invoke_flow_trace_matches_the_recording() {
    let server = TestServer::start().await;
    let agent = server.bedrock_agent_client().await;
    let endpoint = server.endpoint().to_string();

    let definition = serde_json::from_value::<serde_json::Value>(serde_json::json!({
        "nodes": [
            {
                "name": "Start",
                "type": "Input",
                "configuration": {"input": {}},
                "outputs": [{"name": "document", "type": "String"}]
            },
            {
                "name": "End",
                "type": "Output",
                "configuration": {"output": {}},
                "inputs": [{"name": "document", "type": "String", "expression": "$.data"}]
            }
        ],
        "connections": [{
            "name": "c",
            "source": "Start",
            "target": "End",
            "type": "Data",
            "configuration": {"data": {"sourceOutput": "document", "targetInput": "document"}}
        }]
    }))
    .unwrap();

    let auth = "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20260411/us-east-1/bedrock/aws4_request, SignedHeaders=host, Signature=fake";
    let http = reqwest::Client::new();
    let created: serde_json::Value = http
        .post(format!("{endpoint}/flows/"))
        .header("content-type", "application/json")
        .header("authorization", auth)
        .body(
            serde_json::json!({
                "name": "recorded-flow",
                "executionRoleArn": ROLE,
                "definition": definition
            })
            .to_string(),
        )
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let flow_id = created["id"].as_str().expect("flow id").to_string();
    agent
        .prepare_flow()
        .flow_identifier(&flow_id)
        .send()
        .await
        .expect("prepare flow");

    let frames = http
        .post(format!("{endpoint}/flows/{flow_id}/aliases/TSTALIASID"))
        .header("content-type", "application/json")
        .header("authorization", auth)
        .body(
            serde_json::json!({
                "enableTrace": true,
                "inputs": [{
                    "nodeName": "Start",
                    "nodeOutputName": "document",
                    "content": {"document": "hello"}
                }]
            })
            .to_string(),
        )
        .send()
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();

    let events = decode_events(&frames);
    let types: Vec<&str> = events.iter().map(|(t, _)| t.as_str()).collect();
    assert_eq!(
        types,
        [
            "flowTraceEvent",
            "flowTraceEvent",
            "flowOutputEvent",
            "flowCompletionEvent"
        ]
    );

    let mut output_trace = events[0].1["trace"]["nodeOutputTrace"].clone();
    assert!(output_trace["timestamp"].is_string());
    output_trace.as_object_mut().unwrap().remove("timestamp");
    assert_eq!(
        output_trace,
        serde_json::json!({
            "fields": [{
                "content": {"document": "hello"},
                "next": [{"inputFieldName": "document", "nodeName": "End"}],
                "nodeOutputName": "document",
                "type": "STRING"
            }],
            "nodeName": "Start"
        })
    );
    let mut input_trace = events[1].1["trace"]["nodeInputTrace"].clone();
    input_trace.as_object_mut().unwrap().remove("timestamp");
    assert_eq!(
        input_trace,
        serde_json::json!({
            "fields": [{
                "content": {"document": "hello"},
                "nodeInputName": "document",
                "source": {"expression": "$.data", "nodeName": "Start", "outputFieldName": "document"},
                "type": "STRING"
            }],
            "nodeName": "End"
        })
    );
    assert_eq!(
        events[2].1,
        serde_json::json!({"content": {"document": "hello"}, "nodeName": "End"})
    );
    assert_eq!(
        events[3].1,
        serde_json::json!({"completionReason": "SUCCESS"})
    );
}

/// Each event-stream frame's `:event-type` and JSON payload.
fn decode_events(mut buf: &[u8]) -> Vec<(String, serde_json::Value)> {
    let mut out = Vec::new();
    while !buf.is_empty() {
        let total = u32::from_be_bytes(buf[0..4].try_into().unwrap()) as usize;
        let headers_len = u32::from_be_bytes(buf[4..8].try_into().unwrap()) as usize;
        let mut headers = &buf[12..12 + headers_len];
        let mut event_type = String::new();
        while !headers.is_empty() {
            let name_len = headers[0] as usize;
            let name = &headers[1..1 + name_len];
            let value_at = 2 + name_len;
            let value_len = u16::from_be_bytes([headers[value_at], headers[value_at + 1]]) as usize;
            let value = &headers[value_at + 2..value_at + 2 + value_len];
            if name == b":event-type" {
                event_type = String::from_utf8(value.to_vec()).unwrap();
            }
            headers = &headers[value_at + 2 + value_len..];
        }
        out.push((
            event_type,
            serde_json::from_slice(&buf[12 + headers_len..total - 4]).unwrap(),
        ));
        buf = &buf[total..];
    }
    out
}
