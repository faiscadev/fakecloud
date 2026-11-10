//! Bedrock Agents Runtime state (sessions, invocations, invocation steps, flow
//! executions and tags) survives a restart in persistent mode. The
//! `/_fakecloud/bedrock-agent-runtime/invocations` introspection log does not.

mod helpers;

use aws_sdk_bedrockagent::types::{
    FlowAliasRoutingConfigurationListItem, FlowConnection, FlowConnectionConfiguration,
    FlowConnectionType, FlowDataConnectionConfiguration, FlowDefinition, FlowNode,
    FlowNodeConfiguration, FlowNodeInput, FlowNodeIoDataType, FlowNodeOutput, FlowNodeType,
    InputFlowNodeConfiguration, OutputFlowNodeConfiguration,
};
use aws_sdk_bedrockagentruntime::primitives::DateTime;
use aws_sdk_bedrockagentruntime::types::{
    BedrockSessionContentBlock, FlowExecutionStatus, InvocationStepPayload, SessionStatus,
};
use helpers::TestServer;

const ROLE: &str = "arn:aws:iam::123456789012:role/service-role/persist-flow-role";

/// A flow that prepares cleanly: Input node `PersistInput` passes its document
/// straight to an Output node.
fn definition() -> FlowDefinition {
    FlowDefinition::builder()
        .nodes(
            FlowNode::builder()
                .name("PersistInput")
                .r#type(FlowNodeType::Input)
                .configuration(FlowNodeConfiguration::Input(
                    InputFlowNodeConfiguration::builder().build(),
                ))
                .outputs(
                    FlowNodeOutput::builder()
                        .name("document")
                        .r#type(FlowNodeIoDataType::String)
                        .build()
                        .expect("node output"),
                )
                .build()
                .expect("flow node"),
        )
        .nodes(
            FlowNode::builder()
                .name("FlowOutputNode")
                .r#type(FlowNodeType::Output)
                .configuration(FlowNodeConfiguration::Output(
                    OutputFlowNodeConfiguration::builder().build(),
                ))
                .inputs(
                    FlowNodeInput::builder()
                        .name("document")
                        .r#type(FlowNodeIoDataType::String)
                        .expression("$.data")
                        .build()
                        .expect("node input"),
                )
                .build()
                .expect("flow node"),
        )
        .connections(
            FlowConnection::builder()
                .name("ToOutput")
                .r#type(FlowConnectionType::Data)
                .source("PersistInput")
                .target("FlowOutputNode")
                .configuration(FlowConnectionConfiguration::Data(
                    FlowDataConnectionConfiguration::builder()
                        .source_output("document")
                        .target_input("document")
                        .build()
                        .expect("data connection"),
                ))
                .build()
                .expect("flow connection"),
        )
        .build()
}

#[tokio::test]
async fn persistence_round_trip_sessions_invocations_and_flow_executions() {
    let tmp = tempfile::tempdir().unwrap();
    let mut server = TestServer::start_persistent(tmp.path()).await;
    let agent = server.bedrock_agent_client().await;
    let runtime = server.bedrock_agent_runtime_client().await;

    // A session with metadata and tags, one invocation, one invocation step.
    let created = runtime
        .create_session()
        .session_metadata("owner", "persistence-e2e")
        .tags("team", "flows")
        .send()
        .await
        .expect("create session");
    let session_id = created.session_id;
    let session_arn = created.session_arn;
    runtime
        .tag_resource()
        .resource_arn(&session_arn)
        .tags("stage", "restart")
        .send()
        .await
        .expect("tag session");
    let invocation_id = runtime
        .create_invocation()
        .session_identifier(&session_id)
        .description("first turn")
        .send()
        .await
        .expect("create invocation")
        .invocation_id;
    let step_id = runtime
        .put_invocation_step()
        .session_identifier(&session_id)
        .invocation_identifier(&invocation_id)
        .invocation_step_time(DateTime::from_secs(1_700_000_000))
        .payload(InvocationStepPayload::ContentBlocks(vec![
            BedrockSessionContentBlock::Text("remember me".to_string()),
        ]))
        .send()
        .await
        .expect("put invocation step")
        .invocation_step_id;

    // A flow alias with one running and one stopped execution.
    let flow_id = agent
        .create_flow()
        .name("persist-flow")
        .execution_role_arn(ROLE)
        .definition(definition())
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
    let running_arn = runtime
        .start_flow_execution()
        .flow_identifier(&flow_id)
        .flow_alias_identifier(&alias_id)
        .flow_execution_name("persist-running")
        .set_inputs(Some(Vec::new()))
        .send()
        .await
        .expect("start running execution")
        .execution_arn
        .expect("execution arn");
    let stopped_arn = runtime
        .start_flow_execution()
        .flow_identifier(&flow_id)
        .flow_alias_identifier(&alias_id)
        .flow_execution_name("persist-stopped")
        .set_inputs(Some(Vec::new()))
        .send()
        .await
        .expect("start stopped execution")
        .execution_arn
        .expect("execution arn");
    runtime
        .stop_flow_execution()
        .flow_identifier(&flow_id)
        .flow_alias_identifier(&alias_id)
        .execution_identifier(&stopped_arn)
        .send()
        .await
        .expect("stop execution");

    server.restart().await;
    let runtime = server.bedrock_agent_runtime_client().await;

    let session = runtime
        .get_session()
        .session_identifier(&session_id)
        .send()
        .await
        .expect("get session after restart");
    assert_eq!(session.session_arn(), session_arn);
    assert_eq!(session.session_status(), &SessionStatus::Active);
    assert_eq!(
        session
            .session_metadata()
            .and_then(|m| m.get("owner"))
            .map(String::as_str),
        Some("persistence-e2e")
    );

    let tags = runtime
        .list_tags_for_resource()
        .resource_arn(&session_arn)
        .send()
        .await
        .expect("list tags after restart");
    let tags = tags.tags().expect("tags");
    assert_eq!(tags.get("team").map(String::as_str), Some("flows"));
    assert_eq!(tags.get("stage").map(String::as_str), Some("restart"));

    let invocations = runtime
        .list_invocations()
        .session_identifier(&session_id)
        .send()
        .await
        .expect("list invocations after restart");
    assert_eq!(invocations.invocation_summaries().len(), 1);
    assert_eq!(
        invocations.invocation_summaries()[0].invocation_id(),
        invocation_id
    );

    let step = runtime
        .get_invocation_step()
        .session_identifier(&session_id)
        .invocation_identifier(&invocation_id)
        .invocation_step_id(&step_id)
        .send()
        .await
        .expect("get invocation step after restart")
        .invocation_step
        .expect("invocation step");
    assert_eq!(
        step.invocation_step_time().secs(),
        1_700_000_000,
        "{step:?}"
    );
    match step.payload() {
        Some(InvocationStepPayload::ContentBlocks(blocks)) => assert_eq!(
            blocks
                .first()
                .and_then(|b| b.as_text().ok())
                .map(String::as_str),
            Some("remember me")
        ),
        other => panic!("unexpected payload {other:?}"),
    }

    let running = runtime
        .get_flow_execution()
        .flow_identifier(&flow_id)
        .flow_alias_identifier(&alias_id)
        .execution_identifier(&running_arn)
        .send()
        .await
        .expect("get running execution after restart");
    assert_eq!(running.status(), &FlowExecutionStatus::Running);
    assert_eq!(running.flow_version(), "1");
    assert!(running.ended_at().is_none());

    let stopped = runtime
        .get_flow_execution()
        .flow_identifier(&flow_id)
        .flow_alias_identifier(&alias_id)
        .execution_identifier(&stopped_arn)
        .send()
        .await
        .expect("get stopped execution after restart");
    assert_eq!(stopped.status(), &FlowExecutionStatus::Aborted);
    assert!(stopped.ended_at().is_some());

    let snapshot = runtime
        .get_execution_flow_snapshot()
        .flow_identifier(&flow_id)
        .flow_alias_identifier(&alias_id)
        .execution_identifier(&running_arn)
        .send()
        .await
        .expect("get execution flow snapshot after restart");
    assert_eq!(snapshot.execution_role_arn(), ROLE);
    assert!(
        snapshot.definition().contains("PersistInput"),
        "{}",
        snapshot.definition()
    );

    // Execution names stay unique across the restart.
    let err = runtime
        .start_flow_execution()
        .flow_identifier(&flow_id)
        .flow_alias_identifier(&alias_id)
        .flow_execution_name("persist-running")
        .set_inputs(Some(Vec::new()))
        .send()
        .await
        .expect_err("duplicate execution name");
    assert_eq!(
        err.as_service_error()
            .and_then(aws_sdk_bedrockagentruntime::error::ProvideErrorMetadata::code),
        Some("ConflictException"),
        "{err:?}"
    );
}

/// Deleting a session is persisted, and the introspection invocation log
/// resets on restart.
#[tokio::test]
async fn persistence_deleted_session_stays_gone_and_invocation_log_resets() {
    let tmp = tempfile::tempdir().unwrap();
    let mut server = TestServer::start_persistent(tmp.path()).await;
    let runtime = server.bedrock_agent_runtime_client().await;

    let kept = runtime
        .create_session()
        .send()
        .await
        .expect("create kept session")
        .session_id;
    let doomed = runtime
        .create_session()
        .send()
        .await
        .expect("create doomed session")
        .session_id;
    runtime
        .end_session()
        .session_identifier(&doomed)
        .send()
        .await
        .expect("end session");
    runtime
        .delete_session()
        .session_identifier(&doomed)
        .send()
        .await
        .expect("delete session");
    runtime
        .end_session()
        .session_identifier(&kept)
        .send()
        .await
        .expect("end kept session");

    let mut stream = runtime
        .invoke_inline_agent()
        .session_id("persist-inline-1")
        .foundation_model("anthropic.claude-3-haiku-20240307-v1:0")
        .instruction("You are a helpful assistant for persistence tests.")
        .input_text("hello")
        .send()
        .await
        .expect("invoke inline agent");
    while stream
        .completion
        .recv()
        .await
        .expect("inline agent stream")
        .is_some()
    {}
    let before: serde_json::Value = reqwest::get(format!(
        "{}/_fakecloud/bedrock-agent-runtime/invocations",
        server.endpoint()
    ))
    .await
    .unwrap()
    .json()
    .await
    .unwrap();
    assert_eq!(before["invocations"].as_array().map(Vec::len), Some(1));

    server.restart().await;
    let runtime = server.bedrock_agent_runtime_client().await;

    assert!(runtime
        .get_session()
        .session_identifier(&doomed)
        .send()
        .await
        .is_err());
    let kept_session = runtime
        .get_session()
        .session_identifier(&kept)
        .send()
        .await
        .expect("get kept session after restart");
    assert_eq!(kept_session.session_status(), &SessionStatus::Ended);

    let after: serde_json::Value = reqwest::get(format!(
        "{}/_fakecloud/bedrock-agent-runtime/invocations",
        server.endpoint()
    ))
    .await
    .unwrap()
    .json()
    .await
    .unwrap();
    assert_eq!(
        after["invocations"].as_array().map(Vec::len),
        Some(0),
        "{after}"
    );
}
