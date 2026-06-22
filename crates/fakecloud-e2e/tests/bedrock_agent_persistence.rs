mod helpers;

use helpers::TestServer;

/// An agent (with its configured fields) survives a restart in persistent mode.
#[tokio::test]
async fn persistence_round_trip_agent() {
    let tmp = tempfile::tempdir().unwrap();
    let mut server = TestServer::start_persistent(tmp.path()).await;
    let ba = server.bedrock_agent_client().await;

    let agent_id = ba
        .create_agent()
        .agent_name("persist-agent")
        .instruction("Be helpful and concise for testing.")
        .foundation_model("anthropic.claude-3-5-sonnet-20241022-v2:0")
        .send()
        .await
        .unwrap()
        .agent()
        .unwrap()
        .agent_id()
        .to_string();

    server.restart().await;
    let ba = server.bedrock_agent_client().await;

    let agent = ba
        .get_agent()
        .agent_id(&agent_id)
        .send()
        .await
        .unwrap()
        .agent
        .unwrap();
    assert_eq!(agent.agent_name(), "persist-agent");
    assert_eq!(
        agent.foundation_model(),
        Some("anthropic.claude-3-5-sonnet-20241022-v2:0")
    );
    assert_eq!(
        agent.instruction(),
        Some("Be helpful and concise for testing.")
    );
}

/// A deleted agent stays gone after restart.
#[tokio::test]
async fn persistence_delete_agent_survives_restart() {
    let tmp = tempfile::tempdir().unwrap();
    let mut server = TestServer::start_persistent(tmp.path()).await;
    let ba = server.bedrock_agent_client().await;

    let agent_id = ba
        .create_agent()
        .agent_name("ephemeral-agent")
        .instruction("Temporary agent for testing.")
        .foundation_model("anthropic.claude-3-5-sonnet-20241022-v2:0")
        .send()
        .await
        .unwrap()
        .agent()
        .unwrap()
        .agent_id()
        .to_string();
    ba.delete_agent().agent_id(&agent_id).send().await.unwrap();

    server.restart().await;
    let ba = server.bedrock_agent_client().await;

    assert!(ba.get_agent().agent_id(&agent_id).send().await.is_err());
}
