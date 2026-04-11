mod helpers;

use aws_sdk_sfn::types::Tag;
use helpers::TestServer;

fn simple_definition() -> String {
    serde_json::json!({
        "StartAt": "Hello",
        "States": {
            "Hello": {
                "Type": "Pass",
                "Result": "Hello, World!",
                "End": true
            }
        }
    })
    .to_string()
}

fn two_step_definition() -> String {
    serde_json::json!({
        "StartAt": "First",
        "States": {
            "First": {
                "Type": "Pass",
                "Result": "step1",
                "Next": "Second"
            },
            "Second": {
                "Type": "Pass",
                "Result": "step2",
                "End": true
            }
        }
    })
    .to_string()
}

#[tokio::test]
async fn sfn_create_describe_delete_state_machine() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    let create = client
        .create_state_machine()
        .name("test-sm")
        .definition(simple_definition())
        .role_arn("arn:aws:iam::123456789012:role/test-role")
        .send()
        .await
        .unwrap();

    let arn = create.state_machine_arn();
    assert!(arn.contains("stateMachine:test-sm"));
    let _ = create.creation_date(); // DateTime - just verify it's accessible

    // Describe
    let describe = client
        .describe_state_machine()
        .state_machine_arn(arn)
        .send()
        .await
        .unwrap();
    assert_eq!(describe.name(), "test-sm");
    assert_eq!(describe.state_machine_arn(), arn);
    assert_eq!(describe.status().unwrap().as_str(), "ACTIVE");
    assert_eq!(describe.r#type().as_str(), "STANDARD");
    assert!(describe.definition().contains("Hello"));
    assert_eq!(
        describe.role_arn(),
        "arn:aws:iam::123456789012:role/test-role"
    );

    // Delete
    client
        .delete_state_machine()
        .state_machine_arn(arn)
        .send()
        .await
        .unwrap();

    // Describe after delete should fail
    let err = client
        .describe_state_machine()
        .state_machine_arn(arn)
        .send()
        .await;
    assert!(err.is_err());
}

#[tokio::test]
async fn sfn_list_state_machines() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    // Create 3 state machines
    for name in &["alpha-sm", "beta-sm", "gamma-sm"] {
        client
            .create_state_machine()
            .name(*name)
            .definition(simple_definition())
            .role_arn("arn:aws:iam::123456789012:role/test-role")
            .send()
            .await
            .unwrap();
    }

    let list = client.list_state_machines().send().await.unwrap();
    let machines = list.state_machines();
    assert_eq!(machines.len(), 3);

    // Should be sorted by name
    let names: Vec<&str> = machines.iter().map(|m| m.name()).collect();
    assert_eq!(names, vec!["alpha-sm", "beta-sm", "gamma-sm"]);

    // Each should have the correct type
    for m in machines {
        assert_eq!(m.r#type().as_str(), "STANDARD");
        let _ = m.creation_date();
    }
}

#[tokio::test]
async fn sfn_list_state_machines_pagination() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    for i in 0..5 {
        client
            .create_state_machine()
            .name(format!("sm-{i:02}"))
            .definition(simple_definition())
            .role_arn("arn:aws:iam::123456789012:role/test-role")
            .send()
            .await
            .unwrap();
    }

    // Page 1: 2 items
    let page1 = client
        .list_state_machines()
        .max_results(2)
        .send()
        .await
        .unwrap();
    assert_eq!(page1.state_machines().len(), 2);
    assert!(page1.next_token().is_some());

    // Page 2
    let page2 = client
        .list_state_machines()
        .max_results(2)
        .next_token(page1.next_token().unwrap())
        .send()
        .await
        .unwrap();
    assert_eq!(page2.state_machines().len(), 2);
    assert!(page2.next_token().is_some());

    // Page 3 (last)
    let page3 = client
        .list_state_machines()
        .max_results(2)
        .next_token(page2.next_token().unwrap())
        .send()
        .await
        .unwrap();
    assert_eq!(page3.state_machines().len(), 1);
    assert!(page3.next_token().is_none());
}

#[tokio::test]
async fn sfn_update_state_machine() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    let create = client
        .create_state_machine()
        .name("update-test")
        .definition(simple_definition())
        .role_arn("arn:aws:iam::123456789012:role/test-role")
        .send()
        .await
        .unwrap();
    let arn = create.state_machine_arn();

    // Update definition
    let update = client
        .update_state_machine()
        .state_machine_arn(arn)
        .definition(two_step_definition())
        .send()
        .await
        .unwrap();
    let _ = update.update_date(); // DateTime - just verify it's accessible

    // Verify update
    let describe = client
        .describe_state_machine()
        .state_machine_arn(arn)
        .send()
        .await
        .unwrap();
    assert!(describe.definition().contains("First"));
    assert!(describe.definition().contains("Second"));
}

#[tokio::test]
async fn sfn_create_express_state_machine() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    let create = client
        .create_state_machine()
        .name("express-sm")
        .definition(simple_definition())
        .role_arn("arn:aws:iam::123456789012:role/test-role")
        .r#type(aws_sdk_sfn::types::StateMachineType::Express)
        .send()
        .await
        .unwrap();

    let describe = client
        .describe_state_machine()
        .state_machine_arn(create.state_machine_arn())
        .send()
        .await
        .unwrap();
    assert_eq!(describe.r#type().as_str(), "EXPRESS");
}

#[tokio::test]
async fn sfn_create_duplicate_name_fails() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    client
        .create_state_machine()
        .name("dup-sm")
        .definition(simple_definition())
        .role_arn("arn:aws:iam::123456789012:role/test-role")
        .send()
        .await
        .unwrap();

    let err = client
        .create_state_machine()
        .name("dup-sm")
        .definition(simple_definition())
        .role_arn("arn:aws:iam::123456789012:role/test-role")
        .send()
        .await;
    assert!(err.is_err());
}

#[tokio::test]
async fn sfn_delete_nonexistent_succeeds() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    // AWS returns success for deleting non-existent state machines
    client
        .delete_state_machine()
        .state_machine_arn("arn:aws:states:us-east-1:123456789012:stateMachine:nonexistent")
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn sfn_tag_resource() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    let create = client
        .create_state_machine()
        .name("tagged-sm")
        .definition(simple_definition())
        .role_arn("arn:aws:iam::123456789012:role/test-role")
        .send()
        .await
        .unwrap();
    let arn = create.state_machine_arn();

    // Tag the resource
    client
        .tag_resource()
        .resource_arn(arn)
        .tags(Tag::builder().key("env").value("prod").build())
        .tags(Tag::builder().key("team").value("backend").build())
        .send()
        .await
        .unwrap();

    // List tags
    let tags = client
        .list_tags_for_resource()
        .resource_arn(arn)
        .send()
        .await
        .unwrap();
    let tag_list = tags.tags();
    assert_eq!(tag_list.len(), 2);

    let env_tag = tag_list.iter().find(|t| t.key() == Some("env")).unwrap();
    assert_eq!(env_tag.value(), Some("prod"));

    let team_tag = tag_list.iter().find(|t| t.key() == Some("team")).unwrap();
    assert_eq!(team_tag.value(), Some("backend"));
}

#[tokio::test]
async fn sfn_untag_resource() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    let create = client
        .create_state_machine()
        .name("untag-sm")
        .definition(simple_definition())
        .role_arn("arn:aws:iam::123456789012:role/test-role")
        .send()
        .await
        .unwrap();
    let arn = create.state_machine_arn();

    // Add tags
    client
        .tag_resource()
        .resource_arn(arn)
        .tags(Tag::builder().key("env").value("prod").build())
        .tags(Tag::builder().key("team").value("backend").build())
        .tags(Tag::builder().key("version").value("1").build())
        .send()
        .await
        .unwrap();

    // Remove one tag
    client
        .untag_resource()
        .resource_arn(arn)
        .tag_keys("team")
        .send()
        .await
        .unwrap();

    // Verify remaining
    let tags = client
        .list_tags_for_resource()
        .resource_arn(arn)
        .send()
        .await
        .unwrap();
    let tag_list = tags.tags();
    assert_eq!(tag_list.len(), 2);
    assert!(tag_list.iter().any(|t| t.key() == Some("env")));
    assert!(tag_list.iter().any(|t| t.key() == Some("version")));
    assert!(!tag_list.iter().any(|t| t.key() == Some("team")));
}

#[tokio::test]
async fn sfn_create_with_tags() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    let create = client
        .create_state_machine()
        .name("create-tagged-sm")
        .definition(simple_definition())
        .role_arn("arn:aws:iam::123456789012:role/test-role")
        .tags(Tag::builder().key("created-by").value("test").build())
        .send()
        .await
        .unwrap();

    let tags = client
        .list_tags_for_resource()
        .resource_arn(create.state_machine_arn())
        .send()
        .await
        .unwrap();
    assert_eq!(tags.tags().len(), 1);
    assert_eq!(tags.tags()[0].key(), Some("created-by"));
    assert_eq!(tags.tags()[0].value(), Some("test"));
}

#[tokio::test]
async fn sfn_create_with_invalid_definition_fails() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    // Not valid JSON
    let err = client
        .create_state_machine()
        .name("bad-def")
        .definition("not json")
        .role_arn("arn:aws:iam::123456789012:role/test-role")
        .send()
        .await;
    assert!(err.is_err());

    // Valid JSON but missing StartAt
    let err = client
        .create_state_machine()
        .name("bad-def2")
        .definition(r#"{"States": {"A": {"Type": "Pass", "End": true}}}"#)
        .role_arn("arn:aws:iam::123456789012:role/test-role")
        .send()
        .await;
    assert!(err.is_err());

    // Valid JSON but missing States
    let err = client
        .create_state_machine()
        .name("bad-def3")
        .definition(r#"{"StartAt": "A"}"#)
        .role_arn("arn:aws:iam::123456789012:role/test-role")
        .send()
        .await;
    assert!(err.is_err());
}

#[tokio::test]
async fn sfn_health_includes_states() {
    let server = TestServer::start().await;
    let url = format!("{}/_fakecloud/health", server.endpoint());
    let resp: serde_json::Value = reqwest::get(&url).await.unwrap().json().await.unwrap();
    let services = resp["services"].as_array().unwrap();
    assert!(services.iter().any(|s| s.as_str() == Some("states")));
}

#[tokio::test]
async fn sfn_reset_clears_state() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    client
        .create_state_machine()
        .name("reset-sm")
        .definition(simple_definition())
        .role_arn("arn:aws:iam::123456789012:role/test-role")
        .send()
        .await
        .unwrap();

    // Verify it exists
    let list = client.list_state_machines().send().await.unwrap();
    assert_eq!(list.state_machines().len(), 1);

    // Reset
    let http = reqwest::Client::new();
    http.post(format!("{}/_reset", server.endpoint()))
        .send()
        .await
        .unwrap();

    // Verify cleared
    let list = client.list_state_machines().send().await.unwrap();
    assert_eq!(list.state_machines().len(), 0);
}
