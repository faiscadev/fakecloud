mod helpers;

use fakecloud_conformance_macros::test_action;
use helpers::TestServer;

const SOURCE: &str = "arn:aws:sqs:us-east-1:000000000000:conf-pipe-src";
const TARGET: &str = "arn:aws:sqs:us-east-1:000000000000:conf-pipe-dst";
const ROLE: &str = "arn:aws:iam::000000000000:role/conf-pipe";

async fn create(client: &aws_sdk_pipes::Client, name: &str) {
    client
        .create_pipe()
        .name(name)
        .source(SOURCE)
        .target(TARGET)
        .role_arn(ROLE)
        .send()
        .await
        .unwrap();
}

#[test_action("pipes", "CreatePipe", checksum = "5fa1d05e")]
#[tokio::test]
async fn pipes_create_pipe() {
    let server = TestServer::start().await;
    let client = server.pipes_client().await;
    let resp = client
        .create_pipe()
        .name("conf-create")
        .source(SOURCE)
        .target(TARGET)
        .role_arn(ROLE)
        .send()
        .await
        .unwrap();
    assert!(resp.arn().unwrap().contains("conf-create"));
}

#[test_action("pipes", "DescribePipe", checksum = "89b818cc")]
#[tokio::test]
async fn pipes_describe_pipe() {
    let server = TestServer::start().await;
    let client = server.pipes_client().await;
    create(&client, "conf-describe").await;
    let resp = client
        .describe_pipe()
        .name("conf-describe")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.name().unwrap(), "conf-describe");
}

#[test_action("pipes", "ListPipes", checksum = "b4bb004f")]
#[tokio::test]
async fn pipes_list_pipes() {
    let server = TestServer::start().await;
    let client = server.pipes_client().await;
    create(&client, "conf-list").await;
    let resp = client.list_pipes().send().await.unwrap();
    assert!(resp.pipes().iter().any(|p| p.name() == Some("conf-list")));
}

#[test_action("pipes", "UpdatePipe", checksum = "8547998f")]
#[tokio::test]
async fn pipes_update_pipe() {
    let server = TestServer::start().await;
    let client = server.pipes_client().await;
    create(&client, "conf-update").await;
    client
        .update_pipe()
        .name("conf-update")
        .role_arn(ROLE)
        .description("updated")
        .send()
        .await
        .unwrap();
    let resp = client
        .describe_pipe()
        .name("conf-update")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.description().unwrap(), "updated");
}

#[test_action("pipes", "DeletePipe", checksum = "d77c2a5f")]
#[tokio::test]
async fn pipes_delete_pipe() {
    let server = TestServer::start().await;
    let client = server.pipes_client().await;
    create(&client, "conf-delete").await;
    client
        .delete_pipe()
        .name("conf-delete")
        .send()
        .await
        .unwrap();
}

#[test_action("pipes", "StartPipe", checksum = "9693b60f")]
#[tokio::test]
async fn pipes_start_pipe() {
    let server = TestServer::start().await;
    let client = server.pipes_client().await;
    create(&client, "conf-start").await;
    let resp = client.start_pipe().name("conf-start").send().await.unwrap();
    assert_eq!(resp.name().unwrap(), "conf-start");
}

#[test_action("pipes", "StopPipe", checksum = "2f507492")]
#[tokio::test]
async fn pipes_stop_pipe() {
    let server = TestServer::start().await;
    let client = server.pipes_client().await;
    create(&client, "conf-stop").await;
    let resp = client.stop_pipe().name("conf-stop").send().await.unwrap();
    assert_eq!(resp.name().unwrap(), "conf-stop");
}

#[test_action("pipes", "TagResource", checksum = "98f8de3f")]
#[tokio::test]
async fn pipes_tag_resource() {
    let server = TestServer::start().await;
    let client = server.pipes_client().await;
    let created = client
        .create_pipe()
        .name("conf-tag")
        .source(SOURCE)
        .target(TARGET)
        .role_arn(ROLE)
        .send()
        .await
        .unwrap();
    let arn = created.arn().unwrap();
    client
        .tag_resource()
        .resource_arn(arn)
        .tags("env", "test")
        .send()
        .await
        .unwrap();
    let tags = client
        .list_tags_for_resource()
        .resource_arn(arn)
        .send()
        .await
        .unwrap();
    assert_eq!(
        tags.tags().and_then(|t| t.get("env")).map(String::as_str),
        Some("test")
    );
}

#[test_action("pipes", "ListTagsForResource", checksum = "55b384ee")]
#[tokio::test]
async fn pipes_list_tags_for_resource() {
    let server = TestServer::start().await;
    let client = server.pipes_client().await;
    let created = client
        .create_pipe()
        .name("conf-listtags")
        .source(SOURCE)
        .target(TARGET)
        .role_arn(ROLE)
        .send()
        .await
        .unwrap();
    let arn = created.arn().unwrap();
    let resp = client
        .list_tags_for_resource()
        .resource_arn(arn)
        .send()
        .await
        .unwrap();
    // A freshly created pipe with no tags returns an empty map.
    assert!(resp.tags().map(|t| t.is_empty()).unwrap_or(true));
}

#[test_action("pipes", "UntagResource", checksum = "1b25af79")]
#[tokio::test]
async fn pipes_untag_resource() {
    let server = TestServer::start().await;
    let client = server.pipes_client().await;
    let created = client
        .create_pipe()
        .name("conf-untag")
        .source(SOURCE)
        .target(TARGET)
        .role_arn(ROLE)
        .send()
        .await
        .unwrap();
    let arn = created.arn().unwrap();
    client
        .tag_resource()
        .resource_arn(arn)
        .tags("env", "test")
        .send()
        .await
        .unwrap();
    client
        .untag_resource()
        .resource_arn(arn)
        .tag_keys("env")
        .send()
        .await
        .unwrap();
    let tags = client
        .list_tags_for_resource()
        .resource_arn(arn)
        .send()
        .await
        .unwrap();
    assert!(tags.tags().map(|t| t.is_empty()).unwrap_or(true));
}
