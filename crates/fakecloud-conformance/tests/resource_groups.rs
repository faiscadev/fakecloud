mod helpers;

use fakecloud_conformance_macros::test_action;
use helpers::TestServer;

type Client = aws_sdk_resourcegroups::Client;

const TAG_QUERY: &str = r#"{"ResourceTypeFilters":["AWS::AllSupported"],"TagFilters":[{"Key":"stage","Values":["test"]}]}"#;

fn tag_query() -> aws_sdk_resourcegroups::types::ResourceQuery {
    aws_sdk_resourcegroups::types::ResourceQuery::builder()
        .r#type(aws_sdk_resourcegroups::types::QueryType::TagFilters10)
        .query(TAG_QUERY)
        .build()
        .unwrap()
}

fn generic_config() -> aws_sdk_resourcegroups::types::GroupConfigurationItem {
    aws_sdk_resourcegroups::types::GroupConfigurationItem::builder()
        .r#type("AWS::ResourceGroups::Generic")
        .parameters(
            aws_sdk_resourcegroups::types::GroupConfigurationParameter::builder()
                .name("allowed-resource-types")
                .values("AWS::EC2::Host")
                .build()
                .unwrap(),
        )
        .build()
        .unwrap()
}

/// A query-based group.
async fn create_query_group(client: &Client, name: &str) {
    client
        .create_group()
        .name(name)
        .resource_query(tag_query())
        .send()
        .await
        .unwrap();
}

/// A configuration group (no query) that accepts explicit membership.
async fn create_config_group(client: &Client, name: &str) {
    client
        .create_group()
        .name(name)
        .configuration(generic_config())
        .send()
        .await
        .unwrap();
}

#[test_action("resource-groups", "CreateGroup", checksum = "4559077e")]
#[tokio::test]
async fn create_group() {
    let server = TestServer::start().await;
    let client = server.resourcegroups_client().await;
    let resp = client
        .create_group()
        .name("rg-create")
        .resource_query(tag_query())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.group().unwrap().name(), "rg-create");
    assert!(resp
        .group()
        .unwrap()
        .group_arn()
        .contains(":group/rg-create/"));
}

#[test_action("resource-groups", "GetGroup", checksum = "21a9f112")]
#[tokio::test]
async fn get_group() {
    let server = TestServer::start().await;
    let client = server.resourcegroups_client().await;
    create_query_group(&client, "rg-get").await;
    let resp = client.get_group().group("rg-get").send().await.unwrap();
    assert_eq!(resp.group().unwrap().name(), "rg-get");
}

#[test_action("resource-groups", "GetGroupQuery", checksum = "24b5f086")]
#[tokio::test]
async fn get_group_query() {
    let server = TestServer::start().await;
    let client = server.resourcegroups_client().await;
    create_query_group(&client, "rg-gq").await;
    let resp = client
        .get_group_query()
        .group("rg-gq")
        .send()
        .await
        .unwrap();
    let q = resp.group_query().unwrap();
    assert_eq!(q.group_name(), "rg-gq");
    assert_eq!(
        q.resource_query().unwrap().r#type(),
        &aws_sdk_resourcegroups::types::QueryType::TagFilters10
    );
}

#[test_action("resource-groups", "UpdateGroup", checksum = "1ea74549")]
#[tokio::test]
async fn update_group() {
    let server = TestServer::start().await;
    let client = server.resourcegroups_client().await;
    create_query_group(&client, "rg-upd").await;
    let resp = client
        .update_group()
        .group("rg-upd")
        .description("updated")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.group().unwrap().description(), Some("updated"));
}

#[test_action("resource-groups", "UpdateGroupQuery", checksum = "9f2a3614")]
#[tokio::test]
async fn update_group_query() {
    let server = TestServer::start().await;
    let client = server.resourcegroups_client().await;
    create_query_group(&client, "rg-uq").await;
    let resp = client
        .update_group_query()
        .group("rg-uq")
        .resource_query(tag_query())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.group_query().unwrap().group_name(), "rg-uq");
}

#[test_action("resource-groups", "DeleteGroup", checksum = "a1f54ce4")]
#[tokio::test]
async fn delete_group() {
    let server = TestServer::start().await;
    let client = server.resourcegroups_client().await;
    create_query_group(&client, "rg-del").await;
    let resp = client.delete_group().group("rg-del").send().await.unwrap();
    assert_eq!(resp.group().unwrap().name(), "rg-del");
    assert!(client.get_group().group("rg-del").send().await.is_err());
}

#[test_action("resource-groups", "ListGroups", checksum = "37debf0e")]
#[tokio::test]
async fn list_groups() {
    let server = TestServer::start().await;
    let client = server.resourcegroups_client().await;
    create_query_group(&client, "rg-list").await;
    let resp = client.list_groups().send().await.unwrap();
    assert!(resp
        .group_identifiers()
        .iter()
        .any(|g| g.group_name() == Some("rg-list")));
}

#[test_action("resource-groups", "GroupResources", checksum = "bc9d885f")]
#[tokio::test]
async fn group_resources() {
    let server = TestServer::start().await;
    let client = server.resourcegroups_client().await;
    create_config_group(&client, "rg-gr").await;
    let arn = "arn:aws:ec2:us-east-1:123456789012:instance/i-abc";
    let resp = client
        .group_resources()
        .group("rg-gr")
        .resource_arns(arn)
        .send()
        .await
        .unwrap();
    assert!(resp.succeeded().contains(&arn.to_string()));
}

#[test_action("resource-groups", "UngroupResources", checksum = "fa0e1272")]
#[tokio::test]
async fn ungroup_resources() {
    let server = TestServer::start().await;
    let client = server.resourcegroups_client().await;
    create_config_group(&client, "rg-ug").await;
    let arn = "arn:aws:ec2:us-east-1:123456789012:instance/i-def";
    client
        .group_resources()
        .group("rg-ug")
        .resource_arns(arn)
        .send()
        .await
        .unwrap();
    let resp = client
        .ungroup_resources()
        .group("rg-ug")
        .resource_arns(arn)
        .send()
        .await
        .unwrap();
    assert!(resp.succeeded().contains(&arn.to_string()));
}

#[test_action("resource-groups", "ListGroupResources", checksum = "b0319332")]
#[tokio::test]
async fn list_group_resources() {
    let server = TestServer::start().await;
    let client = server.resourcegroups_client().await;
    create_config_group(&client, "rg-lgr").await;
    let arn = "arn:aws:ec2:us-east-1:123456789012:instance/i-ghi";
    client
        .group_resources()
        .group("rg-lgr")
        .resource_arns(arn)
        .send()
        .await
        .unwrap();
    let resp = client
        .list_group_resources()
        .group("rg-lgr")
        .send()
        .await
        .unwrap();
    assert!(resp
        .resources()
        .iter()
        .any(|r| r.identifier().and_then(|i| i.resource_arn()) == Some(arn)));
}

#[test_action("resource-groups", "SearchResources", checksum = "b63676f2")]
#[tokio::test]
async fn search_resources() {
    let server = TestServer::start().await;
    let client = server.resourcegroups_client().await;
    let resp = client
        .search_resources()
        .resource_query(tag_query())
        .send()
        .await
        .unwrap();
    // No cross-service index yet: an empty, error-free result.
    assert!(resp.resource_identifiers().is_empty());
    assert!(resp.query_errors().is_empty());
}

#[test_action("resource-groups", "GetGroupConfiguration", checksum = "665c6264")]
#[tokio::test]
async fn get_group_configuration() {
    let server = TestServer::start().await;
    let client = server.resourcegroups_client().await;
    create_config_group(&client, "rg-ggc").await;
    let resp = client
        .get_group_configuration()
        .group("rg-ggc")
        .send()
        .await
        .unwrap();
    assert!(!resp
        .group_configuration()
        .unwrap()
        .configuration()
        .is_empty());
}

#[test_action("resource-groups", "PutGroupConfiguration", checksum = "c10070ab")]
#[tokio::test]
async fn put_group_configuration() {
    let server = TestServer::start().await;
    let client = server.resourcegroups_client().await;
    create_config_group(&client, "rg-pgc").await;
    client
        .put_group_configuration()
        .group("rg-pgc")
        .configuration(generic_config())
        .send()
        .await
        .unwrap();
}

#[test_action("resource-groups", "GetTags", checksum = "610888b1")]
#[tokio::test]
async fn get_tags() {
    let server = TestServer::start().await;
    let client = server.resourcegroups_client().await;
    let created = client
        .create_group()
        .name("rg-gt")
        .resource_query(tag_query())
        .tags("team", "data")
        .send()
        .await
        .unwrap();
    let arn = created.group().unwrap().group_arn();
    let resp = client.get_tags().arn(arn).send().await.unwrap();
    assert_eq!(resp.tags().unwrap().get("team"), Some(&"data".to_string()));
}

#[test_action("resource-groups", "Tag", checksum = "f3db8184")]
#[tokio::test]
async fn tag() {
    let server = TestServer::start().await;
    let client = server.resourcegroups_client().await;
    let created = client
        .create_group()
        .name("rg-tag")
        .resource_query(tag_query())
        .send()
        .await
        .unwrap();
    let arn = created.group().unwrap().group_arn().to_string();
    let resp = client
        .tag()
        .arn(&arn)
        .tags("env", "prod")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.tags().unwrap().get("env"), Some(&"prod".to_string()));
}

#[test_action("resource-groups", "Untag", checksum = "6a599515")]
#[tokio::test]
async fn untag() {
    let server = TestServer::start().await;
    let client = server.resourcegroups_client().await;
    let created = client
        .create_group()
        .name("rg-untag")
        .resource_query(tag_query())
        .tags("k", "v")
        .send()
        .await
        .unwrap();
    let arn = created.group().unwrap().group_arn().to_string();
    let resp = client.untag().arn(&arn).keys("k").send().await.unwrap();
    assert!(resp.keys().contains(&"k".to_string()));
}

#[test_action("resource-groups", "GetAccountSettings", checksum = "89d05c03")]
#[tokio::test]
async fn get_account_settings() {
    let server = TestServer::start().await;
    let client = server.resourcegroups_client().await;
    let resp = client.get_account_settings().send().await.unwrap();
    assert!(resp.account_settings().is_some());
}

#[test_action("resource-groups", "UpdateAccountSettings", checksum = "c4ff7723")]
#[tokio::test]
async fn update_account_settings() {
    let server = TestServer::start().await;
    let client = server.resourcegroups_client().await;
    let resp = client
        .update_account_settings()
        .group_lifecycle_events_desired_status(
            aws_sdk_resourcegroups::types::GroupLifecycleEventsDesiredStatus::Active,
        )
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.account_settings()
            .unwrap()
            .group_lifecycle_events_desired_status(),
        Some(&aws_sdk_resourcegroups::types::GroupLifecycleEventsDesiredStatus::Active)
    );
}

#[test_action("resource-groups", "ListGroupingStatuses", checksum = "6c6a6b16")]
#[tokio::test]
async fn list_grouping_statuses() {
    let server = TestServer::start().await;
    let client = server.resourcegroups_client().await;
    create_config_group(&client, "rg-lgs").await;
    let resp = client
        .list_grouping_statuses()
        .group("rg-lgs")
        .send()
        .await
        .unwrap();
    assert!(resp.grouping_statuses().is_empty());
}

#[test_action("resource-groups", "StartTagSyncTask", checksum = "0f87d7da")]
#[tokio::test]
async fn start_tag_sync_task() {
    let server = TestServer::start().await;
    let client = server.resourcegroups_client().await;
    create_config_group(&client, "rg-sts").await;
    let resp = client
        .start_tag_sync_task()
        .group("rg-sts")
        .tag_key("owner")
        .tag_value("team")
        .role_arn("arn:aws:iam::123456789012:role/rg-sync")
        .send()
        .await
        .unwrap();
    assert!(resp.task_arn().is_some());
}

#[test_action("resource-groups", "GetTagSyncTask", checksum = "45261a77")]
#[tokio::test]
async fn get_tag_sync_task() {
    let server = TestServer::start().await;
    let client = server.resourcegroups_client().await;
    create_config_group(&client, "rg-gts").await;
    let started = client
        .start_tag_sync_task()
        .group("rg-gts")
        .tag_key("owner")
        .tag_value("team")
        .role_arn("arn:aws:iam::123456789012:role/rg-sync")
        .send()
        .await
        .unwrap();
    let task_arn = started.task_arn().unwrap();
    let resp = client
        .get_tag_sync_task()
        .task_arn(task_arn)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.task_arn(), Some(task_arn));
}

#[test_action("resource-groups", "ListTagSyncTasks", checksum = "deaa5d91")]
#[tokio::test]
async fn list_tag_sync_tasks() {
    let server = TestServer::start().await;
    let client = server.resourcegroups_client().await;
    create_config_group(&client, "rg-lts").await;
    client
        .start_tag_sync_task()
        .group("rg-lts")
        .tag_key("owner")
        .tag_value("team")
        .role_arn("arn:aws:iam::123456789012:role/rg-sync")
        .send()
        .await
        .unwrap();
    let resp = client.list_tag_sync_tasks().send().await.unwrap();
    assert!(!resp.tag_sync_tasks().is_empty());
}

#[test_action("resource-groups", "CancelTagSyncTask", checksum = "446b57e9")]
#[tokio::test]
async fn cancel_tag_sync_task() {
    let server = TestServer::start().await;
    let client = server.resourcegroups_client().await;
    create_config_group(&client, "rg-cts").await;
    let started = client
        .start_tag_sync_task()
        .group("rg-cts")
        .tag_key("owner")
        .tag_value("team")
        .role_arn("arn:aws:iam::123456789012:role/rg-sync")
        .send()
        .await
        .unwrap();
    client
        .cancel_tag_sync_task()
        .task_arn(started.task_arn().unwrap())
        .send()
        .await
        .unwrap();
}
