//! ECS Batch 1 conformance tests: each `#[test_action]` pairs a real AWS
//! SDK call with the Smithy shape checksum. If AWS rev-bumps the ECS
//! model the checksum goes stale and the build fails loudly.

mod helpers;

use aws_sdk_ecs::types::{ContainerDefinition, Tag};
use fakecloud_conformance_macros::test_action;
use helpers::TestServer;

#[test_action("ecs", "CreateCluster", checksum = "cb27e04e")]
#[tokio::test]
async fn ecs_create_cluster() {
    let server = TestServer::start().await;
    let client = server.ecs_client().await;
    let resp = client
        .create_cluster()
        .cluster_name("confo-create")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.cluster().unwrap().cluster_name(), Some("confo-create"));
}

#[test_action("ecs", "DescribeClusters", checksum = "df3a48bc")]
#[tokio::test]
async fn ecs_describe_clusters() {
    let server = TestServer::start().await;
    let client = server.ecs_client().await;
    client
        .create_cluster()
        .cluster_name("confo-describe")
        .send()
        .await
        .unwrap();
    let resp = client
        .describe_clusters()
        .clusters("confo-describe")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.clusters().len(), 1);
}

#[test_action("ecs", "DeleteCluster", checksum = "00faf628")]
#[tokio::test]
async fn ecs_delete_cluster() {
    let server = TestServer::start().await;
    let client = server.ecs_client().await;
    client
        .create_cluster()
        .cluster_name("confo-delete")
        .send()
        .await
        .unwrap();
    client
        .delete_cluster()
        .cluster("confo-delete")
        .send()
        .await
        .unwrap();
}

#[test_action("ecs", "ListClusters", checksum = "cf37c170")]
#[tokio::test]
async fn ecs_list_clusters() {
    let server = TestServer::start().await;
    let client = server.ecs_client().await;
    client
        .create_cluster()
        .cluster_name("confo-list")
        .send()
        .await
        .unwrap();
    let resp = client.list_clusters().send().await.unwrap();
    assert!(!resp.cluster_arns().is_empty());
}

#[test_action("ecs", "UpdateCluster", checksum = "c38335f1")]
#[tokio::test]
async fn ecs_update_cluster() {
    let server = TestServer::start().await;
    let client = server.ecs_client().await;
    client
        .create_cluster()
        .cluster_name("confo-update")
        .send()
        .await
        .unwrap();
    let resp = client
        .update_cluster()
        .cluster("confo-update")
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.cluster().and_then(|c| c.cluster_name()),
        Some("confo-update")
    );
}

#[test_action("ecs", "UpdateClusterSettings", checksum = "f0e11ce7")]
#[tokio::test]
async fn ecs_update_cluster_settings() {
    use aws_sdk_ecs::types::{ClusterSetting, ClusterSettingName};
    let server = TestServer::start().await;
    let client = server.ecs_client().await;
    client
        .create_cluster()
        .cluster_name("confo-settings")
        .send()
        .await
        .unwrap();
    let resp = client
        .update_cluster_settings()
        .cluster("confo-settings")
        .settings(
            ClusterSetting::builder()
                .name(ClusterSettingName::ContainerInsights)
                .value("enabled")
                .build(),
        )
        .send()
        .await
        .unwrap();
    assert!(resp.cluster().is_some());
}

#[test_action("ecs", "PutClusterCapacityProviders", checksum = "11ce7106")]
#[tokio::test]
async fn ecs_put_cluster_capacity_providers() {
    use aws_sdk_ecs::types::CapacityProviderStrategyItem;
    let server = TestServer::start().await;
    let client = server.ecs_client().await;
    client
        .create_cluster()
        .cluster_name("confo-cp")
        .send()
        .await
        .unwrap();
    let resp = client
        .put_cluster_capacity_providers()
        .cluster("confo-cp")
        .capacity_providers("FARGATE")
        .default_capacity_provider_strategy(
            CapacityProviderStrategyItem::builder()
                .capacity_provider("FARGATE")
                .weight(1)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    assert!(resp.cluster().is_some());
}

#[test_action("ecs", "RegisterTaskDefinition", checksum = "6cfc4ce4")]
#[tokio::test]
async fn ecs_register_task_definition() {
    let server = TestServer::start().await;
    let client = server.ecs_client().await;
    let resp = client
        .register_task_definition()
        .family("confo-td")
        .container_definitions(
            ContainerDefinition::builder()
                .name("app")
                .image("public.ecr.aws/docker/library/alpine:latest")
                .essential(true)
                .build(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(resp.task_definition().unwrap().family(), Some("confo-td"));
}

#[test_action("ecs", "DescribeTaskDefinition", checksum = "313dcc15")]
#[tokio::test]
async fn ecs_describe_task_definition() {
    let server = TestServer::start().await;
    let client = server.ecs_client().await;
    client
        .register_task_definition()
        .family("confo-desc-td")
        .container_definitions(
            ContainerDefinition::builder()
                .name("app")
                .image("public.ecr.aws/docker/library/alpine:latest")
                .essential(true)
                .build(),
        )
        .send()
        .await
        .unwrap();
    let resp = client
        .describe_task_definition()
        .task_definition("confo-desc-td")
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.task_definition().and_then(|t| t.family()),
        Some("confo-desc-td")
    );
}

#[test_action("ecs", "DeregisterTaskDefinition", checksum = "933d0ff2")]
#[tokio::test]
async fn ecs_deregister_task_definition() {
    let server = TestServer::start().await;
    let client = server.ecs_client().await;
    client
        .register_task_definition()
        .family("confo-dereg")
        .container_definitions(
            ContainerDefinition::builder()
                .name("app")
                .image("public.ecr.aws/docker/library/alpine:latest")
                .essential(true)
                .build(),
        )
        .send()
        .await
        .unwrap();
    let resp = client
        .deregister_task_definition()
        .task_definition("confo-dereg:1")
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.task_definition()
            .and_then(|t| t.status())
            .map(|s| s.as_str()),
        Some("INACTIVE")
    );
}

#[test_action("ecs", "DeleteTaskDefinitions", checksum = "aaa7f78b")]
#[tokio::test]
async fn ecs_delete_task_definitions() {
    let server = TestServer::start().await;
    let client = server.ecs_client().await;
    client
        .register_task_definition()
        .family("confo-del-td")
        .container_definitions(
            ContainerDefinition::builder()
                .name("app")
                .image("public.ecr.aws/docker/library/alpine:latest")
                .essential(true)
                .build(),
        )
        .send()
        .await
        .unwrap();
    client
        .deregister_task_definition()
        .task_definition("confo-del-td:1")
        .send()
        .await
        .unwrap();
    let resp = client
        .delete_task_definitions()
        .task_definitions("confo-del-td:1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.task_definitions().len(), 1);
}

#[test_action("ecs", "ListTaskDefinitions", checksum = "bbbbb9b3")]
#[tokio::test]
async fn ecs_list_task_definitions() {
    let server = TestServer::start().await;
    let client = server.ecs_client().await;
    client
        .register_task_definition()
        .family("confo-list-td")
        .container_definitions(
            ContainerDefinition::builder()
                .name("app")
                .image("public.ecr.aws/docker/library/alpine:latest")
                .essential(true)
                .build(),
        )
        .send()
        .await
        .unwrap();
    let resp = client.list_task_definitions().send().await.unwrap();
    assert!(!resp.task_definition_arns().is_empty());
}

#[test_action("ecs", "ListTaskDefinitionFamilies", checksum = "ca148fca")]
#[tokio::test]
async fn ecs_list_task_definition_families() {
    let server = TestServer::start().await;
    let client = server.ecs_client().await;
    client
        .register_task_definition()
        .family("confo-family")
        .container_definitions(
            ContainerDefinition::builder()
                .name("app")
                .image("public.ecr.aws/docker/library/alpine:latest")
                .essential(true)
                .build(),
        )
        .send()
        .await
        .unwrap();
    let resp = client.list_task_definition_families().send().await.unwrap();
    assert!(resp.families().iter().any(|f| f == "confo-family"));
}

#[test_action("ecs", "TagResource", checksum = "fbc4b89a")]
#[tokio::test]
async fn ecs_tag_resource() {
    let server = TestServer::start().await;
    let client = server.ecs_client().await;
    let cluster = client
        .create_cluster()
        .cluster_name("confo-tag")
        .send()
        .await
        .unwrap()
        .cluster()
        .unwrap()
        .clone();
    client
        .tag_resource()
        .resource_arn(cluster.cluster_arn().unwrap())
        .tags(Tag::builder().key("env").value("prod").build())
        .send()
        .await
        .unwrap();
}

#[test_action("ecs", "UntagResource", checksum = "0cff3b01")]
#[tokio::test]
async fn ecs_untag_resource() {
    let server = TestServer::start().await;
    let client = server.ecs_client().await;
    let cluster = client
        .create_cluster()
        .cluster_name("confo-untag")
        .send()
        .await
        .unwrap()
        .cluster()
        .unwrap()
        .clone();
    client
        .tag_resource()
        .resource_arn(cluster.cluster_arn().unwrap())
        .tags(Tag::builder().key("env").value("prod").build())
        .send()
        .await
        .unwrap();
    client
        .untag_resource()
        .resource_arn(cluster.cluster_arn().unwrap())
        .tag_keys("env")
        .send()
        .await
        .unwrap();
}

#[test_action("ecs", "ListTagsForResource", checksum = "2ad51d6a")]
#[tokio::test]
async fn ecs_list_tags_for_resource() {
    let server = TestServer::start().await;
    let client = server.ecs_client().await;
    let cluster = client
        .create_cluster()
        .cluster_name("confo-listtags")
        .tags(Tag::builder().key("env").value("dev").build())
        .send()
        .await
        .unwrap()
        .cluster()
        .unwrap()
        .clone();
    let resp = client
        .list_tags_for_resource()
        .resource_arn(cluster.cluster_arn().unwrap())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.tags().len(), 1);
}

#[test_action("ecs", "PutAccountSetting", checksum = "ef8a7f7b")]
#[tokio::test]
async fn ecs_put_account_setting() {
    use aws_sdk_ecs::types::SettingName;
    let server = TestServer::start().await;
    let client = server.ecs_client().await;
    let resp = client
        .put_account_setting()
        .name(SettingName::TaskLongArnFormat)
        .value("enabled")
        .send()
        .await
        .unwrap();
    assert!(resp.setting().is_some());
}

#[test_action("ecs", "PutAccountSettingDefault", checksum = "dc08dc2d")]
#[tokio::test]
async fn ecs_put_account_setting_default() {
    use aws_sdk_ecs::types::SettingName;
    let server = TestServer::start().await;
    let client = server.ecs_client().await;
    let resp = client
        .put_account_setting_default()
        .name(SettingName::ServiceLongArnFormat)
        .value("enabled")
        .send()
        .await
        .unwrap();
    assert!(resp.setting().is_some());
}

#[test_action("ecs", "DeleteAccountSetting", checksum = "6f293917")]
#[tokio::test]
async fn ecs_delete_account_setting() {
    use aws_sdk_ecs::types::SettingName;
    let server = TestServer::start().await;
    let client = server.ecs_client().await;
    client
        .put_account_setting()
        .name(SettingName::TaskLongArnFormat)
        .value("enabled")
        .send()
        .await
        .unwrap();
    let resp = client
        .delete_account_setting()
        .name(SettingName::TaskLongArnFormat)
        .send()
        .await
        .unwrap();
    assert!(resp.setting().is_some());
}

#[test_action("ecs", "ListAccountSettings", checksum = "96955ca3")]
#[tokio::test]
async fn ecs_list_account_settings() {
    let server = TestServer::start().await;
    let client = server.ecs_client().await;
    let resp = client
        .list_account_settings()
        .effective_settings(true)
        .send()
        .await
        .unwrap();
    // New account has no defaults set; the call should succeed with an
    // empty-or-populated settings list.
    resp.settings();
}

// ── Batch 2: task lifecycle ────────────────────────────────────────

async fn register_conformance_task_def(client: &aws_sdk_ecs::Client, family: &str) {
    client
        .register_task_definition()
        .family(family)
        .container_definitions(
            ContainerDefinition::builder()
                .name("app")
                .image("public.ecr.aws/docker/library/alpine:latest")
                .essential(true)
                .build(),
        )
        .send()
        .await
        .unwrap();
}

#[test_action("ecs", "RunTask", checksum = "ac257001")]
#[tokio::test]
async fn ecs_run_task() {
    let server = TestServer::start().await;
    let client = server.ecs_client().await;
    client
        .create_cluster()
        .cluster_name("confo-run")
        .send()
        .await
        .unwrap();
    register_conformance_task_def(&client, "confo-run-td").await;
    let resp = client
        .run_task()
        .cluster("confo-run")
        .task_definition("confo-run-td")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.tasks().len(), 1);
    assert!(resp.failures().is_empty());
}

#[test_action("ecs", "StartTask", checksum = "0ae77c9c")]
#[tokio::test]
async fn ecs_start_task() {
    let server = TestServer::start().await;
    let client = server.ecs_client().await;
    client
        .create_cluster()
        .cluster_name("confo-start")
        .send()
        .await
        .unwrap();
    register_conformance_task_def(&client, "confo-start-td").await;
    // StartTask places the task on a real registered container instance.
    let ci = client
        .register_container_instance()
        .cluster("confo-start")
        .send()
        .await
        .unwrap();
    let ci_arn = ci
        .container_instance()
        .and_then(|c| c.container_instance_arn())
        .unwrap()
        .to_string();
    let resp = client
        .start_task()
        .cluster("confo-start")
        .task_definition("confo-start-td")
        .container_instances(ci_arn)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.tasks().len(), 1);
}

#[test_action("ecs", "DescribeTasks", checksum = "c0470f9b")]
#[tokio::test]
async fn ecs_describe_tasks() {
    let server = TestServer::start().await;
    let client = server.ecs_client().await;
    client
        .create_cluster()
        .cluster_name("confo-desc")
        .send()
        .await
        .unwrap();
    register_conformance_task_def(&client, "confo-desc-td").await;
    let run = client
        .run_task()
        .cluster("confo-desc")
        .task_definition("confo-desc-td")
        .send()
        .await
        .unwrap();
    let arn = run.tasks()[0].task_arn().unwrap().to_string();
    let described = client
        .describe_tasks()
        .cluster("confo-desc")
        .tasks(arn)
        .send()
        .await
        .unwrap();
    assert_eq!(described.tasks().len(), 1);
}

#[test_action("ecs", "ListTasks", checksum = "5e257f00")]
#[tokio::test]
async fn ecs_list_tasks() {
    let server = TestServer::start().await;
    let client = server.ecs_client().await;
    client
        .create_cluster()
        .cluster_name("confo-list-t")
        .send()
        .await
        .unwrap();
    register_conformance_task_def(&client, "confo-list-t-td").await;
    client
        .run_task()
        .cluster("confo-list-t")
        .task_definition("confo-list-t-td")
        .send()
        .await
        .unwrap();
    let resp = client
        .list_tasks()
        .cluster("confo-list-t")
        .send()
        .await
        .unwrap();
    assert!(!resp.task_arns().is_empty());
}

#[test_action("ecs", "StopTask", checksum = "4e25cd79")]
#[tokio::test]
async fn ecs_stop_task() {
    let server = TestServer::start().await;
    let client = server.ecs_client().await;
    client
        .create_cluster()
        .cluster_name("confo-stop")
        .send()
        .await
        .unwrap();
    register_conformance_task_def(&client, "confo-stop-td").await;
    let run = client
        .run_task()
        .cluster("confo-stop")
        .task_definition("confo-stop-td")
        .send()
        .await
        .unwrap();
    let arn = run.tasks()[0].task_arn().unwrap().to_string();
    let resp = client
        .stop_task()
        .cluster("confo-stop")
        .task(arn)
        .reason("test")
        .send()
        .await
        .unwrap();
    assert!(resp.task().is_some());
}

// ── Batch 3: services ──────────────────────────────────────────────

async fn bootstrap_service_fixtures(client: &aws_sdk_ecs::Client, cluster: &str, family: &str) {
    client
        .create_cluster()
        .cluster_name(cluster)
        .send()
        .await
        .unwrap();
    client
        .register_task_definition()
        .family(family)
        .container_definitions(
            ContainerDefinition::builder()
                .name("app")
                .image("public.ecr.aws/docker/library/alpine:latest")
                .essential(true)
                .build(),
        )
        .send()
        .await
        .unwrap();
}

/// Task sets require a service with `deploymentController=EXTERNAL`. This
/// helper provisions cluster + task def + service wired up that way so the
/// Batch 4 task-set tests exercise the realistic AWS path.
async fn bootstrap_external_service_fixtures(
    client: &aws_sdk_ecs::Client,
    cluster: &str,
    family: &str,
    service: &str,
) {
    use aws_sdk_ecs::types::{DeploymentController, DeploymentControllerType};
    bootstrap_service_fixtures(client, cluster, family).await;
    client
        .create_service()
        .cluster(cluster)
        .service_name(service)
        .task_definition(family)
        .deployment_controller(
            DeploymentController::builder()
                .r#type(DeploymentControllerType::External)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
}

#[test_action("ecs", "CreateService", checksum = "700f97cb")]
#[tokio::test]
async fn ecs_create_service() {
    let server = TestServer::start().await;
    let client = server.ecs_client().await;
    bootstrap_service_fixtures(&client, "confo-svc", "confo-svc-td").await;
    let resp = client
        .create_service()
        .cluster("confo-svc")
        .service_name("svc-a")
        .task_definition("confo-svc-td")
        .desired_count(1)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.service().unwrap().service_name(), Some("svc-a"));
}

#[test_action("ecs", "DescribeServices", checksum = "7209f0c8")]
#[tokio::test]
async fn ecs_describe_services() {
    let server = TestServer::start().await;
    let client = server.ecs_client().await;
    bootstrap_service_fixtures(&client, "confo-svc-desc", "confo-svc-desc-td").await;
    client
        .create_service()
        .cluster("confo-svc-desc")
        .service_name("svc-d")
        .task_definition("confo-svc-desc-td")
        .send()
        .await
        .unwrap();
    let resp = client
        .describe_services()
        .cluster("confo-svc-desc")
        .services("svc-d")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.services().len(), 1);
}

#[test_action("ecs", "ListServices", checksum = "4bc85a42")]
#[tokio::test]
async fn ecs_list_services() {
    let server = TestServer::start().await;
    let client = server.ecs_client().await;
    bootstrap_service_fixtures(&client, "confo-svc-list", "confo-svc-list-td").await;
    client
        .create_service()
        .cluster("confo-svc-list")
        .service_name("svc-l")
        .task_definition("confo-svc-list-td")
        .send()
        .await
        .unwrap();
    let resp = client
        .list_services()
        .cluster("confo-svc-list")
        .send()
        .await
        .unwrap();
    assert!(!resp.service_arns().is_empty());
}

#[test_action("ecs", "ListServicesByNamespace", checksum = "13f69425")]
#[tokio::test]
async fn ecs_list_services_by_namespace() {
    let server = TestServer::start().await;
    let client = server.ecs_client().await;
    let resp = client
        .list_services_by_namespace()
        .namespace("arn:aws:servicediscovery:us-east-1:111122223333:namespace/ns-1")
        .send()
        .await
        .unwrap();
    resp.service_arns();
}

#[test_action("ecs", "UpdateService", checksum = "ed4d312d")]
#[tokio::test]
async fn ecs_update_service() {
    let server = TestServer::start().await;
    let client = server.ecs_client().await;
    bootstrap_service_fixtures(&client, "confo-svc-up", "confo-svc-up-td").await;
    client
        .create_service()
        .cluster("confo-svc-up")
        .service_name("svc-u")
        .task_definition("confo-svc-up-td")
        .desired_count(1)
        .send()
        .await
        .unwrap();
    let resp = client
        .update_service()
        .cluster("confo-svc-up")
        .service("svc-u")
        .desired_count(2)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.service().unwrap().desired_count(), 2);
}

#[test_action("ecs", "DeleteService", checksum = "1341aa43")]
#[tokio::test]
async fn ecs_delete_service() {
    let server = TestServer::start().await;
    let client = server.ecs_client().await;
    bootstrap_service_fixtures(&client, "confo-svc-del", "confo-svc-del-td").await;
    client
        .create_service()
        .cluster("confo-svc-del")
        .service_name("svc-del")
        .task_definition("confo-svc-del-td")
        .desired_count(0)
        .send()
        .await
        .unwrap();
    let resp = client
        .delete_service()
        .cluster("confo-svc-del")
        .service("svc-del")
        .send()
        .await
        .unwrap();
    assert!(resp.service().is_some());
}

// ── Batch 4: completeness ──────────────────────────────────────────

#[test_action("ecs", "RegisterContainerInstance", checksum = "bc109d0c")]
#[tokio::test]
async fn ecs_register_container_instance() {
    let server = TestServer::start().await;
    let client = server.ecs_client().await;
    client
        .create_cluster()
        .cluster_name("confo-ci")
        .send()
        .await
        .unwrap();
    let resp = client
        .register_container_instance()
        .cluster("confo-ci")
        .send()
        .await
        .unwrap();
    assert!(resp.container_instance().is_some());
}

#[test_action("ecs", "DeregisterContainerInstance", checksum = "908101b7")]
#[tokio::test]
async fn ecs_deregister_container_instance() {
    let server = TestServer::start().await;
    let client = server.ecs_client().await;
    client
        .create_cluster()
        .cluster_name("confo-ci-dereg")
        .send()
        .await
        .unwrap();
    let ci = client
        .register_container_instance()
        .cluster("confo-ci-dereg")
        .send()
        .await
        .unwrap();
    let arn = ci
        .container_instance()
        .unwrap()
        .container_instance_arn()
        .unwrap()
        .to_string();
    let resp = client
        .deregister_container_instance()
        .cluster("confo-ci-dereg")
        .container_instance(arn)
        .send()
        .await
        .unwrap();
    assert!(resp.container_instance().is_some());
}

#[test_action("ecs", "DescribeContainerInstances", checksum = "07eac6ac")]
#[tokio::test]
async fn ecs_describe_container_instances() {
    let server = TestServer::start().await;
    let client = server.ecs_client().await;
    client
        .create_cluster()
        .cluster_name("confo-ci-desc")
        .send()
        .await
        .unwrap();
    let ci = client
        .register_container_instance()
        .cluster("confo-ci-desc")
        .send()
        .await
        .unwrap();
    let arn = ci
        .container_instance()
        .unwrap()
        .container_instance_arn()
        .unwrap()
        .to_string();
    let resp = client
        .describe_container_instances()
        .cluster("confo-ci-desc")
        .container_instances(arn)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.container_instances().len(), 1);
}

#[test_action("ecs", "ListContainerInstances", checksum = "6cb88efb")]
#[tokio::test]
async fn ecs_list_container_instances() {
    let server = TestServer::start().await;
    let client = server.ecs_client().await;
    client
        .create_cluster()
        .cluster_name("confo-ci-list")
        .send()
        .await
        .unwrap();
    client
        .register_container_instance()
        .cluster("confo-ci-list")
        .send()
        .await
        .unwrap();
    let resp = client
        .list_container_instances()
        .cluster("confo-ci-list")
        .send()
        .await
        .unwrap();
    assert!(!resp.container_instance_arns().is_empty());
}

#[test_action("ecs", "UpdateContainerAgent", checksum = "0c267e1e")]
#[tokio::test]
async fn ecs_update_container_agent() {
    let server = TestServer::start().await;
    let client = server.ecs_client().await;
    client
        .create_cluster()
        .cluster_name("confo-ci-agent")
        .send()
        .await
        .unwrap();
    let ci = client
        .register_container_instance()
        .cluster("confo-ci-agent")
        .send()
        .await
        .unwrap();
    let arn = ci
        .container_instance()
        .unwrap()
        .container_instance_arn()
        .unwrap()
        .to_string();
    let resp = client
        .update_container_agent()
        .cluster("confo-ci-agent")
        .container_instance(arn)
        .send()
        .await
        .unwrap();
    assert!(resp.container_instance().is_some());
}

#[test_action("ecs", "UpdateContainerInstancesState", checksum = "6dfac371")]
#[tokio::test]
async fn ecs_update_container_instances_state() {
    use aws_sdk_ecs::types::ContainerInstanceStatus;
    let server = TestServer::start().await;
    let client = server.ecs_client().await;
    client
        .create_cluster()
        .cluster_name("confo-ci-state")
        .send()
        .await
        .unwrap();
    let ci = client
        .register_container_instance()
        .cluster("confo-ci-state")
        .send()
        .await
        .unwrap();
    let arn = ci
        .container_instance()
        .unwrap()
        .container_instance_arn()
        .unwrap()
        .to_string();
    let resp = client
        .update_container_instances_state()
        .cluster("confo-ci-state")
        .container_instances(arn)
        .status(ContainerInstanceStatus::Draining)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.container_instances().len(), 1);
}

#[test_action("ecs", "PutAttributes", checksum = "c99d393b")]
#[tokio::test]
async fn ecs_put_attributes() {
    use aws_sdk_ecs::types::{Attribute, TargetType};
    let server = TestServer::start().await;
    let client = server.ecs_client().await;
    client
        .create_cluster()
        .cluster_name("confo-attr")
        .send()
        .await
        .unwrap();
    let resp = client
        .put_attributes()
        .cluster("confo-attr")
        .attributes(
            Attribute::builder()
                .name("env")
                .value("prod")
                .target_type(TargetType::ContainerInstance)
                .target_id("ci-1")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    assert!(!resp.attributes().is_empty());
}

#[test_action("ecs", "DeleteAttributes", checksum = "e60bd0b7")]
#[tokio::test]
async fn ecs_delete_attributes() {
    use aws_sdk_ecs::types::{Attribute, TargetType};
    let server = TestServer::start().await;
    let client = server.ecs_client().await;
    client
        .create_cluster()
        .cluster_name("confo-attr-del")
        .send()
        .await
        .unwrap();
    client
        .put_attributes()
        .cluster("confo-attr-del")
        .attributes(
            Attribute::builder()
                .name("env")
                .target_type(TargetType::ContainerInstance)
                .target_id("ci-1")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    let resp = client
        .delete_attributes()
        .cluster("confo-attr-del")
        .attributes(
            Attribute::builder()
                .name("env")
                .target_type(TargetType::ContainerInstance)
                .target_id("ci-1")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    resp.attributes();
}

#[test_action("ecs", "ListAttributes", checksum = "c4f675bd")]
#[tokio::test]
async fn ecs_list_attributes() {
    use aws_sdk_ecs::types::TargetType;
    let server = TestServer::start().await;
    let client = server.ecs_client().await;
    client
        .create_cluster()
        .cluster_name("confo-attr-list")
        .send()
        .await
        .unwrap();
    let resp = client
        .list_attributes()
        .cluster("confo-attr-list")
        .target_type(TargetType::ContainerInstance)
        .send()
        .await
        .unwrap();
    resp.attributes();
}

#[test_action("ecs", "CreateCapacityProvider", checksum = "0b1e10ac")]
#[tokio::test]
async fn ecs_create_capacity_provider() {
    use aws_sdk_ecs::types::{
        AutoScalingGroupProvider, ManagedScaling, ManagedTerminationProtection,
    };
    let server = TestServer::start().await;
    let client = server.ecs_client().await;
    let resp = client
        .create_capacity_provider()
        .name("my-provider")
        .auto_scaling_group_provider(
            AutoScalingGroupProvider::builder()
                .auto_scaling_group_arn(
                    "arn:aws:autoscaling:us-east-1:111122223333:autoScalingGroup:1",
                )
                .managed_scaling(ManagedScaling::builder().build())
                .managed_termination_protection(ManagedTerminationProtection::Disabled)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    assert!(resp.capacity_provider().is_some());
}

#[test_action("ecs", "DeleteCapacityProvider", checksum = "edd5d031")]
#[tokio::test]
async fn ecs_delete_capacity_provider() {
    use aws_sdk_ecs::types::{
        AutoScalingGroupProvider, ManagedScaling, ManagedTerminationProtection,
    };
    let server = TestServer::start().await;
    let client = server.ecs_client().await;
    client
        .create_capacity_provider()
        .name("my-provider-del")
        .auto_scaling_group_provider(
            AutoScalingGroupProvider::builder()
                .auto_scaling_group_arn(
                    "arn:aws:autoscaling:us-east-1:111122223333:autoScalingGroup:1",
                )
                .managed_scaling(ManagedScaling::builder().build())
                .managed_termination_protection(ManagedTerminationProtection::Disabled)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    let resp = client
        .delete_capacity_provider()
        .capacity_provider("my-provider-del")
        .send()
        .await
        .unwrap();
    assert!(resp.capacity_provider().is_some());
}

#[test_action("ecs", "DescribeCapacityProviders", checksum = "30d26f80")]
#[tokio::test]
async fn ecs_describe_capacity_providers() {
    let server = TestServer::start().await;
    let client = server.ecs_client().await;
    let resp = client.describe_capacity_providers().send().await.unwrap();
    resp.capacity_providers();
}

#[test_action("ecs", "UpdateCapacityProvider", checksum = "def5b8f2")]
#[tokio::test]
async fn ecs_update_capacity_provider() {
    use aws_sdk_ecs::types::{
        AutoScalingGroupProvider, AutoScalingGroupProviderUpdate, ManagedScaling,
        ManagedTerminationProtection,
    };
    let server = TestServer::start().await;
    let client = server.ecs_client().await;
    client
        .create_capacity_provider()
        .name("my-provider-up")
        .auto_scaling_group_provider(
            AutoScalingGroupProvider::builder()
                .auto_scaling_group_arn(
                    "arn:aws:autoscaling:us-east-1:111122223333:autoScalingGroup:1",
                )
                .managed_scaling(ManagedScaling::builder().build())
                .managed_termination_protection(ManagedTerminationProtection::Disabled)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    let resp = client
        .update_capacity_provider()
        .name("my-provider-up")
        .auto_scaling_group_provider(
            AutoScalingGroupProviderUpdate::builder()
                .managed_scaling(ManagedScaling::builder().build())
                .build(),
        )
        .send()
        .await
        .unwrap();
    assert!(resp.capacity_provider().is_some());
}

#[test_action("ecs", "GetTaskProtection", checksum = "487581f2")]
#[tokio::test]
async fn ecs_get_task_protection() {
    let server = TestServer::start().await;
    let client = server.ecs_client().await;
    client
        .create_cluster()
        .cluster_name("confo-prot")
        .send()
        .await
        .unwrap();
    let resp = client
        .get_task_protection()
        .cluster("confo-prot")
        .send()
        .await
        .unwrap();
    resp.protected_tasks();
}

#[test_action("ecs", "UpdateTaskProtection", checksum = "5b5526a7")]
#[tokio::test]
async fn ecs_update_task_protection() {
    let server = TestServer::start().await;
    let client = server.ecs_client().await;
    client
        .create_cluster()
        .cluster_name("confo-prot-up")
        .send()
        .await
        .unwrap();
    register_conformance_task_def(&client, "confo-prot-up-td").await;
    let run = client
        .run_task()
        .cluster("confo-prot-up")
        .task_definition("confo-prot-up-td")
        .send()
        .await
        .unwrap();
    let arn = run.tasks()[0].task_arn().unwrap().to_string();
    let resp = client
        .update_task_protection()
        .cluster("confo-prot-up")
        .tasks(arn)
        .protection_enabled(true)
        .send()
        .await
        .unwrap();
    resp.protected_tasks();
}

#[test_action("ecs", "CreateTaskSet", checksum = "bf51b8b6")]
#[tokio::test]
async fn ecs_create_task_set() {
    let server = TestServer::start().await;
    let client = server.ecs_client().await;
    bootstrap_external_service_fixtures(&client, "confo-ts", "confo-ts-td", "svc").await;
    let resp = client
        .create_task_set()
        .cluster("confo-ts")
        .service("svc")
        .task_definition("confo-ts-td")
        .send()
        .await
        .unwrap();
    assert!(resp.task_set().is_some());
}

#[test_action("ecs", "UpdateTaskSet", checksum = "b4f9a7ab")]
#[tokio::test]
async fn ecs_update_task_set() {
    use aws_sdk_ecs::types::{Scale, ScaleUnit};
    let server = TestServer::start().await;
    let client = server.ecs_client().await;
    bootstrap_external_service_fixtures(&client, "confo-ts-up", "confo-ts-up-td", "svc").await;
    let ts = client
        .create_task_set()
        .cluster("confo-ts-up")
        .service("svc")
        .task_definition("confo-ts-up-td")
        .send()
        .await
        .unwrap();
    let id = ts.task_set().unwrap().id().unwrap().to_string();
    let resp = client
        .update_task_set()
        .cluster("confo-ts-up")
        .service("svc")
        .task_set(id)
        .scale(
            Scale::builder()
                .unit(ScaleUnit::Percent)
                .value(50.0)
                .build(),
        )
        .send()
        .await
        .unwrap();
    assert!(resp.task_set().is_some());
}

#[test_action("ecs", "DeleteTaskSet", checksum = "5da66385")]
#[tokio::test]
async fn ecs_delete_task_set() {
    let server = TestServer::start().await;
    let client = server.ecs_client().await;
    bootstrap_external_service_fixtures(&client, "confo-ts-del", "confo-ts-del-td", "svc").await;
    let ts = client
        .create_task_set()
        .cluster("confo-ts-del")
        .service("svc")
        .task_definition("confo-ts-del-td")
        .send()
        .await
        .unwrap();
    let id = ts.task_set().unwrap().id().unwrap().to_string();
    let resp = client
        .delete_task_set()
        .cluster("confo-ts-del")
        .service("svc")
        .task_set(id)
        .send()
        .await
        .unwrap();
    assert!(resp.task_set().is_some());
}

#[test_action("ecs", "DescribeTaskSets", checksum = "443b23f3")]
#[tokio::test]
async fn ecs_describe_task_sets() {
    let server = TestServer::start().await;
    let client = server.ecs_client().await;
    bootstrap_external_service_fixtures(&client, "confo-ts-desc", "confo-ts-desc-td", "svc").await;
    client
        .create_task_set()
        .cluster("confo-ts-desc")
        .service("svc")
        .task_definition("confo-ts-desc-td")
        .send()
        .await
        .unwrap();
    let resp = client
        .describe_task_sets()
        .cluster("confo-ts-desc")
        .service("svc")
        .send()
        .await
        .unwrap();
    assert!(!resp.task_sets().is_empty());
}

#[test_action("ecs", "UpdateServicePrimaryTaskSet", checksum = "4c3b87f0")]
#[tokio::test]
async fn ecs_update_service_primary_task_set() {
    let server = TestServer::start().await;
    let client = server.ecs_client().await;
    bootstrap_external_service_fixtures(&client, "confo-ts-primary", "confo-ts-primary-td", "svc")
        .await;
    let ts = client
        .create_task_set()
        .cluster("confo-ts-primary")
        .service("svc")
        .task_definition("confo-ts-primary-td")
        .send()
        .await
        .unwrap();
    let id = ts.task_set().unwrap().id().unwrap().to_string();
    let resp = client
        .update_service_primary_task_set()
        .cluster("confo-ts-primary")
        .service("svc")
        .primary_task_set(id)
        .send()
        .await
        .unwrap();
    assert!(resp.task_set().is_some());
}

#[test_action("ecs", "ExecuteCommand", checksum = "8a4b9a25")]
#[tokio::test]
async fn ecs_execute_command() {
    let server = TestServer::start().await;
    let client = server.ecs_client().await;
    client
        .create_cluster()
        .cluster_name("confo-exec")
        .send()
        .await
        .unwrap();
    register_conformance_task_def(&client, "confo-exec-td").await;
    let run = client
        .run_task()
        .cluster("confo-exec")
        .task_definition("confo-exec-td")
        .enable_execute_command(true)
        .send()
        .await
        .unwrap();
    let arn = run.tasks()[0].task_arn().unwrap().to_string();
    let resp = client
        .execute_command()
        .cluster("confo-exec")
        .task(arn)
        .command("ls")
        .interactive(true)
        .send()
        .await
        .unwrap();
    resp.session();
}

#[test_action("ecs", "SubmitContainerStateChange", checksum = "129dc8b3")]
#[tokio::test]
async fn ecs_submit_container_state_change() {
    let server = TestServer::start().await;
    let client = server.ecs_client().await;
    let resp = client
        .submit_container_state_change()
        .cluster("any")
        .send()
        .await
        .unwrap();
    resp.acknowledgment();
}

#[test_action("ecs", "SubmitTaskStateChange", checksum = "8dbcf4ff")]
#[tokio::test]
async fn ecs_submit_task_state_change() {
    let server = TestServer::start().await;
    let client = server.ecs_client().await;
    let resp = client.submit_task_state_change().send().await.unwrap();
    resp.acknowledgment();
}

#[test_action("ecs", "SubmitAttachmentStateChanges", checksum = "95374e0d")]
#[tokio::test]
async fn ecs_submit_attachment_state_changes() {
    use aws_sdk_ecs::types::AttachmentStateChange;
    let server = TestServer::start().await;
    let client = server.ecs_client().await;
    let resp = client
        .submit_attachment_state_changes()
        .attachments(
            AttachmentStateChange::builder()
                .attachment_arn("arn:aws:ecs:us-east-1:111122223333:attachment/x")
                .status("ATTACHED")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    resp.acknowledgment();
}

#[test_action("ecs", "DiscoverPollEndpoint", checksum = "c9e6854a")]
#[tokio::test]
async fn ecs_discover_poll_endpoint() {
    let server = TestServer::start().await;
    let client = server.ecs_client().await;
    let resp = client.discover_poll_endpoint().send().await.unwrap();
    assert!(resp.endpoint().is_some());
}

#[test_action("ecs", "StopServiceDeployment", checksum = "aecfb385")]
#[tokio::test]
async fn ecs_stop_service_deployment() {
    let server = TestServer::start().await;
    let client = server.ecs_client().await;
    bootstrap_service_fixtures(&client, "confo-sd-stop", "confo-sd-stop-td").await;
    let created = client
        .create_service()
        .cluster("confo-sd-stop")
        .service_name("svc")
        .task_definition("confo-sd-stop-td")
        .send()
        .await
        .unwrap();
    let svc = created.service().unwrap();
    let dep_id = svc.deployments()[0].id().unwrap();
    let arn = format!("{}/{}", svc.service_arn().unwrap(), dep_id);
    let resp = client
        .stop_service_deployment()
        .service_deployment_arn(arn)
        .send()
        .await
        .unwrap();
    resp.service_deployment_arn();
}

#[test_action("ecs", "ListServiceDeployments", checksum = "7c21263a")]
#[tokio::test]
async fn ecs_list_service_deployments() {
    let server = TestServer::start().await;
    let client = server.ecs_client().await;
    bootstrap_service_fixtures(&client, "confo-sd-list", "confo-sd-list-td").await;
    client
        .create_service()
        .cluster("confo-sd-list")
        .service_name("svc")
        .task_definition("confo-sd-list-td")
        .send()
        .await
        .unwrap();
    let resp = client
        .list_service_deployments()
        .cluster("confo-sd-list")
        .service("svc")
        .send()
        .await
        .unwrap();
    resp.service_deployments();
}

#[test_action("ecs", "DescribeServiceDeployments", checksum = "b1eeff34")]
#[tokio::test]
async fn ecs_describe_service_deployments() {
    let server = TestServer::start().await;
    let client = server.ecs_client().await;
    bootstrap_service_fixtures(&client, "confo-sd-desc", "confo-sd-desc-td").await;
    let created = client
        .create_service()
        .cluster("confo-sd-desc")
        .service_name("svc")
        .task_definition("confo-sd-desc-td")
        .send()
        .await
        .unwrap();
    let svc = created.service().unwrap();
    let dep_id = svc.deployments()[0].id().unwrap();
    let arn = format!("{}/{}", svc.service_arn().unwrap(), dep_id);
    let resp = client
        .describe_service_deployments()
        .service_deployment_arns(arn)
        .send()
        .await
        .unwrap();
    resp.service_deployments();
}

/// POST an awsJson1.1 ECS request directly. `ContinueServiceDeployment` and
/// the PAUSE lifecycle-hook target type are newer than aws-sdk-ecs 1.x, so the
/// typed client can't express them yet — drive them over raw HTTP instead.
async fn ecs_json(
    http: &reqwest::Client,
    endpoint: &str,
    action: &str,
    body: serde_json::Value,
) -> serde_json::Value {
    let resp = http
        .post(endpoint)
        .header("content-type", "application/x-amz-json-1.1")
        .header(
            "x-amz-target",
            format!("AmazonEC2ContainerServiceV20141113.{action}"),
        )
        .header(
            "authorization",
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20260411/us-east-1/ecs/aws4_request, SignedHeaders=host, Signature=fake",
        )
        .body(body.to_string())
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_success(),
        "ECS {action} failed: {:?}",
        resp.status()
    );
    resp.json().await.unwrap()
}

#[test_action("ecs", "ContinueServiceDeployment", checksum = "e1f3b96b")]
#[tokio::test]
async fn ecs_continue_service_deployment() {
    let server = TestServer::start().await;
    let http = reqwest::Client::new();
    let ep = server.endpoint();

    ecs_json(
        &http,
        ep,
        "CreateCluster",
        serde_json::json!({"clusterName": "confo-csd"}),
    )
    .await;
    ecs_json(
        &http,
        ep,
        "RegisterTaskDefinition",
        serde_json::json!({
            "family": "confo-csd-td",
            "containerDefinitions": [{"name": "app", "image": "nginx:latest", "memory": 128}],
        }),
    )
    .await;

    // Create a service whose deployment pauses at a PAUSE lifecycle hook.
    let created = ecs_json(
        &http,
        ep,
        "CreateService",
        serde_json::json!({
            "cluster": "confo-csd",
            "serviceName": "csd-svc",
            "taskDefinition": "confo-csd-td",
            "desiredCount": 1,
            "deploymentConfiguration": {
                "lifecycleHooks": [{
                    "targetType": "PAUSE",
                    "lifecycleStages": ["POST_PRODUCTION_TRAFFIC_SHIFT"],
                    "hookTargetArn": "arn:aws:lambda:us-east-1:123456789012:function:noop",
                    "roleArn": "arn:aws:iam::123456789012:role/hook"
                }]
            }
        }),
    )
    .await;
    let svc = &created["service"];
    let service_arn = svc["serviceArn"].as_str().unwrap();
    let deployment_id = svc["deployments"][0]["id"].as_str().unwrap();
    let arn = format!("{service_arn}/{deployment_id}");

    // The paused deployment surfaces its hookId via DescribeServiceDeployments.
    let described = ecs_json(
        &http,
        ep,
        "DescribeServiceDeployments",
        serde_json::json!({"serviceDeploymentArns": [arn]}),
    )
    .await;
    let hook_id = described["serviceDeployments"][0]["lifecycleHookDetails"][0]["hookId"]
        .as_str()
        .expect("paused deployment should expose a hookId");

    // Continue the paused deployment.
    let resp = ecs_json(
        &http,
        ep,
        "ContinueServiceDeployment",
        serde_json::json!({
            "serviceDeploymentArn": arn,
            "hookId": hook_id,
            "action": "CONTINUE"
        }),
    )
    .await;
    assert_eq!(resp["serviceDeploymentArn"].as_str().unwrap(), arn);
}

#[tokio::test]
async fn ecs_continue_service_deployment_rollback_and_errors() {
    let server = TestServer::start().await;
    let http = reqwest::Client::new();
    let ep = server.endpoint();

    ecs_json(
        &http,
        ep,
        "CreateCluster",
        serde_json::json!({"clusterName": "confo-csd-rb"}),
    )
    .await;
    ecs_json(
        &http,
        ep,
        "RegisterTaskDefinition",
        serde_json::json!({
            "family": "confo-csd-rb-td",
            "containerDefinitions": [{"name": "app", "image": "nginx:latest", "memory": 128}],
        }),
    )
    .await;
    let created = ecs_json(
        &http,
        ep,
        "CreateService",
        serde_json::json!({
            "cluster": "confo-csd-rb",
            "serviceName": "csd-rb-svc",
            "taskDefinition": "confo-csd-rb-td",
            "desiredCount": 1,
            "deploymentConfiguration": {
                "lifecycleHooks": [{
                    "targetType": "PAUSE",
                    "lifecycleStages": ["PRODUCTION_TRAFFIC_SHIFT"]
                }]
            }
        }),
    )
    .await;
    let svc = &created["service"];
    let arn = format!(
        "{}/{}",
        svc["serviceArn"].as_str().unwrap(),
        svc["deployments"][0]["id"].as_str().unwrap()
    );
    let hook_id = ecs_json(
        &http,
        ep,
        "DescribeServiceDeployments",
        serde_json::json!({"serviceDeploymentArns": [arn]}),
    )
    .await["serviceDeployments"][0]["lifecycleHookDetails"][0]["hookId"]
        .as_str()
        .unwrap()
        .to_string();

    // ROLLBACK stops the deployment.
    ecs_json(
        &http,
        ep,
        "ContinueServiceDeployment",
        serde_json::json!({"serviceDeploymentArn": arn, "hookId": hook_id, "action": "ROLLBACK"}),
    )
    .await;
    let after = ecs_json(
        &http,
        ep,
        "DescribeServiceDeployments",
        serde_json::json!({"serviceDeploymentArns": [arn]}),
    )
    .await;
    assert_eq!(
        after["serviceDeployments"][0]["status"].as_str().unwrap(),
        "STOPPED"
    );

    // An unknown service deployment ARN errors.
    let resp = http
        .post(ep)
        .header("content-type", "application/x-amz-json-1.1")
        .header(
            "x-amz-target",
            "AmazonEC2ContainerServiceV20141113.ContinueServiceDeployment",
        )
        .header(
            "authorization",
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20260411/us-east-1/ecs/aws4_request, SignedHeaders=host, Signature=fake",
        )
        .body(
            serde_json::json!({
                "serviceDeploymentArn": "arn:aws:ecs:us-east-1:111122223333:service-deployment/c/s/missing",
                "hookId": "hook-x",
                "action": "CONTINUE"
            })
            .to_string(),
        )
        .send()
        .await
        .unwrap();
    assert!(!resp.status().is_success());
}

#[test_action("ecs", "DescribeServiceRevisions", checksum = "771818ca")]
#[tokio::test]
async fn ecs_describe_service_revisions() {
    let server = TestServer::start().await;
    let client = server.ecs_client().await;
    let resp = client
        .describe_service_revisions()
        .service_revision_arns("arn:aws:ecs:us-east-1:111122223333:service-revision/c/s/1")
        .send()
        .await
        .unwrap();
    resp.service_revisions();
}

// ── Daemon + ExpressGatewayService conformance (2026 ops) ───────────────

async fn create_default_cluster(client: &aws_sdk_ecs::Client) {
    client
        .create_cluster()
        .cluster_name("default")
        .send()
        .await
        .unwrap();
}

async fn register_daemon_td_arn(client: &aws_sdk_ecs::Client, family: &str) -> String {
    use aws_sdk_ecs::types::DaemonContainerDefinition;
    let container = DaemonContainerDefinition::builder()
        .name("agent")
        .image("nginx:latest")
        .build()
        .unwrap();
    let resp = client
        .register_daemon_task_definition()
        .family(family)
        .container_definitions(container)
        .send()
        .await
        .unwrap();
    resp.daemon_task_definition_arn().unwrap().to_string()
}

#[test_action("ecs", "RegisterDaemonTaskDefinition", checksum = "53384b62")]
#[tokio::test]
async fn ecs_register_daemon_task_definition() {
    let server = TestServer::start().await;
    let client = server.ecs_client().await;
    let arn = register_daemon_td_arn(&client, "daemon-conf-register").await;
    assert!(arn.contains(":daemon-task-definition/"));
}

#[test_action("ecs", "DescribeDaemonTaskDefinition", checksum = "46f06ed8")]
#[tokio::test]
async fn ecs_describe_daemon_task_definition() {
    let server = TestServer::start().await;
    let client = server.ecs_client().await;
    let arn = register_daemon_td_arn(&client, "daemon-conf-desc").await;
    let resp = client
        .describe_daemon_task_definition()
        .daemon_task_definition(&arn)
        .send()
        .await
        .unwrap();
    assert!(resp.daemon_task_definition().is_some());
}

#[test_action("ecs", "DeleteDaemonTaskDefinition", checksum = "cbd6b909")]
#[tokio::test]
async fn ecs_delete_daemon_task_definition() {
    let server = TestServer::start().await;
    let client = server.ecs_client().await;
    let arn = register_daemon_td_arn(&client, "daemon-conf-del").await;
    let resp = client
        .delete_daemon_task_definition()
        .daemon_task_definition(&arn)
        .send()
        .await
        .unwrap();
    assert!(resp.daemon_task_definition_arn().is_some());
}

#[test_action("ecs", "ListDaemonTaskDefinitions", checksum = "b6ebee95")]
#[tokio::test]
async fn ecs_list_daemon_task_definitions() {
    let server = TestServer::start().await;
    let client = server.ecs_client().await;
    register_daemon_td_arn(&client, "daemon-conf-list").await;
    let resp = client.list_daemon_task_definitions().send().await.unwrap();
    assert!(!resp.daemon_task_definitions().is_empty());
}

async fn create_daemon_with_arn(client: &aws_sdk_ecs::Client, name: &str) -> String {
    let arn = register_daemon_td_arn(client, name).await;
    let resp = client
        .create_daemon()
        .daemon_name(name)
        .daemon_task_definition_arn(arn)
        .capacity_provider_arns("FARGATE")
        .send()
        .await
        .unwrap();
    resp.daemon_arn().unwrap().to_string()
}

#[test_action("ecs", "CreateDaemon", checksum = "2c40f5eb")]
#[tokio::test]
async fn ecs_create_daemon() {
    let server = TestServer::start().await;
    let client = server.ecs_client().await;
    create_default_cluster(&client).await;
    let arn = create_daemon_with_arn(&client, "conf-create").await;
    assert!(arn.contains(":daemon/"));
}

#[test_action("ecs", "DescribeDaemon", checksum = "25f93304")]
#[tokio::test]
async fn ecs_describe_daemon() {
    let server = TestServer::start().await;
    let client = server.ecs_client().await;
    create_default_cluster(&client).await;
    let arn = create_daemon_with_arn(&client, "conf-desc-d").await;
    let resp = client
        .describe_daemon()
        .daemon_arn(arn)
        .send()
        .await
        .unwrap();
    assert!(resp.daemon().is_some());
}

#[test_action("ecs", "UpdateDaemon", checksum = "2b41a668")]
#[tokio::test]
async fn ecs_update_daemon() {
    let server = TestServer::start().await;
    let client = server.ecs_client().await;
    create_default_cluster(&client).await;
    let arn = create_daemon_with_arn(&client, "conf-upd").await;
    let new_td = register_daemon_td_arn(&client, "conf-upd").await;
    let resp = client
        .update_daemon()
        .daemon_arn(arn)
        .daemon_task_definition_arn(new_td)
        .capacity_provider_arns("FARGATE")
        .send()
        .await
        .unwrap();
    assert!(resp.daemon_arn().is_some());
}

#[test_action("ecs", "DeleteDaemon", checksum = "b10fefb1")]
#[tokio::test]
async fn ecs_delete_daemon() {
    let server = TestServer::start().await;
    let client = server.ecs_client().await;
    create_default_cluster(&client).await;
    let arn = create_daemon_with_arn(&client, "conf-del-d").await;
    let resp = client.delete_daemon().daemon_arn(arn).send().await.unwrap();
    assert!(resp.daemon_arn().is_some());
}

#[test_action("ecs", "ListDaemons", checksum = "7f97207d")]
#[tokio::test]
async fn ecs_list_daemons() {
    let server = TestServer::start().await;
    let client = server.ecs_client().await;
    create_default_cluster(&client).await;
    create_daemon_with_arn(&client, "conf-list-d").await;
    let resp = client.list_daemons().send().await.unwrap();
    assert!(!resp.daemon_summaries_list().is_empty());
}

async fn create_daemon_get_deployment(
    client: &aws_sdk_ecs::Client,
    name: &str,
) -> (String, String) {
    let td = register_daemon_td_arn(client, name).await;
    let resp = client
        .create_daemon()
        .daemon_name(name)
        .daemon_task_definition_arn(td)
        .capacity_provider_arns("FARGATE")
        .send()
        .await
        .unwrap();
    (
        resp.daemon_arn().unwrap().to_string(),
        resp.deployment_arn().unwrap().to_string(),
    )
}

#[test_action("ecs", "DescribeDaemonDeployments", checksum = "09589af9")]
#[tokio::test]
async fn ecs_describe_daemon_deployments() {
    let server = TestServer::start().await;
    let client = server.ecs_client().await;
    create_default_cluster(&client).await;
    let (_d, dep) = create_daemon_get_deployment(&client, "conf-desc-deps").await;
    let resp = client
        .describe_daemon_deployments()
        .daemon_deployment_arns(dep)
        .send()
        .await
        .unwrap();
    assert!(!resp.daemon_deployments().is_empty());
}

#[test_action("ecs", "ListDaemonDeployments", checksum = "842a7d48")]
#[tokio::test]
async fn ecs_list_daemon_deployments() {
    let server = TestServer::start().await;
    let client = server.ecs_client().await;
    create_default_cluster(&client).await;
    let (d_arn, _) = create_daemon_get_deployment(&client, "conf-list-deps").await;
    let resp = client
        .list_daemon_deployments()
        .daemon_arn(d_arn)
        .send()
        .await
        .unwrap();
    assert!(!resp.daemon_deployments().is_empty());
}

#[test_action("ecs", "DescribeDaemonRevisions", checksum = "f3f01047")]
#[tokio::test]
async fn ecs_describe_daemon_revisions() {
    let server = TestServer::start().await;
    let client = server.ecs_client().await;
    create_default_cluster(&client).await;
    let (_d, dep) = create_daemon_get_deployment(&client, "conf-desc-revs").await;
    let resp = client
        .describe_daemon_revisions()
        .daemon_revision_arns(dep)
        .send()
        .await
        .unwrap();
    assert!(!resp.daemon_revisions().is_empty());
}

fn primary_eg_container() -> aws_sdk_ecs::types::ExpressGatewayContainer {
    aws_sdk_ecs::types::ExpressGatewayContainer::builder()
        .image("nginx:latest")
        .build()
        .unwrap()
}

#[test_action("ecs", "CreateExpressGatewayService", checksum = "a61aa54e")]
#[tokio::test]
async fn ecs_create_express_gateway_service() {
    let server = TestServer::start().await;
    let client = server.ecs_client().await;
    create_default_cluster(&client).await;
    let resp = client
        .create_express_gateway_service()
        .execution_role_arn("arn:aws:iam::000000000000:role/execution")
        .infrastructure_role_arn("arn:aws:iam::000000000000:role/infra")
        .service_name("eg-conf-create")
        .primary_container(primary_eg_container())
        .send()
        .await
        .unwrap();
    assert!(resp.service().is_some());
}

#[test_action("ecs", "DescribeExpressGatewayService", checksum = "aa739a71")]
#[tokio::test]
async fn ecs_describe_express_gateway_service() {
    let server = TestServer::start().await;
    let client = server.ecs_client().await;
    create_default_cluster(&client).await;
    let created = client
        .create_express_gateway_service()
        .execution_role_arn("arn:aws:iam::000000000000:role/execution")
        .infrastructure_role_arn("arn:aws:iam::000000000000:role/infra")
        .service_name("eg-conf-desc")
        .primary_container(primary_eg_container())
        .send()
        .await
        .unwrap();
    let eg_arn = created
        .service()
        .unwrap()
        .service_arn()
        .unwrap()
        .to_string();
    let resp = client
        .describe_express_gateway_service()
        .service_arn(eg_arn.clone())
        .send()
        .await
        .unwrap();
    assert!(resp.service().is_some());
}

#[test_action("ecs", "UpdateExpressGatewayService", checksum = "03be7c30")]
#[tokio::test]
async fn ecs_update_express_gateway_service() {
    let server = TestServer::start().await;
    let client = server.ecs_client().await;
    create_default_cluster(&client).await;
    let created = client
        .create_express_gateway_service()
        .execution_role_arn("arn:aws:iam::000000000000:role/execution")
        .infrastructure_role_arn("arn:aws:iam::000000000000:role/infra")
        .service_name("eg-conf-upd")
        .primary_container(primary_eg_container())
        .send()
        .await
        .unwrap();
    let eg_arn = created
        .service()
        .unwrap()
        .service_arn()
        .unwrap()
        .to_string();
    let resp = client
        .update_express_gateway_service()
        .service_arn(eg_arn.clone())
        .cpu("512")
        .send()
        .await
        .unwrap();
    assert!(resp.service().is_some());
}

#[test_action("ecs", "DeleteExpressGatewayService", checksum = "b1113f96")]
#[tokio::test]
async fn ecs_delete_express_gateway_service() {
    let server = TestServer::start().await;
    let client = server.ecs_client().await;
    create_default_cluster(&client).await;
    let created = client
        .create_express_gateway_service()
        .execution_role_arn("arn:aws:iam::000000000000:role/execution")
        .infrastructure_role_arn("arn:aws:iam::000000000000:role/infra")
        .service_name("eg-conf-del")
        .primary_container(primary_eg_container())
        .send()
        .await
        .unwrap();
    let eg_arn = created
        .service()
        .unwrap()
        .service_arn()
        .unwrap()
        .to_string();
    let resp = client
        .delete_express_gateway_service()
        .service_arn(eg_arn.clone())
        .send()
        .await
        .unwrap();
    assert!(resp.service().is_some());
}

// ── Gateways (2026 ops) ─────────────────────────────────────────────────
//
// aws-sdk-ecs 1.x predates the gateway operations, so they are driven over
// raw awsJson1.1 with their exact model wire names.

async fn create_test_gateway(http: &reqwest::Client, ep: &str, name: &str) -> String {
    let resp = ecs_json(
        http,
        ep,
        "CreateGateway",
        serde_json::json!({
            "name": name,
            "infrastructureRoleArn": "arn:aws:iam::123456789012:role/ecsInfrastructureRole",
            "configuration": {"hostSharedAlb": {
                "networkConfiguration": {"subnets": ["subnet-0123456789abcdef0"]},
                "scheme": "internet-facing",
                "protocol": "HTTPS",
                "port": 443,
                "maxAlbCount": 10,
            }},
            "tags": [{"key": "team", "value": "web"}],
            "propagateTags": "GATEWAY",
        }),
    )
    .await;
    resp["gatewayArn"].as_str().unwrap().to_string()
}

/// Cluster + gateway associated with it + a service whose gateway connector
/// targets the task definition's `web` port, so the gateway has one route.
/// Returns `(gateway_arn, service_arn)`.
async fn gateway_with_route(http: &reqwest::Client, ep: &str, prefix: &str) -> (String, String) {
    let cluster = format!("{prefix}-cluster");
    ecs_json(
        http,
        ep,
        "CreateCluster",
        serde_json::json!({"clusterName": cluster}),
    )
    .await;
    ecs_json(
        http,
        ep,
        "RegisterTaskDefinition",
        serde_json::json!({
            "family": format!("{prefix}-td"),
            "containerDefinitions": [{
                "name": "app",
                "image": "nginx:latest",
                "memory": 128,
                "portMappings": [{"containerPort": 80, "name": "web"}],
            }],
        }),
    )
    .await;
    let gateway_arn = create_test_gateway(http, ep, &format!("{prefix}-gw")).await;
    ecs_json(
        http,
        ep,
        "AssociateGatewayWithCluster",
        serde_json::json!({"gateway": gateway_arn, "cluster": cluster}),
    )
    .await;
    let svc = ecs_json(
        http,
        ep,
        "CreateService",
        serde_json::json!({
            "cluster": cluster,
            "serviceName": format!("{prefix}-svc"),
            "taskDefinition": format!("{prefix}-td"),
            "desiredCount": 0,
            "gatewayConnectors": [{"portName": "web", "filters": {"accessTypes": ["PUBLIC"]}}],
        }),
    )
    .await;
    let service_arn = svc["service"]["serviceArn"].as_str().unwrap().to_string();
    (gateway_arn, service_arn)
}

#[test_action("ecs", "CreateGateway", checksum = "e9610747")]
#[tokio::test]
async fn ecs_create_gateway() {
    let server = TestServer::start().await;
    let http = reqwest::Client::new();
    let ep = server.endpoint();
    let arn = create_test_gateway(&http, ep, "conf-gw-create").await;
    assert!(arn.contains(":gateway/conf-gw-create/"));
    let described = ecs_json(
        &http,
        ep,
        "DescribeGateway",
        serde_json::json!({"gateway": "conf-gw-create"}),
    )
    .await;
    assert_eq!(described["gateway"]["status"], "ACTIVE");
}

#[test_action("ecs", "DescribeGateway", checksum = "45de2400")]
#[tokio::test]
async fn ecs_describe_gateway() {
    let server = TestServer::start().await;
    let http = reqwest::Client::new();
    let ep = server.endpoint();
    let arn = create_test_gateway(&http, ep, "conf-gw-describe").await;
    let resp = ecs_json(
        &http,
        ep,
        "DescribeGateway",
        serde_json::json!({"gateway": arn}),
    )
    .await;
    let gw = &resp["gateway"];
    assert_eq!(gw["name"], "conf-gw-describe");
    assert_eq!(gw["accessType"], "PUBLIC");
    assert_eq!(gw["configuration"]["hostSharedAlb"]["currentAlbCount"], 1);
    assert_eq!(gw["tags"][0]["key"], "team");
}

#[test_action("ecs", "UpdateGateway", checksum = "331d57d2")]
#[tokio::test]
async fn ecs_update_gateway() {
    let server = TestServer::start().await;
    let http = reqwest::Client::new();
    let ep = server.endpoint();
    create_test_gateway(&http, ep, "conf-gw-update").await;
    let resp = ecs_json(
        &http,
        ep,
        "UpdateGateway",
        serde_json::json!({
            "gateway": "conf-gw-update",
            "configuration": {"hostSharedAlb": {"maxAlbCount": 20, "httpDesync": "strictest"}},
        }),
    )
    .await;
    assert_eq!(resp["status"], "UPDATE_IN_PROGRESS");
    let described = ecs_json(
        &http,
        ep,
        "DescribeGateway",
        serde_json::json!({"gateway": "conf-gw-update"}),
    )
    .await;
    let alb = &described["gateway"]["configuration"]["hostSharedAlb"];
    assert_eq!(alb["maxAlbCount"], 20);
    assert_eq!(alb["httpDesync"], "strictest");
}

#[test_action("ecs", "DeleteGateway", checksum = "fe06124f")]
#[tokio::test]
async fn ecs_delete_gateway() {
    let server = TestServer::start().await;
    let http = reqwest::Client::new();
    let ep = server.endpoint();
    let arn = create_test_gateway(&http, ep, "conf-gw-delete").await;
    let resp = ecs_json(
        &http,
        ep,
        "DeleteGateway",
        serde_json::json!({"gateway": "conf-gw-delete"}),
    )
    .await;
    assert_eq!(resp["status"], "DELETED");
    assert_eq!(resp["gatewayArn"], arn);
}

#[test_action("ecs", "ListGateways", checksum = "28d38054")]
#[tokio::test]
async fn ecs_list_gateways() {
    let server = TestServer::start().await;
    let http = reqwest::Client::new();
    let ep = server.endpoint();
    create_test_gateway(&http, ep, "conf-gw-list-a").await;
    create_test_gateway(&http, ep, "conf-gw-list-b").await;
    let first = ecs_json(
        &http,
        ep,
        "ListGateways",
        serde_json::json!({"status": ["ACTIVE"], "maxResults": 1}),
    )
    .await;
    assert_eq!(first["gateways"].as_array().unwrap().len(), 1);
    let token = first["nextToken"].as_str().unwrap();
    let second = ecs_json(
        &http,
        ep,
        "ListGateways",
        serde_json::json!({"status": ["ACTIVE"], "maxResults": 1, "nextToken": token}),
    )
    .await;
    assert_eq!(second["gateways"][0]["name"], "conf-gw-list-b");
    assert!(second.get("nextToken").is_none());
}

#[test_action("ecs", "AssociateGatewayWithCluster", checksum = "ee55b6fe")]
#[tokio::test]
async fn ecs_associate_gateway_with_cluster() {
    let server = TestServer::start().await;
    let http = reqwest::Client::new();
    let ep = server.endpoint();
    ecs_json(
        &http,
        ep,
        "CreateCluster",
        serde_json::json!({"clusterName": "conf-gw-assoc"}),
    )
    .await;
    let arn = create_test_gateway(&http, ep, "conf-gw-assoc").await;
    let resp = ecs_json(
        &http,
        ep,
        "AssociateGatewayWithCluster",
        serde_json::json!({"gateway": "conf-gw-assoc", "cluster": "conf-gw-assoc"}),
    )
    .await;
    assert_eq!(resp["gatewayArn"], arn);
    assert_eq!(resp["status"], "ACTIVE");
    assert!(resp["clusterArn"]
        .as_str()
        .unwrap()
        .ends_with(":cluster/conf-gw-assoc"));
}

#[test_action("ecs", "DisassociateGatewayFromCluster", checksum = "ffea9d60")]
#[tokio::test]
async fn ecs_disassociate_gateway_from_cluster() {
    let server = TestServer::start().await;
    let http = reqwest::Client::new();
    let ep = server.endpoint();
    let (gateway_arn, _) = gateway_with_route(&http, ep, "conf-gw-disassoc").await;
    let resp = ecs_json(
        &http,
        ep,
        "DisassociateGatewayFromCluster",
        serde_json::json!({"gateway": gateway_arn, "cluster": "conf-gw-disassoc-cluster"}),
    )
    .await;
    assert_eq!(resp["status"], "DISASSOCIATING");
    let routes = ecs_json(
        &http,
        ep,
        "ListGatewayRoutes",
        serde_json::json!({"gateway": gateway_arn}),
    )
    .await;
    assert!(routes["gatewayRoutes"].as_array().unwrap().is_empty());
}

#[test_action("ecs", "ListGatewayClusterAssociations", checksum = "35668912")]
#[tokio::test]
async fn ecs_list_gateway_cluster_associations() {
    let server = TestServer::start().await;
    let http = reqwest::Client::new();
    let ep = server.endpoint();
    let (gateway_arn, _) = gateway_with_route(&http, ep, "conf-gw-lassoc").await;
    let resp = ecs_json(
        &http,
        ep,
        "ListGatewayClusterAssociations",
        serde_json::json!({"gateway": "conf-gw-lassoc-gw"}),
    )
    .await;
    let rows = resp["gatewayClusterAssociations"].as_array().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["gatewayArn"], gateway_arn);
    assert_eq!(rows[0]["status"], "ACTIVE");
}

#[test_action("ecs", "ListGatewayRoutes", checksum = "d09f96fd")]
#[tokio::test]
async fn ecs_list_gateway_routes() {
    let server = TestServer::start().await;
    let http = reqwest::Client::new();
    let ep = server.endpoint();
    let (gateway_arn, service_arn) = gateway_with_route(&http, ep, "conf-gw-lroutes").await;
    let resp = ecs_json(
        &http,
        ep,
        "ListGatewayRoutes",
        serde_json::json!({"gateway": gateway_arn, "cluster": "conf-gw-lroutes-cluster"}),
    )
    .await;
    let routes = resp["gatewayRoutes"].as_array().unwrap();
    assert_eq!(routes.len(), 1);
    assert_eq!(routes[0]["serviceArn"], service_arn);
    assert_eq!(routes[0]["serviceContainerPortName"], "web");
    assert_eq!(routes[0]["status"], "ACTIVE");
}

#[test_action("ecs", "DescribeGatewayRoute", checksum = "c98ce7cd")]
#[tokio::test]
async fn ecs_describe_gateway_route() {
    let server = TestServer::start().await;
    let http = reqwest::Client::new();
    let ep = server.endpoint();
    let (gateway_arn, service_arn) = gateway_with_route(&http, ep, "conf-gw-droute").await;
    let listed = ecs_json(
        &http,
        ep,
        "ListGatewayRoutes",
        serde_json::json!({"gateway": gateway_arn}),
    )
    .await;
    let route_arn = listed["gatewayRoutes"][0]["gatewayRouteArn"]
        .as_str()
        .unwrap()
        .to_string();
    let resp = ecs_json(
        &http,
        ep,
        "DescribeGatewayRoute",
        serde_json::json!({"gatewayRouteArn": route_arn}),
    )
    .await;
    let route = &resp["gatewayRoute"];
    assert_eq!(route["serviceArn"], service_arn);
    assert!(route["serviceUrl"]
        .as_str()
        .unwrap()
        .starts_with("https://"));
    assert!(route["resources"]["hostSharedAlb"][0]["listenerRuleArn"].is_string());
}

#[test_action("ecs", "DescribeGatewayResources", checksum = "ca15541a")]
#[tokio::test]
async fn ecs_describe_gateway_resources() {
    let server = TestServer::start().await;
    let http = reqwest::Client::new();
    let ep = server.endpoint();
    let (gateway_arn, _) = gateway_with_route(&http, ep, "conf-gw-res").await;
    let resp = ecs_json(
        &http,
        ep,
        "DescribeGatewayResources",
        serde_json::json!({"gateway": gateway_arn}),
    )
    .await;
    let res = &resp["gatewayResources"];
    assert_eq!(res["gatewayArn"], gateway_arn);
    let albs = res["resources"]["hostSharedAlb"].as_array().unwrap();
    assert_eq!(albs.len(), 1);
    assert_eq!(albs[0]["gatewayRouteArns"].as_array().unwrap().len(), 1);
}
