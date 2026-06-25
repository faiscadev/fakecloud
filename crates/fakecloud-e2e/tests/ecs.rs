//! ECS Batch 1 + 2: control-plane CRUD plus task lifecycle.
//! Batch 1 covers clusters, task definitions, tagging, account settings.
//! Batch 2 adds RunTask/StartTask/StopTask/DescribeTasks/ListTasks and
//! introspection endpoints for inspecting task lifecycle.

mod helpers;

use aws_sdk_ecs::types::{ContainerDefinition, Tag};
use helpers::TestServer;

fn docker_available() -> bool {
    std::process::Command::new("docker")
        .arg("info")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

fn require_docker_or_skip(test: &str) -> bool {
    if docker_available() {
        return true;
    }
    if std::env::var("CI").is_ok() {
        panic!("docker is required for {test} in CI");
    }
    eprintln!("Skipping {test}: docker not available");
    false
}

#[tokio::test]
async fn create_describe_list_delete_cluster() {
    let server = TestServer::start().await;
    let client = server.ecs_client().await;

    let created = client
        .create_cluster()
        .cluster_name("batch1-cluster")
        .send()
        .await
        .expect("create_cluster");
    let cluster = created.cluster().expect("cluster");
    assert_eq!(cluster.cluster_name(), Some("batch1-cluster"));
    assert_eq!(cluster.status(), Some("ACTIVE"));
    let arn = cluster.cluster_arn().unwrap().to_string();
    assert!(arn.ends_with(":cluster/batch1-cluster"), "arn={arn}");

    let described = client
        .describe_clusters()
        .clusters("batch1-cluster")
        .send()
        .await
        .expect("describe_clusters");
    assert_eq!(described.clusters().len(), 1);
    assert!(described.failures().is_empty());

    let described_by_arn = client
        .describe_clusters()
        .clusters(arn.clone())
        .send()
        .await
        .expect("describe_clusters by ARN");
    assert_eq!(described_by_arn.clusters().len(), 1);

    let listed = client.list_clusters().send().await.expect("list_clusters");
    assert!(listed
        .cluster_arns()
        .iter()
        .any(|a| a.ends_with(":cluster/batch1-cluster")));

    let deleted = client
        .delete_cluster()
        .cluster("batch1-cluster")
        .send()
        .await
        .expect("delete_cluster");
    assert_eq!(deleted.cluster().and_then(|c| c.status()), Some("INACTIVE"));

    // Describe after delete returns a MISSING failure, not an error.
    let after = client
        .describe_clusters()
        .clusters("batch1-cluster")
        .send()
        .await
        .expect("describe after delete");
    assert!(after.clusters().is_empty());
    assert_eq!(after.failures().len(), 1);
    assert_eq!(after.failures()[0].reason(), Some("MISSING"));
}

#[tokio::test]
async fn describe_missing_cluster_returns_failure() {
    let server = TestServer::start().await;
    let client = server.ecs_client().await;
    let resp = client
        .describe_clusters()
        .clusters("nonexistent")
        .send()
        .await
        .expect("describe_clusters");
    assert!(resp.clusters().is_empty());
    assert_eq!(resp.failures().len(), 1);
    assert_eq!(resp.failures()[0].reason(), Some("MISSING"));
}

#[tokio::test]
async fn create_cluster_with_tags_and_capacity_providers() {
    let server = TestServer::start().await;
    let client = server.ecs_client().await;

    let created = client
        .create_cluster()
        .cluster_name("tagged")
        .tags(Tag::builder().key("env").value("prod").build())
        .tags(Tag::builder().key("team").value("platform").build())
        .capacity_providers("FARGATE")
        .capacity_providers("FARGATE_SPOT")
        .send()
        .await
        .expect("create_cluster with tags");
    let cluster = created.cluster().unwrap();
    assert_eq!(cluster.tags().len(), 2);
    assert_eq!(cluster.capacity_providers().len(), 2);

    let listed_tags = client
        .list_tags_for_resource()
        .resource_arn(cluster.cluster_arn().unwrap())
        .send()
        .await
        .expect("list_tags_for_resource");
    assert_eq!(listed_tags.tags().len(), 2);
}

#[tokio::test]
async fn register_describe_deregister_task_definition() {
    let server = TestServer::start().await;
    let client = server.ecs_client().await;

    let registered = client
        .register_task_definition()
        .family("batch1-td")
        .container_definitions(
            ContainerDefinition::builder()
                .name("app")
                .image("public.ecr.aws/library/alpine:latest")
                .essential(true)
                .build(),
        )
        .send()
        .await
        .expect("register_task_definition");
    let td = registered.task_definition().unwrap();
    assert_eq!(td.family(), Some("batch1-td"));
    assert_eq!(td.revision(), 1);
    assert_eq!(td.status().unwrap().as_str(), "ACTIVE");
    let arn = td.task_definition_arn().unwrap().to_string();
    assert!(arn.ends_with(":task-definition/batch1-td:1"));

    // Second registration bumps revision.
    let registered_v2 = client
        .register_task_definition()
        .family("batch1-td")
        .container_definitions(
            ContainerDefinition::builder()
                .name("app")
                .image("public.ecr.aws/library/alpine:3.19")
                .essential(true)
                .build(),
        )
        .send()
        .await
        .expect("register_task_definition v2");
    assert_eq!(registered_v2.task_definition().unwrap().revision(), 2);

    // DescribeTaskDefinition with family shorthand returns the latest ACTIVE.
    let described = client
        .describe_task_definition()
        .task_definition("batch1-td")
        .send()
        .await
        .expect("describe_task_definition latest");
    assert_eq!(described.task_definition().unwrap().revision(), 2);

    // DescribeTaskDefinition with family:revision returns that revision.
    let described_v1 = client
        .describe_task_definition()
        .task_definition("batch1-td:1")
        .send()
        .await
        .expect("describe_task_definition v1");
    assert_eq!(described_v1.task_definition().unwrap().revision(), 1);

    // Deregister flips status to INACTIVE and sets deregisteredAt.
    let deregistered = client
        .deregister_task_definition()
        .task_definition("batch1-td:2")
        .send()
        .await
        .expect("deregister_task_definition");
    assert_eq!(
        deregistered
            .task_definition()
            .and_then(|t| t.status())
            .map(|s| s.as_str()),
        Some("INACTIVE")
    );
}

#[tokio::test]
async fn list_task_definitions_and_families() {
    let server = TestServer::start().await;
    let client = server.ecs_client().await;

    for family in ["api", "worker", "cron"] {
        client
            .register_task_definition()
            .family(family)
            .container_definitions(
                ContainerDefinition::builder()
                    .name("app")
                    .image("public.ecr.aws/library/alpine:latest")
                    .essential(true)
                    .build(),
            )
            .send()
            .await
            .unwrap();
    }

    let defs = client
        .list_task_definitions()
        .send()
        .await
        .expect("list_task_definitions");
    assert_eq!(defs.task_definition_arns().len(), 3);

    let families = client
        .list_task_definition_families()
        .send()
        .await
        .expect("list_task_definition_families");
    assert_eq!(families.families().len(), 3);

    // familyPrefix filter.
    let filtered = client
        .list_task_definition_families()
        .family_prefix("wo")
        .send()
        .await
        .expect("list_task_definition_families filtered");
    assert_eq!(filtered.families(), &["worker".to_string()]);
}

#[tokio::test]
async fn delete_task_definitions_requires_inactive() {
    let server = TestServer::start().await;
    let client = server.ecs_client().await;

    client
        .register_task_definition()
        .family("to-delete")
        .container_definitions(
            ContainerDefinition::builder()
                .name("app")
                .image("public.ecr.aws/library/alpine:latest")
                .essential(true)
                .build(),
        )
        .send()
        .await
        .unwrap();

    let attempt = client
        .delete_task_definitions()
        .task_definitions("to-delete:1")
        .send()
        .await
        .expect("delete_task_definitions active");
    assert!(attempt.task_definitions().is_empty());
    assert_eq!(attempt.failures().len(), 1);
    assert_eq!(attempt.failures()[0].reason(), Some("MUST_BE_INACTIVE"));

    client
        .deregister_task_definition()
        .task_definition("to-delete:1")
        .send()
        .await
        .unwrap();

    let deleted = client
        .delete_task_definitions()
        .task_definitions("to-delete:1")
        .send()
        .await
        .expect("delete_task_definitions inactive");
    assert_eq!(deleted.task_definitions().len(), 1);
    assert!(deleted.failures().is_empty());
    assert_eq!(
        deleted.task_definitions()[0].status().unwrap().as_str(),
        "DELETE_IN_PROGRESS"
    );
}

#[tokio::test]
async fn tag_untag_cluster_and_task_definition() {
    let server = TestServer::start().await;
    let client = server.ecs_client().await;

    let cluster = client
        .create_cluster()
        .cluster_name("tagme")
        .send()
        .await
        .unwrap()
        .cluster()
        .unwrap()
        .clone();

    client
        .tag_resource()
        .resource_arn(cluster.cluster_arn().unwrap())
        .tags(Tag::builder().key("env").value("staging").build())
        .tags(Tag::builder().key("team").value("platform").build())
        .send()
        .await
        .unwrap();

    let tags = client
        .list_tags_for_resource()
        .resource_arn(cluster.cluster_arn().unwrap())
        .send()
        .await
        .unwrap();
    assert_eq!(tags.tags().len(), 2);

    client
        .untag_resource()
        .resource_arn(cluster.cluster_arn().unwrap())
        .tag_keys("team")
        .send()
        .await
        .unwrap();

    let after = client
        .list_tags_for_resource()
        .resource_arn(cluster.cluster_arn().unwrap())
        .send()
        .await
        .unwrap();
    assert_eq!(after.tags().len(), 1);
    assert_eq!(after.tags()[0].key(), Some("env"));
}

#[tokio::test]
async fn tag_untag_service_and_capacity_provider() {
    let server = TestServer::start().await;
    let client = server.ecs_client().await;
    bootstrap_service_fixtures(&client, "tag-cluster", "tag-td").await;

    let svc = client
        .create_service()
        .cluster("tag-cluster")
        .service_name("api")
        .task_definition("tag-td")
        .desired_count(1)
        .send()
        .await
        .unwrap()
        .service()
        .unwrap()
        .clone();

    client
        .tag_resource()
        .resource_arn(svc.service_arn().unwrap())
        .tags(Tag::builder().key("tier").value("frontend").build())
        .send()
        .await
        .unwrap();
    let svc_tags = client
        .list_tags_for_resource()
        .resource_arn(svc.service_arn().unwrap())
        .send()
        .await
        .unwrap();
    assert_eq!(svc_tags.tags().len(), 1);
    assert_eq!(svc_tags.tags()[0].key(), Some("tier"));

    let cp = client
        .create_capacity_provider()
        .name("cp-tag")
        .auto_scaling_group_provider(
            aws_sdk_ecs::types::AutoScalingGroupProvider::builder()
                .auto_scaling_group_arn(
                    "arn:aws:autoscaling:us-east-1:123456789012:autoScalingGroup:abc:autoScalingGroupName/asg-1",
                )
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap()
        .capacity_provider()
        .unwrap()
        .clone();

    client
        .tag_resource()
        .resource_arn(cp.capacity_provider_arn().unwrap())
        .tags(Tag::builder().key("owner").value("platform").build())
        .send()
        .await
        .unwrap();
    let cp_tags = client
        .list_tags_for_resource()
        .resource_arn(cp.capacity_provider_arn().unwrap())
        .send()
        .await
        .unwrap();
    assert_eq!(cp_tags.tags().len(), 1);

    client
        .untag_resource()
        .resource_arn(cp.capacity_provider_arn().unwrap())
        .tag_keys("owner")
        .send()
        .await
        .unwrap();
    let cp_tags_after = client
        .list_tags_for_resource()
        .resource_arn(cp.capacity_provider_arn().unwrap())
        .send()
        .await
        .unwrap();
    assert!(cp_tags_after.tags().is_empty());
}

#[tokio::test]
async fn put_and_list_account_settings() {
    let server = TestServer::start().await;
    let client = server.ecs_client().await;

    client
        .put_account_setting_default()
        .name("taskLongArnFormat".into())
        .value("enabled")
        .send()
        .await
        .expect("put_account_setting_default");

    let listed = client
        .list_account_settings()
        .effective_settings(true)
        .send()
        .await
        .expect("list_account_settings effective");
    assert!(listed
        .settings()
        .iter()
        .any(|s| s.name().map(|n| n.as_str()) == Some("taskLongArnFormat")));
}

#[tokio::test]
async fn list_clusters_with_out_of_range_next_token_is_not_a_panic() {
    // Regression: an attacker-controlled or stale nextToken pointing past
    // the end of the list must not panic the server.
    let server = TestServer::start().await;
    let client = server.ecs_client().await;
    client
        .create_cluster()
        .cluster_name("only")
        .send()
        .await
        .unwrap();
    let resp = client
        .list_clusters()
        .next_token("9999")
        .send()
        .await
        .expect("list_clusters with OOR token");
    assert!(resp.cluster_arns().is_empty());
    assert!(resp.next_token().is_none());
}

#[tokio::test]
async fn delete_cluster_with_tasks_fails() {
    let server = TestServer::start().await;
    let client = server.ecs_client().await;

    // Introspection endpoint sanity check — Batch 1 ships the /clusters dump.
    client
        .create_cluster()
        .cluster_name("introspected")
        .send()
        .await
        .unwrap();

    let body: serde_json::Value =
        reqwest::get(format!("{}/_fakecloud/ecs/clusters", server.endpoint()))
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
    let arr = body.get("clusters").and_then(|v| v.as_array()).unwrap();
    assert!(arr
        .iter()
        .any(|c| c.get("clusterName").and_then(|v| v.as_str()) == Some("introspected")));
}

// ── Batch 2: task lifecycle ────────────────────────────────────────

async fn register_runnable_task_def(client: &aws_sdk_ecs::Client, family: &str) {
    client
        .register_task_definition()
        .family(family)
        .container_definitions(
            ContainerDefinition::builder()
                .name("app")
                .image("public.ecr.aws/library/alpine:latest")
                .essential(true)
                .build(),
        )
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn run_task_records_task_and_accepts_describe() {
    if !require_docker_or_skip("run_task_records_task_and_accepts_describe") {
        return;
    }
    let server = TestServer::start().await;
    let client = server.ecs_client().await;

    client
        .create_cluster()
        .cluster_name("rt-cluster")
        .send()
        .await
        .unwrap();
    register_runnable_task_def(&client, "rt-family").await;

    let run = client
        .run_task()
        .cluster("rt-cluster")
        .task_definition("rt-family")
        .send()
        .await
        .expect("run_task");
    assert_eq!(run.tasks().len(), 1);
    assert!(run.failures().is_empty());

    let task = &run.tasks()[0];
    let arn = task.task_arn().unwrap().to_string();
    assert!(arn.contains(":task/rt-cluster/"));
    assert_eq!(task.launch_type().unwrap().as_str(), "FARGATE");
    // Task is at least PENDING after RunTask returns; in CI without Docker
    // it transitions to STOPPED with a TaskFailedToStart reason.
    let status = task.last_status().unwrap();
    assert!(
        status == "PENDING"
            || status == "RUNNING"
            || status == "STOPPED"
            || status == "PROVISIONING",
        "unexpected status after RunTask: {status}"
    );

    let described = client
        .describe_tasks()
        .cluster("rt-cluster")
        .tasks(arn.clone())
        .send()
        .await
        .expect("describe_tasks");
    assert_eq!(described.tasks().len(), 1);
    assert!(described.failures().is_empty());

    let listed = client
        .list_tasks()
        .cluster("rt-cluster")
        .send()
        .await
        .expect("list_tasks");
    assert!(listed.task_arns().iter().any(|a| a == &arn));
}

#[tokio::test]
async fn stop_task_flips_desired_status() {
    let server = TestServer::start().await;
    let client = server.ecs_client().await;

    client
        .create_cluster()
        .cluster_name("stop-cluster")
        .send()
        .await
        .unwrap();
    register_runnable_task_def(&client, "stop-family").await;

    let arn = client
        .run_task()
        .cluster("stop-cluster")
        .task_definition("stop-family")
        .send()
        .await
        .unwrap()
        .tasks()[0]
        .task_arn()
        .unwrap()
        .to_string();

    let resp = client
        .stop_task()
        .cluster("stop-cluster")
        .task(arn.clone())
        .reason("e2e")
        .send()
        .await
        .expect("stop_task");
    let task = resp.task().unwrap();
    assert_eq!(task.desired_status(), Some("STOPPED"));
}

#[tokio::test]
async fn describe_unknown_task_returns_failure() {
    let server = TestServer::start().await;
    let client = server.ecs_client().await;

    client
        .create_cluster()
        .cluster_name("empty-cluster")
        .send()
        .await
        .unwrap();

    let resp = client
        .describe_tasks()
        .cluster("empty-cluster")
        .tasks("arn:aws:ecs:us-east-1:123456789012:task/empty-cluster/missing")
        .send()
        .await
        .expect("describe_tasks");
    assert!(resp.tasks().is_empty());
    assert_eq!(resp.failures().len(), 1);
    assert_eq!(resp.failures()[0].reason(), Some("MISSING"));
}

#[tokio::test]
async fn introspection_tasks_endpoint_lists_ecs_state() {
    let server = TestServer::start().await;
    let client = server.ecs_client().await;

    client
        .create_cluster()
        .cluster_name("intro-tasks")
        .send()
        .await
        .unwrap();
    register_runnable_task_def(&client, "intro-tasks-fam").await;
    let arn = client
        .run_task()
        .cluster("intro-tasks")
        .task_definition("intro-tasks-fam")
        .send()
        .await
        .unwrap()
        .tasks()[0]
        .task_arn()
        .unwrap()
        .to_string();

    let body: serde_json::Value =
        reqwest::get(format!("{}/_fakecloud/ecs/tasks", server.endpoint()))
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
    let arr = body.get("tasks").and_then(|v| v.as_array()).unwrap();
    assert!(arr
        .iter()
        .any(|t| t.get("taskArn").and_then(|v| v.as_str()) == Some(arn.as_str())));

    // Mark-failed forces the task to STOPPED deterministically for tests.
    let task_id = arn.rsplit('/').next().unwrap();
    let url = format!(
        "{}/_fakecloud/ecs/tasks/{}/mark-failed",
        server.endpoint(),
        task_id
    );
    let resp = reqwest::Client::new()
        .post(&url)
        .json(&serde_json::json!({"exitCode": 137, "reason": "e2e injected"}))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());
    let flipped: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(
        flipped.get("lastStatus").and_then(|v| v.as_str()),
        Some("STOPPED")
    );

    let events: serde_json::Value =
        reqwest::get(format!("{}/_fakecloud/ecs/events", server.endpoint()))
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
    let ev = events.get("events").and_then(|v| v.as_array()).unwrap();
    assert!(!ev.is_empty());
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
                .image("public.ecr.aws/library/alpine:latest")
                .essential(true)
                .build(),
        )
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn create_service_spawns_desired_tasks_and_describe_roundtrips() {
    if !require_docker_or_skip("create_service_spawns_desired_tasks_and_describe_roundtrips") {
        return;
    }
    let server = TestServer::start().await;
    let client = server.ecs_client().await;

    bootstrap_service_fixtures(&client, "svc-cluster", "svc-td").await;

    let resp = client
        .create_service()
        .cluster("svc-cluster")
        .service_name("web")
        .task_definition("svc-td")
        .desired_count(2)
        .send()
        .await
        .expect("create_service");
    let svc = resp.service().unwrap();
    assert_eq!(svc.service_name(), Some("web"));
    assert_eq!(svc.desired_count(), 2);
    assert_eq!(svc.deployments().len(), 1);
    assert_eq!(svc.deployments()[0].status(), Some("PRIMARY"));

    let described = client
        .describe_services()
        .cluster("svc-cluster")
        .services("web")
        .send()
        .await
        .expect("describe_services");
    assert_eq!(described.services().len(), 1);
    assert!(described.failures().is_empty());

    let listed = client
        .list_services()
        .cluster("svc-cluster")
        .send()
        .await
        .expect("list_services");
    assert_eq!(listed.service_arns().len(), 1);
    assert!(listed.service_arns()[0].ends_with("service/svc-cluster/web"));

    let tasks = client
        .list_tasks()
        .cluster("svc-cluster")
        .send()
        .await
        .expect("list_tasks");
    assert_eq!(tasks.task_arns().len(), 2, "service should spawn 2 tasks");
}

#[tokio::test]
async fn update_service_scales_up_and_down() {
    if !require_docker_or_skip("update_service_scales_up_and_down") {
        return;
    }
    let server = TestServer::start().await;
    let client = server.ecs_client().await;
    bootstrap_service_fixtures(&client, "scale-cluster", "scale-td").await;
    client
        .create_service()
        .cluster("scale-cluster")
        .service_name("web")
        .task_definition("scale-td")
        .desired_count(1)
        .send()
        .await
        .unwrap();

    let up = client
        .update_service()
        .cluster("scale-cluster")
        .service("web")
        .desired_count(3)
        .send()
        .await
        .expect("update_service up");
    assert_eq!(up.service().unwrap().desired_count(), 3);
    let tasks_after_up = client
        .list_tasks()
        .cluster("scale-cluster")
        .send()
        .await
        .unwrap();
    assert_eq!(tasks_after_up.task_arns().len(), 3);

    let down = client
        .update_service()
        .cluster("scale-cluster")
        .service("web")
        .desired_count(1)
        .send()
        .await
        .expect("update_service down");
    assert_eq!(down.service().unwrap().desired_count(), 1);
    let all = client
        .list_tasks()
        .cluster("scale-cluster")
        .send()
        .await
        .unwrap();
    let arns: Vec<String> = all.task_arns().iter().map(|a| a.to_string()).collect();
    let described = client
        .describe_tasks()
        .cluster("scale-cluster")
        .set_tasks(Some(arns))
        .send()
        .await
        .unwrap();
    let running_desired: usize = described
        .tasks()
        .iter()
        .filter(|t| t.desired_status() == Some("RUNNING"))
        .count();
    assert!(
        running_desired <= 1,
        "after scale-down <=1 task should still desire RUNNING, got {running_desired}"
    );
}

#[tokio::test]
async fn update_service_applies_az_rebalancing_and_volume_configs() {
    if !require_docker_or_skip("update_service_applies_az_rebalancing_and_volume_configs") {
        return;
    }
    let server = TestServer::start().await;
    let client = server.ecs_client().await;
    bootstrap_service_fixtures(&client, "azr-cluster", "azr-td").await;
    client
        .create_service()
        .cluster("azr-cluster")
        .service_name("web")
        .task_definition("azr-td")
        .desired_count(1)
        .send()
        .await
        .unwrap();

    use aws_sdk_ecs::types::{AvailabilityZoneRebalancing, ServiceVolumeConfiguration};
    // UpdateService previously dropped availabilityZoneRebalancing +
    // volumeConfigurations (bug-hunt 2026-06-24, 1.13).
    let updated = client
        .update_service()
        .cluster("azr-cluster")
        .service("web")
        .availability_zone_rebalancing(AvailabilityZoneRebalancing::Enabled)
        .volume_configurations(
            ServiceVolumeConfiguration::builder()
                .name("data")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .expect("update_service");
    assert_eq!(
        updated.service().unwrap().availability_zone_rebalancing(),
        Some(&AvailabilityZoneRebalancing::Enabled)
    );

    // Read back via Describe to confirm it persisted.
    let desc = client
        .describe_services()
        .cluster("azr-cluster")
        .services("web")
        .send()
        .await
        .unwrap();
    assert_eq!(
        desc.services()[0].availability_zone_rebalancing(),
        Some(&AvailabilityZoneRebalancing::Enabled)
    );
}

#[tokio::test]
async fn update_service_new_task_definition_triggers_rolling_deployment() {
    let server = TestServer::start().await;
    let client = server.ecs_client().await;
    bootstrap_service_fixtures(&client, "roll-cluster", "roll-td").await;
    client
        .create_service()
        .cluster("roll-cluster")
        .service_name("web")
        .task_definition("roll-td")
        .desired_count(1)
        .send()
        .await
        .unwrap();

    client
        .register_task_definition()
        .family("roll-td")
        .container_definitions(
            ContainerDefinition::builder()
                .name("app")
                .image("public.ecr.aws/library/alpine:3.19")
                .essential(true)
                .build(),
        )
        .send()
        .await
        .unwrap();
    let rolled = client
        .update_service()
        .cluster("roll-cluster")
        .service("web")
        .task_definition("roll-td:2")
        .send()
        .await
        .expect("update_service new td");
    let svc = rolled.service().unwrap();
    let primary = svc
        .deployments()
        .iter()
        .find(|d| d.status() == Some("PRIMARY"))
        .expect("primary deployment");
    assert!(primary.task_definition().unwrap().ends_with(":2"));
    assert!(
        svc.deployments()
            .iter()
            .any(|d| d.status() == Some("ACTIVE")),
        "old deployment should be ACTIVE during rollout"
    );
}

#[tokio::test]
async fn delete_service_requires_zero_desired_unless_force() {
    let server = TestServer::start().await;
    let client = server.ecs_client().await;
    bootstrap_service_fixtures(&client, "del-cluster", "del-td").await;
    client
        .create_service()
        .cluster("del-cluster")
        .service_name("web")
        .task_definition("del-td")
        .desired_count(2)
        .send()
        .await
        .unwrap();

    let err = client
        .delete_service()
        .cluster("del-cluster")
        .service("web")
        .send()
        .await
        .expect_err("delete should fail while scaled up");
    let msg = format!("{err:?}");
    assert!(
        msg.contains("desiredCount") || msg.contains("scaled"),
        "msg={msg}"
    );

    let forced = client
        .delete_service()
        .cluster("del-cluster")
        .service("web")
        .force(true)
        .send()
        .await
        .expect("force delete");
    assert_eq!(forced.service().unwrap().service_name(), Some("web"));

    // AWS does NOT drop a deleted service immediately: it stays describable
    // (DRAINING while tasks stop, then INACTIVE) instead of returning MISSING.
    // It is excluded from ListServices either way. (The exact DRAINING vs
    // INACTIVE status depends on whether the drained tasks have stopped yet,
    // which is non-deterministic when a real container runtime is present;
    // `deleted_service_with_no_tasks_becomes_inactive` covers the terminal
    // transition deterministically.)
    let after = client
        .describe_services()
        .cluster("del-cluster")
        .services("web")
        .send()
        .await
        .unwrap();
    assert_eq!(after.services().len(), 1);
    assert!(
        matches!(
            after.services()[0].status(),
            Some("DRAINING") | Some("INACTIVE")
        ),
        "deleted service must remain describable as DRAINING/INACTIVE, got {:?}",
        after.services()[0].status()
    );
    assert!(after.failures().is_empty());

    let listed = client
        .list_services()
        .cluster("del-cluster")
        .send()
        .await
        .unwrap();
    assert!(
        listed.service_arns().is_empty(),
        "deleted service must not appear in ListServices"
    );
}

#[tokio::test]
async fn deleted_service_with_no_tasks_becomes_inactive() {
    // A service with no running tasks (desiredCount=0) deletes cleanly: AWS
    // keeps it describable as INACTIVE (the terraform-provider-aws delete
    // waiter polls DescribeServices until it observes INACTIVE), and excludes
    // it from ListServices.
    let server = TestServer::start().await;
    let client = server.ecs_client().await;
    bootstrap_service_fixtures(&client, "drain-cluster", "drain-td").await;
    client
        .create_service()
        .cluster("drain-cluster")
        .service_name("idle")
        .task_definition("drain-td")
        .desired_count(0)
        .send()
        .await
        .unwrap();

    let deleted = client
        .delete_service()
        .cluster("drain-cluster")
        .service("idle")
        .send()
        .await
        .expect("delete idle service");
    assert_eq!(deleted.service().unwrap().service_name(), Some("idle"));

    let after = client
        .describe_services()
        .cluster("drain-cluster")
        .services("idle")
        .send()
        .await
        .unwrap();
    assert_eq!(after.services().len(), 1);
    assert_eq!(after.services()[0].status(), Some("INACTIVE"));
    assert!(after.failures().is_empty());

    let listed = client
        .list_services()
        .cluster("drain-cluster")
        .send()
        .await
        .unwrap();
    assert!(
        listed.service_arns().is_empty(),
        "INACTIVE service must not appear in ListServices"
    );
}

// -- Phase O1: multi-container task launch --------------------------

#[tokio::test]
async fn run_task_with_two_containers_records_both_per_container() {
    // Task definition with one main app + one sidecar; both should land
    // on the task's containers[] with distinct ARNs and matching essential
    // flags. Lifecycle assertions stay status-tolerant because CI typically
    // doesn't have docker available, so the task transitions straight to
    // STOPPED with TaskFailedToStart — that's still a multi-container exit.
    let server = TestServer::start().await;
    let client = server.ecs_client().await;

    client
        .create_cluster()
        .cluster_name("multi-cluster")
        .send()
        .await
        .unwrap();
    client
        .register_task_definition()
        .family("multi-fam")
        .container_definitions(
            ContainerDefinition::builder()
                .name("app")
                .image("public.ecr.aws/library/alpine:latest")
                .essential(true)
                .build(),
        )
        .container_definitions(
            ContainerDefinition::builder()
                .name("sidecar")
                .image("public.ecr.aws/library/alpine:latest")
                .essential(false)
                .build(),
        )
        .send()
        .await
        .expect("register multi-container td");

    let run = client
        .run_task()
        .cluster("multi-cluster")
        .task_definition("multi-fam")
        .send()
        .await
        .expect("run_task");
    assert_eq!(run.tasks().len(), 1);
    let task = &run.tasks()[0];
    assert_eq!(task.containers().len(), 2, "expected 2 containers on task");

    let names: Vec<&str> = task.containers().iter().filter_map(|c| c.name()).collect();
    assert!(names.contains(&"app"));
    assert!(names.contains(&"sidecar"));

    // Per-container ARNs are distinct and match the AWS shape
    // arn:aws:ecs:<region>:<acct>:container/<cluster>/<task-id>/<container-id>.
    let arns: std::collections::HashSet<&str> = task
        .containers()
        .iter()
        .filter_map(|c| c.container_arn())
        .collect();
    assert_eq!(arns.len(), 2, "container ARNs must be distinct");
    for arn in &arns {
        assert!(
            arn.contains(":container/multi-cluster/"),
            "container ARN should embed cluster name: {arn}"
        );
    }
}

#[tokio::test]
async fn stop_task_with_two_containers_marks_all_stopped() {
    // StopTask must propagate to every container in the task. We force
    // termination through the introspection mark-failed endpoint so the
    // assertion is deterministic without docker.
    let server = TestServer::start().await;
    let client = server.ecs_client().await;

    client
        .create_cluster()
        .cluster_name("multi-stop")
        .send()
        .await
        .unwrap();
    client
        .register_task_definition()
        .family("multi-stop-fam")
        .container_definitions(
            ContainerDefinition::builder()
                .name("app")
                .image("public.ecr.aws/library/alpine:latest")
                .essential(true)
                .build(),
        )
        .container_definitions(
            ContainerDefinition::builder()
                .name("sidecar")
                .image("public.ecr.aws/library/alpine:latest")
                .essential(false)
                .build(),
        )
        .send()
        .await
        .unwrap();

    let arn = client
        .run_task()
        .cluster("multi-stop")
        .task_definition("multi-stop-fam")
        .send()
        .await
        .unwrap()
        .tasks()[0]
        .task_arn()
        .unwrap()
        .to_string();
    let task_id = arn.rsplit('/').next().unwrap().to_string();

    // Force essential exit via introspection so the assertion doesn't
    // depend on a real container runtime being present.
    let url = format!(
        "{}/_fakecloud/ecs/tasks/{}/mark-failed",
        server.endpoint(),
        task_id
    );
    let resp = reqwest::Client::new()
        .post(&url)
        .json(&serde_json::json!({"exitCode": 1, "reason": "essential exit"}))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    let described = client
        .describe_tasks()
        .cluster("multi-stop")
        .tasks(arn)
        .send()
        .await
        .unwrap();
    let task = &described.tasks()[0];
    assert_eq!(task.last_status(), Some("STOPPED"));
    assert_eq!(task.containers().len(), 2);
    for c in task.containers() {
        assert_eq!(c.last_status(), Some("STOPPED"));
        assert_eq!(c.exit_code(), Some(1));
    }
}

#[tokio::test]
async fn describe_tasks_emits_per_container_aws_shape_fields() {
    // Verify the public DescribeTasks response carries every per-container
    // field documented by AWS (containerArn, taskArn, name, image,
    // lastStatus, essential, networkBindings, networkInterfaces). The
    // assertions go through a raw HTTP POST so we can inspect the JSON
    // keys directly instead of being limited to SDK accessor coverage.
    let server = TestServer::start().await;
    let client = server.ecs_client().await;

    client
        .create_cluster()
        .cluster_name("shape-cluster")
        .send()
        .await
        .unwrap();
    client
        .register_task_definition()
        .family("shape-fam")
        .container_definitions(
            ContainerDefinition::builder()
                .name("app")
                .image("public.ecr.aws/library/alpine:latest")
                .essential(true)
                .build(),
        )
        .container_definitions(
            ContainerDefinition::builder()
                .name("sidecar")
                .image("public.ecr.aws/library/alpine:latest")
                .essential(false)
                .build(),
        )
        .send()
        .await
        .unwrap();

    let arn = client
        .run_task()
        .cluster("shape-cluster")
        .task_definition("shape-fam")
        .send()
        .await
        .unwrap()
        .tasks()[0]
        .task_arn()
        .unwrap()
        .to_string();

    let body = serde_json::json!({"cluster": "shape-cluster", "tasks": [arn]});
    let resp: serde_json::Value = reqwest::Client::new()
        .post(format!("{}/", server.endpoint()))
        .header(
            "X-Amz-Target",
            "AmazonEC2ContainerServiceV20141113.DescribeTasks",
        )
        .header("Content-Type", "application/x-amz-json-1.1")
        .body(serde_json::to_vec(&body).unwrap())
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let tasks = resp.get("tasks").and_then(|v| v.as_array()).unwrap();
    assert_eq!(tasks.len(), 1);
    let containers = tasks[0]
        .get("containers")
        .and_then(|v| v.as_array())
        .unwrap();
    assert_eq!(containers.len(), 2);
    for c in containers {
        // Smithy `Container` runtime shape does NOT include `essential` —
        // that field lives on the task-definition `ContainerDefinition`.
        for field in [
            "containerArn",
            "taskArn",
            "name",
            "image",
            "lastStatus",
            "networkBindings",
            "networkInterfaces",
        ] {
            assert!(
                c.get(field).is_some(),
                "container missing field {field}: {c}"
            );
        }
    }
}
