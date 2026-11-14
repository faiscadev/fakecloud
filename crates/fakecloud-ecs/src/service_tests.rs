use super::*;

#[test]
fn parse_family_revision_with_revision() {
    assert_eq!(parse_family_revision("web:3"), ("web".to_string(), Some(3)));
}

#[test]
fn parse_family_revision_without_revision() {
    assert_eq!(parse_family_revision("web"), ("web".to_string(), None));
}

#[test]
fn parse_family_revision_non_numeric_treated_as_no_revision() {
    assert_eq!(
        parse_family_revision("web:latest"),
        ("web:latest".to_string(), None)
    );
}

#[test]
fn decode_ecs_arn_cluster() {
    let (account, rtype, tail) =
        decode_ecs_arn("arn:aws:ecs:us-east-1:111122223333:cluster/prod").unwrap();
    assert_eq!(account, "111122223333");
    assert_eq!(rtype, "cluster");
    assert_eq!(tail, "prod");
}

#[test]
fn decode_ecs_arn_task_definition() {
    let (account, rtype, tail) =
        decode_ecs_arn("arn:aws:ecs:us-east-1:111122223333:task-definition/web:5").unwrap();
    assert_eq!(account, "111122223333");
    assert_eq!(rtype, "task-definition");
    assert_eq!(tail, "web:5");
}

#[test]
fn decode_ecs_arn_rejects_non_ecs() {
    assert!(decode_ecs_arn("arn:aws:s3:::bucket").is_err());
}

#[test]
fn resolve_service_key_handles_short_and_long() {
    let mut state = EcsState::new("123456789012", "us-east-1");
    state.services.insert(
        "default/api".to_string(),
        Service {
            service_name: "api".into(),
            service_arn: "arn".into(),
            cluster_name: "default".into(),
            cluster_arn: "arn".into(),
            task_definition_arn: "td-arn".into(),
            family: "td".into(),
            revision: 1,
            desired_count: 0,
            running_count: 0,
            pending_count: 0,
            launch_type: "FARGATE".into(),
            status: "ACTIVE".into(),
            scheduling_strategy: "REPLICA".into(),
            deployment_controller: "ECS".into(),
            minimum_healthy_percent: None,
            maximum_percent: None,
            circuit_breaker: None,
            deployments: vec![],
            load_balancers: vec![],
            service_registries: vec![],
            placement_constraints: vec![],
            placement_strategy: vec![],
            network_configuration: None,
            tags: vec![],
            created_at: chrono::Utc::now(),
            created_by: None,
            role_arn: None,
            platform_version: None,
            health_check_grace_period_seconds: None,
            enable_execute_command: false,
            enable_ecs_managed_tags: false,
            propagate_tags: None,
            capacity_provider_strategy: vec![],
            availability_zone_rebalancing: None,
            volume_configurations: vec![],
            service_connect_configuration: None,
            vpc_lattice_configurations: vec![],
            gateway_connectors: vec![],
        },
    );
    // Long-form: cluster/service.
    assert_eq!(
        resolve_service_key(&state, "default/api"),
        Some("default/api".to_string())
    );
    // Short-form: bare service name resolves via ends_with scan.
    assert_eq!(
        resolve_service_key(&state, "api"),
        Some("default/api".to_string())
    );
    // Unknown name returns None.
    assert_eq!(resolve_service_key(&state, "nope"), None);
}

#[test]
fn resolve_container_instance_key_handles_short_and_long() {
    let mut state = EcsState::new("123456789012", "us-east-1");
    state.container_instances.insert(
        "default/abc-123".to_string(),
        ContainerInstance {
            container_instance_arn: "arn".into(),
            ec2_instance_id: Some("i-x".into()),
            cluster_name: "default".into(),
            cluster_arn: "arn".into(),
            status: "ACTIVE".into(),
            version: 0,
            version_info: None,
            agent_connected: true,
            agent_update_status: None,
            remaining_resources: vec![],
            registered_resources: vec![],
            running_tasks_count: 0,
            pending_tasks_count: 0,
            registered_at: chrono::Utc::now(),
            attributes: vec![],
            tags: vec![],
            capacity_provider_name: None,
            health_status: None,
        },
    );
    assert_eq!(
        resolve_container_instance_key(&state, "default/abc-123"),
        Some("default/abc-123".to_string())
    );
    assert_eq!(
        resolve_container_instance_key(&state, "abc-123"),
        Some("default/abc-123".to_string())
    );
    assert_eq!(resolve_container_instance_key(&state, "nope"), None);
}

#[test]
fn validate_family_name_accepts_hyphen_underscore() {
    assert!(validate_family_name("web_server-2").is_ok());
}

#[test]
fn validate_family_name_rejects_empty() {
    assert!(validate_family_name("").is_err());
}

#[test]
fn validate_family_name_rejects_slash() {
    assert!(validate_family_name("web/server").is_err());
}

#[test]
fn resolve_task_definition_ref_bare_family() {
    let (account, family, rev) = resolve_task_definition_ref("web").unwrap();
    assert_eq!(account, None);
    assert_eq!(family, "web");
    assert_eq!(rev, None);
}

#[test]
fn resolve_task_definition_ref_family_revision() {
    let (account, family, rev) = resolve_task_definition_ref("web:3").unwrap();
    assert_eq!(account, None);
    assert_eq!(family, "web");
    assert_eq!(rev, Some(3));
}

#[test]
fn resolve_task_definition_ref_full_arn() {
    let (account, family, rev) =
        resolve_task_definition_ref("arn:aws:ecs:us-east-1:111122223333:task-definition/web:3")
            .unwrap();
    assert_eq!(account, Some("111122223333".to_string()));
    assert_eq!(family, "web");
    assert_eq!(rev, Some(3));
}

#[test]
fn merge_tags_replaces_existing_value() {
    let mut current = vec![TagEntry {
        key: "env".into(),
        value: "dev".into(),
    }];
    merge_tags(
        &mut current,
        vec![TagEntry {
            key: "env".into(),
            value: "prod".into(),
        }],
    );
    assert_eq!(current.len(), 1);
    assert_eq!(current[0].value, "prod");
}

#[test]
fn merge_tags_adds_new() {
    let mut current = vec![TagEntry {
        key: "env".into(),
        value: "dev".into(),
    }];
    merge_tags(
        &mut current,
        vec![TagEntry {
            key: "team".into(),
            value: "platform".into(),
        }],
    );
    assert_eq!(current.len(), 2);
}

#[test]
fn parse_tags_reads_lowercase_keys() {
    let body = json!({
        "tags": [
            {"key": "env", "value": "prod"},
            {"key": "team", "value": "platform"},
        ]
    });
    let tags = parse_tags(&body);
    assert_eq!(tags.len(), 2);
    assert_eq!(tags[0].key, "env");
    assert_eq!(tags[0].value, "prod");
}

#[test]
fn matches_filter_respects_none() {
    assert!(matches_filter(None, "anything"));
    assert!(matches_filter(Some("x"), "x"));
    assert!(!matches_filter(Some("x"), "y"));
}

// bug-audit 2026-05-28, 1.7: the daemon list operations now reject a malformed
// nextToken (paginate_checked) instead of silently restarting at page 0. The
// rejection primitive itself is exercised here; the wiring lives in
// service_daemons.rs (ListDaemonTaskDefinitions/ListDaemons/ListDaemonDeployments).
#[test]
fn paginate_checked_rejects_invalid_token() {
    use fakecloud_core::pagination::paginate_checked;
    let items: Vec<i32> = (0..5).collect();
    assert!(paginate_checked(&items, Some("not-a-valid-token"), 3).is_err());
    assert!(paginate_checked(&items, Some("2"), 3).is_ok());
    assert!(paginate_checked(&items, None, 3).is_ok());
}

// bug-audit 2026-06-15, 4.7: a service with desiredCount=N and 0 running tasks
// must converge to N tasks once the scheduler ticker runs. Before the fix
// `reconcile_persisted_tasks` zeroed counts and STOPped tasks on restart
// trusting a scheduler ticker that did not exist, so services stayed stuck at
// runningCount=0. Here we drive `reconcile_service_desired_counts` directly.
mod scheduler_reconcile {
    use super::*;
    use crate::state::{Service, SharedEcsState, TaskDefinition};
    use fakecloud_core::multi_account::MultiAccountState;
    use parking_lot::RwLock;
    use std::sync::Arc;

    const ACCOUNT: &str = "000000000000";

    fn make_task_definition() -> TaskDefinition {
        TaskDefinition {
            family: "web".into(),
            revision: 1,
            task_definition_arn: format!("arn:aws:ecs:us-east-1:{ACCOUNT}:task-definition/web:1"),
            container_definitions: vec![serde_json::json!({
                "name": "app",
                "image": "public.ecr.aws/nginx/nginx:latest",
                "essential": true,
            })],
            status: "ACTIVE".into(),
            task_role_arn: None,
            execution_role_arn: None,
            network_mode: Some("awsvpc".into()),
            requires_compatibilities: vec!["FARGATE".into()],
            compatibilities: vec!["FARGATE".into()],
            cpu: Some("256".into()),
            memory: Some("512".into()),
            pid_mode: None,
            ipc_mode: None,
            volumes: vec![],
            placement_constraints: vec![],
            proxy_configuration: None,
            inference_accelerators: vec![],
            ephemeral_storage: None,
            runtime_platform: None,
            requires_attributes: vec![],
            registered_at: chrono::Utc::now(),
            registered_by: None,
            deregistered_at: None,
            tags: vec![],
            enable_fault_injection: None,
        }
    }

    fn make_service(desired: i32) -> Service {
        Service {
            service_name: "api".into(),
            service_arn: format!("arn:aws:ecs:us-east-1:{ACCOUNT}:service/default/api"),
            cluster_name: "default".into(),
            cluster_arn: format!("arn:aws:ecs:us-east-1:{ACCOUNT}:cluster/default"),
            task_definition_arn: format!("arn:aws:ecs:us-east-1:{ACCOUNT}:task-definition/web:1"),
            family: "web".into(),
            revision: 1,
            desired_count: desired,
            running_count: 0,
            pending_count: 0,
            launch_type: "FARGATE".into(),
            status: "ACTIVE".into(),
            scheduling_strategy: "REPLICA".into(),
            deployment_controller: "ECS".into(),
            minimum_healthy_percent: None,
            maximum_percent: None,
            circuit_breaker: None,
            deployments: vec![],
            load_balancers: vec![],
            service_registries: vec![],
            placement_constraints: vec![],
            placement_strategy: vec![],
            network_configuration: None,
            tags: vec![],
            created_at: chrono::Utc::now(),
            created_by: None,
            role_arn: None,
            platform_version: None,
            health_check_grace_period_seconds: None,
            enable_execute_command: false,
            enable_ecs_managed_tags: false,
            propagate_tags: None,
            capacity_provider_strategy: vec![],
            availability_zone_rebalancing: None,
            volume_configurations: vec![],
            service_connect_configuration: None,
            vpc_lattice_configurations: vec![],
            gateway_connectors: vec![],
        }
    }

    fn count_service_tasks(state: &SharedEcsState, status_filter: &[&str]) -> usize {
        let accounts = state.read();
        let s = accounts.get(ACCOUNT).unwrap();
        s.tasks
            .values()
            .filter(|t| {
                t.started_by.as_deref() == Some("ecs-svc/api")
                    && status_filter.contains(&t.last_status.as_str())
            })
            .count()
    }

    #[tokio::test]
    async fn service_converges_to_desired_count_and_is_idempotent() {
        let mut accounts: MultiAccountState<EcsState> =
            MultiAccountState::new(ACCOUNT, "us-east-1", "http://localhost:4566");
        let acct = accounts.get_or_create(ACCOUNT);
        acct.task_definitions
            .entry("web".to_string())
            .or_default()
            .insert(1, make_task_definition());
        acct.services
            .insert("default/api".to_string(), make_service(2));
        let state: SharedEcsState = Arc::new(RwLock::new(accounts));

        // No runtime configured: tasks are spawned then marked STOPPED
        // ("no container runtime"). The convergence intent — spawning
        // desiredCount tasks for the shortfall — is what we assert.
        let svc = EcsService::new(state.clone());
        svc.reconcile_service_desired_counts().await;
        assert_eq!(
            count_service_tasks(&state, &["STOPPED"]),
            2,
            "scheduler must spawn desiredCount tasks for a service at 0 running"
        );

        // The stop reason must stay actionable: it names the missing runtime
        // and tells the operator how to enable real task execution.
        {
            let accounts = state.read();
            let reason = accounts
                .get(ACCOUNT)
                .unwrap()
                .tasks
                .values()
                .find_map(|t| t.stopped_reason.clone())
                .expect("a STOPPED task must carry a stopped_reason");
            assert!(
                reason.contains("No container runtime available")
                    && reason.contains("Install and start Docker or Podman")
                    && reason.contains("FAKECLOUD_CONTAINER_CLI"),
                "runtime-missing stop reason must explain how to fix it: {reason}"
            );
        }

        // Simulate those tasks actually running (as the runtime would), then
        // tick again: the service is now at desiredCount so NO new tasks spawn.
        {
            let mut accounts = state.write();
            let s = accounts.get_mut(ACCOUNT).unwrap();
            for t in s.tasks.values_mut() {
                if t.started_by.as_deref() == Some("ecs-svc/api") {
                    t.last_status = "RUNNING".into();
                    t.desired_status = "RUNNING".into();
                    t.stop_code = None;
                    t.stopped_reason = None;
                    t.stopped_at = None;
                }
            }
        }
        svc.reconcile_service_desired_counts().await;
        assert_eq!(
            count_service_tasks(&state, &["RUNNING"]),
            2,
            "converged service must stay at desiredCount"
        );
        assert_eq!(
            count_service_tasks(&state, &["PENDING", "PROVISIONING", "STOPPED"]),
            0,
            "no extra tasks once desiredCount is met (no over-provisioning)"
        );
    }

    fn empty_state() -> SharedEcsState {
        let accounts: MultiAccountState<EcsState> =
            MultiAccountState::new(ACCOUNT, "us-east-1", "http://localhost:4566");
        Arc::new(RwLock::new(accounts))
    }

    fn ecs_request(action: &str, body: serde_json::Value) -> AwsRequest {
        AwsRequest {
            service: "ecs".into(),
            action: action.into(),
            region: "us-east-1".into(),
            account_id: ACCOUNT.into(),
            request_id: uuid::Uuid::new_v4().to_string(),
            headers: http::HeaderMap::new(),
            query_params: std::collections::HashMap::new(),
            body: bytes::Bytes::from(serde_json::to_vec(&body).unwrap()),
            body_stream: parking_lot::Mutex::new(None),
            path_segments: Vec::new(),
            raw_path: "/".into(),
            raw_query: String::new(),
            method: http::Method::POST,
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        }
    }

    #[tokio::test]
    async fn start_task_places_on_specified_container_instance() {
        let svc = EcsService::new(empty_state());
        svc.create_cluster(&ecs_request("CreateCluster", json!({"clusterName": "c1"})))
            .unwrap();
        svc.register_task_definition(&ecs_request(
            "RegisterTaskDefinition",
            json!({
                "family": "web",
                "containerDefinitions": [{"name": "app", "image": "nginx", "essential": true}]
            }),
        ))
        .unwrap();
        // Register two container instances.
        let mut ci_ids = Vec::new();
        for _ in 0..2 {
            let resp = svc
                .register_container_instance(&ecs_request(
                    "RegisterContainerInstance",
                    json!({"cluster": "c1"}),
                ))
                .unwrap();
            let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
            ci_ids.push(
                v["containerInstance"]["containerInstanceArn"]
                    .as_str()
                    .unwrap()
                    .to_string(),
            );
        }

        // StartTask on the SECOND instance -> the task lands there, not on a
        // synthetic i-fakecloud-1.
        let resp = svc
            .start_task(&ecs_request(
                "StartTask",
                json!({
                    "cluster": "c1",
                    "taskDefinition": "web",
                    "containerInstances": [ci_ids[1].clone()]
                }),
            ))
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(v["tasks"].as_array().unwrap().len(), 1);
        assert_eq!(v["tasks"][0]["containerInstanceArn"], json!(ci_ids[1]));
        assert!(
            !v["tasks"][0]["containerInstanceArn"]
                .as_str()
                .unwrap()
                .contains("i-fakecloud-1"),
            "must not use the synthetic placeholder instance"
        );

        // StartTask with no containerInstances is rejected.
        assert!(svc
            .start_task(&ecs_request(
                "StartTask",
                json!({"cluster": "c1", "taskDefinition": "web"}),
            ))
            .is_err());
    }

    #[tokio::test]
    async fn put_attributes_reflected_in_describe_container_instances() {
        let svc = EcsService::new(empty_state());
        svc.create_cluster(&ecs_request("CreateCluster", json!({"clusterName": "c1"})))
            .unwrap();
        let resp = svc
            .register_container_instance(&ecs_request(
                "RegisterContainerInstance",
                json!({"cluster": "c1"}),
            ))
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let ci_arn = v["containerInstance"]["containerInstanceArn"]
            .as_str()
            .unwrap()
            .to_string();

        svc.put_attributes(&ecs_request(
            "PutAttributes",
            json!({
                "cluster": "c1",
                "attributes": [{
                    "name": "stack",
                    "value": "prod",
                    "targetType": "container-instance",
                    "targetId": ci_arn.clone()
                }]
            }),
        ))
        .unwrap();

        // DescribeContainerInstances must now include the attribute set via PutAttributes.
        let resp = svc
            .describe_container_instances(&ecs_request(
                "DescribeContainerInstances",
                json!({"cluster": "c1", "containerInstances": [ci_arn.clone()]}),
            ))
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let attrs = v["containerInstances"][0]["attributes"].as_array().unwrap();
        let found = attrs
            .iter()
            .find(|a| a["name"] == json!("stack"))
            .expect("PutAttributes attribute must appear in DescribeContainerInstances");
        assert_eq!(found["value"], json!("prod"));

        // DeleteAttributes removes it from the Describe view too.
        svc.delete_attributes(&ecs_request(
            "DeleteAttributes",
            json!({
                "cluster": "c1",
                "attributes": [{
                    "name": "stack",
                    "targetType": "container-instance",
                    "targetId": ci_arn.clone()
                }]
            }),
        ))
        .unwrap();
        let resp = svc
            .describe_container_instances(&ecs_request(
                "DescribeContainerInstances",
                json!({"cluster": "c1", "containerInstances": [ci_arn]}),
            ))
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let attrs = v["containerInstances"][0]["attributes"].as_array().unwrap();
        assert!(attrs.iter().all(|a| a["name"] != json!("stack")));
    }

    #[tokio::test]
    async fn update_service_applies_extended_fields() {
        // UpdateService previously read only desiredCount/taskDefinition/
        // lifecycleHooks and dropped everything else (bug-audit 2026-06-20, 1.15).
        let state = empty_state();
        {
            let mut g = state.write();
            let st = g.get_or_create(ACCOUNT);
            st.services
                .insert("default/api".to_string(), make_service(2));
        }
        let svc = EcsService::new(state.clone());

        let body = serde_json::json!({
            "service": "api",
            "cluster": "default",
            "enableExecuteCommand": true,
            "enableECSManagedTags": true,
            "healthCheckGracePeriodSeconds": 120,
            "platformVersion": "1.4.0",
            "propagateTags": "SERVICE",
            "networkConfiguration": { "awsvpcConfiguration": { "subnets": ["subnet-aaa"] } },
            "deploymentConfiguration": {
                "minimumHealthyPercent": 50,
                "maximumPercent": 200,
                "deploymentCircuitBreaker": { "enable": true, "rollback": true }
            }
        });
        svc.update_service(&ecs_request("UpdateService", body))
            .expect("UpdateService");

        let g = state.read();
        let s = &g.get(ACCOUNT).unwrap().services["default/api"];
        assert!(s.enable_execute_command, "enableExecuteCommand must apply");
        assert!(s.enable_ecs_managed_tags);
        assert_eq!(s.health_check_grace_period_seconds, Some(120));
        assert_eq!(s.platform_version.as_deref(), Some("1.4.0"));
        assert_eq!(s.propagate_tags.as_deref(), Some("SERVICE"));
        assert!(s.network_configuration.is_some());
        assert_eq!(s.minimum_healthy_percent, Some(50));
        assert_eq!(s.maximum_percent, Some(200));
        assert!(s
            .circuit_breaker
            .as_ref()
            .map(|c| c.enable)
            .unwrap_or(false));
    }

    /// No snapshot store (memory mode) -> no persist hook for the CFN provisioner.
    #[test]
    fn snapshot_hook_is_none_without_store() {
        let svc = EcsService::new(empty_state());
        assert!(svc.snapshot_hook().is_none());
    }

    #[tokio::test]
    async fn snapshot_hook_fires_with_store() {
        let store: Arc<dyn fakecloud_persistence::SnapshotStore> =
            Arc::new(fakecloud_persistence::MemorySnapshotStore::new());
        let svc = EcsService::new(empty_state()).with_snapshot_store(store);
        let hook = svc
            .snapshot_hook()
            .expect("hook present when a store is set");
        hook().await;
    }

    #[test]
    fn china_region_arns_use_the_aws_cn_partition() {
        let svc = EcsService::new(empty_state());
        let in_region = |region: &str, action: &str, body: Value| {
            let mut r = ecs_request(action, body);
            r.region = region.into();
            r
        };
        let body = |resp: AwsResponse| -> Value {
            serde_json::from_slice(resp.body.expect_bytes()).unwrap()
        };

        let v = body(
            svc.create_cluster(&in_region(
                "cn-north-1",
                "CreateCluster",
                json!({"clusterName": "c1"}),
            ))
            .unwrap(),
        );
        let cluster_arn = v["cluster"]["clusterArn"].as_str().unwrap().to_string();
        assert_eq!(
            cluster_arn,
            format!("arn:aws-cn:ecs:cn-north-1:{ACCOUNT}:cluster/c1")
        );

        let v = body(
            svc.register_task_definition(&in_region(
                "cn-north-1",
                "RegisterTaskDefinition",
                json!({
                    "family": "web",
                    "containerDefinitions": [{"name": "app", "image": "nginx", "essential": true}]
                }),
            ))
            .unwrap(),
        );
        let td_arn = v["taskDefinition"]["taskDefinitionArn"]
            .as_str()
            .unwrap()
            .to_string();
        assert_eq!(
            td_arn,
            format!("arn:aws-cn:ecs:cn-north-1:{ACCOUNT}:task-definition/web:1")
        );

        let v = body(
            svc.describe_clusters(&in_region(
                "cn-north-1",
                "DescribeClusters",
                json!({"clusters": [cluster_arn]}),
            ))
            .unwrap(),
        );
        assert_eq!(v["clusters"][0]["clusterArn"], json!(cluster_arn));
        let v = body(
            svc.describe_task_definition(&in_region(
                "cn-north-1",
                "DescribeTaskDefinition",
                json!({"taskDefinition": td_arn}),
            ))
            .unwrap(),
        );
        assert_eq!(v["taskDefinition"]["taskDefinitionArn"], json!(td_arn));
    }

    #[test]
    fn principal_account_settings_are_shared_across_partitions() {
        let svc = EcsService::new(empty_state());
        let in_region = |region: &str, action: &str, body: Value| {
            let mut r = ecs_request(action, body);
            r.region = region.into();
            r
        };
        let body = |resp: AwsResponse| -> Value {
            serde_json::from_slice(resp.body.expect_bytes()).unwrap()
        };

        let v = body(
            svc.put_account_setting(&in_region(
                "cn-north-1",
                "PutAccountSetting",
                json!({"name": "containerInsights", "value": "enabled"}),
            ))
            .unwrap(),
        );
        assert_eq!(
            v["setting"]["principalArn"],
            json!(format!("arn:aws-cn:iam::{ACCOUNT}:root"))
        );

        let v = body(
            svc.list_account_settings(&in_region(
                "us-east-1",
                "ListAccountSettings",
                json!({"name": "containerInsights", "effectiveSettings": true}),
            ))
            .unwrap(),
        );
        assert_eq!(v["settings"][0]["value"], "enabled");
        assert_eq!(
            v["settings"][0]["principalArn"],
            json!(format!("arn:aws:iam::{ACCOUNT}:root"))
        );

        let v = body(
            svc.list_account_settings(&in_region(
                "cn-north-1",
                "ListAccountSettings",
                json!({"name": "containerInsights"}),
            ))
            .unwrap(),
        );
        assert_eq!(v["settings"][0]["value"], "enabled");
        assert_eq!(
            v["settings"][0]["principalArn"],
            json!(format!("arn:aws-cn:iam::{ACCOUNT}:root"))
        );

        svc.delete_account_setting(&in_region(
            "us-east-1",
            "DeleteAccountSetting",
            json!({"name": "containerInsights"}),
        ))
        .unwrap();
        let v = body(
            svc.list_account_settings(&in_region(
                "cn-north-1",
                "ListAccountSettings",
                json!({"name": "containerInsights"}),
            ))
            .unwrap(),
        );
        assert_eq!(v["settings"], json!([]));
    }

    fn tags_of(svc: &EcsService, arn: &str) -> Value {
        let resp = svc
            .list_tags_for_resource(&ecs_request(
                "ListTagsForResource",
                json!({"resourceArn": arn}),
            ))
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        v["tags"].clone()
    }

    #[tokio::test]
    async fn daemon_task_definition_persists_pid_and_ipc_modes() {
        let svc = EcsService::new(empty_state());
        for (field, bad) in [("pidMode", "host"), ("ipcMode", "task")] {
            let mut body = json!({
                "family": "agent",
                "containerDefinitions": [{"name": "a", "image": "busybox"}]
            });
            body[field] = json!(bad);
            let err = svc
                .register_daemon_task_definition(&ecs_request("RegisterDaemonTaskDefinition", body))
                .err()
                .expect("off-enum mode must be rejected");
            assert_eq!(err.code(), "InvalidParameterException", "{field}");
        }
        let resp = svc
            .register_daemon_task_definition(&ecs_request(
                "RegisterDaemonTaskDefinition",
                json!({
                    "family": "agent",
                    "containerDefinitions": [{"name": "a", "image": "busybox"}],
                    "pidMode": "shared",
                    "ipcMode": "none"
                }),
            ))
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let arn = v["daemonTaskDefinitionArn"].as_str().unwrap().to_string();
        let resp = svc
            .describe_daemon_task_definition(&ecs_request(
                "DescribeDaemonTaskDefinition",
                json!({"daemonTaskDefinition": arn}),
            ))
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let def = &v["daemonTaskDefinition"];
        assert_eq!(def["daemonTaskDefinitionArn"], json!(arn));
        assert_eq!(def["pidMode"], "shared");
        assert_eq!(def["ipcMode"], "none");
        // Tags live behind ListTagsForResource, not on the definition shape.
        assert!(def.get("tags").is_none());
        assert!(def.get("taskDefinitionArn").is_none());
    }

    #[tokio::test]
    async fn daemons_daemon_task_definitions_and_express_gateway_services_are_taggable() {
        let svc = EcsService::new(empty_state());
        svc.create_cluster(&ecs_request("CreateCluster", json!({"clusterName": "c1"})))
            .unwrap();
        let resp = svc
            .register_daemon_task_definition(&ecs_request(
                "RegisterDaemonTaskDefinition",
                json!({
                    "family": "agent",
                    "containerDefinitions": [{"name": "a", "image": "busybox"}],
                    "tags": [{"key": "env", "value": "prod"}]
                }),
            ))
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let td_arn = v["daemonTaskDefinitionArn"].as_str().unwrap().to_string();

        let resp = svc
            .create_daemon(&ecs_request(
                "CreateDaemon",
                json!({
                    "daemonName": "d1",
                    "clusterArn": "c1",
                    "daemonTaskDefinitionArn": td_arn,
                    "capacityProviderArns": [],
                    "tags": [{"key": "env", "value": "prod"}]
                }),
            ))
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let daemon_arn = v["daemonArn"].as_str().unwrap().to_string();

        let resp = svc
            .create_express_gateway_service(&ecs_request(
                "CreateExpressGatewayService",
                json!({
                    "cluster": "c1",
                    "serviceName": "eg1",
                    "executionRoleArn": "arn:aws:iam::123456789012:role/exec",
                    "infrastructureRoleArn": "arn:aws:iam::123456789012:role/infra",
                    "primaryContainer": {"image": "nginx"},
                    "tags": [{"key": "env", "value": "prod"}]
                }),
            ))
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let eg_arn = v["service"]["serviceArn"].as_str().unwrap().to_string();

        for arn in [&td_arn, &daemon_arn, &eg_arn] {
            // Creation-time tags are visible through the generic API.
            assert_eq!(
                tags_of(&svc, arn),
                json!([{"key": "env", "value": "prod"}]),
                "{arn}"
            );
            svc.tag_resource(&ecs_request(
                "TagResource",
                json!({"resourceArn": arn, "tags": [{"key": "team", "value": "infra"}]}),
            ))
            .unwrap();
            assert_eq!(
                tags_of(&svc, arn),
                json!([{"key": "env", "value": "prod"}, {"key": "team", "value": "infra"}]),
                "{arn}"
            );
            svc.untag_resource(&ecs_request(
                "UntagResource",
                json!({"resourceArn": arn, "tagKeys": ["env"]}),
            ))
            .unwrap();
            assert_eq!(
                tags_of(&svc, arn),
                json!([{"key": "team", "value": "infra"}]),
                "{arn}"
            );
        }

        let err = svc
            .list_tags_for_resource(&ecs_request(
                "ListTagsForResource",
                json!({"resourceArn": format!("{daemon_arn}-missing")}),
            ))
            .err()
            .unwrap();
        // ECS reports an unknown tag target as a ClientException.
        assert_eq!(err.code(), "ClientException");
    }

    #[test]
    fn capacity_providers_kinds_scoping_and_builtins() {
        let svc = EcsService::new(empty_state());
        let json_of = |resp: AwsResponse| -> Value {
            serde_json::from_slice(resp.body.expect_bytes()).unwrap()
        };
        let asg =
            json!({"autoScalingGroupArn": "arn:aws:autoscaling:us-east-1:1:autoScalingGroup:x"});
        let mip = json!({
            "infrastructureRoleArn": "arn:aws:iam::1:role/infra",
            "instanceLaunchTemplate": {"ec2InstanceProfileArn": "arn:aws:iam::1:instance-profile/p",
                "networkConfiguration": {"subnets": ["subnet-1"]}}
        });

        // A provider needs exactly one backing; reserved prefixes are refused.
        for body in [
            json!({"name": "bare"}),
            json!({"name": "both", "autoScalingGroupProvider": asg, "managedInstancesProvider": mip, "cluster": "c1"}),
            json!({"name": "FargateX", "autoScalingGroupProvider": asg}),
        ] {
            let err = svc
                .create_capacity_provider(&ecs_request("CreateCapacityProvider", body))
                .err()
                .expect("invalid provider must be rejected");
            assert_eq!(err.code(), "InvalidParameterException");
        }
        // Managed Instances providers need an existing cluster.
        let err = svc
            .create_capacity_provider(&ecs_request(
                "CreateCapacityProvider",
                json!({"name": "mi", "managedInstancesProvider": mip}),
            ))
            .err()
            .unwrap();
        assert_eq!(err.code(), "InvalidParameterException");
        let err = svc
            .create_capacity_provider(&ecs_request(
                "CreateCapacityProvider",
                json!({"name": "mi", "managedInstancesProvider": mip, "cluster": "nope"}),
            ))
            .err()
            .unwrap();
        assert_eq!(err.code(), "ClusterNotFoundException");

        svc.create_cluster(&ecs_request("CreateCluster", json!({"clusterName": "c1"})))
            .unwrap();
        let created = json_of(
            svc.create_capacity_provider(&ecs_request(
                "CreateCapacityProvider",
                json!({"name": "mi", "managedInstancesProvider": mip, "cluster": "c1"}),
            ))
            .unwrap(),
        );
        let cp = &created["capacityProvider"];
        assert_eq!(cp["type"], "MANAGED_INSTANCES");
        assert_eq!(cp["cluster"], "c1");
        assert_eq!(cp["managedInstancesProvider"], mip);
        assert!(cp.get("autoScalingGroupProvider").is_none(), "{cp}");
        assert!(cp.get("updateStatus").is_none(), "no nulls: {cp}");

        let created = json_of(
            svc.create_capacity_provider(&ecs_request(
                "CreateCapacityProvider",
                json!({"name": "asg-cp", "autoScalingGroupProvider": asg}),
            ))
            .unwrap(),
        );
        assert_eq!(created["capacityProvider"]["type"], "EC2_AUTOSCALING");
        assert!(created["capacityProvider"]
            .get("managedInstancesProvider")
            .is_none());

        // Listing without a cluster: account-level providers, then FARGATE and
        // FARGATE_SPOT; the cluster-scoped Managed Instances provider is not
        // listed.
        let all = json_of(
            svc.describe_capacity_providers(&ecs_request("DescribeCapacityProviders", json!({})))
                .unwrap(),
        );
        let names: Vec<&str> = all["capacityProviders"]
            .as_array()
            .unwrap()
            .iter()
            .map(|c| c["name"].as_str().unwrap())
            .collect();
        assert_eq!(names, ["asg-cp", "FARGATE", "FARGATE_SPOT"]);
        assert_eq!(all["capacityProviders"][1]["type"], "FARGATE");

        // Listing for the cluster returns its Managed Instances provider.
        let scoped = json_of(
            svc.describe_capacity_providers(&ecs_request(
                "DescribeCapacityProviders",
                json!({"cluster": "c1"}),
            ))
            .unwrap(),
        );
        let names: Vec<&str> = scoped["capacityProviders"]
            .as_array()
            .unwrap()
            .iter()
            .map(|c| c["name"].as_str().unwrap())
            .collect();
        assert_eq!(names, ["mi"]);

        // Built-ins resolve by name; pagination walks the full list.
        let by_name = json_of(
            svc.describe_capacity_providers(&ecs_request(
                "DescribeCapacityProviders",
                json!({"capacityProviders": ["FARGATE_SPOT", "missing"]}),
            ))
            .unwrap(),
        );
        assert_eq!(by_name["capacityProviders"][0]["name"], "FARGATE_SPOT");
        assert_eq!(by_name["failures"][0]["arn"], "missing");
        let page = json_of(
            svc.describe_capacity_providers(&ecs_request(
                "DescribeCapacityProviders",
                json!({"maxResults": 2}),
            ))
            .unwrap(),
        );
        assert_eq!(page["capacityProviders"].as_array().unwrap().len(), 2);
        let token = page["nextToken"].as_str().unwrap().to_string();
        let rest = json_of(
            svc.describe_capacity_providers(&ecs_request(
                "DescribeCapacityProviders",
                json!({"maxResults": 2, "nextToken": token}),
            ))
            .unwrap(),
        );
        assert_eq!(rest["capacityProviders"][0]["name"], "FARGATE_SPOT");
        assert!(rest.get("nextToken").is_none());

        // An update must match the provider's kind.
        let err = svc
            .update_capacity_provider(&ecs_request(
                "UpdateCapacityProvider",
                json!({"name": "asg-cp", "managedInstancesProvider": {"propagateTags": "NONE"}}),
            ))
            .err()
            .unwrap();
        assert_eq!(err.code(), "InvalidParameterException");
        let updated = json_of(
            svc.update_capacity_provider(&ecs_request(
                "UpdateCapacityProvider",
                json!({"name": "mi", "cluster": "c1",
                    "managedInstancesProvider": {"propagateTags": "CAPACITY_PROVIDER"}}),
            ))
            .unwrap(),
        );
        let mip_out = &updated["capacityProvider"]["managedInstancesProvider"];
        assert_eq!(mip_out["propagateTags"], "CAPACITY_PROVIDER");
        assert_eq!(
            mip_out["infrastructureRoleArn"],
            "arn:aws:iam::1:role/infra"
        );

        // A Managed Instances provider is updated/deleted within its cluster.
        svc.create_cluster(&ecs_request("CreateCluster", json!({"clusterName": "c2"})))
            .unwrap();
        let update = |cluster: Option<&str>| {
            let mut body = json!({"name": "mi",
                "managedInstancesProvider": {"propagateTags": "NONE"}});
            if let Some(c) = cluster {
                body["cluster"] = json!(c);
            }
            svc.update_capacity_provider(&ecs_request("UpdateCapacityProvider", body))
                .err()
                .map(|e| e.code().to_string())
        };
        assert_eq!(update(None).as_deref(), Some("InvalidParameterException"));
        assert_eq!(update(Some("c2")).as_deref(), Some("ClientException"));
        assert_eq!(
            update(Some("ghost")).as_deref(),
            Some("ClusterNotFoundException")
        );
        let c1_arn = format!("arn:aws:ecs:us-east-1:{ACCOUNT}:cluster/c1");
        assert_eq!(update(Some(&c1_arn)), None);
        let err = svc
            .delete_capacity_provider(&ecs_request(
                "DeleteCapacityProvider",
                json!({"capacityProvider": "mi", "cluster": "c2"}),
            ))
            .err()
            .unwrap();
        assert_eq!(err.code(), "ClientException");

        // maxResults is bounded to 1..=10.
        for bad in [0, 11] {
            let err = svc
                .describe_capacity_providers(&ecs_request(
                    "DescribeCapacityProviders",
                    json!({"maxResults": bad}),
                ))
                .err()
                .unwrap();
            assert_eq!(err.code(), "InvalidParameterException", "{bad}");
        }

        // A provider attached to a cluster by ARN is listed for that cluster.
        let asg_arn = format!("arn:aws:ecs:us-east-1:{ACCOUNT}:capacity-provider/asg-cp");
        svc.put_cluster_capacity_providers(&ecs_request(
            "PutClusterCapacityProviders",
            json!({"cluster": "c2", "capacityProviders": [asg_arn],
                "defaultCapacityProviderStrategy": []}),
        ))
        .unwrap();
        let scoped = json_of(
            svc.describe_capacity_providers(&ecs_request(
                "DescribeCapacityProviders",
                json!({"cluster": "c2"}),
            ))
            .unwrap(),
        );
        assert_eq!(scoped["capacityProviders"][0]["name"], "asg-cp", "{scoped}");
    }
}

/// Trusts exactly one role, like a role whose trust policy names
/// `ecs-tasks.amazonaws.com`; rejects every other.
struct TrustsOnly(&'static str);

impl fakecloud_core::auth::RoleTrustValidator for TrustsOnly {
    fn validate(
        &self,
        _account_id: &str,
        role_arn: &str,
        service_principal: &str,
    ) -> Result<(), fakecloud_core::auth::PassRoleError> {
        assert_eq!(service_principal, "ecs-tasks.amazonaws.com");
        if role_arn == self.0 {
            Ok(())
        } else {
            Err(fakecloud_core::auth::PassRoleError::TrustPolicyDenies {
                role_arn: role_arn.to_string(),
                service_principal: service_principal.to_string(),
            })
        }
    }
}

#[test]
fn validate_task_role_matches_ecs_errors() {
    use fakecloud_core::auth::IamMode;
    let trusted = "arn:aws:iam::123456789012:role/trusted";
    let untrusted = "arn:aws:iam::123456789012:role/untrusted";
    let validator = TrustsOnly(trusted);

    validate_task_role("123456789012", trusted, Some(&validator), IamMode::Strict).unwrap();

    let err =
        validate_task_role("123456789012", untrusted, Some(&validator), IamMode::Off).unwrap_err();
    assert_eq!(err.code(), "ClientException");
    assert_eq!(err.message(), unable_to_assume_role_message(untrusted));
    assert!(err.message().starts_with(
        "ECS was unable to assume the role 'arn:aws:iam::123456789012:role/untrusted' that was provided for this task."
    ));

    // Another account's role: accepted with IAM off or soft, refused under
    // strict whatever its trust policy says.
    let foreign = "arn:aws:iam::999999999999:role/elsewhere";
    let trusts_foreign = TrustsOnly(foreign);
    validate_task_role("123456789012", foreign, Some(&trusts_foreign), IamMode::Off).unwrap();
    validate_task_role(
        "123456789012",
        foreign,
        Some(&trusts_foreign),
        IamMode::Soft,
    )
    .unwrap();
    let err = validate_task_role(
        "123456789012",
        foreign,
        Some(&trusts_foreign),
        IamMode::Strict,
    )
    .unwrap_err();
    assert_eq!(err.code(), "ClientException");
    assert_eq!(err.message(), unable_to_assume_role_message(foreign));
}

#[test]
fn register_task_definition_checks_both_roles() {
    let svc = EcsService::new(std::sync::Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new(
            "000000000000",
            "us-east-1",
            "http://localhost:4566",
        ),
    )))
    .with_role_trust_validator(std::sync::Arc::new(TrustsOnly(
        "arn:aws:iam::000000000000:role/ok",
    )));
    let register = |task_role: &str, exec_role: &str| {
        svc.register_task_definition(&service_tasks::multi_container_tests::make_request(
            "RegisterTaskDefinition",
            json!({
                "family": "f",
                "taskRoleArn": task_role,
                "executionRoleArn": exec_role,
                "containerDefinitions": [{"name": "app", "image": "alpine"}]
            }),
        ))
    };
    let ok = "arn:aws:iam::000000000000:role/ok";
    let bad = "arn:aws:iam::000000000000:role/bad";
    register(ok, ok).expect("trusted roles register");
    for (task_role, exec_role) in [(bad, ok), (ok, bad)] {
        let Err(err) = register(task_role, exec_role) else {
            panic!("untrusted role registered");
        };
        assert_eq!(err.code(), "ClientException");
        assert_eq!(err.message(), unable_to_assume_role_message(bad));
    }
}

/// Trust decided per account: the role named `app` trusts ECS tasks only in
/// account 999999999999; the caller's own `app` role does not.
struct TrustsOnlyIn(&'static str);

impl fakecloud_core::auth::RoleTrustValidator for TrustsOnlyIn {
    fn validate(
        &self,
        account_id: &str,
        role_arn: &str,
        service_principal: &str,
    ) -> Result<(), fakecloud_core::auth::PassRoleError> {
        if account_id == self.0 {
            Ok(())
        } else {
            Err(fakecloud_core::auth::PassRoleError::TrustPolicyDenies {
                role_arn: role_arn.to_string(),
                service_principal: service_principal.to_string(),
            })
        }
    }
}

#[test]
fn another_accounts_role_is_checked_against_its_own_trust_policy() {
    use fakecloud_core::auth::IamMode;
    let validator = TrustsOnlyIn("999999999999");
    // The role's own account trusts ECS tasks; a same-named role in the
    // caller's account that does not must not decide the outcome.
    validate_task_role(
        "123456789012",
        "arn:aws:iam::999999999999:role/app",
        Some(&validator),
        IamMode::Off,
    )
    .expect("checked in the role's account");
    let Err(err) = validate_task_role(
        "123456789012",
        "arn:aws:iam::123456789012:role/app",
        Some(&validator),
        IamMode::Off,
    ) else {
        panic!("caller's own untrusted role accepted");
    };
    assert_eq!(err.code(), "ClientException");
}

mod refreshed_members {
    use super::*;
    use fakecloud_core::multi_account::MultiAccountState;

    fn svc() -> EcsService {
        let accounts: MultiAccountState<EcsState> =
            MultiAccountState::new("000000000000", "us-east-1", "http://localhost:4566");
        let state = Arc::new(parking_lot::RwLock::new(accounts));
        let svc = EcsService::new(state);
        svc.create_cluster(&req("CreateCluster", json!({"clusterName": "default"})))
            .unwrap();
        svc
    }

    fn req(action: &str, body: Value) -> AwsRequest {
        AwsRequest {
            service: "ecs".into(),
            action: action.into(),
            region: "us-east-1".into(),
            account_id: "000000000000".into(),
            request_id: "req".into(),
            headers: http::HeaderMap::new(),
            query_params: std::collections::HashMap::new(),
            body: bytes::Bytes::from(serde_json::to_vec(&body).unwrap()),
            body_stream: parking_lot::Mutex::new(None),
            path_segments: Vec::new(),
            raw_path: "/".into(),
            raw_query: String::new(),
            method: http::Method::POST,
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        }
    }

    fn body(resp: AwsResponse) -> Value {
        serde_json::from_slice(resp.body.expect_bytes()).unwrap()
    }

    fn gpu_td(reqs: Value) -> Value {
        json!({"family": "gpu", "containerDefinitions": [
            {"name": "app", "image": "cuda", "resourceRequirements": reqs}
        ]})
    }

    #[test]
    fn gpu_sharing_strategy_is_validated_and_reported_on_task_containers() {
        let svc = svc();
        let mps = json!({"nvidiaMps": {"memory": 2048, "maxComputePercent": 50}});
        for bad in [
            json!([{"type": "GPU", "value": "1", "sharingStrategy": mps}]),
            json!([{"type": "InferenceAccelerator", "sharingStrategy": mps}]),
            json!([{"type": "GPU", "sharingStrategy": {"nvidiaMps": {"memory": 0}}}]),
            json!([{"type": "GPU", "sharingStrategy": {"nvidiaMps": {"memory": 1, "maxComputePercent": 101}}}]),
            json!([{"type": "GPU"}]),
            json!([{"type": "TPU", "value": "1"}]),
        ] {
            let err = svc
                .register_task_definition(&req("RegisterTaskDefinition", gpu_td(bad.clone())))
                .err()
                .unwrap_or_else(|| panic!("accepted {bad}"));
            assert_eq!(err.code(), "ClientException", "{bad}");
        }
        // Mixing shared and exclusive GPU containers in one task is refused.
        let mixed = json!({"family": "gpu", "containerDefinitions": [
            {"name": "a", "image": "cuda", "resourceRequirements": [{"type": "GPU", "sharingStrategy": mps}]},
            {"name": "b", "image": "cuda", "resourceRequirements": [{"type": "GPU", "value": "1"}]},
        ]});
        assert!(svc
            .register_task_definition(&req("RegisterTaskDefinition", mixed))
            .is_err());
        // EXTERNAL task definitions can't share GPUs.
        let mut external = gpu_td(json!([{"type": "GPU", "sharingStrategy": mps}]));
        external["requiresCompatibilities"] = json!(["EXTERNAL"]);
        assert!(svc
            .register_task_definition(&req("RegisterTaskDefinition", external))
            .is_err());

        let td = body(
            svc.register_task_definition(&req(
                "RegisterTaskDefinition",
                gpu_td(json!([{"type": "GPU", "sharingStrategy": mps}])),
            ))
            .unwrap(),
        );
        assert_eq!(
            td["taskDefinition"]["containerDefinitions"][0]["resourceRequirements"][0]
                ["sharingStrategy"],
            mps
        );
        let ran = body(
            svc.run_task(&req("RunTask", json!({"taskDefinition": "gpu"})))
                .unwrap(),
        );
        assert_eq!(ran["tasks"][0]["containers"][0]["gpuSharingStrategy"], mps);

        // A container override replaces the task definition's GPU option.
        let other = json!({"nvidiaMps": {"memory": 512}});
        let ran = body(
            svc.run_task(&req(
                "RunTask",
                json!({"taskDefinition": "gpu", "overrides": {"containerOverrides": [
                    {"name": "app", "resourceRequirements": [{"type": "GPU", "sharingStrategy": other}]}
                ]}}),
            ))
            .unwrap(),
        );
        assert_eq!(
            ran["tasks"][0]["containers"][0]["gpuSharingStrategy"],
            other
        );
    }

    #[test]
    fn daemon_log_driver_volumes_are_validated_and_round_trip() {
        let svc = svc();
        let td = |volumes: Value| {
            json!({"family": "logs", "containerDefinitions": [{"name": "a", "image": "fluent"}],
                   "volumes": volumes})
        };
        for bad in [
            json!([{"name": "v", "logDriverVolumeConfiguration": {}}]),
            json!([{"name": "v", "logDriverVolumeConfiguration": {"driver": "awslogs"}}]),
            json!([{"name": "v", "logDriverVolumeConfiguration": {"driver": "nope"}}]),
            json!([{"name": "v", "host": {"sourcePath": "/x"},
                    "logDriverVolumeConfiguration": {"driver": "json-file"}}]),
        ] {
            assert!(
                svc.register_daemon_task_definition(&req(
                    "RegisterDaemonTaskDefinition",
                    td(bad.clone())
                ))
                .is_err(),
                "accepted {bad}"
            );
        }
        let vols = json!([{"name": "v", "logDriverVolumeConfiguration": {"driver": "json-file"}}]);
        let out = body(
            svc.register_daemon_task_definition(&req(
                "RegisterDaemonTaskDefinition",
                td(vols.clone()),
            ))
            .unwrap(),
        );
        let arn = out["daemonTaskDefinitionArn"].as_str().unwrap();
        let described = body(
            svc.describe_daemon_task_definition(&req(
                "DescribeDaemonTaskDefinition",
                json!({"daemonTaskDefinition": arn}),
            ))
            .unwrap(),
        );
        assert_eq!(described["daemonTaskDefinition"]["volumes"], vols);
    }

    #[test]
    fn service_connect_container_configurations_are_validated() {
        assert!(validate_service_connect_container_configurations(Some(
            &json!({"containerConfigurations": [{"logLevel": "debug"}]})
        ))
        .is_ok());
        for bad in [
            json!({"containerConfigurations": [{"logLevel": "trace"}]}),
            json!({"containerConfigurations": [{"logLevel": "info"}, {"logLevel": "debug"}]}),
            json!({"containerConfigurations": [{}]}),
            json!({"containerConfigurations": [{"other": 1}]}),
        ] {
            assert!(
                validate_service_connect_container_configurations(Some(&bad)).is_err(),
                "accepted {bad}"
            );
        }
    }
}
