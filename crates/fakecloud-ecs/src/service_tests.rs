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
}
