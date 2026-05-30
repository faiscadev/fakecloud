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
