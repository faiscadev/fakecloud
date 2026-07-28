//! Helpers that translate raw service state into the public introspection
//! response types from ``fakecloud-sdk``.
//!
//! These mappings exist so the ``GET /_fakecloud/...`` introspection endpoints
//! can hand back stable structs without leaking secrets (e.g. RDS master
//! passwords) or runtime-internal fields.

use fakecloud_sdk::types;

pub(crate) fn rds_instance_response(instance: &fakecloud_rds::DbInstance) -> types::RdsInstance {
    types::RdsInstance {
        db_instance_identifier: instance.db_instance_identifier.clone(),
        db_instance_arn: instance.db_instance_arn.clone(),
        db_instance_class: instance.db_instance_class.clone(),
        engine: instance.engine.clone(),
        engine_version: instance.engine_version.clone(),
        db_instance_status: instance.db_instance_status.clone(),
        master_username: instance.master_username.clone(),
        db_name: instance.db_name.clone(),
        endpoint_address: instance.endpoint_address.clone(),
        port: instance.port,
        allocated_storage: instance.allocated_storage,
        publicly_accessible: instance.publicly_accessible,
        deletion_protection: instance.deletion_protection,
        created_at: instance.created_at.to_rfc3339(),
        dbi_resource_id: instance.dbi_resource_id.clone(),
        container_id: instance.container_id.clone(),
        host_port: instance.host_port,
        tags: instance
            .tags
            .iter()
            .map(|tag| types::RdsTag {
                key: tag.key.clone(),
                value: tag.value.clone(),
            })
            .collect(),
    }
}

pub(crate) fn ecr_repository_response(repo: &fakecloud_ecr::Repository) -> types::EcrRepository {
    let image_count = repo.images.len() as u64;
    let layer_count = repo.layers.len() as u64;
    types::EcrRepository {
        repository_name: repo.repository_name.clone(),
        repository_arn: repo.repository_arn.clone(),
        registry_id: repo.registry_id.clone(),
        repository_uri: repo.repository_uri.clone(),
        image_tag_mutability: repo.image_tag_mutability.clone(),
        scan_on_push: repo.image_scanning_configuration.scan_on_push,
        created_at: repo.created_at.to_rfc3339(),
        tags: repo
            .tags
            .iter()
            .map(|(k, v)| types::EcrTag {
                key: k.clone(),
                value: v.clone(),
            })
            .collect(),
        has_policy: repo.policy.is_some(),
        has_lifecycle_policy: repo.lifecycle_policy.is_some(),
        image_count,
        layer_count,
    }
}

pub(crate) fn ecr_image_response(
    repo: &fakecloud_ecr::Repository,
    image: &fakecloud_ecr::Image,
) -> types::EcrImage {
    let tags: Vec<String> = repo
        .image_tags
        .iter()
        .filter(|(_, d)| d.as_str() == image.image_digest)
        .map(|(t, _)| t.clone())
        .collect();
    types::EcrImage {
        repository_name: repo.repository_name.clone(),
        image_digest: image.image_digest.clone(),
        image_tags: tags,
        image_size_in_bytes: image.image_size_in_bytes,
        image_manifest_media_type: image.image_manifest_media_type.clone(),
        image_pushed_at: image.image_pushed_at.to_rfc3339(),
    }
}

pub(crate) fn ecr_pull_through_rule_response(
    rule: &fakecloud_ecr::PullThroughCacheRule,
) -> types::EcrPullThroughRule {
    types::EcrPullThroughRule {
        ecr_repository_prefix: rule.ecr_repository_prefix.clone(),
        upstream_registry_url: rule.upstream_registry_url.clone(),
        upstream_registry: rule.upstream_registry.clone(),
        credential_arn: rule.credential_arn.clone(),
        custom_role_arn: rule.custom_role_arn.clone(),
        created_at: rule.created_at.to_rfc3339(),
        updated_at: rule.updated_at.to_rfc3339(),
    }
}

pub(crate) fn ecs_task_response(task: &fakecloud_ecs::Task) -> types::EcsTask {
    types::EcsTask {
        task_arn: task.task_arn.clone(),
        task_id: task.task_id.clone(),
        cluster_arn: task.cluster_arn.clone(),
        cluster_name: task.cluster_name.clone(),
        task_definition_arn: task.task_definition_arn.clone(),
        family: task.family.clone(),
        revision: task.revision,
        last_status: task.last_status.clone(),
        desired_status: task.desired_status.clone(),
        launch_type: task.launch_type.clone(),
        created_at: task.created_at.to_rfc3339(),
        started_at: task.started_at.map(|t| t.to_rfc3339()),
        stopping_at: task.stopping_at.map(|t| t.to_rfc3339()),
        stopped_at: task.stopped_at.map(|t| t.to_rfc3339()),
        stop_code: task.stop_code.clone(),
        stopped_reason: task.stopped_reason.clone(),
        group: task.group.clone(),
        captured_log_bytes: task.captured_logs.len(),
        containers: task
            .containers
            .iter()
            .map(|c| types::EcsTaskContainer {
                name: c.name.clone(),
                image: c.image.clone(),
                last_status: c.last_status.clone(),
                exit_code: c.exit_code,
                runtime_id: c.runtime_id.clone(),
                essential: c.essential,
            })
            .collect(),
    }
}

pub(crate) fn ecs_task_metadata_response(
    task: &fakecloud_ecs::Task,
) -> types::EcsTaskMetadataResponse {
    // Pull the ENI/VPC bits out of the awsvpc attachment, if any.
    let mut eni_id: Option<String> = None;
    let mut vpc_id: Option<String> = None;
    for att in &task.attachments {
        if att.attachment_type.eq_ignore_ascii_case("eni") {
            eni_id = Some(att.id.clone());
            for d in &att.details {
                if d.name == "vpcId" {
                    vpc_id = Some(d.value.clone());
                }
            }
        }
    }
    // fakecloud doesn't model a separate VPC store today, so synthesise a
    // stable placeholder if the task has an ENI but no explicit vpcId.
    if eni_id.is_some() && vpc_id.is_none() {
        vpc_id = Some("vpc-fakecloud".into());
    }

    let containers = task
        .containers
        .iter()
        .map(|c| {
            let ports = c
                .network_bindings
                .iter()
                .map(|nb| types::EcsTaskMetadataPort {
                    container_port: nb.get("containerPort").and_then(|v| v.as_i64()),
                    host_port: nb.get("hostPort").and_then(|v| v.as_i64()),
                    protocol: nb
                        .get("protocol")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string()),
                })
                .collect();
            types::EcsTaskMetadataContainer {
                name: c.name.clone(),
                image: c.image.clone(),
                image_id: c.image_digest.clone(),
                ports,
                labels: std::collections::BTreeMap::new(),
                desired_status: c.last_status.clone(),
                known_status: c.last_status.clone(),
                limits: types::EcsTaskMetadataLimits {
                    cpu: c.cpu.as_ref().and_then(|v| v.parse::<f64>().ok()),
                    memory: c.memory.as_ref().and_then(|v| v.parse::<i64>().ok()),
                },
                created_at: Some(task.created_at.to_rfc3339()),
                started_at: task.started_at.map(|d| d.to_rfc3339()),
                exit_code: c.exit_code,
            }
        })
        .collect();

    types::EcsTaskMetadataResponse {
        task: types::EcsTaskMetadata {
            cluster: task.cluster_name.clone(),
            task_arn: task.task_arn.clone(),
            family: task.family.clone(),
            revision: task.revision,
            desired_status: task.desired_status.clone(),
            known_status: task.last_status.clone(),
            containers,
            pull_started_at: task.pull_started_at.map(|d| d.to_rfc3339()),
            pull_stopped_at: task.pull_stopped_at.map(|d| d.to_rfc3339()),
            availability_zone: "us-east-1a".into(),
            launch_type: task.launch_type.clone(),
            vpc_id,
            eni_id,
        },
    }
}

pub(crate) fn ecs_lifecycle_event(
    event: &fakecloud_ecs::LifecycleEvent,
) -> types::EcsLifecycleEvent {
    types::EcsLifecycleEvent {
        at: event.at.to_rfc3339(),
        event_type: event.event_type.clone(),
        task_arn: event.task_arn.clone(),
        cluster_arn: event.cluster_arn.clone(),
        last_status: event.last_status.clone(),
        detail: event.detail.clone(),
    }
}

pub(crate) fn ecs_cluster_response(cluster: &fakecloud_ecs::Cluster) -> types::EcsCluster {
    types::EcsCluster {
        cluster_name: cluster.cluster_name.clone(),
        cluster_arn: cluster.cluster_arn.clone(),
        status: cluster.status.clone(),
        running_tasks_count: cluster.running_tasks_count,
        pending_tasks_count: cluster.pending_tasks_count,
        active_services_count: cluster.active_services_count,
        registered_container_instances_count: cluster.registered_container_instances_count,
        capacity_providers: cluster.capacity_providers.clone(),
        tags: cluster
            .tags
            .iter()
            .map(|t| types::EcsTag {
                key: t.key.clone(),
                value: t.value.clone(),
            })
            .collect(),
        created_at: cluster.created_at.to_rfc3339(),
    }
}

pub(crate) fn elasticache_cluster_response(
    cluster: &fakecloud_elasticache::CacheCluster,
) -> types::ElastiCacheCluster {
    types::ElastiCacheCluster {
        cache_cluster_id: cluster.cache_cluster_id.clone(),
        cache_cluster_status: cluster.cache_cluster_status.clone(),
        engine: cluster.engine.clone(),
        engine_version: cluster.engine_version.clone(),
        cache_node_type: cluster.cache_node_type.clone(),
        num_cache_nodes: cluster.num_cache_nodes,
        replication_group_id: cluster.replication_group_id.clone(),
        port: Some(cluster.endpoint_port as i32),
        host_port: Some(cluster.host_port),
        container_id: Some(cluster.container_id.clone()),
    }
}

pub(crate) fn elasticache_replication_group_response(
    group: &fakecloud_elasticache::ReplicationGroup,
) -> types::ElastiCacheReplicationGroupIntrospection {
    types::ElastiCacheReplicationGroupIntrospection {
        replication_group_id: group.replication_group_id.clone(),
        status: group.status.clone(),
        description: group.description.clone(),
        member_clusters: group.member_clusters.clone(),
        automatic_failover: group.automatic_failover_enabled,
        multi_az: group.automatic_failover_enabled,
        engine: group.engine.clone(),
        engine_version: group.engine_version.clone(),
        cache_node_type: group.cache_node_type.clone(),
        num_cache_clusters: group.num_cache_clusters,
    }
}

/// Aggregate ACL state (users + user groups) for every replication group
/// that has at least one user group attached.
///
/// AWS ElastiCache exposes user/usergroup state via `DescribeUsers` and
/// `DescribeUserGroups`, but pinning the membership graph to specific
/// clusters requires correlating `ReplicationGroup.user_group_ids` →
/// `UserGroup.user_ids` → `User`. We do that join here so test authors
/// can assert against a single shape.
pub(crate) fn elasticache_acls_response(
    state: &fakecloud_elasticache::ElastiCacheState,
) -> types::ElastiCacheAclsResponse {
    let mut acls: Vec<types::ElastiCacheAclCluster> = state
        .replication_groups
        .values()
        .filter(|rg| !rg.user_group_ids.is_empty())
        .map(|rg| {
            let groups: Vec<types::ElastiCacheAclGroup> = rg
                .user_group_ids
                .iter()
                .filter_map(|gid| state.user_groups.get(gid))
                .map(|g| types::ElastiCacheAclGroup {
                    name: g.user_group_id.clone(),
                    members: g.user_ids.clone(),
                })
                .collect();
            // Collect the unique user ids referenced by all attached groups.
            let mut user_ids: Vec<String> = groups
                .iter()
                .flat_map(|g| g.members.iter().cloned())
                .collect();
            user_ids.sort();
            user_ids.dedup();
            let users: Vec<types::ElastiCacheAclUser> = user_ids
                .iter()
                .filter_map(|uid| state.users.get(uid))
                .map(|u| types::ElastiCacheAclUser {
                    name: u.user_name.clone(),
                    status: u.status.clone(),
                    access_string: u.access_string.clone(),
                    no_password_required: u.authentication_type == "no-password",
                    password_count: u.password_count,
                })
                .collect();
            types::ElastiCacheAclCluster {
                cluster_id: rg.replication_group_id.clone(),
                engine: rg.engine.clone(),
                users,
                groups,
            }
        })
        .collect();
    acls.sort_by(|a, b| a.cluster_id.cmp(&b.cluster_id));
    types::ElastiCacheAclsResponse { acls }
}

pub(crate) fn elasticache_serverless_cache_response(
    cache: &fakecloud_elasticache::ServerlessCache,
) -> types::ElastiCacheServerlessCacheIntrospection {
    types::ElastiCacheServerlessCacheIntrospection {
        serverless_cache_name: cache.serverless_cache_name.clone(),
        status: cache.status.clone(),
        engine: cache.engine.clone(),
        engine_version: cache.full_engine_version.clone(),
        cache_node_type: None,
    }
}

pub(crate) fn athena_named_query_response(
    q: &fakecloud_athena::NamedQuery,
) -> types::AthenaNamedQuery {
    types::AthenaNamedQuery {
        named_query_id: q.named_query_id.clone(),
        name: q.name.clone(),
        description: q.description.clone(),
        database: q.database.clone(),
        query_string: q.query_string.clone(),
        workgroup: q.work_group.clone(),
        last_used_at: q.last_used_at.map(|t| t.to_rfc3339()),
    }
}

pub(crate) fn cloudfront_distribution_response(
    dist: &fakecloud_cloudfront::StoredDistribution,
) -> types::CloudFrontDistribution {
    types::CloudFrontDistribution {
        id: dist.id.clone(),
        domain_name: dist.domain_name.clone(),
        enabled: dist.config.enabled,
        // Served on the main listener (routed by Host) exactly when the
        // distribution is enabled and the data plane isn't globally disabled.
        served: dist.config.enabled && fakecloud_cloudfront::dataplane::dataplane_enabled(),
    }
}

pub(crate) fn elbv2_load_balancer_response(
    lb: &fakecloud_elbv2::LoadBalancer,
) -> types::Elbv2LoadBalancer {
    types::Elbv2LoadBalancer {
        arn: lb.arn.clone(),
        name: lb.name.clone(),
        dns_name: lb.dns_name.clone(),
        scheme: lb.scheme.clone(),
        vpc_id: lb.vpc_id.clone(),
        state_code: lb.state_code.clone(),
        state_reason: lb.state_reason.clone(),
        lb_type: lb.lb_type.clone(),
        ip_address_type: lb.ip_address_type.clone(),
        availability_zones: lb
            .availability_zones
            .iter()
            .map(|az| types::Elbv2AvailabilityZone {
                zone_name: az.zone_name.clone(),
                subnet_id: az.subnet_id.clone(),
            })
            .collect(),
        security_groups: lb.security_groups.clone(),
        created_time: lb.created_time.to_rfc3339(),
        tags: lb
            .tags
            .iter()
            .map(|t| types::Elbv2Tag {
                key: t.key.clone(),
                value: t.value.clone(),
            })
            .collect(),
        bound_port: lb.bound_port,
    }
}

pub(crate) fn elbv2_target_group_response(
    tg: &fakecloud_elbv2::TargetGroup,
) -> types::Elbv2TargetGroup {
    types::Elbv2TargetGroup {
        arn: tg.arn.clone(),
        name: tg.name.clone(),
        protocol: tg.protocol.clone(),
        port: tg.port,
        vpc_id: tg.vpc_id.clone(),
        target_type: tg.target_type.clone(),
        load_balancer_arns: tg.load_balancer_arns.clone(),
        targets: tg
            .targets
            .iter()
            .map(|t| types::Elbv2Target {
                id: t.id.clone(),
                port: t.port,
                availability_zone: t.availability_zone.clone(),
                health_state: t.health.state.clone(),
                health_reason: t.health.reason.clone(),
                health_description: t.health.description.clone(),
            })
            .collect(),
        health_check_protocol: tg.health_check_protocol.clone(),
        health_check_port: tg.health_check_port.clone(),
        health_check_path: tg.health_check_path.clone(),
        healthy_threshold_count: tg.healthy_threshold_count,
        unhealthy_threshold_count: tg.unhealthy_threshold_count,
        created_time: tg.created_time.to_rfc3339(),
        tags: tg
            .tags
            .iter()
            .map(|t| types::Elbv2Tag {
                key: t.key.clone(),
                value: t.value.clone(),
            })
            .collect(),
    }
}

pub(crate) fn elbv2_listener_response(l: &fakecloud_elbv2::Listener) -> types::Elbv2Listener {
    let default = l.default_actions.first();
    types::Elbv2Listener {
        arn: l.arn.clone(),
        load_balancer_arn: l.load_balancer_arn.clone(),
        port: l.port,
        protocol: l.protocol.clone(),
        ssl_policy: l.ssl_policy.clone(),
        certificate_arns: l
            .certificates
            .iter()
            .map(|c| c.certificate_arn.clone())
            .collect(),
        default_action_type: default.map(|a| a.action_type.clone()),
        default_target_group_arn: default.and_then(|a| a.target_group_arn.clone()),
    }
}

pub(crate) fn elbv2_rule_response(r: &fakecloud_elbv2::Rule) -> types::Elbv2Rule {
    types::Elbv2Rule {
        arn: r.arn.clone(),
        listener_arn: r.listener_arn.clone(),
        priority: r.priority.clone(),
        is_default: r.is_default,
        condition_fields: r.conditions.iter().map(|c| c.field.clone()).collect(),
        action_type: r.actions.first().map(|a| a.action_type.clone()),
    }
}

/// Build the `GET /_fakecloud/organizations/accounts` snapshot.
///
/// Walks the org's account directory, attaches resolved parent OU,
/// tags, and the SCP ids directly attached to each account (no
/// inherited policies — callers can resolve those via
/// `DescribeEffectivePolicy`). When no organization has been created
/// the response is empty with `None` management/master ids.
pub(crate) fn organizations_accounts_snapshot(
    state: &fakecloud_organizations::SharedOrganizationsState,
) -> types::OrganizationsAccountsResponse {
    let guard = state.read();
    let Some(org) = guard.as_ref() else {
        return types::OrganizationsAccountsResponse {
            accounts: Vec::new(),
            management_account_id: None,
            master_account_id: None,
        };
    };

    let mut accounts: Vec<types::OrganizationsAccount> = org
        .accounts
        .values()
        .map(|a| {
            let mut tags: Vec<types::OrganizationsTag> = org
                .resource_tags
                .get(&a.id)
                .map(|m| {
                    m.iter()
                        .map(|(k, v)| types::OrganizationsTag {
                            key: k.clone(),
                            value: v.clone(),
                        })
                        .collect()
                })
                .unwrap_or_default();
            tags.sort_by(|x, y| x.key.cmp(&y.key));

            let mut scp_attached: Vec<String> = org
                .attachments
                .get(&a.id)
                .map(|set| {
                    set.iter()
                        .filter(|pid| {
                            org.policies
                                .get(*pid)
                                .map(|p| p.policy_type == fakecloud_organizations::POLICY_TYPE_SCP)
                                .unwrap_or(false)
                        })
                        .cloned()
                        .collect()
                })
                .unwrap_or_default();
            scp_attached.sort();

            types::OrganizationsAccount {
                id: a.id.clone(),
                arn: a.arn.clone(),
                email: a.email.clone(),
                name: a.name.clone(),
                status: a.status.clone(),
                joined_method: a.joined_method.clone(),
                joined_timestamp: a.joined_timestamp.to_rfc3339(),
                parent_ou_id: Some(a.parent_id.clone()),
                tags,
                scp_attached,
            }
        })
        .collect();
    accounts.sort_by(|x, y| x.id.cmp(&y.id));

    types::OrganizationsAccountsResponse {
        accounts,
        management_account_id: Some(org.management_account_id.clone()),
        master_account_id: Some(org.management_account_id.clone()),
    }
}

/// Build the backing-network view of one instance for
/// `GET /_fakecloud/ec2/instance-networks`. `summary` is the runtime's
/// isolation summary, or `None` when no container runtime is configured
/// (metadata-only).
pub(crate) fn ec2_instance_network_response(
    instance: &fakecloud_ec2::state::Instance,
    summary: Option<&fakecloud_ec2::runtime::NetworkIsolationSummary>,
) -> types::Ec2InstanceNetwork {
    let running = instance.state_name == "running";
    let backing_network = match summary {
        // Only a running, container-backed instance has a live backing network.
        Some(s) if running && instance.container_id.is_some() => {
            if s.backend == "kubernetes" {
                Some(fakecloud_ec2::runtime::netpolicy::policy_name(
                    &instance.instance_id,
                ))
            } else {
                instance
                    .subnet_id
                    .as_ref()
                    .map(|sid| fakecloud_ec2::runtime::subnet_network_name(sid))
            }
        }
        _ => None,
    };
    types::Ec2InstanceNetwork {
        instance_id: instance.instance_id.clone(),
        vpc_id: instance.vpc_id.clone(),
        subnet_id: instance.subnet_id.clone(),
        private_ip: instance.private_ip.clone(),
        backing_network,
        isolation_backend: summary.map(|s| s.backend).unwrap_or("none").to_string(),
        security_group_enforcement: summary
            .map(|s| s.sg_enforcement)
            .unwrap_or("disabled")
            .to_string(),
        enforcement_active: summary.map(|s| s.enforced).unwrap_or(false),
    }
}

pub(crate) fn ec2_instance_response(
    instance: &fakecloud_ec2::state::Instance,
) -> types::Ec2Instance {
    types::Ec2Instance {
        instance_id: instance.instance_id.clone(),
        image_id: instance.image_id.clone(),
        instance_type: instance.instance_type.clone(),
        state: instance.state_name.clone(),
        private_ip: instance.private_ip.clone(),
        public_ip: instance.public_ip.clone(),
        subnet_id: instance.subnet_id.clone(),
        vpc_id: instance.vpc_id.clone(),
        key_name: instance.key_name.clone(),
        security_group_ids: instance.security_group_ids.clone(),
        availability_zone: instance.az.clone(),
        launch_time: instance.launch_time.clone(),
        container_id: instance.container_id.clone(),
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use fakecloud_elasticache::{
        ElastiCacheState, ElastiCacheUser, ElastiCacheUserGroup, ReplicationGroup,
    };
    use fakecloud_rds::DbInstance;

    use super::{elasticache_acls_response, rds_instance_response};

    fn test_replication_group(id: &str, user_group_ids: Vec<String>) -> ReplicationGroup {
        ReplicationGroup {
            replication_group_id: id.to_string(),
            description: "test".to_string(),
            global_replication_group_id: None,
            global_replication_group_role: None,
            status: "available".to_string(),
            cache_node_type: "cache.t3.micro".to_string(),
            engine: "redis".to_string(),
            engine_version: "7.1".to_string(),
            num_cache_clusters: 1,
            automatic_failover_enabled: false,
            endpoint_address: "127.0.0.1".to_string(),
            endpoint_port: 6379,
            arn: format!("arn:aws:elasticache:us-east-1:123456789012:replicationgroup:{id}"),
            created_at: "2024-01-01T00:00:00Z".to_string(),
            container_id: "abc123".to_string(),
            host_port: 12345,
            member_clusters: vec![format!("{id}-001")],
            snapshot_retention_limit: 0,
            snapshot_window: "05:00-09:00".to_string(),
            transit_encryption_enabled: false,
            at_rest_encryption_enabled: false,
            cluster_enabled: false,
            kms_key_id: None,
            auth_token_enabled: false,
            user_group_ids,
            multi_az_enabled: false,
            log_delivery_configurations: Vec::new(),
            data_tiering: None,
            ip_discovery: None,
            network_type: None,
            transit_encryption_mode: None,
            num_node_groups: 1,
            configuration_endpoint_address: None,
            configuration_endpoint_port: None,
            replicas_per_node_group: None,
            auth_token: None,
            port: 6379,
            notification_topic_arn: None,
            cluster_mode: None,
            data_tiering_enabled: None,
            notification_topic_status: None,
            cache_parameter_group_name: None,
            cache_subnet_group_name: None,
            security_group_ids: Vec::new(),
            preferred_maintenance_window: None,
            snapshot_name: None,
            snapshot_arns: Vec::new(),
            auto_minor_version_upgrade: true,
        }
    }

    #[test]
    fn elasticache_acls_aggregates_users_groups_per_cluster() {
        let mut state = ElastiCacheState::new("123456789012", "us-east-1");

        // Replication group with an attached user group; should appear in ACLs.
        state.replication_groups.insert(
            "rg-acl".to_string(),
            test_replication_group("rg-acl", vec!["ug-prod".to_string()]),
        );
        // Replication group with no user groups; should be filtered out.
        state.replication_groups.insert(
            "rg-noacl".to_string(),
            test_replication_group("rg-noacl", Vec::new()),
        );

        state.user_groups.insert(
            "ug-prod".to_string(),
            ElastiCacheUserGroup {
                user_group_id: "ug-prod".to_string(),
                engine: "redis".to_string(),
                status: "active".to_string(),
                user_ids: vec!["default".to_string(), "appuser".to_string()],
                arn: "arn:aws:elasticache:us-east-1:123456789012:usergroup:ug-prod".to_string(),
                minimum_engine_version: "6.0".to_string(),
                pending_changes: None,
                replication_groups: vec!["rg-acl".to_string()],
            },
        );
        state.users.insert(
            "appuser".to_string(),
            ElastiCacheUser {
                user_id: "appuser".to_string(),
                user_name: "appuser".to_string(),
                engine: "redis".to_string(),
                access_string: "on ~app:* +get +set".to_string(),
                status: "active".to_string(),
                authentication_type: "password".to_string(),
                password_count: 1,
                arn: "arn:aws:elasticache:us-east-1:123456789012:user:appuser".to_string(),
                minimum_engine_version: "6.0".to_string(),
                user_group_ids: vec!["ug-prod".to_string()],
            },
        );

        let resp = elasticache_acls_response(&state);

        assert_eq!(resp.acls.len(), 1, "only clusters with ACLs included");
        let cluster = &resp.acls[0];
        assert_eq!(cluster.cluster_id, "rg-acl");
        assert_eq!(cluster.engine, "redis");
        assert_eq!(cluster.groups.len(), 1);
        assert_eq!(cluster.groups[0].name, "ug-prod");
        assert_eq!(cluster.groups[0].members, vec!["default", "appuser"]);

        assert_eq!(cluster.users.len(), 2);
        let names: Vec<&str> = cluster.users.iter().map(|u| u.name.as_str()).collect();
        assert!(names.contains(&"default"));
        assert!(names.contains(&"appuser"));

        let default_user = cluster.users.iter().find(|u| u.name == "default").unwrap();
        assert!(default_user.no_password_required);
        assert_eq!(default_user.password_count, 0);
        assert_eq!(default_user.access_string, "on ~* +@all");

        let app_user = cluster.users.iter().find(|u| u.name == "appuser").unwrap();
        assert!(!app_user.no_password_required);
        assert_eq!(app_user.password_count, 1);
    }

    #[test]
    fn rds_instance_response_omits_password_but_keeps_runtime_metadata() {
        let created_at = Utc::now();
        let instance = DbInstance {
            db_instance_identifier: "db-1".to_string(),
            db_instance_arn: "arn:aws:rds:us-east-1:123456789012:db:db-1".to_string(),
            db_instance_class: "db.t3.micro".to_string(),
            engine: "postgres".to_string(),
            engine_version: "16.3".to_string(),
            db_instance_status: "available".to_string(),
            master_username: "admin".to_string(),
            db_name: Some("appdb".to_string()),
            db_subnet_group_name: None,
            endpoint_address: "127.0.0.1".to_string(),
            port: 15432,
            allocated_storage: 20,
            publicly_accessible: true,
            deletion_protection: false,
            created_at,
            dbi_resource_id: "db-test".to_string(),
            master_user_password: "secret123".to_string(),
            container_id: "container-id".to_string(),
            host_port: 15432,
            tags: vec![fakecloud_rds::RdsTag {
                key: "env".to_string(),
                value: "test".to_string(),
            }],
            read_replica_source_db_instance_identifier: None,
            read_replica_db_instance_identifiers: Vec::new(),
            vpc_security_group_ids: Vec::new(),
            db_parameter_group_name: None,
            backup_retention_period: 1,
            preferred_backup_window: "03:00-04:00".to_string(),
            preferred_maintenance_window: None,
            latest_restorable_time: Some(created_at),
            option_group_name: None,
            multi_az: false,
            pending_modified_values: None,
            availability_zone: None,
            storage_type: None,
            storage_encrypted: false,
            kms_key_id: None,
            iam_database_authentication_enabled: false,
            iops: None,
            monitoring_interval: None,
            monitoring_role_arn: None,
            performance_insights_enabled: false,
            performance_insights_kms_key_id: None,
            performance_insights_retention_period: None,
            enabled_cloudwatch_logs_exports: Vec::new(),
            ca_certificate_identifier: None,
            network_type: None,
            character_set_name: None,
            auto_minor_version_upgrade: None,
            copy_tags_to_snapshot: None,
            master_user_secret_arn: None,
            master_user_secret_kms_key_id: None,
            license_model: None,
            max_allocated_storage: None,
            multi_tenant: None,
            storage_throughput: None,
            tde_credential_arn: None,
            delete_automated_backups: None,
            db_security_groups: Vec::new(),
            domain: None,
            domain_fqdn: None,
            domain_ou: None,
            domain_iam_role_name: None,
            domain_auth_secret_arn: None,
            domain_dns_ips: Vec::new(),
            db_cluster_identifier: None,
            activity_stream: None,
        };

        let response = rds_instance_response(&instance);

        assert_eq!(response.db_instance_identifier, "db-1");
        assert_eq!(response.container_id, "container-id");
        assert_eq!(response.host_port, 15432);
        assert_eq!(response.tags.len(), 1);
    }

    #[test]
    fn organizations_accounts_snapshot_empty_when_no_org() {
        use std::sync::Arc;
        let state: fakecloud_organizations::SharedOrganizationsState =
            Arc::new(parking_lot::RwLock::new(None));
        let snap = super::organizations_accounts_snapshot(&state);
        assert!(snap.accounts.is_empty());
        assert!(snap.management_account_id.is_none());
        assert!(snap.master_account_id.is_none());
    }

    #[test]
    fn organizations_accounts_snapshot_returns_management_and_member_state() {
        use std::sync::Arc;
        let mut org = fakecloud_organizations::OrganizationState::bootstrap("111111111111");
        // Add a member account under the root.
        let parent_id = org.root_id.clone();
        org.accounts.insert(
            "222222222222".to_string(),
            fakecloud_organizations::MemberAccount {
                id: "222222222222".to_string(),
                arn: format!(
                    "arn:aws:organizations::111111111111:account/{}/222222222222",
                    org.org_id
                ),
                email: "member@example.com".to_string(),
                name: "Member".to_string(),
                status: "ACTIVE".to_string(),
                joined_method: "CREATED".to_string(),
                joined_timestamp: Utc::now(),
                parent_id: parent_id.clone(),
            },
        );

        // Tag the member account and attach a custom SCP directly.
        org.set_resource_tags("222222222222", &[("env".to_string(), "prod".to_string())]);
        let policy = org
            .create_policy(
                "DenyDelete",
                "Deny S3 delete",
                r#"{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Action":"s3:DeleteBucket","Resource":"*"}]}"#,
                "SERVICE_CONTROL_POLICY",
            )
            .expect("create policy");
        let policy_id = policy.id.clone();
        org.attach_policy(&policy_id, "222222222222")
            .expect("attach to member");

        let state: fakecloud_organizations::SharedOrganizationsState =
            Arc::new(parking_lot::RwLock::new(Some(org)));
        let snap = super::organizations_accounts_snapshot(&state);

        assert_eq!(snap.management_account_id.as_deref(), Some("111111111111"));
        assert_eq!(snap.master_account_id.as_deref(), Some("111111111111"));
        assert_eq!(snap.accounts.len(), 2);

        // Sorted by id, so management (1...) is first.
        let mgmt = &snap.accounts[0];
        assert_eq!(mgmt.id, "111111111111");
        assert_eq!(mgmt.status, "ACTIVE");
        assert_eq!(mgmt.parent_ou_id.as_deref(), Some(parent_id.as_str()));
        assert!(mgmt.tags.is_empty());
        assert!(mgmt.scp_attached.is_empty());

        let member = &snap.accounts[1];
        assert_eq!(member.id, "222222222222");
        assert_eq!(member.joined_method, "CREATED");
        assert_eq!(member.tags.len(), 1);
        assert_eq!(member.tags[0].key, "env");
        assert_eq!(member.tags[0].value, "prod");
        assert_eq!(member.scp_attached, vec![policy_id]);
    }
}
