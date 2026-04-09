use std::sync::Arc;

use async_trait::async_trait;
use http::StatusCode;

use fakecloud_aws::xml::xml_escape;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};

use crate::runtime::{ElastiCacheRuntime, RuntimeError};
use crate::state::{
    default_engine_versions, default_parameters_for_family, CacheEngineVersion,
    CacheParameterGroup, CacheSubnetGroup, ElastiCacheUser, ElastiCacheUserGroup,
    EngineDefaultParameter, ReplicationGroup, SharedElastiCacheState,
};

const ELASTICACHE_NS: &str = "http://elasticache.amazonaws.com/doc/2015-02-02/";
const SUPPORTED_ACTIONS: &[&str] = &[
    "AddTagsToResource",
    "CreateCacheSubnetGroup",
    "CreateReplicationGroup",
    "CreateUser",
    "CreateUserGroup",
    "DecreaseReplicaCount",
    "DeleteCacheSubnetGroup",
    "DeleteReplicationGroup",
    "DeleteUser",
    "DeleteUserGroup",
    "DescribeCacheEngineVersions",
    "DescribeCacheParameterGroups",
    "DescribeCacheSubnetGroups",
    "DescribeEngineDefaultParameters",
    "DescribeReplicationGroups",
    "DescribeUserGroups",
    "DescribeUsers",
    "IncreaseReplicaCount",
    "ListTagsForResource",
    "ModifyCacheSubnetGroup",
    "ModifyReplicationGroup",
    "RemoveTagsFromResource",
    "TestFailover",
];

pub struct ElastiCacheService {
    state: SharedElastiCacheState,
    runtime: Option<Arc<ElastiCacheRuntime>>,
}

impl ElastiCacheService {
    pub fn new(state: SharedElastiCacheState) -> Self {
        Self {
            state,
            runtime: None,
        }
    }

    pub fn with_runtime(mut self, runtime: Arc<ElastiCacheRuntime>) -> Self {
        self.runtime = Some(runtime);
        self
    }
}

#[async_trait]
impl AwsService for ElastiCacheService {
    fn service_name(&self) -> &str {
        "elasticache"
    }

    async fn handle(&self, request: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        match request.action.as_str() {
            "AddTagsToResource" => self.add_tags_to_resource(&request),
            "CreateCacheSubnetGroup" => self.create_cache_subnet_group(&request),
            "CreateReplicationGroup" => self.create_replication_group(&request).await,
            "CreateUser" => self.create_user(&request),
            "CreateUserGroup" => self.create_user_group(&request),
            "DecreaseReplicaCount" => self.decrease_replica_count(&request),
            "DeleteCacheSubnetGroup" => self.delete_cache_subnet_group(&request),
            "DeleteReplicationGroup" => self.delete_replication_group(&request).await,
            "DeleteUser" => self.delete_user(&request),
            "DeleteUserGroup" => self.delete_user_group(&request),
            "DescribeCacheEngineVersions" => self.describe_cache_engine_versions(&request),
            "DescribeCacheParameterGroups" => self.describe_cache_parameter_groups(&request),
            "DescribeCacheSubnetGroups" => self.describe_cache_subnet_groups(&request),
            "DescribeEngineDefaultParameters" => self.describe_engine_default_parameters(&request),
            "DescribeReplicationGroups" => self.describe_replication_groups(&request),
            "DescribeUserGroups" => self.describe_user_groups(&request),
            "DescribeUsers" => self.describe_users(&request),
            "IncreaseReplicaCount" => self.increase_replica_count(&request),
            "ListTagsForResource" => self.list_tags_for_resource(&request),
            "ModifyCacheSubnetGroup" => self.modify_cache_subnet_group(&request),
            "ModifyReplicationGroup" => self.modify_replication_group(&request),
            "RemoveTagsFromResource" => self.remove_tags_from_resource(&request),
            "TestFailover" => self.test_failover(&request),
            _ => Err(AwsServiceError::action_not_implemented(
                self.service_name(),
                &request.action,
            )),
        }
    }

    fn supported_actions(&self) -> &[&str] {
        SUPPORTED_ACTIONS
    }
}

impl ElastiCacheService {
    fn describe_cache_engine_versions(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let engine = optional_param(request, "Engine");
        let engine_version = optional_param(request, "EngineVersion");
        let family = optional_param(request, "CacheParameterGroupFamily");
        let default_only = parse_optional_bool(optional_param(request, "DefaultOnly").as_deref())?;
        let max_records = optional_usize_param(request, "MaxRecords")?;
        let marker = optional_param(request, "Marker");

        let mut versions = filter_engine_versions(
            &default_engine_versions(),
            &engine,
            &engine_version,
            &family,
        );

        if default_only.unwrap_or(false) {
            // Keep only one version per engine (the latest)
            let mut seen_engines = std::collections::HashSet::new();
            versions.retain(|v| seen_engines.insert(v.engine.clone()));
        }

        let (page, next_marker) = paginate(&versions, marker.as_deref(), max_records);

        let members_xml: String = page.iter().map(engine_version_xml).collect();
        let marker_xml = next_marker
            .map(|m| format!("<Marker>{}</Marker>", xml_escape(&m)))
            .unwrap_or_default();

        Ok(AwsResponse::xml(
            StatusCode::OK,
            xml_wrap(
                "DescribeCacheEngineVersions",
                &format!("<CacheEngineVersions>{members_xml}</CacheEngineVersions>{marker_xml}"),
                &request.request_id,
            ),
        ))
    }

    fn describe_cache_parameter_groups(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let group_name = optional_param(request, "CacheParameterGroupName");
        let max_records = optional_usize_param(request, "MaxRecords")?;
        let marker = optional_param(request, "Marker");

        let state = self.state.read();

        let groups: Vec<&CacheParameterGroup> = state
            .parameter_groups
            .iter()
            .filter(|g| {
                group_name
                    .as_ref()
                    .is_none_or(|name| g.cache_parameter_group_name == *name)
            })
            .collect();

        if let Some(ref name) = group_name {
            if groups.is_empty() {
                return Err(AwsServiceError::aws_error(
                    StatusCode::NOT_FOUND,
                    "CacheParameterGroupNotFound",
                    format!("CacheParameterGroup {name} not found."),
                ));
            }
        }

        let (page, next_marker) = paginate(&groups, marker.as_deref(), max_records);

        let members_xml: String = page.iter().map(|g| cache_parameter_group_xml(g)).collect();
        let marker_xml = next_marker
            .map(|m| format!("<Marker>{}</Marker>", xml_escape(&m)))
            .unwrap_or_default();

        Ok(AwsResponse::xml(
            StatusCode::OK,
            xml_wrap(
                "DescribeCacheParameterGroups",
                &format!("<CacheParameterGroups>{members_xml}</CacheParameterGroups>{marker_xml}"),
                &request.request_id,
            ),
        ))
    }

    fn describe_engine_default_parameters(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let family = required_param(request, "CacheParameterGroupFamily")?;
        let max_records = optional_usize_param(request, "MaxRecords")?;
        let marker = optional_param(request, "Marker");

        let params = default_parameters_for_family(&family);
        let (page, next_marker) = paginate(&params, marker.as_deref(), max_records);

        let params_xml: String = page.iter().map(parameter_xml).collect();
        let marker_xml = next_marker
            .map(|m| format!("<Marker>{}</Marker>", xml_escape(&m)))
            .unwrap_or_default();

        Ok(AwsResponse::xml(
            StatusCode::OK,
            xml_wrap(
                "DescribeEngineDefaultParameters",
                &format!(
                    "<EngineDefaults>\
                     <CacheParameterGroupFamily>{}</CacheParameterGroupFamily>\
                     <Parameters>{params_xml}</Parameters>\
                     {marker_xml}\
                     </EngineDefaults>",
                    xml_escape(&family),
                ),
                &request.request_id,
            ),
        ))
    }

    fn create_cache_subnet_group(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = required_param(request, "CacheSubnetGroupName")?;
        let description = required_param(request, "CacheSubnetGroupDescription")?;
        let subnet_ids = parse_member_list(&request.query_params, "SubnetIds", "SubnetIdentifier");

        if subnet_ids.is_empty() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValue",
                "At least one subnet ID must be specified.".to_string(),
            ));
        }

        let mut state = self.state.write();

        if state.subnet_groups.contains_key(&name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "CacheSubnetGroupAlreadyExists",
                format!("Cache subnet group {name} already exists."),
            ));
        }

        let arn = format!(
            "arn:aws:elasticache:{}:{}:subnetgroup:{}",
            state.region, state.account_id, name
        );
        let vpc_id = format!(
            "vpc-{:08x}",
            name.as_bytes()
                .iter()
                .fold(0u32, |acc, &b| acc.wrapping_add(b as u32))
        );

        let group = CacheSubnetGroup {
            cache_subnet_group_name: name.clone(),
            cache_subnet_group_description: description,
            vpc_id,
            subnet_ids,
            arn,
        };

        let xml = cache_subnet_group_xml(&group, &state.region);
        state.register_arn(&group.arn);
        state.subnet_groups.insert(name, group);

        Ok(AwsResponse::xml(
            StatusCode::OK,
            xml_wrap(
                "CreateCacheSubnetGroup",
                &format!("<CacheSubnetGroup>{xml}</CacheSubnetGroup>"),
                &request.request_id,
            ),
        ))
    }

    fn describe_cache_subnet_groups(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let group_name = optional_param(request, "CacheSubnetGroupName");
        let max_records = optional_usize_param(request, "MaxRecords")?;
        let marker = optional_param(request, "Marker");

        let state = self.state.read();

        let groups: Vec<&CacheSubnetGroup> = if let Some(ref name) = group_name {
            match state.subnet_groups.get(name) {
                Some(g) => vec![g],
                None => {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::NOT_FOUND,
                        "CacheSubnetGroupNotFoundFault",
                        format!("Cache subnet group {name} not found."),
                    ));
                }
            }
        } else {
            let mut groups: Vec<&CacheSubnetGroup> = state.subnet_groups.values().collect();
            groups.sort_by(|a, b| a.cache_subnet_group_name.cmp(&b.cache_subnet_group_name));
            groups
        };

        let (page, next_marker) = paginate(&groups, marker.as_deref(), max_records);

        let members_xml: String = page
            .iter()
            .map(|g| {
                format!(
                    "<CacheSubnetGroup>{}</CacheSubnetGroup>",
                    cache_subnet_group_xml(g, &state.region)
                )
            })
            .collect();
        let marker_xml = next_marker
            .map(|m| format!("<Marker>{}</Marker>", xml_escape(&m)))
            .unwrap_or_default();

        Ok(AwsResponse::xml(
            StatusCode::OK,
            xml_wrap(
                "DescribeCacheSubnetGroups",
                &format!("<CacheSubnetGroups>{members_xml}</CacheSubnetGroups>{marker_xml}"),
                &request.request_id,
            ),
        ))
    }

    fn delete_cache_subnet_group(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = required_param(request, "CacheSubnetGroupName")?;

        let mut state = self.state.write();

        if name == "default" {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "CacheSubnetGroupInUse",
                "Cannot delete default cache subnet group.".to_string(),
            ));
        }

        if let Some(group) = state.subnet_groups.remove(&name) {
            state.tags.remove(&group.arn);
        } else {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "CacheSubnetGroupNotFoundFault",
                format!("Cache subnet group {name} not found."),
            ));
        }

        Ok(AwsResponse::xml(
            StatusCode::OK,
            xml_wrap("DeleteCacheSubnetGroup", "", &request.request_id),
        ))
    }

    fn modify_cache_subnet_group(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = required_param(request, "CacheSubnetGroupName")?;
        let description = optional_param(request, "CacheSubnetGroupDescription");
        let subnet_ids = parse_member_list(&request.query_params, "SubnetIds", "SubnetIdentifier");

        let mut state = self.state.write();
        let region = state.region.clone();

        let group = state.subnet_groups.get_mut(&name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "CacheSubnetGroupNotFoundFault",
                format!("Cache subnet group {name} not found."),
            )
        })?;

        if let Some(desc) = description {
            group.cache_subnet_group_description = desc;
        }
        if !subnet_ids.is_empty() {
            group.subnet_ids = subnet_ids;
        }

        let xml = cache_subnet_group_xml(group, &region);

        Ok(AwsResponse::xml(
            StatusCode::OK,
            xml_wrap(
                "ModifyCacheSubnetGroup",
                &format!("<CacheSubnetGroup>{xml}</CacheSubnetGroup>"),
                &request.request_id,
            ),
        ))
    }

    async fn create_replication_group(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let replication_group_id = required_param(request, "ReplicationGroupId")?;
        let description = required_param(request, "ReplicationGroupDescription")?;
        let engine = optional_param(request, "Engine").unwrap_or_else(|| "redis".to_string());
        if engine != "redis" && engine != "valkey" {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValue",
                format!("Invalid value for Engine: {engine}. Supported engines: redis, valkey"),
            ));
        }
        let default_version = if engine == "valkey" { "8.0" } else { "7.1" };
        let engine_version =
            optional_param(request, "EngineVersion").unwrap_or_else(|| default_version.to_string());
        let cache_node_type = optional_param(request, "CacheNodeType")
            .unwrap_or_else(|| "cache.t3.micro".to_string());
        let num_cache_clusters = match optional_param(request, "NumCacheClusters") {
            Some(v) => {
                let n = v.parse::<i32>().map_err(|_| {
                    AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "InvalidParameterValue",
                        format!("Invalid value for NumCacheClusters: '{v}'"),
                    )
                })?;
                if n < 1 {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "InvalidParameterValue",
                        format!("NumCacheClusters must be a positive integer, got {n}"),
                    ));
                }
                n
            }
            None => 1,
        };
        let automatic_failover =
            parse_optional_bool(optional_param(request, "AutomaticFailoverEnabled").as_deref())?
                .unwrap_or(false);
        // Reserve the ID under a write lock before starting the container.
        {
            let mut state = self.state.write();
            if !state.begin_replication_group_creation(&replication_group_id) {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ReplicationGroupAlreadyExistsFault",
                    format!("ReplicationGroup {replication_group_id} already exists."),
                ));
            }
        }

        let runtime = self.runtime.as_ref().ok_or_else(|| {
            self.state
                .write()
                .cancel_replication_group_creation(&replication_group_id);
            AwsServiceError::aws_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "InvalidParameterValue",
                "Docker/Podman is required for ElastiCache replication groups but is not available"
                    .to_string(),
            )
        })?;

        let running = match runtime.ensure_redis(&replication_group_id).await {
            Ok(r) => r,
            Err(e) => {
                self.state
                    .write()
                    .cancel_replication_group_creation(&replication_group_id);
                return Err(runtime_error_to_service_error(e));
            }
        };

        let member_clusters: Vec<String> = (1..=num_cache_clusters)
            .map(|i| format!("{replication_group_id}-{i:03}"))
            .collect();

        let (arn, region) = {
            let state = self.state.read();
            let arn = format!(
                "arn:aws:elasticache:{}:{}:replicationgroup:{}",
                state.region, state.account_id, replication_group_id
            );
            (arn, state.region.clone())
        };

        let group = ReplicationGroup {
            replication_group_id: replication_group_id.clone(),
            description,
            status: "available".to_string(),
            cache_node_type,
            engine,
            engine_version,
            num_cache_clusters,
            automatic_failover_enabled: automatic_failover,
            endpoint_address: "127.0.0.1".to_string(),
            endpoint_port: running.host_port,
            arn,
            created_at: chrono::Utc::now().to_rfc3339(),
            container_id: running.container_id,
            host_port: running.host_port,
            member_clusters,
            snapshot_retention_limit: 0,
            snapshot_window: "05:00-09:00".to_string(),
        };

        let xml = replication_group_xml(&group, &region);
        self.state.write().finish_replication_group_creation(group);

        Ok(AwsResponse::xml(
            StatusCode::OK,
            xml_wrap(
                "CreateReplicationGroup",
                &format!("<ReplicationGroup>{xml}</ReplicationGroup>"),
                &request.request_id,
            ),
        ))
    }

    fn describe_replication_groups(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let group_id = optional_param(request, "ReplicationGroupId");
        let max_records = optional_usize_param(request, "MaxRecords")?;
        let marker = optional_param(request, "Marker");

        let state = self.state.read();
        let region = state.region.clone();

        let groups: Vec<&ReplicationGroup> = if let Some(ref id) = group_id {
            match state.replication_groups.get(id) {
                Some(g) => vec![g],
                None => {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::NOT_FOUND,
                        "ReplicationGroupNotFoundFault",
                        format!("ReplicationGroup {id} not found."),
                    ));
                }
            }
        } else {
            let mut groups: Vec<&ReplicationGroup> = state.replication_groups.values().collect();
            groups.sort_by(|a, b| a.replication_group_id.cmp(&b.replication_group_id));
            groups
        };

        let (page, next_marker) = paginate(&groups, marker.as_deref(), max_records);

        let members_xml: String = page
            .iter()
            .map(|g| {
                format!(
                    "<ReplicationGroup>{}</ReplicationGroup>",
                    replication_group_xml(g, &region)
                )
            })
            .collect();
        let marker_xml = next_marker
            .map(|m| format!("<Marker>{}</Marker>", xml_escape(&m)))
            .unwrap_or_default();

        Ok(AwsResponse::xml(
            StatusCode::OK,
            xml_wrap(
                "DescribeReplicationGroups",
                &format!("<ReplicationGroups>{members_xml}</ReplicationGroups>{marker_xml}"),
                &request.request_id,
            ),
        ))
    }

    async fn delete_replication_group(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let replication_group_id = required_param(request, "ReplicationGroupId")?;

        let group = {
            let mut state = self.state.write();
            let g = state
                .replication_groups
                .remove(&replication_group_id)
                .ok_or_else(|| {
                    AwsServiceError::aws_error(
                        StatusCode::NOT_FOUND,
                        "ReplicationGroupNotFoundFault",
                        format!("ReplicationGroup {replication_group_id} not found."),
                    )
                })?;
            state.tags.remove(&g.arn);
            g
        };

        if let Some(ref runtime) = self.runtime {
            runtime.stop_container(&replication_group_id).await;
        }

        let region = self.state.read().region.clone();
        let mut deleted_group = group;
        deleted_group.status = "deleting".to_string();
        let xml = replication_group_xml(&deleted_group, &region);

        Ok(AwsResponse::xml(
            StatusCode::OK,
            xml_wrap(
                "DeleteReplicationGroup",
                &format!("<ReplicationGroup>{xml}</ReplicationGroup>"),
                &request.request_id,
            ),
        ))
    }

    fn modify_replication_group(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let replication_group_id = required_param(request, "ReplicationGroupId")?;

        let new_description = optional_param(request, "ReplicationGroupDescription");
        let new_cache_node_type = optional_param(request, "CacheNodeType");
        let new_engine_version = optional_param(request, "EngineVersion");
        let new_automatic_failover =
            parse_optional_bool(optional_param(request, "AutomaticFailoverEnabled").as_deref())?;
        let new_snapshot_retention_limit = optional_param(request, "SnapshotRetentionLimit")
            .map(|v| {
                v.parse::<i32>().map_err(|_| {
                    AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "InvalidParameterValue",
                        format!("Invalid value for SnapshotRetentionLimit: '{v}'"),
                    )
                })
            })
            .transpose()?;
        let new_snapshot_window = optional_param(request, "SnapshotWindow");
        let user_group_ids_to_add =
            parse_member_list(&request.query_params, "UserGroupIdsToAdd", "member");
        let user_group_ids_to_remove =
            parse_member_list(&request.query_params, "UserGroupIdsToRemove", "member");

        let mut state = self.state.write();

        let group = state
            .replication_groups
            .get_mut(&replication_group_id)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::NOT_FOUND,
                    "ReplicationGroupNotFoundFault",
                    format!("ReplicationGroup {replication_group_id} not found."),
                )
            })?;

        if let Some(desc) = new_description {
            group.description = desc;
        }
        if let Some(node_type) = new_cache_node_type {
            group.cache_node_type = node_type;
        }
        if let Some(version) = new_engine_version {
            group.engine_version = version;
        }
        if let Some(af) = new_automatic_failover {
            group.automatic_failover_enabled = af;
        }
        if let Some(limit) = new_snapshot_retention_limit {
            group.snapshot_retention_limit = limit;
        }
        if let Some(window) = new_snapshot_window {
            group.snapshot_window = window;
        }

        // Associate/disassociate user groups
        for ug_id in &user_group_ids_to_add {
            if let Some(ug) = state.user_groups.get_mut(ug_id) {
                if !ug.replication_groups.contains(&replication_group_id) {
                    ug.replication_groups.push(replication_group_id.clone());
                }
            }
        }
        for ug_id in &user_group_ids_to_remove {
            if let Some(ug) = state.user_groups.get_mut(ug_id) {
                ug.replication_groups
                    .retain(|id| id != &replication_group_id);
            }
        }

        let group = state.replication_groups[&replication_group_id].clone();
        let region = state.region.clone();
        let xml = replication_group_xml(&group, &region);

        Ok(AwsResponse::xml(
            StatusCode::OK,
            xml_wrap(
                "ModifyReplicationGroup",
                &format!("<ReplicationGroup>{xml}</ReplicationGroup>"),
                &request.request_id,
            ),
        ))
    }

    fn increase_replica_count(&self, request: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let replication_group_id = required_param(request, "ReplicationGroupId")?;
        let apply_str = required_param(request, "ApplyImmediately")?;
        let _apply_immediately = parse_optional_bool(Some(&apply_str))?.ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValue",
                format!(
                    "Invalid boolean value for ApplyImmediately: '{}'",
                    apply_str
                ),
            )
        })?;

        let new_replica_count = optional_param(request, "NewReplicaCount")
            .map(|v| {
                v.parse::<i32>().map_err(|_| {
                    AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "InvalidParameterValue",
                        format!("Invalid value for NewReplicaCount: '{v}'"),
                    )
                })
            })
            .transpose()?
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "MissingParameter",
                    "The request must contain the parameter NewReplicaCount.".to_string(),
                )
            })?;

        if new_replica_count < 1 {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValue",
                format!("NewReplicaCount must be a positive integer, got {new_replica_count}"),
            ));
        }

        let mut state = self.state.write();

        let group = state
            .replication_groups
            .get_mut(&replication_group_id)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::NOT_FOUND,
                    "ReplicationGroupNotFoundFault",
                    format!("ReplicationGroup {replication_group_id} not found."),
                )
            })?;

        // new_replica_count is number of replicas (excluding primary), so total clusters = replicas + 1
        let new_total = new_replica_count + 1;
        if new_total <= group.num_cache_clusters {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValue",
                format!(
                    "NewReplicaCount ({new_replica_count}) must result in more clusters than current count ({}).",
                    group.num_cache_clusters
                ),
            ));
        }

        group.num_cache_clusters = new_total;
        group.member_clusters = (1..=new_total)
            .map(|i| format!("{replication_group_id}-{i:03}"))
            .collect();

        let group = group.clone();
        let region = state.region.clone();
        let xml = replication_group_xml(&group, &region);

        Ok(AwsResponse::xml(
            StatusCode::OK,
            xml_wrap(
                "IncreaseReplicaCount",
                &format!("<ReplicationGroup>{xml}</ReplicationGroup>"),
                &request.request_id,
            ),
        ))
    }

    fn decrease_replica_count(&self, request: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let replication_group_id = required_param(request, "ReplicationGroupId")?;
        let apply_str = required_param(request, "ApplyImmediately")?;
        let _apply_immediately = parse_optional_bool(Some(&apply_str))?.ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValue",
                format!(
                    "Invalid boolean value for ApplyImmediately: '{}'",
                    apply_str
                ),
            )
        })?;

        let new_replica_count = optional_param(request, "NewReplicaCount")
            .map(|v| {
                v.parse::<i32>().map_err(|_| {
                    AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "InvalidParameterValue",
                        format!("Invalid value for NewReplicaCount: '{v}'"),
                    )
                })
            })
            .transpose()?
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "MissingParameter",
                    "The request must contain the parameter NewReplicaCount.".to_string(),
                )
            })?;

        if new_replica_count < 0 {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValue",
                format!("NewReplicaCount must be non-negative, got {new_replica_count}"),
            ));
        }

        let mut state = self.state.write();

        let group = state
            .replication_groups
            .get_mut(&replication_group_id)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::NOT_FOUND,
                    "ReplicationGroupNotFoundFault",
                    format!("ReplicationGroup {replication_group_id} not found."),
                )
            })?;

        // new_replica_count is number of replicas (excluding primary), so total clusters = replicas + 1
        let new_total = new_replica_count + 1;
        if new_total >= group.num_cache_clusters {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValue",
                format!(
                    "NewReplicaCount ({new_replica_count}) must result in fewer clusters than current count ({}).",
                    group.num_cache_clusters
                ),
            ));
        }

        group.num_cache_clusters = new_total;
        group.member_clusters = (1..=new_total)
            .map(|i| format!("{replication_group_id}-{i:03}"))
            .collect();

        let group = group.clone();
        let region = state.region.clone();
        let xml = replication_group_xml(&group, &region);

        Ok(AwsResponse::xml(
            StatusCode::OK,
            xml_wrap(
                "DecreaseReplicaCount",
                &format!("<ReplicationGroup>{xml}</ReplicationGroup>"),
                &request.request_id,
            ),
        ))
    }

    fn test_failover(&self, request: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let replication_group_id = required_param(request, "ReplicationGroupId")?;
        let node_group_id = required_param(request, "NodeGroupId")?;

        let state = self.state.read();

        let group = state
            .replication_groups
            .get(&replication_group_id)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::NOT_FOUND,
                    "ReplicationGroupNotFoundFault",
                    format!("ReplicationGroup {replication_group_id} not found."),
                )
            })?;

        // Our replication groups always have a single node group with ID "0001"
        if node_group_id != "0001" {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NodeGroupNotFoundFault",
                format!("NodeGroup {node_group_id} not found in ReplicationGroup {replication_group_id}."),
            ));
        }

        let region = state.region.clone();
        let xml = replication_group_xml(group, &region);

        Ok(AwsResponse::xml(
            StatusCode::OK,
            xml_wrap(
                "TestFailover",
                &format!("<ReplicationGroup>{xml}</ReplicationGroup>"),
                &request.request_id,
            ),
        ))
    }

    fn create_user(&self, request: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let user_id = required_param(request, "UserId")?;
        let user_name = required_param(request, "UserName")?;
        let engine = required_param(request, "Engine")?;
        let access_string = required_param(request, "AccessString")?;

        if engine != "redis" && engine != "valkey" {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValue",
                format!("Invalid value for Engine: {engine}. Supported engines: redis, valkey"),
            ));
        }

        let no_password_required =
            parse_optional_bool(optional_param(request, "NoPasswordRequired").as_deref())?
                .unwrap_or(false);
        let passwords = parse_member_list(&request.query_params, "Passwords", "member");
        let auth_mode_type = optional_param(request, "AuthenticationMode.Type");

        let (authentication_type, password_count) = if no_password_required {
            if !passwords.is_empty() {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterCombination",
                    "Passwords cannot be provided when NoPasswordRequired is true.".to_string(),
                ));
            }
            ("no-password".to_string(), 0)
        } else if let Some(ref mode) = auth_mode_type {
            let mode_passwords = parse_member_list(
                &request.query_params,
                "AuthenticationMode.Passwords",
                "member",
            );
            match mode.as_str() {
                "password" => {
                    if mode_passwords.is_empty() {
                        return Err(AwsServiceError::aws_error(
                            StatusCode::BAD_REQUEST,
                            "InvalidParameterValue",
                            "At least one password is required when AuthenticationMode.Type is password.".to_string(),
                        ));
                    }
                    ("password".to_string(), mode_passwords.len() as i32)
                }
                "no-password-required" | "iam" => {
                    if !mode_passwords.is_empty() {
                        return Err(AwsServiceError::aws_error(
                            StatusCode::BAD_REQUEST,
                            "InvalidParameterValue",
                            format!("Passwords cannot be provided when AuthenticationMode.Type is {mode}."),
                        ));
                    }
                    (mode.clone(), 0)
                }
                _ => {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "InvalidParameterValue",
                        format!("Invalid value for AuthenticationMode.Type: {mode}. Supported values: password, iam, no-password-required"),
                    ));
                }
            }
        } else if !passwords.is_empty() {
            ("password".to_string(), passwords.len() as i32)
        } else {
            ("no-password".to_string(), 0)
        };

        let minimum_engine_version = if engine == "valkey" {
            "8.0".to_string()
        } else {
            "6.0".to_string()
        };

        let mut state = self.state.write();

        if state.users.contains_key(&user_id) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "UserAlreadyExistsFault",
                format!("User {user_id} already exists."),
            ));
        }

        let arn = format!(
            "arn:aws:elasticache:{}:{}:user:{}",
            state.region, state.account_id, user_id
        );

        let user = ElastiCacheUser {
            user_id: user_id.clone(),
            user_name,
            engine,
            access_string,
            status: "active".to_string(),
            authentication_type,
            password_count,
            arn,
            minimum_engine_version,
            user_group_ids: Vec::new(),
        };

        let xml = user_xml(&user);
        state.register_arn(&user.arn);
        state.users.insert(user_id, user);

        Ok(AwsResponse::xml(
            StatusCode::OK,
            xml_wrap("CreateUser", &xml, &request.request_id),
        ))
    }

    fn describe_users(&self, request: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let user_id = optional_param(request, "UserId");
        let max_records = optional_usize_param(request, "MaxRecords")?;
        let marker = optional_param(request, "Marker");

        let state = self.state.read();

        let users: Vec<&ElastiCacheUser> = if let Some(ref id) = user_id {
            match state.users.get(id) {
                Some(u) => vec![u],
                None => {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::NOT_FOUND,
                        "UserNotFoundFault",
                        format!("User {id} not found."),
                    ));
                }
            }
        } else {
            let mut users: Vec<&ElastiCacheUser> = state.users.values().collect();
            users.sort_by(|a, b| a.user_id.cmp(&b.user_id));
            users
        };

        let (page, next_marker) = paginate(&users, marker.as_deref(), max_records);

        let members_xml: String = page
            .iter()
            .map(|u| format!("<member>{}</member>", user_xml(u)))
            .collect();
        let marker_xml = next_marker
            .map(|m| format!("<Marker>{}</Marker>", xml_escape(&m)))
            .unwrap_or_default();

        Ok(AwsResponse::xml(
            StatusCode::OK,
            xml_wrap(
                "DescribeUsers",
                &format!("<Users>{members_xml}</Users>{marker_xml}"),
                &request.request_id,
            ),
        ))
    }

    fn delete_user(&self, request: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let user_id = required_param(request, "UserId")?;

        if user_id == "default" {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValue",
                "Cannot delete the default user.".to_string(),
            ));
        }

        let mut state = self.state.write();

        let user = state.users.remove(&user_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "UserNotFoundFault",
                format!("User {user_id} not found."),
            )
        })?;

        state.tags.remove(&user.arn);

        // Remove user from any user groups
        for group in state.user_groups.values_mut() {
            group.user_ids.retain(|id| id != &user_id);
        }

        let mut deleted_user = user;
        deleted_user.status = "deleting".to_string();
        let xml = user_xml(&deleted_user);

        Ok(AwsResponse::xml(
            StatusCode::OK,
            xml_wrap("DeleteUser", &xml, &request.request_id),
        ))
    }

    fn create_user_group(&self, request: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let user_group_id = required_param(request, "UserGroupId")?;
        let engine = required_param(request, "Engine")?;

        if engine != "redis" && engine != "valkey" {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValue",
                format!("Invalid value for Engine: {engine}. Supported engines: redis, valkey"),
            ));
        }

        let user_ids = parse_member_list(&request.query_params, "UserIds", "member");

        let minimum_engine_version = if engine == "valkey" {
            "8.0".to_string()
        } else {
            "6.0".to_string()
        };

        let mut state = self.state.write();

        if state.user_groups.contains_key(&user_group_id) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "UserGroupAlreadyExistsFault",
                format!("User Group {user_group_id} already exists."),
            ));
        }

        // Validate all referenced users exist and have a matching engine
        for uid in &user_ids {
            match state.users.get(uid) {
                None => {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::NOT_FOUND,
                        "UserNotFoundFault",
                        format!("User {uid} not found."),
                    ));
                }
                Some(user) if user.engine != engine => {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "InvalidParameterValue",
                        format!(
                            "User {uid} has engine {} which does not match the user group engine {engine}.",
                            user.engine
                        ),
                    ));
                }
                _ => {}
            }
        }

        let arn = format!(
            "arn:aws:elasticache:{}:{}:usergroup:{}",
            state.region, state.account_id, user_group_id
        );

        let group = ElastiCacheUserGroup {
            user_group_id: user_group_id.clone(),
            engine,
            status: "active".to_string(),
            user_ids: user_ids.clone(),
            arn,
            minimum_engine_version,
            pending_changes: None,
            replication_groups: Vec::new(),
        };

        // Update user_group_ids on referenced users
        for uid in &user_ids {
            if let Some(user) = state.users.get_mut(uid) {
                user.user_group_ids.push(user_group_id.clone());
            }
        }

        let xml = user_group_xml(&group);
        state.register_arn(&group.arn);
        state.user_groups.insert(user_group_id, group);

        Ok(AwsResponse::xml(
            StatusCode::OK,
            xml_wrap("CreateUserGroup", &xml, &request.request_id),
        ))
    }

    fn describe_user_groups(&self, request: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let user_group_id = optional_param(request, "UserGroupId");
        let max_records = optional_usize_param(request, "MaxRecords")?;
        let marker = optional_param(request, "Marker");

        let state = self.state.read();

        let groups: Vec<&ElastiCacheUserGroup> = if let Some(ref id) = user_group_id {
            match state.user_groups.get(id) {
                Some(g) => vec![g],
                None => {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::NOT_FOUND,
                        "UserGroupNotFoundFault",
                        format!("User Group {id} not found."),
                    ));
                }
            }
        } else {
            let mut groups: Vec<&ElastiCacheUserGroup> = state.user_groups.values().collect();
            groups.sort_by(|a, b| a.user_group_id.cmp(&b.user_group_id));
            groups
        };

        let (page, next_marker) = paginate(&groups, marker.as_deref(), max_records);

        let members_xml: String = page
            .iter()
            .map(|g| format!("<member>{}</member>", user_group_xml(g)))
            .collect();
        let marker_xml = next_marker
            .map(|m| format!("<Marker>{}</Marker>", xml_escape(&m)))
            .unwrap_or_default();

        Ok(AwsResponse::xml(
            StatusCode::OK,
            xml_wrap(
                "DescribeUserGroups",
                &format!("<UserGroups>{members_xml}</UserGroups>{marker_xml}"),
                &request.request_id,
            ),
        ))
    }

    fn delete_user_group(&self, request: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let user_group_id = required_param(request, "UserGroupId")?;

        let mut state = self.state.write();

        let group = state.user_groups.remove(&user_group_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "UserGroupNotFoundFault",
                format!("User Group {user_group_id} not found."),
            )
        })?;

        state.tags.remove(&group.arn);

        // Remove this group from users' user_group_ids
        for uid in &group.user_ids {
            if let Some(user) = state.users.get_mut(uid) {
                user.user_group_ids.retain(|gid| gid != &user_group_id);
            }
        }

        let mut deleted_group = group;
        deleted_group.status = "deleting".to_string();
        let xml = user_group_xml(&deleted_group);

        Ok(AwsResponse::xml(
            StatusCode::OK,
            xml_wrap("DeleteUserGroup", &xml, &request.request_id),
        ))
    }

    fn add_tags_to_resource(&self, request: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let resource_name = required_param(request, "ResourceName")?;
        let tags = parse_tags(request)?;

        let mut state = self.state.write();
        let tag_list = state.tags.get_mut(&resource_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "CacheClusterNotFound",
                format!("The resource {resource_name} could not be found."),
            )
        })?;

        merge_tags(tag_list, &tags);

        let tag_xml: String = tag_list.iter().map(tag_xml).collect();

        Ok(AwsResponse::xml(
            StatusCode::OK,
            xml_wrap(
                "AddTagsToResource",
                &format!("<TagList>{tag_xml}</TagList>"),
                &request.request_id,
            ),
        ))
    }

    fn list_tags_for_resource(&self, request: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let resource_name = required_param(request, "ResourceName")?;

        let state = self.state.read();
        let tag_list = state.tags.get(&resource_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "CacheClusterNotFound",
                format!("The resource {resource_name} could not be found."),
            )
        })?;

        let tag_xml: String = tag_list.iter().map(tag_xml).collect();

        Ok(AwsResponse::xml(
            StatusCode::OK,
            xml_wrap(
                "ListTagsForResource",
                &format!("<TagList>{tag_xml}</TagList>"),
                &request.request_id,
            ),
        ))
    }

    fn remove_tags_from_resource(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let resource_name = required_param(request, "ResourceName")?;
        let tag_keys = parse_tag_keys(request)?;

        let mut state = self.state.write();
        let tag_list = state.tags.get_mut(&resource_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "CacheClusterNotFound",
                format!("The resource {resource_name} could not be found."),
            )
        })?;

        tag_list.retain(|(key, _)| !tag_keys.contains(key));

        Ok(AwsResponse::xml(
            StatusCode::OK,
            xml_wrap("RemoveTagsFromResource", "", &request.request_id),
        ))
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn optional_param(req: &AwsRequest, name: &str) -> Option<String> {
    req.query_params
        .get(name)
        .cloned()
        .filter(|value| !value.is_empty())
}

fn required_param(req: &AwsRequest, name: &str) -> Result<String, AwsServiceError> {
    optional_param(req, name).ok_or_else(|| {
        AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "MissingParameter",
            format!("The request must contain the parameter {name}."),
        )
    })
}

fn parse_optional_bool(value: Option<&str>) -> Result<Option<bool>, AwsServiceError> {
    value
        .map(|raw| match raw {
            "true" | "True" | "TRUE" => Ok(true),
            "false" | "False" | "FALSE" => Ok(false),
            _ => Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValue",
                format!("Boolean parameter value '{raw}' is invalid."),
            )),
        })
        .transpose()
}

/// Parse an AWS Query protocol member list.
///
/// AWS encodes lists in the query string as:
///   `{param}.{member_name}.1=val1&{param}.{member_name}.2=val2`
///
/// Returns the values sorted by index.
fn parse_member_list(
    params: &std::collections::HashMap<String, String>,
    param: &str,
    member_name: &str,
) -> Vec<String> {
    let prefix = format!("{param}.{member_name}.");
    let mut indexed: Vec<(usize, String)> = params
        .iter()
        .filter_map(|(k, v)| {
            k.strip_prefix(&prefix)
                .and_then(|idx| idx.parse::<usize>().ok())
                .map(|idx| (idx, v.clone()))
        })
        .collect();
    indexed.sort_by_key(|(idx, _)| *idx);
    indexed.into_iter().map(|(_, v)| v).collect()
}

fn optional_usize_param(req: &AwsRequest, name: &str) -> Result<Option<usize>, AwsServiceError> {
    optional_param(req, name)
        .map(|v| {
            v.parse::<usize>().map_err(|_| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterValue",
                    format!("Value '{v}' for parameter {name} is not a valid integer."),
                )
            })
        })
        .transpose()
}

/// Simple index-based pagination. Returns the current page and an optional next marker.
fn paginate<T: Clone>(
    items: &[T],
    marker: Option<&str>,
    max_records: Option<usize>,
) -> (Vec<T>, Option<String>) {
    let start = marker.and_then(|m| m.parse::<usize>().ok()).unwrap_or(0);
    let limit = max_records.unwrap_or(100).min(100);

    if start >= items.len() {
        return (Vec::new(), None);
    }

    let end = (start + limit).min(items.len());
    let page = items[start..end].to_vec();
    let next_marker = if end < items.len() {
        Some(end.to_string())
    } else {
        None
    };
    (page, next_marker)
}

// ---------------------------------------------------------------------------
// Tag helpers
// ---------------------------------------------------------------------------

fn parse_tags(req: &AwsRequest) -> Result<Vec<(String, String)>, AwsServiceError> {
    let mut tags = Vec::new();
    for index in 1.. {
        let key_name = format!("Tags.Tag.{index}.Key");
        let value_name = format!("Tags.Tag.{index}.Value");
        let key = optional_param(req, &key_name);
        let value = optional_param(req, &value_name);
        match (key, value) {
            (Some(k), Some(v)) => tags.push((k, v)),
            (None, None) => break,
            _ => {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterValue",
                    "Each tag must include both Key and Value.",
                ));
            }
        }
    }
    Ok(tags)
}

fn parse_tag_keys(req: &AwsRequest) -> Result<Vec<String>, AwsServiceError> {
    let mut keys = Vec::new();
    for index in 1.. {
        let key_name = format!("TagKeys.member.{index}");
        match optional_param(req, &key_name) {
            Some(key) => keys.push(key),
            None => break,
        }
    }
    Ok(keys)
}

fn merge_tags(existing: &mut Vec<(String, String)>, incoming: &[(String, String)]) {
    for (key, value) in incoming {
        if let Some(existing_tag) = existing.iter_mut().find(|(k, _)| k == key) {
            existing_tag.1 = value.clone();
        } else {
            existing.push((key.clone(), value.clone()));
        }
    }
}

fn tag_xml(tag: &(String, String)) -> String {
    format!(
        "<Tag><Key>{}</Key><Value>{}</Value></Tag>",
        xml_escape(&tag.0),
        xml_escape(&tag.1),
    )
}

// ---------------------------------------------------------------------------
// Filtering
// ---------------------------------------------------------------------------

fn filter_engine_versions(
    versions: &[CacheEngineVersion],
    engine: &Option<String>,
    engine_version: &Option<String>,
    family: &Option<String>,
) -> Vec<CacheEngineVersion> {
    versions
        .iter()
        .filter(|v| engine.as_ref().is_none_or(|expected| v.engine == *expected))
        .filter(|v| {
            engine_version
                .as_ref()
                .is_none_or(|expected| v.engine_version == *expected)
        })
        .filter(|v| {
            family
                .as_ref()
                .is_none_or(|expected| v.cache_parameter_group_family == *expected)
        })
        .cloned()
        .collect()
}

// ---------------------------------------------------------------------------
// XML formatting
// ---------------------------------------------------------------------------

fn xml_wrap(action: &str, inner: &str, request_id: &str) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
         <{action}Response xmlns=\"{ELASTICACHE_NS}\">\
         <{action}Result>{inner}</{action}Result>\
         <ResponseMetadata><RequestId>{request_id}</RequestId></ResponseMetadata>\
         </{action}Response>"
    )
}

fn engine_version_xml(v: &CacheEngineVersion) -> String {
    format!(
        "<CacheEngineVersion>\
         <Engine>{}</Engine>\
         <EngineVersion>{}</EngineVersion>\
         <CacheParameterGroupFamily>{}</CacheParameterGroupFamily>\
         <CacheEngineDescription>{}</CacheEngineDescription>\
         <CacheEngineVersionDescription>{}</CacheEngineVersionDescription>\
         </CacheEngineVersion>",
        xml_escape(&v.engine),
        xml_escape(&v.engine_version),
        xml_escape(&v.cache_parameter_group_family),
        xml_escape(&v.cache_engine_description),
        xml_escape(&v.cache_engine_version_description),
    )
}

fn cache_parameter_group_xml(g: &CacheParameterGroup) -> String {
    format!(
        "<CacheParameterGroup>\
         <CacheParameterGroupName>{}</CacheParameterGroupName>\
         <CacheParameterGroupFamily>{}</CacheParameterGroupFamily>\
         <Description>{}</Description>\
         <IsGlobal>{}</IsGlobal>\
         <ARN>{}</ARN>\
         </CacheParameterGroup>",
        xml_escape(&g.cache_parameter_group_name),
        xml_escape(&g.cache_parameter_group_family),
        xml_escape(&g.description),
        g.is_global,
        xml_escape(&g.arn),
    )
}

fn cache_subnet_group_xml(g: &CacheSubnetGroup, region: &str) -> String {
    let subnets_xml: String = g
        .subnet_ids
        .iter()
        .enumerate()
        .map(|(i, id)| {
            let az = format!("{}{}", region, (b'a' + (i % 6) as u8) as char);
            format!(
                "<Subnet>\
                 <SubnetIdentifier>{}</SubnetIdentifier>\
                 <SubnetAvailabilityZone><Name>{}</Name></SubnetAvailabilityZone>\
                 </Subnet>",
                xml_escape(id),
                xml_escape(&az),
            )
        })
        .collect();
    format!(
        "<CacheSubnetGroupName>{}</CacheSubnetGroupName>\
         <CacheSubnetGroupDescription>{}</CacheSubnetGroupDescription>\
         <VpcId>{}</VpcId>\
         <Subnets>{subnets_xml}</Subnets>\
         <ARN>{}</ARN>",
        xml_escape(&g.cache_subnet_group_name),
        xml_escape(&g.cache_subnet_group_description),
        xml_escape(&g.vpc_id),
        xml_escape(&g.arn),
    )
}

fn replication_group_xml(g: &ReplicationGroup, region: &str) -> String {
    let member_clusters_xml: String = g
        .member_clusters
        .iter()
        .map(|c| format!("<ClusterId>{}</ClusterId>", xml_escape(c)))
        .collect();

    let primary_az = format!("{region}a");

    format!(
        "<ReplicationGroupId>{}</ReplicationGroupId>\
         <Description>{}</Description>\
         <Status>{}</Status>\
         <MemberClusters>{member_clusters_xml}</MemberClusters>\
         <NodeGroups>\
         <NodeGroup>\
         <NodeGroupId>0001</NodeGroupId>\
         <Status>available</Status>\
         <PrimaryEndpoint>\
         <Address>{}</Address>\
         <Port>{}</Port>\
         </PrimaryEndpoint>\
         <ReaderEndpoint>\
         <Address>{}</Address>\
         <Port>{}</Port>\
         </ReaderEndpoint>\
         <NodeGroupMembers>\
         <NodeGroupMember>\
         <CacheClusterId>{}</CacheClusterId>\
         <CacheNodeId>0001</CacheNodeId>\
         <PreferredAvailabilityZone>{}</PreferredAvailabilityZone>\
         <CurrentRole>primary</CurrentRole>\
         </NodeGroupMember>\
         </NodeGroupMembers>\
         </NodeGroup>\
         </NodeGroups>\
         <AutomaticFailover>{}</AutomaticFailover>\
         <SnapshotRetentionLimit>{}</SnapshotRetentionLimit>\
         <SnapshotWindow>{}</SnapshotWindow>\
         <ClusterEnabled>false</ClusterEnabled>\
         <CacheNodeType>{}</CacheNodeType>\
         <TransitEncryptionEnabled>false</TransitEncryptionEnabled>\
         <AtRestEncryptionEnabled>false</AtRestEncryptionEnabled>\
         <ARN>{}</ARN>",
        xml_escape(&g.replication_group_id),
        xml_escape(&g.description),
        xml_escape(&g.status),
        xml_escape(&g.endpoint_address),
        g.endpoint_port,
        xml_escape(&g.endpoint_address),
        g.endpoint_port,
        xml_escape(g.member_clusters.first().map(|s| s.as_str()).unwrap_or("")),
        xml_escape(&primary_az),
        if g.automatic_failover_enabled {
            "enabled"
        } else {
            "disabled"
        },
        g.snapshot_retention_limit,
        xml_escape(&g.snapshot_window),
        xml_escape(&g.cache_node_type),
        xml_escape(&g.arn),
    )
}

fn user_xml(u: &ElastiCacheUser) -> String {
    let user_group_ids_xml: String = u
        .user_group_ids
        .iter()
        .map(|id| format!("<member>{}</member>", xml_escape(id)))
        .collect();
    format!(
        "<UserId>{}</UserId>\
         <UserName>{}</UserName>\
         <Status>{}</Status>\
         <Engine>{}</Engine>\
         <MinimumEngineVersion>{}</MinimumEngineVersion>\
         <AccessString>{}</AccessString>\
         <UserGroupIds>{user_group_ids_xml}</UserGroupIds>\
         <Authentication>\
         <Type>{}</Type>\
         <PasswordCount>{}</PasswordCount>\
         </Authentication>\
         <ARN>{}</ARN>",
        xml_escape(&u.user_id),
        xml_escape(&u.user_name),
        xml_escape(&u.status),
        xml_escape(&u.engine),
        xml_escape(&u.minimum_engine_version),
        xml_escape(&u.access_string),
        xml_escape(&u.authentication_type),
        u.password_count,
        xml_escape(&u.arn),
    )
}

fn user_group_xml(g: &ElastiCacheUserGroup) -> String {
    let user_ids_xml: String = g
        .user_ids
        .iter()
        .map(|id| format!("<member>{}</member>", xml_escape(id)))
        .collect();
    let replication_groups_xml: String = g
        .replication_groups
        .iter()
        .map(|id| format!("<member>{}</member>", xml_escape(id)))
        .collect();
    let pending_xml = if let Some(ref pc) = g.pending_changes {
        let to_add: String = pc
            .user_ids_to_add
            .iter()
            .map(|id| format!("<member>{}</member>", xml_escape(id)))
            .collect();
        let to_remove: String = pc
            .user_ids_to_remove
            .iter()
            .map(|id| format!("<member>{}</member>", xml_escape(id)))
            .collect();
        format!(
            "<PendingChanges>\
             <UserIdsToAdd>{to_add}</UserIdsToAdd>\
             <UserIdsToRemove>{to_remove}</UserIdsToRemove>\
             </PendingChanges>"
        )
    } else {
        String::new()
    };
    format!(
        "<UserGroupId>{}</UserGroupId>\
         <Status>{}</Status>\
         <Engine>{}</Engine>\
         <MinimumEngineVersion>{}</MinimumEngineVersion>\
         <UserIds>{user_ids_xml}</UserIds>\
         <ReplicationGroups>{replication_groups_xml}</ReplicationGroups>\
         {pending_xml}\
         <ARN>{}</ARN>",
        xml_escape(&g.user_group_id),
        xml_escape(&g.status),
        xml_escape(&g.engine),
        xml_escape(&g.minimum_engine_version),
        xml_escape(&g.arn),
    )
}

fn runtime_error_to_service_error(error: RuntimeError) -> AwsServiceError {
    match error {
        RuntimeError::Unavailable => AwsServiceError::aws_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "InvalidParameterValue",
            "Docker/Podman is required for ElastiCache replication groups but is not available"
                .to_string(),
        ),
        RuntimeError::ContainerStartFailed(msg) => AwsServiceError::aws_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "InvalidParameterValue",
            format!("Failed to start Redis container: {msg}"),
        ),
    }
}

fn parameter_xml(p: &EngineDefaultParameter) -> String {
    format!(
        "<Parameter>\
         <ParameterName>{}</ParameterName>\
         <ParameterValue>{}</ParameterValue>\
         <Description>{}</Description>\
         <Source>{}</Source>\
         <DataType>{}</DataType>\
         <AllowedValues>{}</AllowedValues>\
         <IsModifiable>{}</IsModifiable>\
         <MinimumEngineVersion>{}</MinimumEngineVersion>\
         </Parameter>",
        xml_escape(&p.parameter_name),
        xml_escape(&p.parameter_value),
        xml_escape(&p.description),
        xml_escape(&p.source),
        xml_escape(&p.data_type),
        xml_escape(&p.allowed_values),
        p.is_modifiable,
        xml_escape(&p.minimum_engine_version),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::default_engine_versions;
    use bytes::Bytes;
    use http::HeaderMap;
    use std::collections::HashMap;

    fn request(action: &str, params: &[(&str, &str)]) -> AwsRequest {
        let mut query_params = HashMap::from([("Action".to_string(), action.to_string())]);
        for (key, value) in params {
            query_params.insert((*key).to_string(), (*value).to_string());
        }

        AwsRequest {
            service: "elasticache".to_string(),
            action: action.to_string(),
            region: "us-east-1".to_string(),
            account_id: "123456789012".to_string(),
            request_id: "test-request-id".to_string(),
            headers: HeaderMap::new(),
            query_params,
            body: Bytes::new(),
            path_segments: vec![],
            raw_path: "/".to_string(),
            raw_query: String::new(),
            method: http::Method::POST,
            is_query_protocol: true,
            access_key_id: None,
        }
    }

    #[test]
    fn parse_member_list_extracts_indexed_values() {
        let mut params = HashMap::new();
        params.insert(
            "SubnetIds.SubnetIdentifier.1".to_string(),
            "subnet-aaa".to_string(),
        );
        params.insert(
            "SubnetIds.SubnetIdentifier.2".to_string(),
            "subnet-bbb".to_string(),
        );
        params.insert(
            "SubnetIds.SubnetIdentifier.3".to_string(),
            "subnet-ccc".to_string(),
        );
        params.insert("OtherParam".to_string(), "ignored".to_string());

        let result = parse_member_list(&params, "SubnetIds", "SubnetIdentifier");
        assert_eq!(result, vec!["subnet-aaa", "subnet-bbb", "subnet-ccc"]);
    }

    #[test]
    fn parse_member_list_returns_sorted_by_index() {
        let mut params = HashMap::new();
        params.insert(
            "SubnetIds.SubnetIdentifier.3".to_string(),
            "subnet-ccc".to_string(),
        );
        params.insert(
            "SubnetIds.SubnetIdentifier.1".to_string(),
            "subnet-aaa".to_string(),
        );

        let result = parse_member_list(&params, "SubnetIds", "SubnetIdentifier");
        assert_eq!(result, vec!["subnet-aaa", "subnet-ccc"]);
    }

    #[test]
    fn parse_member_list_returns_empty_for_no_matches() {
        let params = HashMap::new();
        let result = parse_member_list(&params, "SubnetIds", "SubnetIdentifier");
        assert!(result.is_empty());
    }

    #[test]
    fn cache_subnet_group_xml_contains_all_fields() {
        let group = CacheSubnetGroup {
            cache_subnet_group_name: "my-group".to_string(),
            cache_subnet_group_description: "My description".to_string(),
            vpc_id: "vpc-123".to_string(),
            subnet_ids: vec!["subnet-aaa".to_string(), "subnet-bbb".to_string()],
            arn: "arn:aws:elasticache:us-east-1:123:subnetgroup:my-group".to_string(),
        };
        let xml = cache_subnet_group_xml(&group, "us-east-1");
        assert!(xml.contains("<CacheSubnetGroupName>my-group</CacheSubnetGroupName>"));
        assert!(xml
            .contains("<CacheSubnetGroupDescription>My description</CacheSubnetGroupDescription>"));
        assert!(xml.contains("<VpcId>vpc-123</VpcId>"));
        assert!(xml.contains("<SubnetIdentifier>subnet-aaa</SubnetIdentifier>"));
        assert!(xml.contains("<SubnetIdentifier>subnet-bbb</SubnetIdentifier>"));
        assert!(xml.contains("<Name>us-east-1a</Name>"));
        assert!(xml.contains("<Name>us-east-1b</Name>"));
        assert!(xml.contains("<ARN>arn:aws:elasticache:us-east-1:123:subnetgroup:my-group</ARN>"));
    }

    #[test]
    fn filter_engine_versions_by_engine() {
        let versions = default_engine_versions();
        let filtered = filter_engine_versions(&versions, &Some("redis".to_string()), &None, &None);
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].engine, "redis");
    }

    #[test]
    fn filter_engine_versions_by_family() {
        let versions = default_engine_versions();
        let filtered =
            filter_engine_versions(&versions, &None, &None, &Some("valkey8".to_string()));
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].engine, "valkey");
    }

    #[test]
    fn filter_engine_versions_no_match() {
        let versions = default_engine_versions();
        let filtered =
            filter_engine_versions(&versions, &Some("memcached".to_string()), &None, &None);
        assert!(filtered.is_empty());
    }

    #[test]
    fn paginate_returns_all_when_within_limit() {
        let items = vec![1, 2, 3];
        let (page, marker) = paginate(&items, None, None);
        assert_eq!(page, vec![1, 2, 3]);
        assert!(marker.is_none());
    }

    #[test]
    fn paginate_respects_max_records() {
        let items = vec![1, 2, 3, 4, 5];
        let (page, marker) = paginate(&items, None, Some(2));
        assert_eq!(page, vec![1, 2]);
        assert_eq!(marker, Some("2".to_string()));

        let (page2, marker2) = paginate(&items, Some("2"), Some(2));
        assert_eq!(page2, vec![3, 4]);
        assert_eq!(marker2, Some("4".to_string()));

        let (page3, marker3) = paginate(&items, Some("4"), Some(2));
        assert_eq!(page3, vec![5]);
        assert!(marker3.is_none());
    }

    #[test]
    fn xml_wrap_produces_valid_response() {
        let xml = xml_wrap("TestAction", "<Data>ok</Data>", "req-123");
        assert!(xml.contains("<TestActionResponse"));
        assert!(xml.contains("<TestActionResult>"));
        assert!(xml.contains("<RequestId>req-123</RequestId>"));
        assert!(xml.contains(ELASTICACHE_NS));
    }

    #[test]
    fn parse_tags_reads_query_shape() {
        let req = request(
            "AddTagsToResource",
            &[
                ("Tags.Tag.1.Key", "env"),
                ("Tags.Tag.1.Value", "prod"),
                ("Tags.Tag.2.Key", "team"),
                ("Tags.Tag.2.Value", "backend"),
            ],
        );

        let tags = parse_tags(&req).expect("tags");
        assert_eq!(
            tags,
            vec![
                ("env".to_string(), "prod".to_string()),
                ("team".to_string(), "backend".to_string()),
            ]
        );
    }

    #[test]
    fn parse_tags_returns_empty_for_no_tags() {
        let req = request("AddTagsToResource", &[]);
        let tags = parse_tags(&req).expect("tags");
        assert!(tags.is_empty());
    }

    #[test]
    fn parse_tag_keys_reads_member_shape() {
        let req = request(
            "RemoveTagsFromResource",
            &[("TagKeys.member.1", "env"), ("TagKeys.member.2", "team")],
        );

        let keys = parse_tag_keys(&req).expect("tag keys");
        assert_eq!(keys, vec!["env".to_string(), "team".to_string()]);
    }

    #[test]
    fn merge_tags_adds_new_and_updates_existing() {
        let mut tags = vec![("env".to_string(), "dev".to_string())];

        merge_tags(
            &mut tags,
            &[
                ("env".to_string(), "prod".to_string()),
                ("team".to_string(), "core".to_string()),
            ],
        );

        assert_eq!(
            tags,
            vec![
                ("env".to_string(), "prod".to_string()),
                ("team".to_string(), "core".to_string()),
            ]
        );
    }

    #[test]
    fn tag_xml_produces_valid_element() {
        let xml = tag_xml(&("env".to_string(), "prod".to_string()));
        assert_eq!(xml, "<Tag><Key>env</Key><Value>prod</Value></Tag>");
    }

    #[test]
    fn user_xml_contains_all_fields() {
        let user = ElastiCacheUser {
            user_id: "myuser".to_string(),
            user_name: "myuser".to_string(),
            engine: "redis".to_string(),
            access_string: "on ~* +@all".to_string(),
            status: "active".to_string(),
            authentication_type: "password".to_string(),
            password_count: 1,
            arn: "arn:aws:elasticache:us-east-1:123:user:myuser".to_string(),
            minimum_engine_version: "6.0".to_string(),
            user_group_ids: vec!["group1".to_string()],
        };
        let xml = user_xml(&user);
        assert!(xml.contains("<UserId>myuser</UserId>"));
        assert!(xml.contains("<UserName>myuser</UserName>"));
        assert!(xml.contains("<Engine>redis</Engine>"));
        assert!(xml.contains("<AccessString>on ~* +@all</AccessString>"));
        assert!(xml.contains("<Status>active</Status>"));
        assert!(xml.contains("<Type>password</Type>"));
        assert!(xml.contains("<PasswordCount>1</PasswordCount>"));
        assert!(xml.contains("<member>group1</member>"));
        assert!(xml.contains("<ARN>arn:aws:elasticache:us-east-1:123:user:myuser</ARN>"));
    }

    #[test]
    fn user_group_xml_contains_all_fields() {
        let group = ElastiCacheUserGroup {
            user_group_id: "mygroup".to_string(),
            engine: "redis".to_string(),
            status: "active".to_string(),
            user_ids: vec!["default".to_string(), "myuser".to_string()],
            arn: "arn:aws:elasticache:us-east-1:123:usergroup:mygroup".to_string(),
            minimum_engine_version: "6.0".to_string(),
            pending_changes: None,
            replication_groups: Vec::new(),
        };
        let xml = user_group_xml(&group);
        assert!(xml.contains("<UserGroupId>mygroup</UserGroupId>"));
        assert!(xml.contains("<Engine>redis</Engine>"));
        assert!(xml.contains("<Status>active</Status>"));
        assert!(xml.contains("<member>default</member>"));
        assert!(xml.contains("<member>myuser</member>"));
        assert!(xml.contains("<ARN>arn:aws:elasticache:us-east-1:123:usergroup:mygroup</ARN>"));
    }

    #[test]
    fn create_user_returns_user_xml() {
        let state = crate::state::ElastiCacheState::new("123456789012", "us-east-1");
        let shared = std::sync::Arc::new(parking_lot::RwLock::new(state));
        let service = ElastiCacheService::new(shared);

        let req = request(
            "CreateUser",
            &[
                ("UserId", "testuser"),
                ("UserName", "testuser"),
                ("Engine", "redis"),
                ("AccessString", "on ~* +@all"),
            ],
        );
        let resp = service.create_user(&req).unwrap();
        let body = String::from_utf8(resp.body.to_vec()).unwrap();
        assert!(body.contains("<UserId>testuser</UserId>"));
        assert!(body.contains("<Status>active</Status>"));
        assert!(body.contains("<CreateUserResponse"));
    }

    #[test]
    fn create_user_rejects_duplicate() {
        let state = crate::state::ElastiCacheState::new("123456789012", "us-east-1");
        let shared = std::sync::Arc::new(parking_lot::RwLock::new(state));
        let service = ElastiCacheService::new(shared);

        let req = request(
            "CreateUser",
            &[
                ("UserId", "default"),
                ("UserName", "default"),
                ("Engine", "redis"),
                ("AccessString", "on ~* +@all"),
            ],
        );
        assert!(service.create_user(&req).is_err());
    }

    #[test]
    fn delete_user_rejects_default() {
        let state = crate::state::ElastiCacheState::new("123456789012", "us-east-1");
        let shared = std::sync::Arc::new(parking_lot::RwLock::new(state));
        let service = ElastiCacheService::new(shared);

        let req = request("DeleteUser", &[("UserId", "default")]);
        assert!(service.delete_user(&req).is_err());
    }

    #[test]
    fn describe_users_returns_default_user() {
        let state = crate::state::ElastiCacheState::new("123456789012", "us-east-1");
        let shared = std::sync::Arc::new(parking_lot::RwLock::new(state));
        let service = ElastiCacheService::new(shared);

        let req = request("DescribeUsers", &[]);
        let resp = service.describe_users(&req).unwrap();
        let body = String::from_utf8(resp.body.to_vec()).unwrap();
        assert!(body.contains("<UserId>default</UserId>"));
    }

    #[test]
    fn create_and_describe_user_group() {
        let state = crate::state::ElastiCacheState::new("123456789012", "us-east-1");
        let shared = std::sync::Arc::new(parking_lot::RwLock::new(state));
        let service = ElastiCacheService::new(shared);

        let req = request(
            "CreateUserGroup",
            &[
                ("UserGroupId", "mygroup"),
                ("Engine", "redis"),
                ("UserIds.member.1", "default"),
            ],
        );
        let resp = service.create_user_group(&req).unwrap();
        let body = String::from_utf8(resp.body.to_vec()).unwrap();
        assert!(body.contains("<UserGroupId>mygroup</UserGroupId>"));
        assert!(body.contains("<member>default</member>"));

        let req = request("DescribeUserGroups", &[]);
        let resp = service.describe_user_groups(&req).unwrap();
        let body = String::from_utf8(resp.body.to_vec()).unwrap();
        assert!(body.contains("<UserGroupId>mygroup</UserGroupId>"));
    }

    #[test]
    fn create_user_group_rejects_unknown_user() {
        let state = crate::state::ElastiCacheState::new("123456789012", "us-east-1");
        let shared = std::sync::Arc::new(parking_lot::RwLock::new(state));
        let service = ElastiCacheService::new(shared);

        let req = request(
            "CreateUserGroup",
            &[
                ("UserGroupId", "mygroup"),
                ("Engine", "redis"),
                ("UserIds.member.1", "nonexistent"),
            ],
        );
        assert!(service.create_user_group(&req).is_err());
    }

    #[test]
    fn delete_user_group_removes_from_state() {
        let state = crate::state::ElastiCacheState::new("123456789012", "us-east-1");
        let shared = std::sync::Arc::new(parking_lot::RwLock::new(state));
        let service = ElastiCacheService::new(shared);

        let req = request(
            "CreateUserGroup",
            &[("UserGroupId", "delgroup"), ("Engine", "redis")],
        );
        service.create_user_group(&req).unwrap();

        let req = request("DeleteUserGroup", &[("UserGroupId", "delgroup")]);
        let resp = service.delete_user_group(&req).unwrap();
        let body = String::from_utf8(resp.body.to_vec()).unwrap();
        assert!(body.contains("<Status>deleting</Status>"));

        let req = request("DescribeUserGroups", &[("UserGroupId", "delgroup")]);
        assert!(service.describe_user_groups(&req).is_err());
    }

    fn service_with_replication_group(group_id: &str, num_clusters: i32) -> ElastiCacheService {
        let state = crate::state::ElastiCacheState::new("123456789012", "us-east-1");
        let shared = std::sync::Arc::new(parking_lot::RwLock::new(state));
        {
            let mut s = shared.write();
            let member_clusters: Vec<String> = (1..=num_clusters)
                .map(|i| format!("{group_id}-{i:03}"))
                .collect();
            let arn =
                format!("arn:aws:elasticache:us-east-1:123456789012:replicationgroup:{group_id}");
            s.tags.insert(arn.clone(), Vec::new());
            s.replication_groups.insert(
                group_id.to_string(),
                ReplicationGroup {
                    replication_group_id: group_id.to_string(),
                    description: "test group".to_string(),
                    status: "available".to_string(),
                    cache_node_type: "cache.t3.micro".to_string(),
                    engine: "redis".to_string(),
                    engine_version: "7.1".to_string(),
                    num_cache_clusters: num_clusters,
                    automatic_failover_enabled: false,
                    endpoint_address: "127.0.0.1".to_string(),
                    endpoint_port: 6379,
                    arn,
                    created_at: "2024-01-01T00:00:00Z".to_string(),
                    container_id: "abc123".to_string(),
                    host_port: 6379,
                    member_clusters,
                    snapshot_retention_limit: 0,
                    snapshot_window: "05:00-09:00".to_string(),
                },
            );
        }
        ElastiCacheService::new(shared)
    }

    #[test]
    fn modify_replication_group_updates_description() {
        let service = service_with_replication_group("my-rg", 1);
        let req = request(
            "ModifyReplicationGroup",
            &[
                ("ReplicationGroupId", "my-rg"),
                ("ReplicationGroupDescription", "Updated description"),
            ],
        );
        let resp = service.modify_replication_group(&req).unwrap();
        let body = String::from_utf8(resp.body.to_vec()).unwrap();
        assert!(body.contains("<Description>Updated description</Description>"));
        assert!(body.contains("<ModifyReplicationGroupResponse"));
    }

    #[test]
    fn modify_replication_group_updates_multiple_fields() {
        let service = service_with_replication_group("my-rg", 1);
        let req = request(
            "ModifyReplicationGroup",
            &[
                ("ReplicationGroupId", "my-rg"),
                ("CacheNodeType", "cache.m5.large"),
                ("AutomaticFailoverEnabled", "true"),
                ("SnapshotRetentionLimit", "5"),
                ("SnapshotWindow", "02:00-06:00"),
            ],
        );
        let resp = service.modify_replication_group(&req).unwrap();
        let body = String::from_utf8(resp.body.to_vec()).unwrap();
        assert!(body.contains("<CacheNodeType>cache.m5.large</CacheNodeType>"));
        assert!(body.contains("<AutomaticFailover>enabled</AutomaticFailover>"));
        assert!(body.contains("<SnapshotRetentionLimit>5</SnapshotRetentionLimit>"));
        assert!(body.contains("<SnapshotWindow>02:00-06:00</SnapshotWindow>"));
    }

    #[test]
    fn modify_replication_group_not_found() {
        let state = crate::state::ElastiCacheState::new("123456789012", "us-east-1");
        let shared = std::sync::Arc::new(parking_lot::RwLock::new(state));
        let service = ElastiCacheService::new(shared);
        let req = request(
            "ModifyReplicationGroup",
            &[("ReplicationGroupId", "nonexistent")],
        );
        assert!(service.modify_replication_group(&req).is_err());
    }

    #[test]
    fn increase_replica_count_updates_member_clusters() {
        let service = service_with_replication_group("my-rg", 1);
        let req = request(
            "IncreaseReplicaCount",
            &[
                ("ReplicationGroupId", "my-rg"),
                ("ApplyImmediately", "true"),
                ("NewReplicaCount", "2"),
            ],
        );
        let resp = service.increase_replica_count(&req).unwrap();
        let body = String::from_utf8(resp.body.to_vec()).unwrap();
        assert!(body.contains("<ClusterId>my-rg-001</ClusterId>"));
        assert!(body.contains("<ClusterId>my-rg-002</ClusterId>"));
        assert!(body.contains("<ClusterId>my-rg-003</ClusterId>"));
        assert!(body.contains("<IncreaseReplicaCountResponse"));
    }

    #[test]
    fn increase_replica_count_rejects_same_or_lower() {
        let service = service_with_replication_group("my-rg", 3);
        let req = request(
            "IncreaseReplicaCount",
            &[
                ("ReplicationGroupId", "my-rg"),
                ("ApplyImmediately", "true"),
                ("NewReplicaCount", "2"),
            ],
        );
        assert!(service.increase_replica_count(&req).is_err());
    }

    #[test]
    fn increase_replica_count_not_found() {
        let state = crate::state::ElastiCacheState::new("123456789012", "us-east-1");
        let shared = std::sync::Arc::new(parking_lot::RwLock::new(state));
        let service = ElastiCacheService::new(shared);
        let req = request(
            "IncreaseReplicaCount",
            &[
                ("ReplicationGroupId", "nonexistent"),
                ("ApplyImmediately", "true"),
                ("NewReplicaCount", "2"),
            ],
        );
        assert!(service.increase_replica_count(&req).is_err());
    }

    #[test]
    fn decrease_replica_count_updates_member_clusters() {
        let service = service_with_replication_group("my-rg", 3);
        let req = request(
            "DecreaseReplicaCount",
            &[
                ("ReplicationGroupId", "my-rg"),
                ("ApplyImmediately", "true"),
                ("NewReplicaCount", "1"),
            ],
        );
        let resp = service.decrease_replica_count(&req).unwrap();
        let body = String::from_utf8(resp.body.to_vec()).unwrap();
        assert!(body.contains("<ClusterId>my-rg-001</ClusterId>"));
        assert!(body.contains("<ClusterId>my-rg-002</ClusterId>"));
        assert!(!body.contains("<ClusterId>my-rg-003</ClusterId>"));
        assert!(body.contains("<DecreaseReplicaCountResponse"));
    }

    #[test]
    fn decrease_replica_count_validates_minimum() {
        let service = service_with_replication_group("my-rg", 1);
        // NewReplicaCount=0 means total=1, which is not fewer than current 1
        let req = request(
            "DecreaseReplicaCount",
            &[
                ("ReplicationGroupId", "my-rg"),
                ("ApplyImmediately", "true"),
                ("NewReplicaCount", "0"),
            ],
        );
        assert!(service.decrease_replica_count(&req).is_err());
    }

    #[test]
    fn decrease_replica_count_rejects_negative() {
        let service = service_with_replication_group("my-rg", 2);
        let req = request(
            "DecreaseReplicaCount",
            &[
                ("ReplicationGroupId", "my-rg"),
                ("ApplyImmediately", "true"),
                ("NewReplicaCount", "-1"),
            ],
        );
        assert!(service.decrease_replica_count(&req).is_err());
    }

    #[test]
    fn test_failover_validates_node_group() {
        let service = service_with_replication_group("my-rg", 1);
        let req = request(
            "TestFailover",
            &[("ReplicationGroupId", "my-rg"), ("NodeGroupId", "0001")],
        );
        let resp = service.test_failover(&req).unwrap();
        let body = String::from_utf8(resp.body.to_vec()).unwrap();
        assert!(body.contains("<Status>available</Status>"));
        assert!(body.contains("<TestFailoverResponse"));
    }

    #[test]
    fn test_failover_rejects_invalid_node_group() {
        let service = service_with_replication_group("my-rg", 1);
        let req = request(
            "TestFailover",
            &[("ReplicationGroupId", "my-rg"), ("NodeGroupId", "9999")],
        );
        assert!(service.test_failover(&req).is_err());
    }

    #[test]
    fn test_failover_not_found() {
        let state = crate::state::ElastiCacheState::new("123456789012", "us-east-1");
        let shared = std::sync::Arc::new(parking_lot::RwLock::new(state));
        let service = ElastiCacheService::new(shared);
        let req = request(
            "TestFailover",
            &[
                ("ReplicationGroupId", "nonexistent"),
                ("NodeGroupId", "0001"),
            ],
        );
        assert!(service.test_failover(&req).is_err());
    }
}
