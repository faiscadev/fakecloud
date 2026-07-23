//! ElastiCache `misc` family handlers extracted from service.rs
//! by audit-2026-05-19 file-split.

use super::*;

/// Map a taggable resource ARN to the AWS not-found fault code appropriate for
/// its resource type. The type is the prefix of the ARN's resource segment
/// before the first `:` (e.g. `parametergroup:default.redis7`). Unparseable or
/// unrecognized ARNs fall back to `CacheClusterNotFound`, matching the
/// historical behavior of the tag handlers.
fn tag_resource_not_found_code(resource_name: &str) -> &'static str {
    let Ok(arn) = resource_name.parse::<fakecloud_aws::arn::Arn>() else {
        return "CacheClusterNotFound";
    };
    match arn.resource.split(':').next().unwrap_or_default() {
        "parametergroup" => "CacheParameterGroupNotFound",
        "subnetgroup" => "CacheSubnetGroupNotFoundFault",
        "cluster" => "CacheClusterNotFound",
        "replicationgroup" => "ReplicationGroupNotFoundFault",
        "globalreplicationgroup" => "GlobalReplicationGroupNotFoundFault",
        "user" => "UserNotFound",
        "usergroup" => "UserGroupNotFound",
        "snapshot" => "SnapshotNotFoundFault",
        "serverlesscache" => "ServerlessCacheNotFoundFault",
        "serverlesssnapshot" => "ServerlessCacheSnapshotNotFoundFault",
        "reserved-instance" => "ReservedCacheNodeNotFound",
        "securitygroup" => "CacheSecurityGroupNotFound",
        _ => "CacheClusterNotFound",
    }
}

/// Build the resource-type-appropriate not-found fault for a tag operation on
/// an ARN that does not correspond to an existing resource.
fn tag_resource_not_found(resource_name: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::NOT_FOUND,
        tag_resource_not_found_code(resource_name),
        format!("The resource {resource_name} could not be found."),
    )
}

impl ElastiCacheService {
    pub(super) fn describe_reserved_cache_nodes(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let reserved_cache_node_id = optional_query_param(request, "ReservedCacheNodeId");
        let reserved_cache_nodes_offering_id =
            optional_query_param(request, "ReservedCacheNodesOfferingId");
        let cache_node_type = optional_query_param(request, "CacheNodeType");
        let duration = parse_reserved_duration_filter(optional_query_param(request, "Duration"))?;
        let product_description = optional_query_param(request, "ProductDescription");
        let offering_type = optional_query_param(request, "OfferingType");
        let max_records = optional_usize_param(request, "MaxRecords")?;
        let marker = optional_query_param(request, "Marker");

        let accounts = self.state.read();
        let empty = ElastiCacheState::new(&request.account_id, &request.region);
        let state = accounts.get(&request.account_id).unwrap_or(&empty);
        let mut nodes: Vec<&ReservedCacheNode> = state.reserved_cache_nodes.values().collect();
        nodes.retain(|node| {
            reserved_cache_node_id
                .as_ref()
                .is_none_or(|expected| node.reserved_cache_node_id == *expected)
                && reserved_cache_nodes_offering_id
                    .as_ref()
                    .is_none_or(|expected| node.reserved_cache_nodes_offering_id == *expected)
                && cache_node_type
                    .as_ref()
                    .is_none_or(|expected| node.cache_node_type == *expected)
                && duration.is_none_or(|expected| node.duration == expected)
                && product_description
                    .as_ref()
                    .is_none_or(|expected| node.product_description == *expected)
                && offering_type
                    .as_ref()
                    .is_none_or(|expected| node.offering_type == *expected)
        });
        nodes.sort_by(|left, right| {
            left.reserved_cache_node_id
                .cmp(&right.reserved_cache_node_id)
        });

        if let Some(ref id) = reserved_cache_node_id {
            if nodes.is_empty() {
                return Err(AwsServiceError::aws_error(
                    StatusCode::NOT_FOUND,
                    "ReservedCacheNodeNotFound",
                    format!("ReservedCacheNode not found: {id}"),
                ));
            }
        }

        let (page, next_marker) = paginate(&nodes, marker.as_deref(), max_records)?;
        let members_xml: String = page
            .iter()
            .map(|node| reserved_cache_node_xml(node))
            .collect();
        let marker_xml = next_marker
            .map(|value| format!("<Marker>{}</Marker>", xml_escape(&value)))
            .unwrap_or_default();

        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml(
                "DescribeReservedCacheNodes",
                ELASTICACHE_NS,
                &format!("<ReservedCacheNodes>{members_xml}</ReservedCacheNodes>{marker_xml}"),
                &request.request_id,
            ),
        ))
    }

    pub(super) fn describe_reserved_cache_nodes_offerings(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let reserved_cache_nodes_offering_id =
            optional_query_param(request, "ReservedCacheNodesOfferingId");
        let cache_node_type = optional_query_param(request, "CacheNodeType");
        let duration = parse_reserved_duration_filter(optional_query_param(request, "Duration"))?;
        let product_description = optional_query_param(request, "ProductDescription");
        let offering_type = optional_query_param(request, "OfferingType");
        let max_records = optional_usize_param(request, "MaxRecords")?;
        let marker = optional_query_param(request, "Marker");

        let accounts = self.state.read();
        let empty = ElastiCacheState::new(&request.account_id, &request.region);
        let state = accounts.get(&request.account_id).unwrap_or(&empty);
        let mut offerings: Vec<&ReservedCacheNodesOffering> =
            state.reserved_cache_nodes_offerings.iter().collect();
        offerings.retain(|offering| {
            reserved_cache_nodes_offering_id
                .as_ref()
                .is_none_or(|expected| offering.reserved_cache_nodes_offering_id == *expected)
                && cache_node_type
                    .as_ref()
                    .is_none_or(|expected| offering.cache_node_type == *expected)
                && duration.is_none_or(|expected| offering.duration == expected)
                && product_description
                    .as_ref()
                    .is_none_or(|expected| offering.product_description == *expected)
                && offering_type
                    .as_ref()
                    .is_none_or(|expected| offering.offering_type == *expected)
        });
        offerings.sort_by(|left, right| {
            left.reserved_cache_nodes_offering_id
                .cmp(&right.reserved_cache_nodes_offering_id)
        });

        if let Some(ref id) = reserved_cache_nodes_offering_id {
            if offerings.is_empty() {
                return Err(AwsServiceError::aws_error(
                    StatusCode::NOT_FOUND,
                    "ReservedCacheNodesOfferingNotFound",
                    format!("ReservedCacheNodesOffering not found: {id}"),
                ));
            }
        }

        let (page, next_marker) = paginate(&offerings, marker.as_deref(), max_records)?;
        let members_xml: String = page
            .iter()
            .map(|offering| reserved_cache_nodes_offering_xml(offering))
            .collect();
        let marker_xml = next_marker
            .map(|value| format!("<Marker>{}</Marker>", xml_escape(&value)))
            .unwrap_or_default();

        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml(
                "DescribeReservedCacheNodesOfferings", ELASTICACHE_NS,
                &format!(
                    "<ReservedCacheNodesOfferings>{members_xml}</ReservedCacheNodesOfferings>{marker_xml}"
                ),
                &request.request_id,
            ),
        ))
    }

    pub(super) fn increase_replica_count(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        self.modify_replica_count(request, true)
    }

    pub(super) fn decrease_replica_count(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        self.modify_replica_count(request, false)
    }

    /// Shared core for IncreaseReplicaCount + DecreaseReplicaCount.
    ///
    /// Both AWS operations take either a uniform `NewReplicaCount` (applied to
    /// every shard) or a per-shard `ReplicaConfiguration.ConfigureShard.N`
    /// list. DecreaseReplicaCount additionally accepts `ReplicasToRemove`,
    /// which we map onto a uniform `replicas_per_node_group - len(remove)`.
    /// The struct fields we mutate are `num_cache_clusters`, `member_clusters`
    /// and `replicas_per_node_group` so DescribeReplicationGroups reflects the
    /// new shape immediately.
    pub(super) fn modify_replica_count(
        &self,
        request: &AwsRequest,
        increase: bool,
    ) -> Result<AwsResponse, AwsServiceError> {
        let replication_group_id = required_query_param(request, "ReplicationGroupId")?;
        let apply_str = required_query_param(request, "ApplyImmediately")?;
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

        let new_replica_count = optional_query_param(request, "NewReplicaCount")
            .map(|v| {
                v.parse::<i32>().map_err(|_| {
                    AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "InvalidParameterValue",
                        format!("Invalid value for NewReplicaCount: '{v}'"),
                    )
                })
            })
            .transpose()?;

        let replica_configuration = parse_replica_configuration(request)?;
        let replicas_to_remove =
            parse_query_list_param(request, "ReplicasToRemove", "ReplicaToRemove");

        let action = if increase {
            "IncreaseReplicaCount"
        } else {
            "DecreaseReplicaCount"
        };

        // AWS requires exactly one of NewReplicaCount or ReplicaConfiguration.
        // DecreaseReplicaCount additionally accepts ReplicasToRemove (alone or
        // with one of the others). We mirror the validation so callers don't
        // accidentally race two configurations against each other.
        let inputs_supplied = (new_replica_count.is_some() as u8)
            + (!replica_configuration.is_empty() as u8)
            + (!replicas_to_remove.is_empty() as u8);
        if inputs_supplied == 0 {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterCombination",
                if increase {
                    "IncreaseReplicaCount requires NewReplicaCount or ReplicaConfiguration."
                        .to_string()
                } else {
                    "DecreaseReplicaCount requires NewReplicaCount, ReplicaConfiguration, or ReplicasToRemove.".to_string()
                },
            ));
        }
        if new_replica_count.is_some() && !replica_configuration.is_empty() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterCombination",
                format!("{action} accepts NewReplicaCount or ReplicaConfiguration, not both."),
            ));
        }
        if let Some(n) = new_replica_count {
            if increase && n < 1 {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterValue",
                    format!("NewReplicaCount must be a positive integer, got {n}"),
                ));
            }
            if !increase && n < 0 {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterValue",
                    format!("NewReplicaCount must be non-negative, got {n}"),
                ));
            }
        }

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request.account_id);

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

        let shard_count = group.num_node_groups.max(1);
        let current_replicas_per_shard = current_replicas_per_shard(group);

        // Resolve the target per-shard replica count. ReplicaConfiguration takes
        // precedence and must specify the same NewReplicaCount for every shard
        // (we don't model per-shard replica counts; the value gets applied
        // uniformly). Without it we fall back to NewReplicaCount, then to
        // ReplicasToRemove for the decrease path.
        let target_replicas = if !replica_configuration.is_empty() {
            let mut counts: Vec<i32> = replica_configuration
                .iter()
                .map(|c| c.new_replica_count)
                .collect();
            counts.sort_unstable();
            counts.dedup();
            if counts.len() != 1 {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterValue",
                    "ReplicaConfiguration entries must specify the same NewReplicaCount across shards.".to_string(),
                ));
            }
            counts[0]
        } else if let Some(n) = new_replica_count {
            n
        } else {
            // ReplicasToRemove: drop the listed count from the current
            // per-shard replica count, distributed evenly across shards.
            let removed_per_shard =
                (replicas_to_remove.len() as i32).div_euclid(shard_count.max(1));
            current_replicas_per_shard - removed_per_shard
        };

        if target_replicas < 0 {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValue",
                format!("Replica count must be non-negative, got {target_replicas}"),
            ));
        }

        if increase && target_replicas <= current_replicas_per_shard {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValue",
                format!(
                    "NewReplicaCount ({target_replicas}) must be greater than the current replica count per shard ({current_replicas_per_shard})."
                ),
            ));
        }
        if !increase && target_replicas >= current_replicas_per_shard {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValue",
                format!(
                    "NewReplicaCount ({target_replicas}) must be less than the current replica count per shard ({current_replicas_per_shard})."
                ),
            ));
        }

        let new_total = shard_count
            .checked_mul(target_replicas + 1)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterValue",
                    format!(
                        "Replica count {target_replicas} across {shard_count} shards overflows."
                    ),
                )
            })?;
        group.num_cache_clusters = new_total;
        group.replicas_per_node_group = Some(target_replicas);
        group.member_clusters = build_member_clusters(&replication_group_id, new_total);

        let group = group.clone();
        let region = request.region.clone();
        let xml = replication_group_xml(&group, &region);

        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml(
                action,
                ELASTICACHE_NS,
                &format!("<ReplicationGroup>{xml}</ReplicationGroup>"),
                &request.request_id,
            ),
        ))
    }

    pub(super) fn test_failover(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let replication_group_id = required_query_param(request, "ReplicationGroupId")?;
        let node_group_id = required_query_param(request, "NodeGroupId")?;

        let accounts = self.state.read();
        let empty = ElastiCacheState::new(&request.account_id, &request.region);
        let state = accounts.get(&request.account_id).unwrap_or(&empty);

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

        let region = request.region.clone();
        let xml = replication_group_xml(group, &region);

        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml(
                "TestFailover",
                ELASTICACHE_NS,
                &format!("<ReplicationGroup>{xml}</ReplicationGroup>"),
                &request.request_id,
            ),
        ))
    }

    pub(super) fn add_tags_to_resource(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let resource_name = required_query_param(request, "ResourceName")?;
        let tags = parse_tags(request)?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request.account_id);
        // The resource must exist, but it need not already have a tag entry:
        // freshly-created resources (e.g. parameter groups) register no tags
        // until the first AddTagsToResource. Create the entry on demand.
        if !state.resource_exists(&resource_name) {
            return Err(tag_resource_not_found(&resource_name));
        }
        let tag_list = state.tags.entry(resource_name.clone()).or_default();

        merge_tags(tag_list, &tags);

        let tag_xml: String = tag_list.iter().map(tag_xml).collect();

        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml(
                "AddTagsToResource",
                ELASTICACHE_NS,
                &format!("<TagList>{tag_xml}</TagList>"),
                &request.request_id,
            ),
        ))
    }

    pub(super) fn list_tags_for_resource(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let resource_name = required_query_param(request, "ResourceName")?;

        let accounts = self.state.read();
        let empty = ElastiCacheState::new(&request.account_id, &request.region);
        let state = accounts.get(&request.account_id).unwrap_or(&empty);
        // Real ElastiCache returns an empty TagList for an existing untagged
        // resource and only faults when the resource is genuinely missing.
        // A missing tag entry must NOT be treated as "resource not found".
        if !state.resource_exists(&resource_name) {
            return Err(tag_resource_not_found(&resource_name));
        }
        let empty_tags = Vec::new();
        let tag_list = state.tags.get(&resource_name).unwrap_or(&empty_tags);

        let tag_xml: String = tag_list.iter().map(tag_xml).collect();

        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml(
                "ListTagsForResource",
                ELASTICACHE_NS,
                &format!("<TagList>{tag_xml}</TagList>"),
                &request.request_id,
            ),
        ))
    }

    pub(super) fn remove_tags_from_resource(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let resource_name = required_query_param(request, "ResourceName")?;
        let tag_keys = parse_tag_keys(request)?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request.account_id);
        // Existence is keyed on the resource, not on a pre-existing tag entry:
        // removing tags from an existing untagged resource is a no-op success.
        if !state.resource_exists(&resource_name) {
            return Err(tag_resource_not_found(&resource_name));
        }
        let tag_list = state.tags.entry(resource_name.clone()).or_default();

        tag_list.retain(|(key, _)| !tag_keys.contains(key));

        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml(
                "RemoveTagsFromResource",
                ELASTICACHE_NS,
                "",
                &request.request_id,
            ),
        ))
    }

    pub(super) fn list_allowed_node_type_modifications(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        // Documented sample of upgradeable node types. Not exhaustive
        // but matches the shape SDKs decode.
        let scale_up = ["cache.t4g.medium", "cache.m6g.large", "cache.r6g.large"];
        let scale_down = ["cache.t4g.micro", "cache.t4g.small"];
        let mut body = String::from("<ScaleUpModifications>");
        for n in scale_up {
            body.push_str(&format!("<member>{}</member>", xml_escape(n)));
        }
        body.push_str("</ScaleUpModifications><ScaleDownModifications>");
        for n in scale_down {
            body.push_str(&format!("<member>{}</member>", xml_escape(n)));
        }
        body.push_str("</ScaleDownModifications>");
        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml(
                "ListAllowedNodeTypeModifications",
                ELASTICACHE_NS,
                &body,
                &request.request_id,
            ),
        ))
    }

    pub(super) fn modify_global_node_groups(
        &self,
        request: &AwsRequest,
        action: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = required_query_param(request, "GlobalReplicationGroupId")?;
        // NodeGroupCount is the target shard count for Increase/Decrease;
        // Rebalance keeps the count and only re-partitions slots.
        let requested_count = optional_query_param(request, "NodeGroupCount")
            .as_deref()
            .and_then(|v| v.parse::<i32>().ok());

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request.account_id);
        let group = state
            .global_replication_groups
            .get_mut(&id)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::NOT_FOUND,
                    "GlobalReplicationGroupNotFoundFault",
                    format!("GlobalReplicationGroup {id} not found."),
                )
            })?;

        match action {
            "IncreaseNodeGroupsInGlobalReplicationGroup" => {
                let target = requested_count.ok_or_else(|| {
                    AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "InvalidParameterValueException",
                        "NodeGroupCount is required",
                    )
                })?;
                if target <= group.num_node_groups {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "InvalidParameterValueException",
                        format!(
                            "NodeGroupCount ({target}) must be greater than the current count ({})",
                            group.num_node_groups
                        ),
                    ));
                }
                group.num_node_groups = target;
            }
            "DecreaseNodeGroupsInGlobalReplicationGroup" => {
                let target = requested_count.ok_or_else(|| {
                    AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "InvalidParameterValueException",
                        "NodeGroupCount is required",
                    )
                })?;
                if target < 1 || target >= group.num_node_groups {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "InvalidParameterValueException",
                        format!(
                            "NodeGroupCount ({target}) must be >= 1 and less than the current count ({})",
                            group.num_node_groups
                        ),
                    ));
                }
                group.num_node_groups = target;
            }
            // RebalanceSlotsInGlobalReplicationGroup: keep the shard count,
            // the slot re-partition is reflected by the xml builder.
            _ => {}
        }

        let xml = global_replication_group_xml(group, true);
        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml(
                action,
                ELASTICACHE_NS,
                &format!("<GlobalReplicationGroup>{xml}</GlobalReplicationGroup>"),
                &request.request_id,
            ),
        ))
    }

    // ── Reserved cache nodes ──

    pub(super) fn purchase_reserved_cache_nodes_offering(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let offering_id = required_query_param(request, "ReservedCacheNodesOfferingId")?;
        let id = optional_query_param(request, "ReservedCacheNodeId").unwrap_or_else(|| {
            format!(
                "ri-{}",
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_nanos())
                    .unwrap_or(0)
            )
        });
        let count = optional_query_param(request, "CacheNodeCount")
            .as_deref()
            .and_then(|v| v.parse::<i32>().ok())
            .unwrap_or(1);

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request.account_id);
        let offering = state
            .reserved_cache_nodes_offerings
            .iter()
            .find(|o| o.reserved_cache_nodes_offering_id == offering_id)
            .cloned()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::NOT_FOUND,
                    "ReservedCacheNodesOfferingNotFound",
                    format!("ReservedCacheNodesOffering {offering_id} not found."),
                )
            })?;
        let arn = format!(
            "arn:aws:elasticache:{}:{}:reserved-instance:{}",
            request.region, request.account_id, id
        );
        let node = ReservedCacheNode {
            reserved_cache_node_id: id.clone(),
            reserved_cache_nodes_offering_id: offering_id,
            cache_node_type: offering.cache_node_type,
            start_time: chrono::Utc::now().to_rfc3339(),
            duration: offering.duration,
            fixed_price: offering.fixed_price,
            usage_price: offering.usage_price,
            cache_node_count: count,
            product_description: offering.product_description,
            offering_type: offering.offering_type,
            state: "payment-pending".to_string(),
            recurring_charges: offering.recurring_charges,
            reservation_arn: arn,
        };
        state.reserved_cache_nodes.insert(id.clone(), node.clone());
        let xml = reserved_cache_node_xml(&node);
        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml(
                "PurchaseReservedCacheNodesOffering",
                ELASTICACHE_NS,
                &format!("<ReservedCacheNode>{xml}</ReservedCacheNode>"),
                &request.request_id,
            ),
        ))
    }

    // ── Events / Service updates / Update actions ──

    pub(super) fn describe_events(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        // Smithy declares SourceType as an enum; validate any value the
        // client supplied so an unknown filter doesn't slip through silently.
        if let Some(raw) = request.query_params.get("SourceType") {
            if !is_valid_source_type(raw) {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterValue",
                    format!("Invalid value for SourceType: '{raw}'"),
                ));
            }
        }
        let max_records = optional_usize_param(request, "MaxRecords")?;
        let marker = optional_query_param(request, "Marker");
        let accounts = self.state.read();
        let empty = ElastiCacheState::new(&request.account_id, &request.region);
        let state = accounts.get(&request.account_id).unwrap_or(&empty);
        let events: Vec<&crate::state::CacheEvent> = state.events.iter().collect();
        let (page, next_marker) = paginate(&events, marker.as_deref(), max_records)?;
        let members: String = page
            .iter()
            .map(|e| {
                format!(
                    "<Event><SourceIdentifier>{}</SourceIdentifier><SourceType>{}</SourceType><Message>{}</Message><Date>{}</Date></Event>",
                    xml_escape(&e.source_identifier),
                    xml_escape(&e.source_type),
                    xml_escape(&e.message),
                    xml_escape(&e.date),
                )
            })
            .collect();
        let marker_xml = next_marker
            .map(|m| format!("<Marker>{}</Marker>", xml_escape(&m)))
            .unwrap_or_default();
        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml(
                "DescribeEvents",
                ELASTICACHE_NS,
                &format!("<Events>{members}</Events>{marker_xml}"),
                &request.request_id,
            ),
        ))
    }

    pub(super) fn describe_service_updates(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name_filter = optional_query_param(request, "ServiceUpdateName");
        let status_filter = collect_indexed_strings(request, "ServiceUpdateStatus.member");
        let accounts = self.state.read();
        let empty = ElastiCacheState::new(&request.account_id, &request.region);
        let state = accounts.get(&request.account_id).unwrap_or(&empty);
        let members: String = state
            .service_updates
            .values()
            .filter(|su| {
                name_filter
                    .as_ref()
                    .is_none_or(|n| &su.service_update_name == n)
                    && (status_filter.is_empty()
                        || status_filter.contains(&su.service_update_status))
            })
            .map(render_service_update)
            .collect();
        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml(
                "DescribeServiceUpdates",
                ELASTICACHE_NS,
                &format!("<ServiceUpdates>{members}</ServiceUpdates>"),
                &request.request_id,
            ),
        ))
    }

    pub(super) fn describe_update_actions(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name_filter = optional_query_param(request, "ServiceUpdateName");
        let cluster_filter = collect_indexed_strings(request, "CacheClusterIds.member");
        let group_filter = collect_indexed_strings(request, "ReplicationGroupIds.member");
        let action_status_filter = collect_indexed_strings(request, "UpdateActionStatus.member");
        // ServiceUpdateStatus filters on the *service update's* status
        // (available/cancelled/expired); Engine filters on redis/memcached.
        let su_status_filter = collect_indexed_strings(request, "ServiceUpdateStatus.member");
        let engine_filter = optional_query_param(request, "Engine");

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request.account_id);
        // Materialize the (service_update x target) actions before reading so
        // callers see actions for their actual clusters/replication groups.
        state.ensure_update_actions();
        let members: String = state
            .update_actions
            .values()
            .filter(|ua| {
                let name_ok = name_filter
                    .as_ref()
                    .is_none_or(|n| &ua.service_update_name == n);
                let action_status_ok = action_status_filter.is_empty()
                    || action_status_filter.contains(&ua.update_action_status);
                let su_status_ok = su_status_filter.is_empty()
                    || su_status_filter.contains(&ua.service_update_status);
                let engine_ok = engine_filter.as_ref().is_none_or(|e| &ua.engine == e);
                // CacheClusterIds and ReplicationGroupIds are OR'd; an empty
                // pair means "all targets".
                let target_ok = if cluster_filter.is_empty() && group_filter.is_empty() {
                    true
                } else {
                    ua.cache_cluster_id
                        .as_ref()
                        .is_some_and(|c| cluster_filter.contains(c))
                        || ua
                            .replication_group_id
                            .as_ref()
                            .is_some_and(|g| group_filter.contains(g))
                };
                name_ok && action_status_ok && su_status_ok && engine_ok && target_ok
            })
            .map(render_update_action)
            .collect();
        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml(
                "DescribeUpdateActions",
                ELASTICACHE_NS,
                &format!("<UpdateActions>{members}</UpdateActions>"),
                &request.request_id,
            ),
        ))
    }

    pub(super) fn batch_apply_update_action(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        self.batch_update_action(request, "BatchApplyUpdateAction", "waiting-to-start")
    }

    pub(super) fn batch_stop_update_action(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        self.batch_update_action(request, "BatchStopUpdateAction", "stopping")
    }

    pub(super) fn batch_update_action(
        &self,
        request: &AwsRequest,
        action: &str,
        new_status: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let svc_update = required_query_param(request, "ServiceUpdateName")?;
        let cluster_ids = collect_indexed_strings(request, "CacheClusterIds.member");
        let group_ids = collect_indexed_strings(request, "ReplicationGroupIds.member");

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request.account_id);
        state.ensure_update_actions();
        let now = chrono::Utc::now().to_rfc3339();

        // (kind, id) where kind is "cc" or "rg"; processed = transitioned,
        // unprocessed = no matching update action for that (service update, id).
        let mut processed: Vec<(&str, String)> = Vec::new();
        let mut unprocessed: Vec<(&str, String)> = Vec::new();
        for (kind, id) in cluster_ids
            .iter()
            .map(|c| ("cc", c))
            .chain(group_ids.iter().map(|g| ("rg", g)))
        {
            let key = format!("{svc_update}#{kind}#{id}");
            if let Some(ua) = state.update_actions.get_mut(&key) {
                ua.update_action_status = new_status.to_string();
                ua.update_action_status_modified_date = now.clone();
                processed.push((kind, id.clone()));
            } else {
                unprocessed.push((kind, id.clone()));
            }
        }

        let processed_xml: String = processed
            .iter()
            .map(|(kind, id)| {
                let target = target_element(kind, id);
                format!(
                    "<member><ServiceUpdateName>{}</ServiceUpdateName>{target}<UpdateActionStatus>{}</UpdateActionStatus></member>",
                    xml_escape(&svc_update),
                    xml_escape(new_status),
                )
            })
            .collect();
        let unprocessed_xml: String = unprocessed
            .iter()
            .map(|(kind, id)| {
                let target = target_element(kind, id);
                format!(
                    "<member><ServiceUpdateName>{}</ServiceUpdateName>{target}<ErrorType>UpdateActionNotFoundFault</ErrorType><ErrorMessage>No update action found for the given service update and target</ErrorMessage></member>",
                    xml_escape(&svc_update),
                )
            })
            .collect();
        let body = format!(
            "<ProcessedUpdateActions>{processed_xml}</ProcessedUpdateActions><UnprocessedUpdateActions>{unprocessed_xml}</UnprocessedUpdateActions>"
        );
        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml(action, ELASTICACHE_NS, &body, &request.request_id),
        ))
    }
}

/// Render `<ReplicationGroupId>` or `<CacheClusterId>` for a batch member.
fn target_element(kind: &str, id: &str) -> String {
    if kind == "rg" {
        format!(
            "<ReplicationGroupId>{}</ReplicationGroupId>",
            xml_escape(id)
        )
    } else {
        format!("<CacheClusterId>{}</CacheClusterId>", xml_escape(id))
    }
}

fn render_service_update(su: &crate::state::ServiceUpdate) -> String {
    format!(
        "<member><ServiceUpdateName>{}</ServiceUpdateName><ServiceUpdateReleaseDate>{}</ServiceUpdateReleaseDate><ServiceUpdateEndDate>{}</ServiceUpdateEndDate><ServiceUpdateSeverity>{}</ServiceUpdateSeverity><ServiceUpdateStatus>{}</ServiceUpdateStatus><ServiceUpdateRecommendedApplyByDate>{}</ServiceUpdateRecommendedApplyByDate><ServiceUpdateType>{}</ServiceUpdateType><Engine>{}</Engine><EngineVersion>{}</EngineVersion><AutoUpdateAfterRecommendedApplyByDate>{}</AutoUpdateAfterRecommendedApplyByDate><EstimatedUpdateTime>{}</EstimatedUpdateTime><ServiceUpdateDescription>{}</ServiceUpdateDescription></member>",
        xml_escape(&su.service_update_name),
        xml_escape(&su.service_update_release_date),
        xml_escape(&su.service_update_end_date),
        xml_escape(&su.service_update_severity),
        xml_escape(&su.service_update_status),
        xml_escape(&su.service_update_recommended_apply_by_date),
        xml_escape(&su.service_update_type),
        xml_escape(&su.engine),
        xml_escape(&su.engine_version),
        su.auto_update_after_recommended_apply_by_date,
        xml_escape(&su.estimated_update_time),
        xml_escape(&su.service_update_description),
    )
}

fn render_update_action(ua: &crate::state::UpdateAction) -> String {
    let mut target = String::new();
    if let Some(rg) = &ua.replication_group_id {
        target.push_str(&format!(
            "<ReplicationGroupId>{}</ReplicationGroupId>",
            xml_escape(rg)
        ));
    }
    if let Some(cc) = &ua.cache_cluster_id {
        target.push_str(&format!(
            "<CacheClusterId>{}</CacheClusterId>",
            xml_escape(cc)
        ));
    }
    format!(
        "<member>{target}<ServiceUpdateName>{}</ServiceUpdateName><ServiceUpdateReleaseDate>{}</ServiceUpdateReleaseDate><ServiceUpdateSeverity>{}</ServiceUpdateSeverity><ServiceUpdateStatus>{}</ServiceUpdateStatus><ServiceUpdateRecommendedApplyByDate>{}</ServiceUpdateRecommendedApplyByDate><ServiceUpdateType>{}</ServiceUpdateType><UpdateActionAvailableDate>{}</UpdateActionAvailableDate><UpdateActionStatus>{}</UpdateActionStatus><NodesUpdated>{}</NodesUpdated><UpdateActionStatusModifiedDate>{}</UpdateActionStatusModifiedDate><SlaMet>{}</SlaMet><EstimatedUpdateTime>{}</EstimatedUpdateTime><Engine>{}</Engine></member>",
        xml_escape(&ua.service_update_name),
        xml_escape(&ua.service_update_release_date),
        xml_escape(&ua.service_update_severity),
        xml_escape(&ua.service_update_status),
        xml_escape(&ua.service_update_recommended_apply_by_date),
        xml_escape(&ua.service_update_type),
        xml_escape(&ua.update_action_available_date),
        xml_escape(&ua.update_action_status),
        xml_escape(&ua.nodes_updated),
        xml_escape(&ua.update_action_status_modified_date),
        xml_escape(&ua.sla_met),
        xml_escape(&ua.estimated_update_time),
        xml_escape(&ua.engine),
    )
}
