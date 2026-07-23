//! ElastiCache `subnet_groups` family handlers extracted from service.rs
//! by audit-2026-05-19 file-split.

use super::*;

impl ElastiCacheService {
    pub(super) fn create_cache_subnet_group(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        // ElastiCache forces cache-subnet-group names to lowercase; the
        // response (and the resource's ID) come back lowercased, so the
        // Terraform Read queries the lowercase name. Store it canonically.
        let name = required_query_param(request, "CacheSubnetGroupName")?.to_lowercase();
        let description = required_query_param(request, "CacheSubnetGroupDescription")?;
        // AWS SDKs serialize this list as `SubnetIds.SubnetIdentifier.N`
        // (the @xmlName on the list member). The conformance probe uses the
        // generic `SubnetIds.member.N` form. Accept both via
        // `parse_query_list_param`, which falls back to `member` if the
        // canonical name yields nothing.
        let subnet_ids = parse_query_list_param(request, "SubnetIds", "SubnetIdentifier");
        let tags = parse_tags(request)?;

        // SubnetIds is @required in the Smithy model but
        // `InvalidParameterValueException` is NOT among
        // CreateCacheSubnetGroup's declared errors. `InvalidSubnet` is
        // declared and matches semantically — an empty list contains no
        // valid subnet identifier.
        if subnet_ids.is_empty() || subnet_ids.iter().any(|s| s.trim().is_empty()) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidSubnet",
                "At least one subnet ID must be specified.".to_string(),
            ));
        }

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request.account_id);

        if state.subnet_groups.contains_key(&name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "CacheSubnetGroupAlreadyExists",
                format!("Cache subnet group {name} already exists."),
            ));
        }

        // ARN carries the request's credential-scope region (req.region), not the
        // frozen server default.
        let arn = format!(
            "arn:aws:elasticache:{}:{}:subnetgroup:{}",
            request.region.as_str(),
            state.account_id,
            name
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

        let xml = cache_subnet_group_xml(&group, request.region.as_str());
        state.register_arn(&group.arn);
        state.tags.entry(group.arn.clone()).or_default();
        if !tags.is_empty() {
            merge_tags(state.tags.entry(group.arn.clone()).or_default(), &tags);
        }
        state.subnet_groups.insert(name, group);

        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml(
                "CreateCacheSubnetGroup",
                ELASTICACHE_NS,
                &format!("<CacheSubnetGroup>{xml}</CacheSubnetGroup>"),
                &request.request_id,
            ),
        ))
    }

    pub(super) fn describe_cache_subnet_groups(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let group_name =
            optional_query_param(request, "CacheSubnetGroupName").map(|n| n.to_lowercase());
        let max_records = optional_usize_param(request, "MaxRecords")?;
        let marker = optional_query_param(request, "Marker");

        let accounts = self.state.read();
        let empty = ElastiCacheState::new(&request.account_id, &request.region);
        let state = accounts.get(&request.account_id).unwrap_or(&empty);

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

        let (page, next_marker) = paginate(&groups, marker.as_deref(), max_records)?;

        let members_xml: String = page
            .iter()
            .map(|g| {
                format!(
                    "<CacheSubnetGroup>{}</CacheSubnetGroup>",
                    cache_subnet_group_xml(g, request.region.as_str())
                )
            })
            .collect();
        let marker_xml = next_marker
            .map(|m| format!("<Marker>{}</Marker>", xml_escape(&m)))
            .unwrap_or_default();

        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml(
                "DescribeCacheSubnetGroups",
                ELASTICACHE_NS,
                &format!("<CacheSubnetGroups>{members_xml}</CacheSubnetGroups>{marker_xml}"),
                &request.request_id,
            ),
        ))
    }

    pub(super) fn delete_cache_subnet_group(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = required_query_param(request, "CacheSubnetGroupName")?.to_lowercase();

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request.account_id);

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
            query_response_xml(
                "DeleteCacheSubnetGroup",
                ELASTICACHE_NS,
                "",
                &request.request_id,
            ),
        ))
    }

    pub(super) fn modify_cache_subnet_group(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = required_query_param(request, "CacheSubnetGroupName")?.to_lowercase();
        let description = optional_query_param(request, "CacheSubnetGroupDescription");
        let subnet_ids = parse_query_list_param(request, "SubnetIds", "SubnetIdentifier");
        if subnet_ids.iter().any(|s| s.trim().is_empty()) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidSubnet",
                "At least one subnet ID must be specified.".to_string(),
            ));
        }

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request.account_id);
        let region = request.region.clone();

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
            query_response_xml(
                "ModifyCacheSubnetGroup",
                ELASTICACHE_NS,
                &format!("<CacheSubnetGroup>{xml}</CacheSubnetGroup>"),
                &request.request_id,
            ),
        ))
    }
}
