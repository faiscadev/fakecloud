//! ElastiCache `security_groups` family handlers extracted from service.rs
//! by audit-2026-05-19 file-split.

use super::*;

impl ElastiCacheService {
    // ── Cache Security Groups ──

    pub(super) fn create_cache_security_group(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = required_query_param(request, "CacheSecurityGroupName")?;
        let description = required_query_param(request, "Description")?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request.account_id);
        if state.security_groups.contains_key(&name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "CacheSecurityGroupAlreadyExists",
                format!("CacheSecurityGroup {name} already exists."),
            ));
        }
        let arn = format!(
            "arn:aws:elasticache:{}:{}:securitygroup:{}",
            request.region, request.account_id, name
        );
        let sg = crate::state::CacheSecurityGroup {
            cache_security_group_name: name.clone(),
            description,
            owner_id: request.account_id.clone(),
            arn: arn.clone(),
            ec2_security_groups: Vec::new(),
        };
        state.security_groups.insert(name.clone(), sg.clone());
        let xml = format!(
            "<CacheSecurityGroup>{}</CacheSecurityGroup>",
            cache_security_group_xml(&sg)
        );
        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml(
                "CreateCacheSecurityGroup",
                ELASTICACHE_NS,
                &xml,
                &request.request_id,
            ),
        ))
    }

    pub(super) fn delete_cache_security_group(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = required_query_param(request, "CacheSecurityGroupName")?;
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request.account_id);
        state.security_groups.remove(&name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "CacheSecurityGroupNotFound",
                format!("CacheSecurityGroup {name} not found."),
            )
        })?;
        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml(
                "DeleteCacheSecurityGroup",
                ELASTICACHE_NS,
                "",
                &request.request_id,
            ),
        ))
    }

    pub(super) fn describe_cache_security_groups(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = optional_query_param(request, "CacheSecurityGroupName");
        let max_records = optional_usize_param(request, "MaxRecords")?;
        let marker = optional_query_param(request, "Marker");

        let accounts = self.state.read();
        let empty = ElastiCacheState::new(&request.account_id, &request.region);
        let state = accounts.get(&request.account_id).unwrap_or(&empty);
        let mut groups: Vec<&crate::state::CacheSecurityGroup> = state
            .security_groups
            .values()
            .filter(|g| {
                name.as_ref()
                    .is_none_or(|n| g.cache_security_group_name == *n)
            })
            .collect();
        groups.sort_by(|a, b| {
            a.cache_security_group_name
                .cmp(&b.cache_security_group_name)
        });
        if let Some(ref n) = name {
            if groups.is_empty() {
                return Err(AwsServiceError::aws_error(
                    StatusCode::NOT_FOUND,
                    "CacheSecurityGroupNotFound",
                    format!("CacheSecurityGroup {n} not found."),
                ));
            }
        }
        let (page, next_marker) = paginate(&groups, marker.as_deref(), max_records);
        let members: String = page
            .iter()
            .map(|g| {
                format!(
                    "<CacheSecurityGroup>{}</CacheSecurityGroup>",
                    cache_security_group_xml(g)
                )
            })
            .collect();
        let marker_xml = next_marker
            .map(|m| format!("<Marker>{}</Marker>", xml_escape(&m)))
            .unwrap_or_default();
        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml(
                "DescribeCacheSecurityGroups",
                ELASTICACHE_NS,
                &format!("<CacheSecurityGroups>{members}</CacheSecurityGroups>{marker_xml}"),
                &request.request_id,
            ),
        ))
    }

    pub(super) fn authorize_cache_security_group_ingress(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = required_query_param(request, "CacheSecurityGroupName")?;
        let ec2_name = required_query_param(request, "EC2SecurityGroupName")?;
        let ec2_owner = required_query_param(request, "EC2SecurityGroupOwnerId")?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request.account_id);
        let sg = state.security_groups.get_mut(&name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "CacheSecurityGroupNotFound",
                format!("CacheSecurityGroup {name} not found."),
            )
        })?;
        if sg.ec2_security_groups.iter().any(|e| {
            e.ec2_security_group_name == ec2_name && e.ec2_security_group_owner_id == ec2_owner
        }) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "AuthorizationAlreadyExists",
                format!("Ingress for {ec2_name} already authorized."),
            ));
        }
        sg.ec2_security_groups
            .push(crate::state::Ec2SecurityGroupAuth {
                status: "authorizing".to_string(),
                ec2_security_group_name: ec2_name,
                ec2_security_group_owner_id: ec2_owner,
            });
        let xml = format!(
            "<CacheSecurityGroup>{}</CacheSecurityGroup>",
            cache_security_group_xml(sg)
        );
        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml(
                "AuthorizeCacheSecurityGroupIngress",
                ELASTICACHE_NS,
                &xml,
                &request.request_id,
            ),
        ))
    }

    pub(super) fn revoke_cache_security_group_ingress(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = required_query_param(request, "CacheSecurityGroupName")?;
        let ec2_name = required_query_param(request, "EC2SecurityGroupName")?;
        let ec2_owner = required_query_param(request, "EC2SecurityGroupOwnerId")?;
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request.account_id);
        let sg = state.security_groups.get_mut(&name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "CacheSecurityGroupNotFound",
                format!("CacheSecurityGroup {name} not found."),
            )
        })?;
        let before = sg.ec2_security_groups.len();
        sg.ec2_security_groups.retain(|e| {
            !(e.ec2_security_group_name == ec2_name && e.ec2_security_group_owner_id == ec2_owner)
        });
        if sg.ec2_security_groups.len() == before {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "AuthorizationNotFound",
                format!("Ingress for {ec2_name} not found."),
            ));
        }
        let xml = format!(
            "<CacheSecurityGroup>{}</CacheSecurityGroup>",
            cache_security_group_xml(sg)
        );
        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml(
                "RevokeCacheSecurityGroupIngress",
                ELASTICACHE_NS,
                &xml,
                &request.request_id,
            ),
        ))
    }
}
