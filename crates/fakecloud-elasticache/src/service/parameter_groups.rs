//! ElastiCache `parameter_groups` family handlers extracted from service.rs
//! by audit-2026-05-19 file-split.

use super::*;

impl ElastiCacheService {
    pub(super) fn describe_cache_engine_versions(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let engine = optional_query_param(request, "Engine");
        let engine_version = optional_query_param(request, "EngineVersion");
        let family = optional_query_param(request, "CacheParameterGroupFamily");
        let default_only =
            parse_optional_bool(optional_query_param(request, "DefaultOnly").as_deref())?;
        let max_records = optional_usize_param(request, "MaxRecords")?;
        let marker = optional_query_param(request, "Marker");

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
            query_response_xml(
                "DescribeCacheEngineVersions",
                ELASTICACHE_NS,
                &format!("<CacheEngineVersions>{members_xml}</CacheEngineVersions>{marker_xml}"),
                &request.request_id,
            ),
        ))
    }

    pub(super) fn describe_cache_parameter_groups(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let group_name = optional_query_param(request, "CacheParameterGroupName");
        let max_records = optional_usize_param(request, "MaxRecords")?;
        let marker = optional_query_param(request, "Marker");

        let accounts = self.state.read();
        let empty = ElastiCacheState::new(&request.account_id, &request.region);
        let state = accounts.get(&request.account_id).unwrap_or(&empty);

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
            query_response_xml(
                "DescribeCacheParameterGroups",
                ELASTICACHE_NS,
                &format!("<CacheParameterGroups>{members_xml}</CacheParameterGroups>{marker_xml}"),
                &request.request_id,
            ),
        ))
    }

    pub(super) fn describe_engine_default_parameters(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let family = required_query_param(request, "CacheParameterGroupFamily")?;
        let max_records = optional_usize_param(request, "MaxRecords")?;
        let marker = optional_query_param(request, "Marker");

        let params = default_parameters_for_family(&family);
        let (page, next_marker) = paginate(&params, marker.as_deref(), max_records);

        let params_xml: String = page.iter().map(parameter_xml).collect();
        let marker_xml = next_marker
            .map(|m| format!("<Marker>{}</Marker>", xml_escape(&m)))
            .unwrap_or_default();

        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml(
                "DescribeEngineDefaultParameters",
                ELASTICACHE_NS,
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

    // ── Cache Parameter Groups (CRUD beyond Describe) ──

    pub(super) fn create_cache_parameter_group(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = required_query_param(request, "CacheParameterGroupName")?;
        let family = required_query_param(request, "CacheParameterGroupFamily")?;
        let description = required_query_param(request, "Description")?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request.account_id);
        if state
            .parameter_groups
            .iter()
            .any(|g| g.cache_parameter_group_name == name)
        {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "CacheParameterGroupAlreadyExists",
                format!("CacheParameterGroup {name} already exists."),
            ));
        }
        let arn = format!(
            "arn:aws:elasticache:{}:{}:parametergroup:{}",
            request.region, request.account_id, name
        );
        let group = CacheParameterGroup {
            cache_parameter_group_name: name.clone(),
            cache_parameter_group_family: family,
            description,
            is_global: false,
            arn,
        };
        state.parameter_groups.push(group.clone());
        let xml = cache_parameter_group_xml(&group);
        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml(
                "CreateCacheParameterGroup",
                ELASTICACHE_NS,
                &xml,
                &request.request_id,
            ),
        ))
    }

    pub(super) fn delete_cache_parameter_group(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = required_query_param(request, "CacheParameterGroupName")?;
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request.account_id);
        let before = state.parameter_groups.len();
        state
            .parameter_groups
            .retain(|g| g.cache_parameter_group_name != name);
        if state.parameter_groups.len() == before {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "CacheParameterGroupNotFound",
                format!("CacheParameterGroup {name} not found."),
            ));
        }
        state.parameter_group_parameters.remove(&name);
        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml(
                "DeleteCacheParameterGroup",
                ELASTICACHE_NS,
                "",
                &request.request_id,
            ),
        ))
    }

    pub(super) async fn modify_cache_parameter_group(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = required_query_param(request, "CacheParameterGroupName")?;
        let updates = collect_indexed_pairs(
            request,
            "ParameterNameValues.member",
            "ParameterName",
            "ParameterValue",
        );
        {
            let mut accounts = self.state.write();
            let state = accounts.get_or_create(&request.account_id);
            if !state
                .parameter_groups
                .iter()
                .any(|g| g.cache_parameter_group_name == name)
            {
                return Err(AwsServiceError::aws_error(
                    StatusCode::NOT_FOUND,
                    "CacheParameterGroupNotFound",
                    format!("CacheParameterGroup {name} not found."),
                ));
            }
            let params = state
                .parameter_group_parameters
                .entry(name.clone())
                .or_default();
            for (param_name, value) in updates {
                if let Some(existing) = params.iter_mut().find(|p| p.parameter_name == param_name) {
                    existing.parameter_value = value;
                    existing.source = "user".to_string();
                } else {
                    params.push(crate::state::CacheParameter {
                        parameter_name: param_name,
                        parameter_value: value,
                        description: String::new(),
                        source: "user".to_string(),
                        data_type: "string".to_string(),
                        allowed_values: String::new(),
                        is_modifiable: true,
                        minimum_engine_version: String::new(),
                    });
                }
            }
        }
        self.apply_parameters_for_group(&request.account_id, &name)
            .await;
        let body = format!(
            "<CacheParameterGroupName>{}</CacheParameterGroupName>",
            xml_escape(&name)
        );
        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml(
                "ModifyCacheParameterGroup",
                ELASTICACHE_NS,
                &body,
                &request.request_id,
            ),
        ))
    }

    pub(super) fn reset_cache_parameter_group(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = required_query_param(request, "CacheParameterGroupName")?;
        let reset_all =
            parse_optional_bool(optional_query_param(request, "ResetAllParameters").as_deref())?
                .unwrap_or(false);
        let to_reset = collect_member_field(request, "ParameterNameValues.member", "ParameterName");
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request.account_id);
        if !state
            .parameter_groups
            .iter()
            .any(|g| g.cache_parameter_group_name == name)
        {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "CacheParameterGroupNotFound",
                format!("CacheParameterGroup {name} not found."),
            ));
        }
        if reset_all {
            state.parameter_group_parameters.remove(&name);
        } else if let Some(params) = state.parameter_group_parameters.get_mut(&name) {
            params.retain(|p| !to_reset.contains(&p.parameter_name));
        }
        let body = format!(
            "<CacheParameterGroupName>{}</CacheParameterGroupName>",
            xml_escape(&name)
        );
        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml(
                "ResetCacheParameterGroup",
                ELASTICACHE_NS,
                &body,
                &request.request_id,
            ),
        ))
    }

    pub(super) fn describe_cache_parameters(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = required_query_param(request, "CacheParameterGroupName")?;
        let max_records = optional_usize_param(request, "MaxRecords")?;
        let marker = optional_query_param(request, "Marker");

        let accounts = self.state.read();
        let empty = ElastiCacheState::new(&request.account_id, &request.region);
        let state = accounts.get(&request.account_id).unwrap_or(&empty);
        if !state
            .parameter_groups
            .iter()
            .any(|g| g.cache_parameter_group_name == name)
        {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "CacheParameterGroupNotFound",
                format!("CacheParameterGroup {name} not found."),
            ));
        }
        let params: Vec<&crate::state::CacheParameter> = state
            .parameter_group_parameters
            .get(&name)
            .map(|v| v.iter().collect())
            .unwrap_or_default();
        let (page, next_marker) = paginate(&params, marker.as_deref(), max_records);
        let members: String = page.iter().map(|p| cache_parameter_xml(p)).collect();
        let marker_xml = next_marker
            .map(|m| format!("<Marker>{}</Marker>", xml_escape(&m)))
            .unwrap_or_default();
        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml(
                "DescribeCacheParameters", ELASTICACHE_NS,
                &format!("<Parameters>{members}</Parameters><CacheNodeTypeSpecificParameters/>{marker_xml}"),
                &request.request_id,
            ),
        ))
    }
}
