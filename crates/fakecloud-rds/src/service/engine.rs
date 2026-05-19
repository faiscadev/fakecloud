//! RDS `engine` family extracted from service.rs by audit-2026-05-19.

use super::*;

impl RdsService {
    pub(super) fn describe_db_engine_versions(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let engine = optional_query_param(request, "Engine");
        let engine_version = optional_query_param(request, "EngineVersion");
        let family = optional_query_param(request, "DBParameterGroupFamily");
        let default_only =
            parse_optional_bool(optional_query_param(request, "DefaultOnly").as_deref())?;

        let mut versions = filter_engine_versions(
            &default_engine_versions(),
            &engine,
            &engine_version,
            &family,
        );

        if default_only.unwrap_or(false) {
            versions.truncate(1);
        }

        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml(
                "DescribeDBEngineVersions",
                RDS_NS,
                &format!(
                    "<DBEngineVersions>{}</DBEngineVersions>",
                    versions.iter().map(engine_version_xml).collect::<String>()
                ),
                &request.request_id,
            ),
        ))
    }
}
