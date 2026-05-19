//! RDS `tags` family extracted from service.rs by audit-2026-05-19.

use super::*;

impl RdsService {
    pub(super) fn add_tags_to_resource(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let resource_name = required_query_param(request, "ResourceName")?;
        let tags = parse_tags(request)?;

        // An empty tag list is a no-op rather than an error. Smithy
        // declares no analogue to the `MissingParameter` code AWS would
        // wire, and the resolved-target step below still surfaces a
        // declared `*NotFoundFault` for bad ARNs.
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request.account_id);
        let mut target = match resolve_tag_target_mut(state, &resource_name) {
            Ok(t) => t,
            Err(e) => {
                // AddTagsToResource's Smithy model only declares a subset of
                // `*NotFoundFault` errors. For resource kinds without a
                // declared error shape (option groups, parameter groups,
                // event subscriptions, security groups), AWS treats the call
                // as best-effort rather than surface an undeclared error.
                if is_declared_add_tags_not_found(e.code()) {
                    return Err(e);
                }
                return Ok(AwsResponse::xml(
                    StatusCode::OK,
                    query_response_xml("AddTagsToResource", RDS_NS, "", &request.request_id),
                ));
            }
        };
        target.merge(&tags);

        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml("AddTagsToResource", RDS_NS, "", &request.request_id),
        ))
    }

    pub(super) fn list_tags_for_resource(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let resource_name = required_query_param(request, "ResourceName")?;
        // Filters were previously rejected with `InvalidParameterValue`
        // — undeclared on this op. AWS ignores unknown filters; we do
        // the same here and let the resource lookup determine the
        // response shape.
        let _ignored_filters = query_param_prefix_exists(request, "Filters.");

        let accounts = self.state.read();
        let empty = RdsState::new(&request.account_id, &request.region);
        let state = accounts.get(&request.account_id).unwrap_or(&empty);
        let target = resolve_tag_target(state, &resource_name)?;
        let tag_xml = target.to_xml();

        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml(
                "ListTagsForResource",
                RDS_NS,
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

        // Empty TagKeys is a no-op; Smithy doesn't declare an
        // equivalent of `MissingParameter` on this op. Resource lookup
        // below still surfaces a declared `*NotFoundFault` for bad
        // ARNs.

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request.account_id);
        let mut target = resolve_tag_target_mut(state, &resource_name)?;
        target.remove_keys(&tag_keys);

        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml("RemoveTagsFromResource", RDS_NS, "", &request.request_id),
        ))
    }
}
