//! `ApiGatewayV2Service` `management` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl ApiGatewayV2Service {
    pub(super) async fn handle_management_api(
        &self,
        req: AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        // API Gateway v2's REST-JSON wire format uses camelCase field names
        // (`name`, `protocolType`, …) via Smithy `@jsonName`. Some clients
        // (and our internal conformance probe) serialize with the raw
        // PascalCase member names instead. Normalize both shapes here so
        // every handler can read a single canonical lowercase-first form.
        let req = normalize_request_body_keys(req);
        let (action, api_id, resource_id) = Self::resolve_action(&req).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                format!("Unknown path: {}", req.raw_path),
            )
        })?;
        // Normalize invalid path-derived ids to None so handlers that
        // require an id reject the request instead of silently
        // operating on a placeholder. See extras::valid_path_id.
        let api_id = api_id.filter(|s| crate::extras::valid_path_id(s));
        let resource_id = resource_id.filter(|s| crate::extras::valid_path_id(s));
        let mutates = action.starts_with("Create")
            || action.starts_with("Update")
            || action.starts_with("Delete")
            || action.starts_with("Put")
            || action.starts_with("Tag")
            || action.starts_with("Untag")
            || action == "ImportApi"
            || action == "ReimportApi"
            || action == "DisablePortal"
            || action == "PreviewPortal"
            || action == "PublishPortal"
            || action == "ResetAuthorizersCache";

        let result = match action {
            "CreateApi" => self.create_api(&req),
            "GetApi" => self.get_api(&req, api_id.as_deref()),
            "GetApis" => self.get_apis(&req),
            "UpdateApi" => self.update_api(&req, api_id.as_deref()),
            "DeleteApi" => self.delete_api(&req, api_id.as_deref()),
            "CreateRoute" => self.create_route(&req, api_id.as_deref()),
            "GetRoute" => self.get_route(&req, api_id.as_deref(), resource_id.as_deref()),
            "GetRoutes" => self.get_routes(&req, api_id.as_deref()),
            "UpdateRoute" => self.update_route(&req, api_id.as_deref(), resource_id.as_deref()),
            "DeleteRoute" => self.delete_route(&req, api_id.as_deref(), resource_id.as_deref()),
            "CreateIntegration" => self.create_integration(&req, api_id.as_deref()),
            "GetIntegration" => {
                self.get_integration(&req, api_id.as_deref(), resource_id.as_deref())
            }
            "GetIntegrations" => self.get_integrations(&req, api_id.as_deref()),
            "UpdateIntegration" => {
                self.update_integration(&req, api_id.as_deref(), resource_id.as_deref())
            }
            "DeleteIntegration" => {
                self.delete_integration(&req, api_id.as_deref(), resource_id.as_deref())
            }
            "CreateStage" => self.create_stage(&req, api_id.as_deref()),
            "GetStage" => self.get_stage(&req, api_id.as_deref(), resource_id.as_deref()),
            "GetStages" => self.get_stages(&req, api_id.as_deref()),
            "UpdateStage" => self.update_stage(&req, api_id.as_deref(), resource_id.as_deref()),
            "DeleteStage" => self.delete_stage(&req, api_id.as_deref(), resource_id.as_deref()),
            "CreateDeployment" => self.create_deployment(&req, api_id.as_deref()),
            "GetDeployment" => self.get_deployment(&req, api_id.as_deref(), resource_id.as_deref()),
            "GetDeployments" => self.get_deployments(&req, api_id.as_deref()),
            "CreateAuthorizer" => self.create_authorizer(&req, api_id.as_deref()),
            "GetAuthorizer" => self.get_authorizer(&req, api_id.as_deref(), resource_id.as_deref()),
            "GetAuthorizers" => self.get_authorizers(&req, api_id.as_deref()),
            "UpdateAuthorizer" => {
                self.update_authorizer(&req, api_id.as_deref(), resource_id.as_deref())
            }
            "DeleteAuthorizer" => {
                self.delete_authorizer(&req, api_id.as_deref(), resource_id.as_deref())
            }
            other => {
                self.handle_extra_action(other, &req, api_id.as_deref(), resource_id.as_deref())
            }
        };
        if mutates && matches!(result.as_ref(), Ok(resp) if resp.status.is_success()) {
            self.save_snapshot().await;
        }
        result
    }
}
