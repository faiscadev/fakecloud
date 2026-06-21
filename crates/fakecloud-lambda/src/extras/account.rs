//! `LambdaService` `account` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl LambdaService {
    pub(crate) async fn handle_extra(
        &self,
        action: &str,
        resource: Option<&str>,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let aid = req.account_id.as_str();
        let res = resource.unwrap_or("");
        match action {
            // Function lifecycle extras
            "GetFunctionConfiguration" => self.get_function_configuration(res, aid, req),
            "UpdateFunctionConfiguration" => self.update_function_configuration(res, req),
            "UpdateFunctionCode" => self.update_function_code(res, req),
            "UpdateEventSourceMapping" => self.update_event_source_mapping_handler(res, req),
            "GetAccountSettings" => self.get_account_settings(aid),
            "InvokeAsync" => Ok(AwsResponse::json(StatusCode::ACCEPTED, "{}".to_string())),
            "InvokeWithResponseStream" => self.invoke_with_response_stream(res, aid, req).await,

            // Versions
            "ListVersionsByFunction" => self.list_versions_by_function(res, aid, req),

            // Aliases
            "CreateAlias" => self.create_alias(res, req),
            "GetAlias" => self.get_alias(res, req),
            "ListAliases" => self.list_aliases(res, aid, req),
            "UpdateAlias" => self.update_alias(res, req),
            "DeleteAlias" => self.delete_alias(res, req),

            // Layers
            "PublishLayerVersion" => self.publish_layer_version(res, req),
            "GetLayerVersion" => self.get_layer_version(req),
            "GetLayerVersionByArn" => self.get_layer_version_by_arn(req),
            "ListLayers" => {
                validate_layer_filters(req)?;
                self.list_layers(aid, req)
            }
            "ListLayerVersions" => {
                validate_layer_filters(req)?;
                if res.is_empty() {
                    return Err(missing("LayerName"));
                }
                // Smithy `LayerName.length 1..140`; ARN form is longer
                // (~200) but the probe drives the bare-name path.
                let limit = if res.starts_with("arn:") { 200 } else { 140 };
                if res.chars().count() > limit {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "InvalidParameterValueException",
                        "LayerName exceeds the 140-character maximum",
                    ));
                }
                self.list_layer_versions(res, aid, req)
            }
            "DeleteLayerVersion" => self.delete_layer_version(req),
            "GetLayerVersionPolicy" => self.get_layer_version_policy(req),
            "AddLayerVersionPermission" => self.add_layer_version_permission(req),
            "RemoveLayerVersionPermission" => self.remove_layer_version_permission(req),

            // Function URL
            "CreateFunctionUrlConfig" => self.create_function_url_config(res, req),
            "GetFunctionUrlConfig" => self.get_function_url_config(res, aid),
            "UpdateFunctionUrlConfig" => self.update_function_url_config(res, req),
            "DeleteFunctionUrlConfig" => self.delete_function_url_config(res, aid),
            "ListFunctionUrlConfigs" => self.list_function_url_configs(aid),

            // Concurrency
            "PutFunctionConcurrency" => self.put_function_concurrency(res, req),
            "GetFunctionConcurrency" => self.get_function_concurrency(res, aid),
            "DeleteFunctionConcurrency" => self.delete_function_concurrency(res, aid),
            "PutProvisionedConcurrencyConfig" => self.put_provisioned_concurrency(res, req),
            "GetProvisionedConcurrencyConfig" => self.get_provisioned_concurrency(res, req),
            "DeleteProvisionedConcurrencyConfig" => self.delete_provisioned_concurrency(res, req),
            "ListProvisionedConcurrencyConfigs" => self.list_provisioned_concurrency(res, aid),

            // Code signing
            "CreateCodeSigningConfig" => self.create_code_signing_config(req),
            "GetCodeSigningConfig" => self.get_code_signing_config(res, aid),
            "UpdateCodeSigningConfig" => self.update_code_signing_config(res, req),
            "DeleteCodeSigningConfig" => self.delete_code_signing_config(res, aid),
            "ListCodeSigningConfigs" => self.list_code_signing_configs(aid),
            "PutFunctionCodeSigningConfig" => self.put_function_code_signing(res, req),
            "GetFunctionCodeSigningConfig" => self.get_function_code_signing(res, aid),
            "DeleteFunctionCodeSigningConfig" => self.delete_function_code_signing(res, aid),
            "ListFunctionsByCodeSigningConfig" => self.list_functions_by_code_signing(res, aid),

            // Event invoke
            "PutFunctionEventInvokeConfig" | "UpdateFunctionEventInvokeConfig" => {
                self.put_function_event_invoke(res, req)
            }
            "GetFunctionEventInvokeConfig" => self.get_function_event_invoke(res, req),
            "DeleteFunctionEventInvokeConfig" => self.delete_function_event_invoke(res, req),
            "ListFunctionEventInvokeConfigs" => self.list_function_event_invoke(res, aid),

            // Runtime management
            "PutRuntimeManagementConfig" => self.put_runtime_management(res, req),
            "GetRuntimeManagementConfig" => self.get_runtime_management(res, req),

            // Scaling
            "PutFunctionScalingConfig" => self.put_scaling_config(res, req),
            "GetFunctionScalingConfig" => {
                require_qualifier(req)?;
                self.get_scaling_config(res, aid)
            }

            // Recursion
            "PutFunctionRecursionConfig" => self.put_recursion_config(res, req),
            "GetFunctionRecursionConfig" => self.get_recursion_config(res, aid),

            // Tags
            "TagResource" => self.tag_resource(res, req),
            "UntagResource" => self.untag_resource(res, req),
            "ListTags" => self.list_tags(res, aid),

            _ => Err(AwsServiceError::action_not_implemented("lambda", action)),
        }
    }

    pub(super) fn get_account_settings(
        &self,
        account_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(account_id);
        let settings = state.account_settings.clone().unwrap_or(AccountSettings {
            concurrent_executions: 1000,
            code_size_zipped: 52_428_800,
            code_size_unzipped: 262_144_000,
            total_code_size: 80_530_636_800,
        });
        if state.account_settings.is_none() {
            state.account_settings = Some(settings.clone());
        }
        // Real AccountUsage so clients monitoring deployment quotas see
        // accurate numbers. AWS sums total code size across all functions.
        let function_count = state.functions.len() as i64;
        let total_code_size: i64 = state.functions.values().map(|f| f.code_size).sum();
        ok(json!({
            "AccountLimit": {
                "ConcurrentExecutions": settings.concurrent_executions,
                "CodeSizeZipped": settings.code_size_zipped,
                "CodeSizeUnzipped": settings.code_size_unzipped,
                "TotalCodeSize": settings.total_code_size,
                "UnreservedConcurrentExecutions": settings.concurrent_executions,
            },
            "AccountUsage": {
                "TotalCodeSize": total_code_size,
                "FunctionCount": function_count,
            },
        }))
    }
}
