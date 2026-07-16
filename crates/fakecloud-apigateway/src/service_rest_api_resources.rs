// Auto-extracted from service.rs as part of carryover service.rs split.

#![allow(clippy::too_many_arguments)]

use serde_json::{json, Value};
use std::collections::BTreeMap;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use super::*;

impl ApiGatewayService {
    pub(super) async fn handle_control(
        &self,
        req: &AwsRequest,
        ResolvedAction { action, params }: ResolvedAction,
    ) -> Result<AwsResponse, AwsServiceError> {
        // Centralized Smithy-aligned prevalidation. Surfaces the
        // declared `BadRequestException` shape for missing required
        // body fields, missing/placeholder path labels, missing
        // required query parameters, and invalid enum values before
        // any per-handler logic runs.
        crate::validation::prevalidate(action, req, &params)?;
        match action {
            "GetAccount" => self.get_account(req),
            "UpdateAccount" => self.update_account(req),
            "CreateRestApi" => self.create_rest_api(req),
            "GetRestApi" => self.get_rest_api(req, &params),
            "GetRestApis" => self.get_rest_apis(req),
            "DeleteRestApi" => self.delete_rest_api(req, &params),
            "UpdateRestApi" => self.update_rest_api(req, &params),
            "PutRestApi" => self.put_rest_api(req, &params),
            "ImportRestApi" => self.import_rest_api(req),
            "CreateResource" => self.create_resource(req, &params),
            "GetResource" => self.get_resource(req, &params),
            "GetResources" => self.get_resources(req, &params),
            "DeleteResource" => self.delete_resource(req, &params),
            "UpdateResource" => self.update_resource(req, &params),
            "PutMethod" => self.put_method(req, &params),
            "GetMethod" => self.get_method(req, &params),
            "DeleteMethod" => self.delete_method(req, &params),
            "UpdateMethod" => self.update_method(req, &params),
            "PutMethodResponse" => self.put_method_response(req, &params),
            "GetMethodResponse" => self.get_method_response(req, &params),
            "DeleteMethodResponse" => self.delete_method_response(req, &params),
            "UpdateMethodResponse" => self.update_method_response(req, &params),
            "PutIntegration" => self.put_integration(req, &params),
            "GetIntegration" => self.get_integration(req, &params),
            "DeleteIntegration" => self.delete_integration(req, &params),
            "UpdateIntegration" => self.update_integration(req, &params),
            "PutIntegrationResponse" => self.put_integration_response(req, &params),
            "GetIntegrationResponse" => self.get_integration_response(req, &params),
            "DeleteIntegrationResponse" => self.delete_integration_response(req, &params),
            "UpdateIntegrationResponse" => self.update_integration_response(req, &params),
            "TestInvokeMethod" => self.test_invoke_method(req, &params).await,
            "TestInvokeAuthorizer" => self.test_invoke_authorizer(req, &params).await,
            "CreateDeployment" => self.create_deployment(req, &params),
            "GetDeployment" => self.get_deployment(req, &params),
            "GetDeployments" => self.get_deployments(req, &params),
            "DeleteDeployment" => self.delete_deployment(req, &params),
            "UpdateDeployment" => self.update_deployment(req, &params),
            "CreateStage" => self.create_stage(req, &params),
            "GetStage" => self.get_stage(req, &params),
            "GetStages" => self.get_stages(req, &params),
            "DeleteStage" => self.delete_stage(req, &params),
            "UpdateStage" => self.update_stage(req, &params),
            "FlushStageCache" => ok_no_content(),
            "FlushStageAuthorizersCache" => ok_no_content(),
            "CreateModel" => self.create_model(req, &params),
            "GetModel" => self.get_model(req, &params),
            "GetModels" => self.get_models(req, &params),
            "DeleteModel" => self.delete_model(req, &params),
            "UpdateModel" => self.update_model(req, &params),
            "GetModelTemplate" => self.get_model_template(req, &params),
            "CreateRequestValidator" => self.create_request_validator(req, &params),
            "GetRequestValidator" => self.get_request_validator(req, &params),
            "GetRequestValidators" => self.get_request_validators(req, &params),
            "DeleteRequestValidator" => self.delete_request_validator(req, &params),
            "UpdateRequestValidator" => self.update_request_validator(req, &params),
            "CreateAuthorizer" => self.create_authorizer(req, &params),
            "GetAuthorizer" => self.get_authorizer(req, &params),
            "GetAuthorizers" => self.get_authorizers(req, &params),
            "DeleteAuthorizer" => self.delete_authorizer(req, &params),
            "UpdateAuthorizer" => self.update_authorizer(req, &params),
            "CreateApiKey" => self.create_api_key(req),
            "GetApiKey" => self.get_api_key(req, &params),
            "GetApiKeys" => self.get_api_keys(req),
            "DeleteApiKey" => self.delete_api_key(req, &params),
            "UpdateApiKey" => self.update_api_key(req, &params),
            "CreateUsagePlan" => self.create_usage_plan(req),
            "GetUsagePlan" => self.get_usage_plan(req, &params),
            "GetUsagePlans" => self.get_usage_plans(req),
            "DeleteUsagePlan" => self.delete_usage_plan(req, &params),
            "UpdateUsagePlan" => self.update_usage_plan(req, &params),
            "CreateUsagePlanKey" => self.create_usage_plan_key(req, &params),
            "GetUsagePlanKey" => self.get_usage_plan_key(req, &params),
            "GetUsagePlanKeys" => self.get_usage_plan_keys(req, &params),
            "DeleteUsagePlanKey" => self.delete_usage_plan_key(req, &params),
            "GetUsage" => self.get_usage(req, &params),
            "UpdateUsage" => self.update_usage(req, &params),
            "CreateVpcLink" => self.create_vpc_link(req),
            "GetVpcLink" => self.get_vpc_link(req, &params),
            "GetVpcLinks" => self.get_vpc_links(req),
            "DeleteVpcLink" => self.delete_vpc_link(req, &params),
            "UpdateVpcLink" => self.update_vpc_link(req, &params),
            "CreateDomainName" => self.create_domain_name(req),
            "GetDomainName" => self.get_domain_name(req, &params),
            "GetDomainNames" => self.get_domain_names(req),
            "DeleteDomainName" => self.delete_domain_name(req, &params),
            "UpdateDomainName" => self.update_domain_name(req, &params),
            "CreateDomainNameAccessAssociation" => self.create_dnaa(req),
            "GetDomainNameAccessAssociations" => self.get_dnaas(req),
            "DeleteDomainNameAccessAssociation" => self.delete_dnaa(req, &params),
            "RejectDomainNameAccessAssociation" => self.reject_dnaa(req, &params),
            "ImportApiKeys" => self.import_api_keys(req),
            "ImportDocumentationParts" => self.import_documentation_parts(req, &params),
            "CreateBasePathMapping" => self.create_base_path_mapping(req, &params),
            "GetBasePathMapping" => self.get_base_path_mapping(req, &params),
            "GetBasePathMappings" => self.get_base_path_mappings(req, &params),
            "DeleteBasePathMapping" => self.delete_base_path_mapping(req, &params),
            "UpdateBasePathMapping" => self.update_base_path_mapping(req, &params),
            "GenerateClientCertificate" => self.generate_client_cert(req),
            "GetClientCertificate" => self.get_client_cert(req, &params),
            "GetClientCertificates" => self.get_client_certs(req),
            "DeleteClientCertificate" => self.delete_client_cert(req, &params),
            "UpdateClientCertificate" => self.update_client_cert(req, &params),
            "CreateDocumentationPart" => self.create_doc_part(req, &params),
            "GetDocumentationPart" => self.get_doc_part(req, &params),
            "GetDocumentationParts" => self.get_doc_parts(req, &params),
            "DeleteDocumentationPart" => self.delete_doc_part(req, &params),
            "UpdateDocumentationPart" => self.update_doc_part(req, &params),
            "CreateDocumentationVersion" => self.create_doc_version(req, &params),
            "GetDocumentationVersion" => self.get_doc_version(req, &params),
            "GetDocumentationVersions" => self.get_doc_versions(req, &params),
            "DeleteDocumentationVersion" => self.delete_doc_version(req, &params),
            "UpdateDocumentationVersion" => self.update_doc_version(req, &params),
            "PutGatewayResponse" => self.put_gateway_response(req, &params),
            "GetGatewayResponse" => self.get_gateway_response(req, &params),
            "GetGatewayResponses" => self.get_gateway_responses(req, &params),
            "DeleteGatewayResponse" => self.delete_gateway_response(req, &params),
            "UpdateGatewayResponse" => self.update_gateway_response(req, &params),
            "GetExport" => self.get_export(req, &params),
            "GetSdk" => self.get_sdk(req, &params),
            "GetSdkType" => self.get_sdk_type(&params),
            "GetSdkTypes" => ok(json!({ "item": sdk_types() })),
            "TagResource" => self.tag_resource(req, &params),
            "UntagResource" => self.untag_resource(req, &params),
            "GetTags" => self.get_tags(req, &params),
            _ => Err(AwsServiceError::ActionNotImplemented {
                service: "apigateway".to_string(),
                action: action.to_string(),
            }),
        }
    }

    // ── Account ──

    pub(super) fn get_account(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request_account(req));
        ok(state.account_settings.clone())
    }

    pub(super) fn update_account(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request_account(req));
        // UpdateAccount input is { patchOperations: [...] }; the Account
        // output shape does not include patchOperations. The old code only
        // copied (non-existent) top-level body fields and so ignored the patch
        // ops entirely -- cloudwatchRoleArn could never be set (bug-audit
        // 2026-06-20, 1.21). Apply each op to account_settings instead.
        apply_patch_operations(req, |op, path, value| {
            if op != "replace" && op != "add" && op != "remove" {
                return;
            }
            let key = path.trim_start_matches('/').to_string();
            if key.is_empty() {
                return;
            }
            if let Some(obj) = state.account_settings.as_object_mut() {
                if op == "remove" {
                    obj.remove(&key);
                } else {
                    obj.insert(key, value.clone());
                }
            }
        });
        ok(state.account_settings.clone())
    }

    // ── REST APIs ──

    pub(super) fn create_rest_api(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = body
            .get("name")
            .and_then(Value::as_str)
            .ok_or_else(|| bad_request("name is required"))?
            .to_string();
        let id = make_id();
        let root_id = make_id();
        let api = RestApi {
            id: id.clone(),
            name,
            description: body
                .get("description")
                .and_then(Value::as_str)
                .map(String::from),
            version: body
                .get("version")
                .and_then(Value::as_str)
                .map(String::from),
            created_date: chrono::Utc::now(),
            api_key_source: body
                .get("apiKeySource")
                .and_then(Value::as_str)
                .unwrap_or("HEADER")
                .to_string(),
            endpoint_configuration: body
                .get("endpointConfiguration")
                .cloned()
                .unwrap_or_else(|| json!({"types": ["EDGE"]})),
            policy: body.get("policy").and_then(Value::as_str).map(String::from),
            binary_media_types: body
                .get("binaryMediaTypes")
                .and_then(Value::as_array)
                .map(|arr| {
                    arr.iter()
                        .filter_map(Value::as_str)
                        .map(String::from)
                        .collect()
                })
                .unwrap_or_default(),
            minimum_compression_size: body.get("minimumCompressionSize").and_then(Value::as_i64),
            disable_execute_api_endpoint: body
                .get("disableExecuteApiEndpoint")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            root_resource_id: root_id.clone(),
            tags: tags_from(&body),
            import_source: None,
        };
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request_account(req));
        state.apis.insert(id.clone(), api.clone());
        // The root resource exists implicitly with path "/".
        let root = Resource {
            id: root_id,
            parent_id: None,
            path_part: None,
            path: "/".to_string(),
        };
        let mut res_map = BTreeMap::new();
        res_map.insert(root.id.clone(), root);
        state.resources.insert(id, res_map);
        ok_status(StatusCode::CREATED, rest_api_to_json(&api))
    }

    pub(super) fn get_rest_api(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = params.get("restApiId").cloned().unwrap_or_default();
        let accounts = self.state.read();
        let state = accounts
            .get(&request_account(req))
            .ok_or_else(|| not_found(format!("RestApi {id} not found")))?;
        let api = state
            .apis
            .get(&id)
            .ok_or_else(|| not_found(format!("RestApi {id} not found")))?;
        ok(rest_api_to_json(api))
    }

    pub(super) fn get_rest_apis(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let items: Vec<Value> = match accounts.get(&request_account(req)) {
            Some(state) => state.apis.values().map(rest_api_to_json).collect(),
            None => Vec::new(),
        };
        ok(json!({"item": items}))
    }

    pub(super) fn delete_rest_api(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = params.get("restApiId").cloned().unwrap_or_default();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request_account(req));
        if state.apis.remove(&id).is_none() {
            return Err(not_found(format!("RestApi {id} not found")));
        }
        state.resources.remove(&id);
        state.deployments.remove(&id);
        state.stages.remove(&id);
        state.models.remove(&id);
        state.request_validators.remove(&id);
        state.authorizers.remove(&id);
        state.documentation_parts.remove(&id);
        state.documentation_versions.remove(&id);
        state.gateway_responses.remove(&id);
        state
            .methods
            .retain(|k, _| !k.starts_with(&format!("{id}/")));
        state
            .integrations
            .retain(|k, _| !k.starts_with(&format!("{id}/")));
        state
            .method_responses
            .retain(|k, _| !k.starts_with(&format!("{id}/")));
        state
            .integration_responses
            .retain(|k, _| !k.starts_with(&format!("{id}/")));
        ok_no_content()
    }

    pub(super) fn update_rest_api(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = params.get("restApiId").cloned().unwrap_or_default();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request_account(req));
        let api = state
            .apis
            .get_mut(&id)
            .ok_or_else(|| not_found(format!("RestApi {id} not found")))?;
        apply_patch_operations(req, |op, path, value| {
            apply_rest_api_patch(api, op, path, value);
        });
        ok(rest_api_to_json(api))
    }

    pub(super) fn put_rest_api(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = params.get("restApiId").cloned().unwrap_or_default();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request_account(req));
        let api = state
            .apis
            .get_mut(&id)
            .ok_or_else(|| not_found(format!("RestApi {id} not found")))?;
        api.import_source = Some(String::from_utf8_lossy(&req.body).to_string());
        ok(rest_api_to_json(api))
    }

    pub(super) fn import_rest_api(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let id = make_id();
        let root_id = make_id();
        let body = serde_json::from_slice::<serde_json::Value>(&req.body).unwrap_or(Value::Null);
        let binary_media_types = body
            .get("binaryMediaTypes")
            .and_then(Value::as_array)
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        let api = RestApi {
            id: id.clone(),
            name: format!("imported-{id}"),
            description: None,
            version: None,
            created_date: chrono::Utc::now(),
            api_key_source: "HEADER".to_string(),
            endpoint_configuration: json!({"types": ["EDGE"]}),
            policy: None,
            binary_media_types,
            minimum_compression_size: None,
            disable_execute_api_endpoint: false,
            root_resource_id: root_id.clone(),
            tags: BTreeMap::new(),
            import_source: Some(String::from_utf8_lossy(&req.body).to_string()),
        };
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request_account(req));
        state.apis.insert(id.clone(), api.clone());
        let mut res_map = BTreeMap::new();
        res_map.insert(
            root_id.clone(),
            Resource {
                id: root_id,
                parent_id: None,
                path_part: None,
                path: "/".to_string(),
            },
        );
        state.resources.insert(id, res_map);
        ok_status(StatusCode::CREATED, rest_api_to_json(&api))
    }

    // ── Resources ──

    pub(super) fn create_resource(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let api_id = params.get("restApiId").cloned().unwrap_or_default();
        let parent_id = params.get("parentId").cloned().unwrap_or_default();
        let body = req.json_body();
        let path_part = body
            .get("pathPart")
            .and_then(Value::as_str)
            .ok_or_else(|| bad_request("pathPart is required"))?
            .to_string();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request_account(req));
        if !state.apis.contains_key(&api_id) {
            return Err(not_found(format!("RestApi {api_id} not found")));
        }
        let resources = state.resources.entry(api_id.clone()).or_default();
        let parent = resources
            .get(&parent_id)
            .cloned()
            .ok_or_else(|| not_found(format!("Resource {parent_id} not found")))?;
        let path = if parent.path == "/" {
            format!("/{path_part}")
        } else {
            format!("{}/{path_part}", parent.path)
        };
        // Reject duplicate.
        if resources.values().any(|r| r.path == path) {
            return Err(conflict(format!("Resource at {path} already exists")));
        }
        let id = make_id();
        let resource = Resource {
            id: id.clone(),
            parent_id: Some(parent_id),
            path_part: Some(path_part),
            path,
        };
        resources.insert(id, resource.clone());
        ok_status(
            StatusCode::CREATED,
            resource_to_json(&resource, BTreeMap::new()),
        )
    }

    pub(super) fn get_resource(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let api_id = params.get("restApiId").cloned().unwrap_or_default();
        let id = params.get("resourceId").cloned().unwrap_or_default();
        let accounts = self.state.read();
        let state = accounts
            .get(&request_account(req))
            .ok_or_else(|| not_found(format!("RestApi {api_id} not found")))?;
        let resources = state
            .resources
            .get(&api_id)
            .ok_or_else(|| not_found(format!("RestApi {api_id} not found")))?;
        let r = resources
            .get(&id)
            .ok_or_else(|| not_found(format!("Resource {id} not found")))?;
        let methods = methods_for_resource(state, &api_id, &id);
        ok(resource_to_json(r, methods))
    }

    pub(super) fn get_resources(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let api_id = params.get("restApiId").cloned().unwrap_or_default();
        let accounts = self.state.read();
        let state = accounts
            .get(&request_account(req))
            .ok_or_else(|| not_found(format!("RestApi {api_id} not found")))?;
        let resources = state
            .resources
            .get(&api_id)
            .ok_or_else(|| not_found(format!("RestApi {api_id} not found")))?;
        let items: Vec<Value> = resources
            .values()
            .map(|r| resource_to_json(r, methods_for_resource(state, &api_id, &r.id)))
            .collect();
        ok(json!({"item": items}))
    }

    pub(super) fn delete_resource(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let api_id = params.get("restApiId").cloned().unwrap_or_default();
        let id = params.get("resourceId").cloned().unwrap_or_default();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request_account(req));
        let resources = state
            .resources
            .get_mut(&api_id)
            .ok_or_else(|| not_found(format!("RestApi {api_id} not found")))?;
        if resources.remove(&id).is_none() {
            return Err(not_found(format!("Resource {id} not found")));
        }
        let prefix = format!("{api_id}/{id}/");
        state.methods.retain(|k, _| !k.starts_with(&prefix));
        state.integrations.retain(|k, _| !k.starts_with(&prefix));
        state
            .method_responses
            .retain(|k, _| !k.starts_with(&prefix));
        state
            .integration_responses
            .retain(|k, _| !k.starts_with(&prefix));
        ok_no_content()
    }

    pub(super) fn update_resource(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let api_id = params.get("restApiId").cloned().unwrap_or_default();
        let id = params.get("resourceId").cloned().unwrap_or_default();
        // Collect the mutations up front so we can borrow the whole resource map
        // afterwards (renames/moves require recomputing descendant paths).
        let mut new_path_part: Option<String> = None;
        let mut new_parent_id: Option<String> = None;
        apply_patch_operations(req, |op, path, value| match (op, path) {
            ("replace", "/pathPart") => {
                if let Some(s) = value.as_str() {
                    new_path_part = Some(s.to_string());
                }
            }
            // `move` carries the new parent in `value` (JSON pointer form),
            // `replace` carries it directly. Accept both.
            ("move", "/parentId") | ("replace", "/parentId") => {
                if let Some(s) = value.as_str() {
                    new_parent_id = Some(s.trim_start_matches('/').to_string());
                }
            }
            _ => {}
        });
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request_account(req));
        let resources = state
            .resources
            .get_mut(&api_id)
            .ok_or_else(|| not_found(format!("RestApi {api_id} not found")))?;
        if !resources.contains_key(&id) {
            return Err(not_found(format!("Resource {id} not found")));
        }
        // Validate a requested reparent target before mutating anything.
        if let Some(pid) = &new_parent_id {
            if !resources.contains_key(pid) {
                return Err(not_found(format!("Resource {pid} not found")));
            }
            if is_self_or_descendant(resources, &id, pid) {
                return Err(bad_request(
                    "Invalid parentId: cannot move a resource under itself or its descendant",
                ));
            }
        }
        // Snapshot the pre-mutation identity so a colliding change can roll back.
        let (prev_path_part, prev_parent_id) = {
            let r = resources.get(&id).expect("presence checked above");
            (r.path_part.clone(), r.parent_id.clone())
        };
        {
            let resource = resources.get_mut(&id).expect("presence checked above");
            if let Some(pp) = new_path_part {
                resource.path_part = Some(pp);
            }
            if let Some(pid) = new_parent_id {
                resource.parent_id = Some(pid);
            }
        }
        // Refresh stored paths for the whole tree (target + descendants).
        recompute_resource_paths(resources);
        // Reject a rename/reparent that collides with a sibling path (matches
        // CreateResource, which rejects duplicates). On collision, roll back.
        let new_path = resources[&id].path.clone();
        if resources
            .iter()
            .any(|(rid, r)| rid != &id && r.path == new_path)
        {
            // Revert this resource's identity to the pre-mutation snapshot,
            // then refresh paths so no partial change is persisted.
            if let Some(r) = resources.get_mut(&id) {
                r.path_part = prev_path_part;
                r.parent_id = prev_parent_id;
            }
            recompute_resource_paths(resources);
            return Err(conflict(format!(
                "Another resource with the same parent already has this name: {new_path}"
            )));
        }
        let resource = resources.get(&id).expect("presence checked above").clone();
        let methods = methods_for_resource(state, &api_id, &id);
        ok(resource_to_json(&resource, methods))
    }

    // ── Methods ──

    pub(super) fn put_method(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let api_id = params.get("restApiId").cloned().unwrap_or_default();
        let res_id = params.get("resourceId").cloned().unwrap_or_default();
        let http_method = params.get("httpMethod").cloned().unwrap_or_default();
        let body = req.json_body();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request_account(req));
        let m = ApiMethod {
            rest_api_id: api_id.clone(),
            resource_id: res_id.clone(),
            http_method: http_method.to_uppercase(),
            authorization_type: body
                .get("authorizationType")
                .and_then(Value::as_str)
                .unwrap_or("NONE")
                .to_string(),
            authorizer_id: body
                .get("authorizerId")
                .and_then(Value::as_str)
                .map(String::from),
            api_key_required: body
                .get("apiKeyRequired")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            operation_name: body
                .get("operationName")
                .and_then(Value::as_str)
                .map(String::from),
            request_parameters: body
                .get("requestParameters")
                .and_then(Value::as_object)
                .map(|m| {
                    m.iter()
                        .filter_map(|(k, v)| v.as_bool().map(|b| (k.clone(), b)))
                        .collect()
                })
                .unwrap_or_default(),
            request_models: body
                .get("requestModels")
                .and_then(Value::as_object)
                .map(|m| {
                    m.iter()
                        .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                        .collect()
                })
                .unwrap_or_default(),
            request_validator_id: body
                .get("requestValidatorId")
                .and_then(Value::as_str)
                .map(String::from),
            authorization_scopes: body
                .get("authorizationScopes")
                .and_then(Value::as_array)
                .map(|arr| {
                    arr.iter()
                        .filter_map(Value::as_str)
                        .map(String::from)
                        .collect()
                })
                .unwrap_or_default(),
        };
        state
            .methods
            .insert(method_key(&api_id, &res_id, &http_method), m.clone());
        ok_status(StatusCode::CREATED, method_to_json(&m))
    }

    pub(super) fn get_method(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let key = method_key(
            params.get("restApiId").map(|s| s.as_str()).unwrap_or(""),
            params.get("resourceId").map(|s| s.as_str()).unwrap_or(""),
            params.get("httpMethod").map(|s| s.as_str()).unwrap_or(""),
        );
        let accounts = self.state.read();
        let state = accounts
            .get(&request_account(req))
            .ok_or_else(|| not_found("Method not found"))?;
        let m = state
            .methods
            .get(&key)
            .ok_or_else(|| not_found("Method not found"))?;
        ok(method_to_json(m))
    }

    pub(super) fn delete_method(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let key = method_key(
            params.get("restApiId").map(|s| s.as_str()).unwrap_or(""),
            params.get("resourceId").map(|s| s.as_str()).unwrap_or(""),
            params.get("httpMethod").map(|s| s.as_str()).unwrap_or(""),
        );
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request_account(req));
        if state.methods.remove(&key).is_none() {
            return Err(not_found("Method not found"));
        }
        state.integrations.remove(&key);
        let prefix = format!("{key}/");
        state
            .method_responses
            .retain(|k, _| !k.starts_with(&prefix));
        state
            .integration_responses
            .retain(|k, _| !k.starts_with(&prefix));
        ok_no_content()
    }

    pub(super) fn update_method(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let key = method_key(
            params.get("restApiId").map(|s| s.as_str()).unwrap_or(""),
            params.get("resourceId").map(|s| s.as_str()).unwrap_or(""),
            params.get("httpMethod").map(|s| s.as_str()).unwrap_or(""),
        );
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request_account(req));
        let m = state
            .methods
            .get_mut(&key)
            .ok_or_else(|| not_found("Method not found"))?;
        apply_patch_operations(req, |op, path, value| {
            apply_method_patch(m, op, path, value);
        });
        ok(method_to_json(m))
    }

    pub(super) fn put_method_response(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let key = response_key(
            params.get("restApiId").map(|s| s.as_str()).unwrap_or(""),
            params.get("resourceId").map(|s| s.as_str()).unwrap_or(""),
            params.get("httpMethod").map(|s| s.as_str()).unwrap_or(""),
            params.get("statusCode").map(|s| s.as_str()).unwrap_or(""),
        );
        let mut payload = req.json_body();
        if let Some(o) = payload.as_object_mut() {
            o.insert(
                "statusCode".to_string(),
                json!(params.get("statusCode").cloned().unwrap_or_default()),
            );
        }
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request_account(req));
        state.method_responses.insert(key, payload.clone());
        ok_status(StatusCode::CREATED, payload)
    }

    pub(super) fn get_method_response(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let key = response_key(
            params.get("restApiId").map(|s| s.as_str()).unwrap_or(""),
            params.get("resourceId").map(|s| s.as_str()).unwrap_or(""),
            params.get("httpMethod").map(|s| s.as_str()).unwrap_or(""),
            params.get("statusCode").map(|s| s.as_str()).unwrap_or(""),
        );
        let accounts = self.state.read();
        let state = accounts
            .get(&request_account(req))
            .ok_or_else(|| not_found("MethodResponse not found"))?;
        let v = state
            .method_responses
            .get(&key)
            .cloned()
            .ok_or_else(|| not_found("MethodResponse not found"))?;
        ok(v)
    }

    pub(super) fn delete_method_response(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let key = response_key(
            params.get("restApiId").map(|s| s.as_str()).unwrap_or(""),
            params.get("resourceId").map(|s| s.as_str()).unwrap_or(""),
            params.get("httpMethod").map(|s| s.as_str()).unwrap_or(""),
            params.get("statusCode").map(|s| s.as_str()).unwrap_or(""),
        );
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request_account(req));
        if state.method_responses.remove(&key).is_none() {
            return Err(not_found("MethodResponse not found"));
        }
        ok_no_content()
    }

    pub(super) fn update_method_response(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let key = response_key(
            params.get("restApiId").map(|s| s.as_str()).unwrap_or(""),
            params.get("resourceId").map(|s| s.as_str()).unwrap_or(""),
            params.get("httpMethod").map(|s| s.as_str()).unwrap_or(""),
            params.get("statusCode").map(|s| s.as_str()).unwrap_or(""),
        );
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request_account(req));
        let v = state
            .method_responses
            .get_mut(&key)
            .ok_or_else(|| not_found("MethodResponse not found"))?;
        apply_patch_operations(req, |_op, path, value| {
            if let Some(o) = v.as_object_mut() {
                o.insert(path.trim_start_matches('/').to_string(), value.clone());
            }
        });
        ok(v.clone())
    }
}

#[cfg(test)]
mod resource_patch_tests {
    use super::*;
    use fakecloud_core::service::ResponseBody;
    use std::collections::HashMap;

    fn svc() -> ApiGatewayService {
        let state =
            std::sync::Arc::new(
                parking_lot::RwLock::new(fakecloud_core::multi_account::MultiAccountState::<
                    crate::state::ApiGatewayState,
                >::new("123456789012", "us-east-1", "")),
            );
        ApiGatewayService::new(state)
    }

    fn req_body(body: Value) -> AwsRequest {
        AwsRequest {
            service: "apigateway".into(),
            action: "x".into(),
            region: "us-east-1".into(),
            account_id: "123456789012".into(),
            request_id: "rid".into(),
            headers: http::HeaderMap::new(),
            query_params: HashMap::new(),
            body: bytes::Bytes::from(serde_json::to_vec(&body).unwrap()),
            body_stream: parking_lot::Mutex::new(None),
            path_segments: Vec::new(),
            raw_path: "/".into(),
            raw_query: String::new(),
            method: http::Method::POST,
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        }
    }

    fn body_json(resp: &AwsResponse) -> Value {
        match &resp.body {
            ResponseBody::Bytes(b) => serde_json::from_slice(b).unwrap(),
            _ => panic!("expected bytes body"),
        }
    }

    fn params(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn update_rest_api_applies_compression_and_endpoint_config() {
        let s = svc();
        let created = body_json(
            &s.create_rest_api(&req_body(json!({
                "name": "api",
                "endpointConfiguration": { "types": ["EDGE"] }
            })))
            .unwrap(),
        );
        let api_id = created["id"].as_str().unwrap().to_string();
        let p = params(&[("restApiId", &api_id)]);

        let updated = body_json(
            &s.update_rest_api(
                &req_body(json!({ "patchOperations": [
                    { "op": "replace", "path": "/minimumCompressionSize", "value": "1024" },
                    { "op": "replace", "path": "/endpointConfiguration/types/0", "value": "REGIONAL" },
                ]})),
                &p,
            )
            .unwrap(),
        );
        assert_eq!(updated["minimumCompressionSize"], json!(1024));
        assert_eq!(
            updated["endpointConfiguration"]["types"][0],
            json!("REGIONAL")
        );

        // Persisted via GetRestApi.
        let got = body_json(&s.get_rest_api(&req_body(json!({})), &p).unwrap());
        assert_eq!(got["minimumCompressionSize"], json!(1024));
        assert_eq!(got["endpointConfiguration"]["types"][0], json!("REGIONAL"));
    }

    #[test]
    fn update_resource_rename_and_reparent_recompute_paths() {
        let s = svc();
        let api = body_json(
            &s.create_rest_api(&req_body(json!({ "name": "api" })))
                .unwrap(),
        );
        let api_id = api["id"].as_str().unwrap().to_string();
        let root_id = api["rootResourceId"].as_str().unwrap().to_string();

        // /pets
        let pets = body_json(
            &s.create_resource(
                &req_body(json!({ "pathPart": "pets" })),
                &params(&[("restApiId", &api_id), ("parentId", &root_id)]),
            )
            .unwrap(),
        );
        let pets_id = pets["id"].as_str().unwrap().to_string();
        assert_eq!(pets["path"], json!("/pets"));

        // /pets/list
        let list = body_json(
            &s.create_resource(
                &req_body(json!({ "pathPart": "list" })),
                &params(&[("restApiId", &api_id), ("parentId", &pets_id)]),
            )
            .unwrap(),
        );
        let list_id = list["id"].as_str().unwrap().to_string();
        assert_eq!(list["path"], json!("/pets/list"));

        // Rename /pets -> /animals; descendant must follow.
        let renamed = body_json(
            &s.update_resource(
                &req_body(json!({ "patchOperations": [
                    { "op": "replace", "path": "/pathPart", "value": "animals" }
                ]})),
                &params(&[("restApiId", &api_id), ("resourceId", &pets_id)]),
            )
            .unwrap(),
        );
        assert_eq!(renamed["path"], json!("/animals"));
        let got_list = body_json(
            &s.get_resource(
                &req_body(json!({})),
                &params(&[("restApiId", &api_id), ("resourceId", &list_id)]),
            )
            .unwrap(),
        );
        assert_eq!(got_list["path"], json!("/animals/list"));

        // Reparent /animals under a new /store resource.
        let store = body_json(
            &s.create_resource(
                &req_body(json!({ "pathPart": "store" })),
                &params(&[("restApiId", &api_id), ("parentId", &root_id)]),
            )
            .unwrap(),
        );
        let store_id = store["id"].as_str().unwrap().to_string();
        let moved = body_json(
            &s.update_resource(
                &req_body(json!({ "patchOperations": [
                    { "op": "replace", "path": "/parentId", "value": store_id }
                ]})),
                &params(&[("restApiId", &api_id), ("resourceId", &pets_id)]),
            )
            .unwrap(),
        );
        assert_eq!(moved["path"], json!("/store/animals"));
        let got_list = body_json(
            &s.get_resource(
                &req_body(json!({})),
                &params(&[("restApiId", &api_id), ("resourceId", &list_id)]),
            )
            .unwrap(),
        );
        assert_eq!(got_list["path"], json!("/store/animals/list"));

        // Cycle guard: cannot move /store under its own descendant.
        let err = s.update_resource(
            &req_body(json!({ "patchOperations": [
                { "op": "replace", "path": "/parentId", "value": list_id }
            ]})),
            &params(&[("restApiId", &api_id), ("resourceId", &store_id)]),
        );
        assert!(err.is_err());

        // Collision guard: create /store/other, then renaming it to "animals"
        // (colliding with the existing /store/animals) must fail and roll back.
        let other = body_json(
            &s.create_resource(
                &req_body(json!({ "pathPart": "other" })),
                &params(&[("restApiId", &api_id), ("parentId", &store_id)]),
            )
            .unwrap(),
        );
        let other_id = other["id"].as_str().unwrap().to_string();
        let collision = s.update_resource(
            &req_body(json!({ "patchOperations": [
                { "op": "replace", "path": "/pathPart", "value": "animals" }
            ]})),
            &params(&[("restApiId", &api_id), ("resourceId", &other_id)]),
        );
        assert!(collision.is_err());
        // Rolled back: /store/other still resolves to its original path.
        let got_other = body_json(
            &s.get_resource(
                &req_body(json!({})),
                &params(&[("restApiId", &api_id), ("resourceId", &other_id)]),
            )
            .unwrap(),
        );
        assert_eq!(got_other["path"], json!("/store/other"));
    }
}
