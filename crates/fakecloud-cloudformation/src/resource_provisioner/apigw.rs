//! Auto-extracted from resource_provisioner/mod.rs by the
//! audit-2026-05-19 file-split. All methods here continue
//! the `impl ResourceProvisioner` block; the family slug is
//! `apigw`.

use super::*;

impl ResourceProvisioner {
    // --- API Gateway v1 ---

    pub(super) fn create_apigw_rest_api(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("Name")
            .and_then(|v| v.as_str())
            .ok_or("Name is required")?
            .to_string();
        let description = props
            .get("Description")
            .and_then(|v| v.as_str())
            .map(String::from);
        let api_key_source = props
            .get("ApiKeySourceType")
            .and_then(|v| v.as_str())
            .unwrap_or("HEADER")
            .to_string();
        let endpoint_configuration = props
            .get("EndpointConfiguration")
            .cloned()
            .unwrap_or_else(|| serde_json::json!({"types": ["EDGE"]}));
        let policy = props
            .get("Policy")
            .map(|v| v.to_string().trim_matches('"').to_string());
        let binary_media_types: Vec<String> = props
            .get("BinaryMediaTypes")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        let minimum_compression_size = props.get("MinimumCompressionSize").and_then(|v| v.as_i64());
        let disable_execute_api_endpoint = props
            .get("DisableExecuteApiEndpoint")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        // CFN exposes optional `Body`/`BodyS3Location`/`CloneFrom` for OpenAPI
        // import. We don't run a full Swagger import — we record the source
        // in the api's `import_source` field so callers can reason about it.
        let import_source = if props.get("Body").is_some() {
            Some("Body".to_string())
        } else if props.get("BodyS3Location").is_some() {
            Some("BodyS3Location".to_string())
        } else if props.get("CloneFrom").is_some() {
            Some("CloneFrom".to_string())
        } else {
            None
        };
        let tags = parse_acm_tags(props.get("Tags"));

        let id = apigw_make_id();
        let root_resource_id = apigw_make_id();
        let now = Utc::now();

        let api = ApiGwRestApi {
            id: id.clone(),
            name,
            description,
            version: props
                .get("Version")
                .and_then(|v| v.as_str())
                .map(String::from),
            created_date: now,
            api_key_source,
            endpoint_configuration,
            policy,
            binary_media_types,
            minimum_compression_size,
            disable_execute_api_endpoint,
            root_resource_id: root_resource_id.clone(),
            tags,
            import_source,
        };

        let mut accounts = self.apigateway_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.apis.insert(id.clone(), api);
        let mut resources = BTreeMap::new();
        resources.insert(
            root_resource_id.clone(),
            ApiGwResource {
                id: root_resource_id.clone(),
                parent_id: None,
                path_part: None,
                path: "/".to_string(),
            },
        );
        state.resources.insert(id.clone(), resources);

        Ok(ProvisionResult::new(id.clone())
            .with("RestApiId", id.clone())
            .with("RootResourceId", root_resource_id))
    }

    pub(super) fn update_apigw_rest_api(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let id = existing.physical_id.clone();
        let mut accounts = self.apigateway_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let api = state
            .apis
            .get_mut(&id)
            .ok_or_else(|| format!("RestApi {id} not found for update"))?;
        if let Some(name) = props.get("Name").and_then(|v| v.as_str()) {
            api.name = name.to_string();
        }
        if let Some(desc) = props.get("Description").and_then(|v| v.as_str()) {
            api.description = Some(desc.to_string());
        }
        if let Some(source) = props.get("ApiKeySourceType").and_then(|v| v.as_str()) {
            api.api_key_source = source.to_string();
        }
        if let Some(ep) = props.get("EndpointConfiguration").cloned() {
            api.endpoint_configuration = ep;
        }
        if let Some(arr) = props.get("BinaryMediaTypes").and_then(|v| v.as_array()) {
            api.binary_media_types = arr
                .iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect();
        }
        if let Some(size) = props.get("MinimumCompressionSize").and_then(|v| v.as_i64()) {
            api.minimum_compression_size = Some(size);
        }
        if let Some(b) = props
            .get("DisableExecuteApiEndpoint")
            .and_then(|v| v.as_bool())
        {
            api.disable_execute_api_endpoint = b;
        }
        if props.get("Tags").is_some() {
            api.tags = parse_acm_tags(props.get("Tags"));
        }
        let root = api.root_resource_id.clone();
        Ok(ProvisionResult::new(id.clone())
            .with("RestApiId", id)
            .with("RootResourceId", root))
    }

    pub(super) fn delete_apigw_rest_api(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.apigateway_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.apis.remove(physical_id);
        state.resources.remove(physical_id);
        let prefix = format!("{physical_id}/");
        state.methods.retain(|k, _| !k.starts_with(&prefix));
        state.integrations.retain(|k, _| !k.starts_with(&prefix));
        state
            .integration_responses
            .retain(|k, _| !k.starts_with(&prefix));
        state
            .method_responses
            .retain(|k, _| !k.starts_with(&prefix));
        state.deployments.remove(physical_id);
        state.stages.remove(physical_id);
        state.models.remove(physical_id);
        state.request_validators.remove(physical_id);
        state.authorizers.remove(physical_id);
        state.gateway_responses.remove(physical_id);
        Ok(())
    }

    pub(super) fn create_apigw_resource(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let rest_api_id = props
            .get("RestApiId")
            .and_then(|v| v.as_str())
            .ok_or("RestApiId is required")?
            .to_string();
        let parent_id = props
            .get("ParentId")
            .and_then(|v| v.as_str())
            .ok_or("ParentId is required")?
            .to_string();
        let path_part = props
            .get("PathPart")
            .and_then(|v| v.as_str())
            .ok_or("PathPart is required")?
            .to_string();

        let mut accounts = self.apigateway_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let api_resources = state
            .resources
            .get(&rest_api_id)
            .ok_or_else(|| format!("RestApi {rest_api_id} not found"))?;
        let parent = api_resources
            .get(&parent_id)
            .ok_or_else(|| format!("Parent resource {parent_id} not found"))?;
        let parent_path = parent.path.clone();
        let path = if parent_path == "/" {
            format!("/{path_part}")
        } else {
            format!("{parent_path}/{path_part}")
        };

        let id = apigw_make_id();
        let new_resource = ApiGwResource {
            id: id.clone(),
            parent_id: Some(parent_id),
            path_part: Some(path_part),
            path,
        };
        state
            .resources
            .entry(rest_api_id.clone())
            .or_default()
            .insert(id.clone(), new_resource);

        Ok(ProvisionResult::new(id.clone())
            .with("ResourceId", id)
            .with("RestApiId", rest_api_id))
    }

    pub(super) fn delete_apigw_resource(
        &self,
        physical_id: &str,
        attributes: &BTreeMap<String, String>,
    ) -> Result<(), String> {
        let Some(rest_api_id) = attributes.get("RestApiId") else {
            return Ok(());
        };
        let mut accounts = self.apigateway_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if let Some(map) = state.resources.get_mut(rest_api_id) {
            map.remove(physical_id);
        }
        let prefix = format!("{rest_api_id}/{physical_id}/");
        state.methods.retain(|k, _| !k.starts_with(&prefix));
        state.integrations.retain(|k, _| !k.starts_with(&prefix));
        Ok(())
    }

    pub(super) fn create_apigw_method(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let rest_api_id = props
            .get("RestApiId")
            .and_then(|v| v.as_str())
            .ok_or("RestApiId is required")?
            .to_string();
        let resource_id = props
            .get("ResourceId")
            .and_then(|v| v.as_str())
            .ok_or("ResourceId is required")?
            .to_string();
        let http_method = props
            .get("HttpMethod")
            .and_then(|v| v.as_str())
            .ok_or("HttpMethod is required")?
            .to_uppercase();
        let authorization_type = props
            .get("AuthorizationType")
            .and_then(|v| v.as_str())
            .unwrap_or("NONE")
            .to_string();
        let authorizer_id = props
            .get("AuthorizerId")
            .and_then(|v| v.as_str())
            .map(String::from);
        let api_key_required = props
            .get("ApiKeyRequired")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let operation_name = props
            .get("OperationName")
            .and_then(|v| v.as_str())
            .map(String::from);
        let request_validator_id = props
            .get("RequestValidatorId")
            .and_then(|v| v.as_str())
            .map(String::from);
        let request_parameters: BTreeMap<String, bool> = props
            .get("RequestParameters")
            .and_then(|v| v.as_object())
            .map(|obj| {
                obj.iter()
                    .map(|(k, v)| (k.clone(), v.as_bool().unwrap_or(false)))
                    .collect()
            })
            .unwrap_or_default();
        let request_models: BTreeMap<String, String> = props
            .get("RequestModels")
            .and_then(|v| v.as_object())
            .map(|obj| {
                obj.iter()
                    .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                    .collect()
            })
            .unwrap_or_default();
        let authorization_scopes: Vec<String> = props
            .get("AuthorizationScopes")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();

        let composite_key = format!("{rest_api_id}/{resource_id}/{http_method}");
        let method = ApiGwMethod {
            rest_api_id: rest_api_id.clone(),
            resource_id: resource_id.clone(),
            http_method: http_method.clone(),
            authorization_type,
            authorizer_id,
            api_key_required,
            operation_name,
            request_parameters,
            request_models,
            request_validator_id,
            authorization_scopes,
        };

        let mut accounts = self.apigateway_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if !state.apis.contains_key(&rest_api_id) {
            return Err(format!("RestApi {rest_api_id} not found"));
        }
        // Multi-pass provisioning: if `Ref: SomeResource` resolved to the
        // logical id (because the referenced resource hasn't been
        // provisioned yet on this pass), bail so CFN retries us next pass.
        let resource_known = state
            .resources
            .get(&rest_api_id)
            .map(|m| m.contains_key(&resource_id))
            .unwrap_or(false);
        if !resource_known {
            return Err(format!(
                "Resource {resource_id} not yet provisioned for api {rest_api_id}"
            ));
        }
        state.methods.insert(composite_key.clone(), method);

        if let Some(integ_props) = props.get("Integration").and_then(|v| v.as_object()) {
            let integration = ApiGwIntegration {
                rest_api_id: rest_api_id.clone(),
                resource_id: resource_id.clone(),
                http_method: http_method.clone(),
                integration_type: integ_props
                    .get("Type")
                    .and_then(|v| v.as_str())
                    .unwrap_or("MOCK")
                    .to_string(),
                integration_http_method: integ_props
                    .get("IntegrationHttpMethod")
                    .and_then(|v| v.as_str())
                    .map(String::from),
                uri: integ_props
                    .get("Uri")
                    .and_then(|v| v.as_str())
                    .map(String::from),
                credentials: integ_props
                    .get("Credentials")
                    .and_then(|v| v.as_str())
                    .map(String::from),
                request_parameters: integ_props
                    .get("RequestParameters")
                    .and_then(|v| v.as_object())
                    .map(|obj| {
                        obj.iter()
                            .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                            .collect()
                    })
                    .unwrap_or_default(),
                request_templates: integ_props
                    .get("RequestTemplates")
                    .and_then(|v| v.as_object())
                    .map(|obj| {
                        obj.iter()
                            .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                            .collect()
                    })
                    .unwrap_or_default(),
                passthrough_behavior: integ_props
                    .get("PassthroughBehavior")
                    .and_then(|v| v.as_str())
                    .unwrap_or("WHEN_NO_MATCH")
                    .to_string(),
                timeout_in_millis: integ_props
                    .get("TimeoutInMillis")
                    .and_then(|v| v.as_i64())
                    .map(|n| n as i32),
                cache_namespace: integ_props
                    .get("CacheNamespace")
                    .and_then(|v| v.as_str())
                    .map(String::from),
                cache_key_parameters: integ_props
                    .get("CacheKeyParameters")
                    .and_then(|v| v.as_array())
                    .map(|arr| {
                        arr.iter()
                            .filter_map(|v| v.as_str().map(String::from))
                            .collect()
                    })
                    .unwrap_or_default(),
                content_handling: integ_props
                    .get("ContentHandling")
                    .and_then(|v| v.as_str())
                    .map(String::from),
                connection_type: integ_props
                    .get("ConnectionType")
                    .and_then(|v| v.as_str())
                    .map(String::from),
                connection_id: integ_props
                    .get("ConnectionId")
                    .and_then(|v| v.as_str())
                    .map(String::from),
                tls_config: integ_props.get("TlsConfig").cloned(),
            };
            state
                .integrations
                .insert(composite_key.clone(), integration);
        }

        Ok(ProvisionResult::new(composite_key.clone())
            .with("MethodKey", composite_key)
            .with("RestApiId", rest_api_id)
            .with("ResourceId", resource_id)
            .with("HttpMethod", http_method))
    }

    pub(super) fn delete_apigw_method(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.apigateway_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.methods.remove(physical_id);
        state.integrations.remove(physical_id);
        let prefix = format!("{physical_id}/");
        state
            .integration_responses
            .retain(|k, _| !k.starts_with(&prefix));
        state
            .method_responses
            .retain(|k, _| !k.starts_with(&prefix));
        Ok(())
    }

    pub(super) fn create_apigw_deployment(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let rest_api_id = props
            .get("RestApiId")
            .and_then(|v| v.as_str())
            .ok_or("RestApiId is required")?
            .to_string();
        let description = props
            .get("Description")
            .and_then(|v| v.as_str())
            .map(String::from);

        let id = apigw_make_id();
        let mut accounts = self.apigateway_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if !state.apis.contains_key(&rest_api_id) {
            return Err(format!("RestApi {rest_api_id} not found"));
        }
        let api_summary = serde_json::to_value(
            state
                .resources
                .get(&rest_api_id)
                .cloned()
                .unwrap_or_default(),
        )
        .unwrap_or(serde_json::Value::Null);
        let deployment = ApiGwDeployment {
            id: id.clone(),
            description,
            created_date: Utc::now(),
            api_summary,
        };
        state
            .deployments
            .entry(rest_api_id.clone())
            .or_default()
            .insert(id.clone(), deployment);

        // CFN inline `StageName` creates a Stage referencing this deployment.
        if let Some(stage_name) = props
            .get("StageName")
            .and_then(|v| v.as_str())
            .map(String::from)
        {
            let stage = ApiGwStage {
                stage_name: stage_name.clone(),
                deployment_id: id.clone(),
                description: props
                    .get("StageDescription")
                    .and_then(|v| v.get("Description"))
                    .and_then(|v| v.as_str())
                    .map(String::from),
                cache_cluster_enabled: false,
                cache_cluster_size: None,
                variables: BTreeMap::new(),
                method_settings: BTreeMap::new(),
                created_date: Utc::now(),
                last_updated_date: Utc::now(),
                tracing_enabled: false,
                web_acl_arn: None,
                canary_settings: None,
                access_log_settings: None,
                tags: BTreeMap::new(),
            };
            state
                .stages
                .entry(rest_api_id.clone())
                .or_default()
                .insert(stage_name, stage);
        }

        Ok(ProvisionResult::new(id.clone())
            .with("DeploymentId", id)
            .with("RestApiId", rest_api_id))
    }

    pub(super) fn delete_apigw_deployment(
        &self,
        physical_id: &str,
        attributes: &BTreeMap<String, String>,
    ) -> Result<(), String> {
        let Some(rest_api_id) = attributes.get("RestApiId") else {
            return Ok(());
        };
        let mut accounts = self.apigateway_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if let Some(map) = state.deployments.get_mut(rest_api_id) {
            map.remove(physical_id);
        }
        Ok(())
    }

    pub(super) fn create_apigw_stage(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let rest_api_id = props
            .get("RestApiId")
            .and_then(|v| v.as_str())
            .ok_or("RestApiId is required")?
            .to_string();
        let stage_name = props
            .get("StageName")
            .and_then(|v| v.as_str())
            .ok_or("StageName is required")?
            .to_string();
        let deployment_id = props
            .get("DeploymentId")
            .and_then(|v| v.as_str())
            .ok_or("DeploymentId is required")?
            .to_string();

        let variables: BTreeMap<String, String> = props
            .get("Variables")
            .and_then(|v| v.as_object())
            .map(|obj| {
                obj.iter()
                    .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                    .collect()
            })
            .unwrap_or_default();
        let tracing_enabled = props
            .get("TracingEnabled")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let cache_cluster_enabled = props
            .get("CacheClusterEnabled")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let cache_cluster_size = props
            .get("CacheClusterSize")
            .and_then(|v| v.as_str())
            .map(String::from);
        // CFN models MethodSettings as a list of `{ResourcePath,HttpMethod,...}`
        // entries; the live API stores them as a `path/method -> settings`
        // map. Translate by joining ResourcePath + HttpMethod into the key.
        let method_settings: BTreeMap<String, serde_json::Value> = props
            .get("MethodSettings")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|s| {
                        let path = s.get("ResourcePath").and_then(|v| v.as_str())?;
                        let http = s.get("HttpMethod").and_then(|v| v.as_str())?;
                        let key = format!("{}/{http}", path.strip_prefix('/').unwrap_or(path));
                        Some((key, s.clone()))
                    })
                    .collect()
            })
            .unwrap_or_default();
        let tags = parse_acm_tags(props.get("Tags"));

        let stage = ApiGwStage {
            stage_name: stage_name.clone(),
            deployment_id,
            description: props
                .get("Description")
                .and_then(|v| v.as_str())
                .map(String::from),
            cache_cluster_enabled,
            cache_cluster_size,
            variables,
            method_settings,
            created_date: Utc::now(),
            last_updated_date: Utc::now(),
            tracing_enabled,
            web_acl_arn: None,
            canary_settings: props.get("CanarySetting").cloned(),
            access_log_settings: props.get("AccessLogSetting").cloned(),
            tags,
        };

        let mut accounts = self.apigateway_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if !state.apis.contains_key(&rest_api_id) {
            return Err(format!("RestApi {rest_api_id} not found"));
        }
        let dep_known = state
            .deployments
            .get(&rest_api_id)
            .map(|m| m.contains_key(&stage.deployment_id))
            .unwrap_or(false);
        if !dep_known {
            return Err(format!(
                "Deployment {} not yet provisioned for api {rest_api_id}",
                stage.deployment_id
            ));
        }
        state
            .stages
            .entry(rest_api_id.clone())
            .or_default()
            .insert(stage_name.clone(), stage);

        Ok(ProvisionResult::new(stage_name.clone())
            .with("StageName", stage_name)
            .with("RestApiId", rest_api_id))
    }

    pub(super) fn delete_apigw_stage(
        &self,
        physical_id: &str,
        attributes: &BTreeMap<String, String>,
    ) -> Result<(), String> {
        let Some(rest_api_id) = attributes.get("RestApiId") else {
            return Ok(());
        };
        let mut accounts = self.apigateway_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if let Some(map) = state.stages.get_mut(rest_api_id) {
            map.remove(physical_id);
        }
        Ok(())
    }

    pub(super) fn create_apigw_authorizer(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let rest_api_id = props
            .get("RestApiId")
            .and_then(|v| v.as_str())
            .ok_or("RestApiId is required")?
            .to_string();
        let name = props
            .get("Name")
            .and_then(|v| v.as_str())
            .ok_or("Name is required")?
            .to_string();
        let authorizer_type = props
            .get("Type")
            .and_then(|v| v.as_str())
            .unwrap_or("TOKEN")
            .to_string();
        let provider_arns: Vec<String> = props
            .get("ProviderARNs")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();

        let id = apigw_make_id();
        let auth = ApiGwAuthorizer {
            id: id.clone(),
            name,
            authorizer_type,
            provider_arns,
            auth_type: props
                .get("AuthType")
                .and_then(|v| v.as_str())
                .map(String::from),
            authorizer_uri: props
                .get("AuthorizerUri")
                .and_then(|v| v.as_str())
                .map(String::from),
            authorizer_credentials: props
                .get("AuthorizerCredentials")
                .and_then(|v| v.as_str())
                .map(String::from),
            identity_source: props
                .get("IdentitySource")
                .and_then(|v| v.as_str())
                .map(String::from),
            identity_validation_expression: props
                .get("IdentityValidationExpression")
                .and_then(|v| v.as_str())
                .map(String::from),
            authorizer_result_ttl_in_seconds: props
                .get("AuthorizerResultTtlInSeconds")
                .and_then(|v| v.as_i64())
                .map(|n| n as i32),
        };

        let mut accounts = self.apigateway_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if !state.apis.contains_key(&rest_api_id) {
            return Err(format!("RestApi {rest_api_id} not found"));
        }
        state
            .authorizers
            .entry(rest_api_id.clone())
            .or_default()
            .insert(id.clone(), auth);

        Ok(ProvisionResult::new(id.clone())
            .with("AuthorizerId", id)
            .with("RestApiId", rest_api_id))
    }

    pub(super) fn delete_apigw_authorizer(
        &self,
        physical_id: &str,
        attributes: &BTreeMap<String, String>,
    ) -> Result<(), String> {
        let Some(rest_api_id) = attributes.get("RestApiId") else {
            return Ok(());
        };
        let mut accounts = self.apigateway_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if let Some(map) = state.authorizers.get_mut(rest_api_id) {
            map.remove(physical_id);
        }
        Ok(())
    }

    pub(super) fn create_apigw_request_validator(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let rest_api_id = props
            .get("RestApiId")
            .and_then(|v| v.as_str())
            .ok_or("RestApiId is required")?
            .to_string();
        let name = props.get("Name").and_then(|v| v.as_str()).map(String::from);
        let validate_body = props
            .get("ValidateRequestBody")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let validate_params = props
            .get("ValidateRequestParameters")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let id = apigw_make_id();
        let body = serde_json::json!({
            "id": id,
            "name": name,
            "validateRequestBody": validate_body,
            "validateRequestParameters": validate_params,
        });
        let mut accounts = self.apigateway_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state
            .request_validators
            .entry(rest_api_id.clone())
            .or_default()
            .insert(id.clone(), body);
        Ok(ProvisionResult::new(id.clone())
            .with("RequestValidatorId", id)
            .with("RestApiId", rest_api_id))
    }

    pub(super) fn delete_apigw_request_validator(
        &self,
        physical_id: &str,
        attributes: &BTreeMap<String, String>,
    ) -> Result<(), String> {
        let Some(rest_api_id) = attributes.get("RestApiId") else {
            return Ok(());
        };
        let mut accounts = self.apigateway_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if let Some(map) = state.request_validators.get_mut(rest_api_id) {
            map.remove(physical_id);
        }
        Ok(())
    }

    pub(super) fn create_apigw_model(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let rest_api_id = props
            .get("RestApiId")
            .and_then(|v| v.as_str())
            .ok_or("RestApiId is required")?
            .to_string();
        let name = props
            .get("Name")
            .and_then(|v| v.as_str())
            .ok_or("Name is required")?
            .to_string();
        let content_type = props
            .get("ContentType")
            .and_then(|v| v.as_str())
            .unwrap_or("application/json")
            .to_string();
        let schema = props.get("Schema").map(|v| {
            if let Some(s) = v.as_str() {
                s.to_string()
            } else {
                v.to_string()
            }
        });
        let id = apigw_make_id();
        let model = ApiGwModel {
            id: id.clone(),
            name: name.clone(),
            description: props
                .get("Description")
                .and_then(|v| v.as_str())
                .map(String::from),
            schema,
            content_type,
        };
        let mut accounts = self.apigateway_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state
            .models
            .entry(rest_api_id.clone())
            .or_default()
            .insert(name.clone(), model);
        Ok(ProvisionResult::new(name.clone())
            .with("ModelName", name)
            .with("RestApiId", rest_api_id))
    }

    pub(super) fn delete_apigw_model(
        &self,
        physical_id: &str,
        attributes: &BTreeMap<String, String>,
    ) -> Result<(), String> {
        let Some(rest_api_id) = attributes.get("RestApiId") else {
            return Ok(());
        };
        let mut accounts = self.apigateway_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if let Some(map) = state.models.get_mut(rest_api_id) {
            map.remove(physical_id);
        }
        Ok(())
    }

    pub(super) fn create_apigw_gateway_response(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let rest_api_id = props
            .get("RestApiId")
            .and_then(|v| v.as_str())
            .ok_or("RestApiId is required")?
            .to_string();
        let response_type = props
            .get("ResponseType")
            .and_then(|v| v.as_str())
            .ok_or("ResponseType is required")?
            .to_string();
        let body = serde_json::json!({
            "responseType": response_type,
            "statusCode": props.get("StatusCode").and_then(|v| v.as_str()),
            "responseParameters": props.get("ResponseParameters").cloned().unwrap_or(serde_json::json!({})),
            "responseTemplates": props.get("ResponseTemplates").cloned().unwrap_or(serde_json::json!({})),
        });
        let mut accounts = self.apigateway_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state
            .gateway_responses
            .entry(rest_api_id.clone())
            .or_default()
            .insert(response_type.clone(), body);
        Ok(ProvisionResult::new(response_type.clone())
            .with("ResponseType", response_type)
            .with("RestApiId", rest_api_id))
    }

    pub(super) fn delete_apigw_gateway_response(
        &self,
        physical_id: &str,
        attributes: &BTreeMap<String, String>,
    ) -> Result<(), String> {
        let Some(rest_api_id) = attributes.get("RestApiId") else {
            return Ok(());
        };
        let mut accounts = self.apigateway_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if let Some(map) = state.gateway_responses.get_mut(rest_api_id) {
            map.remove(physical_id);
        }
        Ok(())
    }

    pub(super) fn create_apigw_usage_plan(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("UsagePlanName")
            .and_then(|v| v.as_str())
            .ok_or("UsagePlanName is required")?
            .to_string();
        let id = apigw_make_id();
        let plan = ApiGwUsagePlan {
            id: id.clone(),
            name,
            description: props
                .get("Description")
                .and_then(|v| v.as_str())
                .map(String::from),
            api_stages: props
                .get("ApiStages")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .map(lowercase_first_keys)
                .collect(),
            throttle: props.get("Throttle").cloned().map(lowercase_first_keys),
            quota: props.get("Quota").cloned().map(lowercase_first_keys),
            product_code: None,
            tags: parse_acm_tags(props.get("Tags")),
        };
        let mut accounts = self.apigateway_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.usage_plans.insert(id.clone(), plan);
        Ok(ProvisionResult::new(id.clone()).with("UsagePlanId", id))
    }

    pub(super) fn delete_apigw_usage_plan(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.apigateway_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.usage_plans.remove(physical_id);
        state.usage_plan_keys.remove(physical_id);
        Ok(())
    }

    pub(super) fn create_apigw_api_key(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let generate_distinct_id = props
            .get("GenerateDistinctId")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let name = props
            .get("Name")
            .and_then(|v| v.as_str())
            .map(String::from)
            .unwrap_or_else(|| {
                if generate_distinct_id {
                    format!("cfn-key-{}-{}", resource.logical_id, apigw_make_id())
                } else {
                    format!("cfn-key-{}", resource.logical_id)
                }
            });
        let value = props
            .get("Value")
            .and_then(|v| v.as_str())
            .map(String::from)
            .unwrap_or_else(|| Uuid::new_v4().simple().to_string());
        let enabled = props
            .get("Enabled")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);
        // CFN's StageKeys are `[{RestApiId, StageName}, ...]` — we store each
        // as `restApiId/stageName` per the live API key shape.
        let stage_keys: Vec<String> = props
            .get("StageKeys")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|s| {
                        let rest = s.get("RestApiId").and_then(|v| v.as_str())?;
                        let stage = s.get("StageName").and_then(|v| v.as_str())?;
                        Some(format!("{rest}/{stage}"))
                    })
                    .collect()
            })
            .unwrap_or_default();
        let id = apigw_make_id();
        let now = Utc::now();
        let key = ApiGwApiKey {
            id: id.clone(),
            value,
            name,
            description: props
                .get("Description")
                .and_then(|v| v.as_str())
                .map(String::from),
            enabled,
            created_date: now,
            last_updated_date: now,
            stage_keys,
            tags: parse_acm_tags(props.get("Tags")),
            customer_id: props
                .get("CustomerId")
                .and_then(|v| v.as_str())
                .map(String::from),
        };
        let mut accounts = self.apigateway_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.api_keys.insert(id.clone(), key);
        Ok(ProvisionResult::new(id.clone()).with("ApiKeyId", id))
    }

    pub(super) fn delete_apigw_api_key(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.apigateway_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.api_keys.remove(physical_id);
        Ok(())
    }

    pub(super) fn create_apigw_usage_plan_key(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let usage_plan_id = props
            .get("UsagePlanId")
            .and_then(|v| v.as_str())
            .ok_or("UsagePlanId is required")?
            .to_string();
        let key_id = props
            .get("KeyId")
            .and_then(|v| v.as_str())
            .ok_or("KeyId is required")?
            .to_string();
        let key_type = props
            .get("KeyType")
            .and_then(|v| v.as_str())
            .unwrap_or("API_KEY")
            .to_string();
        let body = serde_json::json!({
            "id": key_id,
            "type": key_type,
        });
        let mut accounts = self.apigateway_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if !state.usage_plans.contains_key(&usage_plan_id) {
            return Err(format!("UsagePlan {usage_plan_id} not yet provisioned"));
        }
        if !state.api_keys.contains_key(&key_id) {
            return Err(format!("ApiKey {key_id} not yet provisioned"));
        }
        state
            .usage_plan_keys
            .entry(usage_plan_id.clone())
            .or_default()
            .insert(key_id.clone(), body);
        let physical = format!("{usage_plan_id}/{key_id}");
        Ok(ProvisionResult::new(physical)
            .with("UsagePlanId", usage_plan_id)
            .with("KeyId", key_id))
    }

    pub(super) fn delete_apigw_usage_plan_key(
        &self,
        physical_id: &str,
        _attributes: &BTreeMap<String, String>,
    ) -> Result<(), String> {
        let mut parts = physical_id.splitn(2, '/');
        let Some(plan_id) = parts.next() else {
            return Ok(());
        };
        let Some(key_id) = parts.next() else {
            return Ok(());
        };
        let mut accounts = self.apigateway_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if let Some(map) = state.usage_plan_keys.get_mut(plan_id) {
            map.remove(key_id);
        }
        Ok(())
    }

    pub(super) fn create_apigw_domain_name(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let domain_name = props
            .get("DomainName")
            .and_then(|v| v.as_str())
            .ok_or("DomainName is required")?
            .to_string();
        let mtls = props
            .get("MutualTlsAuthentication")
            .cloned()
            .map(lowercase_first_keys);
        let regional_domain = format!(
            "d-{}.execute-api.{}.amazonaws.com",
            apigw_make_id(),
            self.region
        );
        let distribution_domain = format!("d{}.cloudfront.net", apigw_make_id());
        let body = serde_json::json!({
            "domainName": domain_name,
            "certificateArn": props.get("CertificateArn").and_then(|v| v.as_str()),
            "regionalCertificateArn": props.get("RegionalCertificateArn").and_then(|v| v.as_str()),
            "endpointConfiguration": props.get("EndpointConfiguration").cloned().unwrap_or(serde_json::json!({"types": ["EDGE"]})),
            "securityPolicy": props.get("SecurityPolicy").and_then(|v| v.as_str()),
            "ownershipVerificationCertificateArn": props.get("OwnershipVerificationCertificateArn").and_then(|v| v.as_str()),
            "regionalDomainName": regional_domain,
            "regionalHostedZoneId": "Z2FDTNDATAQYW2",
            "distributionDomainName": distribution_domain,
            "distributionHostedZoneId": "Z2FDTNDATAQYW2",
            "mutualTlsAuthentication": mtls,
            "tags": serde_json::Value::Object(
                parse_acm_tags(props.get("Tags"))
                    .into_iter()
                    .map(|(k, v)| (k, serde_json::Value::String(v)))
                    .collect(),
            ),
        });
        let mut accounts = self.apigateway_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.domain_names.insert(domain_name.clone(), body);
        Ok(ProvisionResult::new(domain_name.clone())
            .with("DomainName", domain_name)
            .with("RegionalHostedZoneId", "Z2FDTNDATAQYW2".to_string())
            .with("DistributionHostedZoneId", "Z2FDTNDATAQYW2".to_string()))
    }

    pub(super) fn delete_apigw_domain_name(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.apigateway_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.domain_names.remove(physical_id);
        state.base_path_mappings.remove(physical_id);
        Ok(())
    }

    pub(super) fn create_apigw_base_path_mapping(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let domain_name = props
            .get("DomainName")
            .and_then(|v| v.as_str())
            .ok_or("DomainName is required")?
            .to_string();
        let rest_api_id = props
            .get("RestApiId")
            .and_then(|v| v.as_str())
            .ok_or("RestApiId is required")?
            .to_string();
        let base_path = props
            .get("BasePath")
            .and_then(|v| v.as_str())
            .unwrap_or("(none)")
            .to_string();
        let stage = props
            .get("Stage")
            .and_then(|v| v.as_str())
            .map(String::from);
        let body = serde_json::json!({
            "basePath": base_path,
            "restApiId": rest_api_id,
            "stage": stage,
        });
        let mut accounts = self.apigateway_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state
            .base_path_mappings
            .entry(domain_name.clone())
            .or_default()
            .insert(base_path.clone(), body);
        let physical = format!("{domain_name}/{base_path}");
        Ok(ProvisionResult::new(physical)
            .with("DomainName", domain_name)
            .with("BasePath", base_path))
    }

    pub(super) fn delete_apigw_base_path_mapping(
        &self,
        physical_id: &str,
        _attributes: &BTreeMap<String, String>,
    ) -> Result<(), String> {
        let mut parts = physical_id.splitn(2, '/');
        let Some(domain) = parts.next() else {
            return Ok(());
        };
        let Some(base_path) = parts.next() else {
            return Ok(());
        };
        let mut accounts = self.apigateway_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if let Some(map) = state.base_path_mappings.get_mut(domain) {
            map.remove(base_path);
        }
        Ok(())
    }

    // --- API Gateway v1 update paths ---
    //
    // These mirror the create_* helpers above but mutate an existing
    // resource instead of inserting a new one. The physical id is
    // preserved across updates so other stack resources keep referencing
    // the same logical entity.

    pub(super) fn update_apigw_resource(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let rest_api_id = existing
            .attributes
            .get("RestApiId")
            .cloned()
            .or_else(|| {
                props
                    .get("RestApiId")
                    .and_then(|v| v.as_str())
                    .map(String::from)
            })
            .ok_or("RestApiId is required")?;
        let physical = existing.physical_id.clone();
        let mut accounts = self.apigateway_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let api_resources = state
            .resources
            .get_mut(&rest_api_id)
            .ok_or_else(|| format!("RestApi {rest_api_id} not found"))?;
        if !api_resources.contains_key(&physical) {
            return Err(format!("Resource {physical} not found"));
        }
        if let Some(part) = props.get("PathPart").and_then(|v| v.as_str()) {
            // Read parent's path first (immutable borrow), then mutate.
            let parent_id = api_resources
                .get(&physical)
                .and_then(|r| r.parent_id.clone());
            let parent_path = parent_id
                .as_ref()
                .and_then(|pid| api_resources.get(pid).map(|p| p.path.clone()))
                .unwrap_or_else(|| "/".to_string());
            let new_path = if parent_path == "/" {
                format!("/{part}")
            } else {
                format!("{parent_path}/{part}")
            };
            let res = api_resources
                .get_mut(&physical)
                .expect("contains_key checked above");
            res.path_part = Some(part.to_string());
            res.path = new_path;
        }
        Ok(ProvisionResult::new(physical.clone())
            .with("ResourceId", physical)
            .with("RestApiId", rest_api_id))
    }

    pub(super) fn update_apigw_method(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        // Method's physical id is the composite "rest/resource/method"
        // key. Identity props can't change without replacement, so we
        // simply rewrite the stored Method/Integration with current
        // properties. We delegate to create_apigw_method which already
        // handles the insert-or-replace semantics.
        self.create_apigw_method(resource).map(|r| {
            // Make sure the physical id stays stable.
            ProvisionResult {
                physical_id: existing.physical_id.clone(),
                attributes: r.attributes,
            }
        })
    }

    pub(super) fn update_apigw_deployment(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let rest_api_id = existing
            .attributes
            .get("RestApiId")
            .cloned()
            .or_else(|| {
                props
                    .get("RestApiId")
                    .and_then(|v| v.as_str())
                    .map(String::from)
            })
            .ok_or("RestApiId is required")?;
        let physical = existing.physical_id.clone();
        let mut accounts = self.apigateway_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let dep = state
            .deployments
            .get_mut(&rest_api_id)
            .and_then(|m| m.get_mut(&physical))
            .ok_or_else(|| format!("Deployment {physical} not found"))?;
        if let Some(desc) = props.get("Description").and_then(|v| v.as_str()) {
            dep.description = Some(desc.to_string());
        }
        Ok(ProvisionResult::new(physical.clone())
            .with("DeploymentId", physical)
            .with("RestApiId", rest_api_id))
    }

    pub(super) fn update_apigw_stage(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let rest_api_id = existing
            .attributes
            .get("RestApiId")
            .cloned()
            .or_else(|| {
                props
                    .get("RestApiId")
                    .and_then(|v| v.as_str())
                    .map(String::from)
            })
            .ok_or("RestApiId is required")?;
        let stage_name = existing.physical_id.clone();
        let mut accounts = self.apigateway_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let stage = state
            .stages
            .get_mut(&rest_api_id)
            .and_then(|m| m.get_mut(&stage_name))
            .ok_or_else(|| format!("Stage {stage_name} not found"))?;
        if let Some(desc) = props.get("Description").and_then(|v| v.as_str()) {
            stage.description = Some(desc.to_string());
        }
        if let Some(b) = props.get("TracingEnabled").and_then(|v| v.as_bool()) {
            stage.tracing_enabled = b;
        }
        if let Some(b) = props.get("CacheClusterEnabled").and_then(|v| v.as_bool()) {
            stage.cache_cluster_enabled = b;
        }
        if let Some(s) = props.get("CacheClusterSize").and_then(|v| v.as_str()) {
            stage.cache_cluster_size = Some(s.to_string());
        }
        if let Some(obj) = props.get("Variables").and_then(|v| v.as_object()) {
            stage.variables = obj
                .iter()
                .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                .collect();
        }
        if let Some(dep) = props.get("DeploymentId").and_then(|v| v.as_str()) {
            stage.deployment_id = dep.to_string();
        }
        if props.get("Tags").is_some() {
            stage.tags = parse_acm_tags(props.get("Tags"));
        }
        if let Some(arr) = props.get("MethodSettings").and_then(|v| v.as_array()) {
            stage.method_settings = arr
                .iter()
                .filter_map(|s| {
                    let path = s.get("ResourcePath").and_then(|v| v.as_str())?;
                    let http = s.get("HttpMethod").and_then(|v| v.as_str())?;
                    let key = format!("{}/{http}", path.strip_prefix('/').unwrap_or(path));
                    Some((key, s.clone()))
                })
                .collect();
        }
        if let Some(canary) = props.get("CanarySetting").cloned() {
            stage.canary_settings = Some(canary);
        }
        if let Some(access) = props.get("AccessLogSetting").cloned() {
            stage.access_log_settings = Some(access);
        }
        stage.last_updated_date = Utc::now();
        Ok(ProvisionResult::new(stage_name.clone())
            .with("StageName", stage_name)
            .with("RestApiId", rest_api_id))
    }

    pub(super) fn update_apigw_authorizer(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let rest_api_id = existing
            .attributes
            .get("RestApiId")
            .cloned()
            .or_else(|| {
                props
                    .get("RestApiId")
                    .and_then(|v| v.as_str())
                    .map(String::from)
            })
            .ok_or("RestApiId is required")?;
        let physical = existing.physical_id.clone();
        let mut accounts = self.apigateway_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let auth = state
            .authorizers
            .get_mut(&rest_api_id)
            .and_then(|m| m.get_mut(&physical))
            .ok_or_else(|| format!("Authorizer {physical} not found"))?;
        if let Some(name) = props.get("Name").and_then(|v| v.as_str()) {
            auth.name = name.to_string();
        }
        if let Some(t) = props.get("Type").and_then(|v| v.as_str()) {
            auth.authorizer_type = t.to_string();
        }
        if let Some(uri) = props.get("AuthorizerUri").and_then(|v| v.as_str()) {
            auth.authorizer_uri = Some(uri.to_string());
        }
        if let Some(arr) = props.get("ProviderARNs").and_then(|v| v.as_array()) {
            auth.provider_arns = arr
                .iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect();
        }
        if let Some(s) = props.get("IdentitySource").and_then(|v| v.as_str()) {
            auth.identity_source = Some(s.to_string());
        }
        if let Some(s) = props
            .get("IdentityValidationExpression")
            .and_then(|v| v.as_str())
        {
            auth.identity_validation_expression = Some(s.to_string());
        }
        if let Some(n) = props
            .get("AuthorizerResultTtlInSeconds")
            .and_then(|v| v.as_i64())
        {
            auth.authorizer_result_ttl_in_seconds = Some(n as i32);
        }
        if let Some(s) = props.get("AuthType").and_then(|v| v.as_str()) {
            auth.auth_type = Some(s.to_string());
        }
        if let Some(s) = props.get("AuthorizerCredentials").and_then(|v| v.as_str()) {
            auth.authorizer_credentials = Some(s.to_string());
        }
        Ok(ProvisionResult::new(physical.clone())
            .with("AuthorizerId", physical)
            .with("RestApiId", rest_api_id))
    }

    pub(super) fn update_apigw_request_validator(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let rest_api_id = existing
            .attributes
            .get("RestApiId")
            .cloned()
            .or_else(|| {
                props
                    .get("RestApiId")
                    .and_then(|v| v.as_str())
                    .map(String::from)
            })
            .ok_or("RestApiId is required")?;
        let physical = existing.physical_id.clone();
        let mut accounts = self.apigateway_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let body = state
            .request_validators
            .get_mut(&rest_api_id)
            .and_then(|m| m.get_mut(&physical))
            .ok_or_else(|| format!("RequestValidator {physical} not found"))?;
        let obj = body.as_object_mut().ok_or("validator body not object")?;
        if let Some(name) = props.get("Name").and_then(|v| v.as_str()) {
            obj.insert("name".into(), serde_json::Value::String(name.into()));
        }
        if let Some(b) = props.get("ValidateRequestBody").and_then(|v| v.as_bool()) {
            obj.insert("validateRequestBody".into(), serde_json::Value::Bool(b));
        }
        if let Some(b) = props
            .get("ValidateRequestParameters")
            .and_then(|v| v.as_bool())
        {
            obj.insert(
                "validateRequestParameters".into(),
                serde_json::Value::Bool(b),
            );
        }
        Ok(ProvisionResult::new(physical.clone())
            .with("RequestValidatorId", physical)
            .with("RestApiId", rest_api_id))
    }

    pub(super) fn update_apigw_model(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let rest_api_id = existing
            .attributes
            .get("RestApiId")
            .cloned()
            .or_else(|| {
                props
                    .get("RestApiId")
                    .and_then(|v| v.as_str())
                    .map(String::from)
            })
            .ok_or("RestApiId is required")?;
        let model_name = existing.physical_id.clone();
        let mut accounts = self.apigateway_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let model = state
            .models
            .get_mut(&rest_api_id)
            .and_then(|m| m.get_mut(&model_name))
            .ok_or_else(|| format!("Model {model_name} not found"))?;
        if let Some(desc) = props.get("Description").and_then(|v| v.as_str()) {
            model.description = Some(desc.to_string());
        }
        if let Some(s) = props.get("ContentType").and_then(|v| v.as_str()) {
            model.content_type = s.to_string();
        }
        if let Some(schema) = props.get("Schema") {
            model.schema = Some(if let Some(s) = schema.as_str() {
                s.to_string()
            } else {
                schema.to_string()
            });
        }
        Ok(ProvisionResult::new(model_name.clone())
            .with("ModelName", model_name)
            .with("RestApiId", rest_api_id))
    }

    pub(super) fn update_apigw_gateway_response(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let rest_api_id = existing
            .attributes
            .get("RestApiId")
            .cloned()
            .or_else(|| {
                props
                    .get("RestApiId")
                    .and_then(|v| v.as_str())
                    .map(String::from)
            })
            .ok_or("RestApiId is required")?;
        let response_type = existing.physical_id.clone();
        let mut accounts = self.apigateway_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let body = state
            .gateway_responses
            .get_mut(&rest_api_id)
            .and_then(|m| m.get_mut(&response_type))
            .ok_or_else(|| format!("GatewayResponse {response_type} not found"))?;
        let obj = body.as_object_mut().ok_or("response body not object")?;
        if let Some(s) = props.get("StatusCode").and_then(|v| v.as_str()) {
            obj.insert("statusCode".into(), serde_json::Value::String(s.into()));
        }
        if let Some(v) = props.get("ResponseParameters").cloned() {
            obj.insert("responseParameters".into(), v);
        }
        if let Some(v) = props.get("ResponseTemplates").cloned() {
            obj.insert("responseTemplates".into(), v);
        }
        Ok(ProvisionResult::new(response_type.clone())
            .with("ResponseType", response_type)
            .with("RestApiId", rest_api_id))
    }

    pub(super) fn update_apigw_usage_plan(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let physical = existing.physical_id.clone();
        let mut accounts = self.apigateway_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let plan = state
            .usage_plans
            .get_mut(&physical)
            .ok_or_else(|| format!("UsagePlan {physical} not found"))?;
        if let Some(name) = props.get("UsagePlanName").and_then(|v| v.as_str()) {
            plan.name = name.to_string();
        }
        if let Some(s) = props.get("Description").and_then(|v| v.as_str()) {
            plan.description = Some(s.to_string());
        }
        if let Some(arr) = props.get("ApiStages").and_then(|v| v.as_array()) {
            plan.api_stages = arr.iter().cloned().map(lowercase_first_keys).collect();
        }
        if let Some(t) = props.get("Throttle").cloned() {
            plan.throttle = Some(lowercase_first_keys(t));
        }
        if let Some(q) = props.get("Quota").cloned() {
            plan.quota = Some(lowercase_first_keys(q));
        }
        if props.get("Tags").is_some() {
            plan.tags = parse_acm_tags(props.get("Tags"));
        }
        Ok(ProvisionResult::new(physical.clone()).with("UsagePlanId", physical))
    }

    pub(super) fn update_apigw_api_key(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let physical = existing.physical_id.clone();
        let mut accounts = self.apigateway_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let key = state
            .api_keys
            .get_mut(&physical)
            .ok_or_else(|| format!("ApiKey {physical} not found"))?;
        if let Some(name) = props.get("Name").and_then(|v| v.as_str()) {
            key.name = name.to_string();
        }
        if let Some(s) = props.get("Description").and_then(|v| v.as_str()) {
            key.description = Some(s.to_string());
        }
        if let Some(b) = props.get("Enabled").and_then(|v| v.as_bool()) {
            key.enabled = b;
        }
        if let Some(s) = props.get("CustomerId").and_then(|v| v.as_str()) {
            key.customer_id = Some(s.to_string());
        }
        if props.get("Tags").is_some() {
            key.tags = parse_acm_tags(props.get("Tags"));
        }
        if let Some(arr) = props.get("StageKeys").and_then(|v| v.as_array()) {
            key.stage_keys = arr
                .iter()
                .filter_map(|s| {
                    let rest = s.get("RestApiId").and_then(|v| v.as_str())?;
                    let stage = s.get("StageName").and_then(|v| v.as_str())?;
                    Some(format!("{rest}/{stage}"))
                })
                .collect();
        }
        key.last_updated_date = Utc::now();
        Ok(ProvisionResult::new(physical.clone()).with("ApiKeyId", physical))
    }

    pub(super) fn update_apigw_usage_plan_key(
        &self,
        existing: &StackResource,
        _resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        // UsagePlanKey is a pure association (UsagePlan + ApiKey + Type) —
        // CFN treats every property as `requires-replacement`, so a real
        // UpdateStack would Delete+Create. Here we just preserve the
        // existing physical id so resolution stays stable.
        let physical = existing.physical_id.clone();
        let mut parts = physical.splitn(2, '/');
        let plan = parts.next().unwrap_or("").to_string();
        let key = parts.next().unwrap_or("").to_string();
        Ok(ProvisionResult::new(physical)
            .with("UsagePlanId", plan)
            .with("KeyId", key))
    }

    pub(super) fn update_apigw_domain_name(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let domain = existing.physical_id.clone();
        let mut accounts = self.apigateway_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let body = state
            .domain_names
            .get_mut(&domain)
            .ok_or_else(|| format!("DomainName {domain} not found"))?;
        let obj = body.as_object_mut().ok_or("domain body not object")?;
        if let Some(s) = props.get("CertificateArn").and_then(|v| v.as_str()) {
            obj.insert("certificateArn".into(), serde_json::Value::String(s.into()));
        }
        if let Some(s) = props.get("RegionalCertificateArn").and_then(|v| v.as_str()) {
            obj.insert(
                "regionalCertificateArn".into(),
                serde_json::Value::String(s.into()),
            );
        }
        if let Some(v) = props.get("EndpointConfiguration").cloned() {
            obj.insert("endpointConfiguration".into(), v);
        }
        if let Some(s) = props.get("SecurityPolicy").and_then(|v| v.as_str()) {
            obj.insert("securityPolicy".into(), serde_json::Value::String(s.into()));
        }
        if let Some(v) = props.get("MutualTlsAuthentication").cloned() {
            obj.insert("mutualTlsAuthentication".into(), lowercase_first_keys(v));
        }
        if let Some(s) = props
            .get("OwnershipVerificationCertificateArn")
            .and_then(|v| v.as_str())
        {
            obj.insert(
                "ownershipVerificationCertificateArn".into(),
                serde_json::Value::String(s.into()),
            );
        }
        if props.get("Tags").is_some() {
            obj.insert(
                "tags".into(),
                serde_json::Value::Object(
                    parse_acm_tags(props.get("Tags"))
                        .into_iter()
                        .map(|(k, v)| (k, serde_json::Value::String(v)))
                        .collect(),
                ),
            );
        }
        Ok(ProvisionResult::new(domain.clone())
            .with("DomainName", domain)
            .with("RegionalHostedZoneId", "Z2FDTNDATAQYW2".to_string())
            .with("DistributionHostedZoneId", "Z2FDTNDATAQYW2".to_string()))
    }

    pub(super) fn update_apigw_base_path_mapping(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let physical = existing.physical_id.clone();
        let mut parts = physical.splitn(2, '/');
        let domain = parts
            .next()
            .ok_or("malformed base path mapping id")?
            .to_string();
        let base_path = parts
            .next()
            .ok_or("malformed base path mapping id")?
            .to_string();
        let mut accounts = self.apigateway_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let map = state
            .base_path_mappings
            .get_mut(&domain)
            .ok_or_else(|| format!("DomainName {domain} not found"))?;
        let body = map
            .get_mut(&base_path)
            .ok_or_else(|| format!("BasePath {base_path} not found"))?;
        let obj = body.as_object_mut().ok_or("mapping body not object")?;
        if let Some(s) = props.get("RestApiId").and_then(|v| v.as_str()) {
            obj.insert("restApiId".into(), serde_json::Value::String(s.into()));
        }
        if let Some(s) = props.get("Stage").and_then(|v| v.as_str()) {
            obj.insert("stage".into(), serde_json::Value::String(s.into()));
        }
        Ok(ProvisionResult::new(physical)
            .with("DomainName", domain)
            .with("BasePath", base_path))
    }
}
