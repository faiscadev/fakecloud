//! Auto-extracted from resource_provisioner/mod.rs by the
//! audit-2026-05-19 file-split. All methods here continue
//! the `impl ResourceProvisioner` block; the family slug is
//! `apigwv2`.

use super::*;

impl ResourceProvisioner {
    // --- API Gateway v2 (HTTP/WebSocket APIs) ---

    pub(super) fn create_apigwv2_api(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("Name")
            .and_then(|v| v.as_str())
            .ok_or("Name is required")?
            .to_string();
        let protocol_type = props
            .get("ProtocolType")
            .and_then(|v| v.as_str())
            .unwrap_or("HTTP")
            .to_string();
        let description = props
            .get("Description")
            .and_then(|v| v.as_str())
            .map(String::from);
        let tags: Option<BTreeMap<String, String>> =
            props.get("Tags").and_then(|v| v.as_object()).map(|obj| {
                obj.iter()
                    .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                    .collect()
            });

        let id = make_apigwv2_id(10);
        let mut api = ApiGwV2HttpApi::new(id.clone(), name, description, tags, &self.region);
        api.protocol_type = protocol_type.clone();
        if let Some(expr) = props
            .get("RouteSelectionExpression")
            .and_then(|v| v.as_str())
        {
            api.route_selection_expression = expr.to_string();
        }
        if let Some(expr) = props
            .get("ApiKeySelectionExpression")
            .and_then(|v| v.as_str())
        {
            api.api_key_selection_expression = expr.to_string();
        }
        if let Some(b) = props
            .get("DisableExecuteApiEndpoint")
            .and_then(|v| v.as_bool())
        {
            api.disable_execute_api_endpoint = b;
        }
        if let Some(s) = props.get("IpAddressType").and_then(|v| v.as_str()) {
            api.ip_address_type = s.to_string();
        }
        if let Some(cors) = props.get("CorsConfiguration").and_then(|v| v.as_object()) {
            api.cors_configuration = Some(ApiGwV2CorsConfiguration {
                allow_credentials: cors.get("AllowCredentials").and_then(|v| v.as_bool()),
                allow_headers: cors
                    .get("AllowHeaders")
                    .and_then(|v| v.as_array())
                    .map(|a| {
                        a.iter()
                            .filter_map(|v| v.as_str().map(String::from))
                            .collect()
                    }),
                allow_methods: cors
                    .get("AllowMethods")
                    .and_then(|v| v.as_array())
                    .map(|a| {
                        a.iter()
                            .filter_map(|v| v.as_str().map(String::from))
                            .collect()
                    }),
                allow_origins: cors
                    .get("AllowOrigins")
                    .and_then(|v| v.as_array())
                    .map(|a| {
                        a.iter()
                            .filter_map(|v| v.as_str().map(String::from))
                            .collect()
                    }),
                expose_headers: cors
                    .get("ExposeHeaders")
                    .and_then(|v| v.as_array())
                    .map(|a| {
                        a.iter()
                            .filter_map(|v| v.as_str().map(String::from))
                            .collect()
                    }),
                max_age: cors
                    .get("MaxAge")
                    .and_then(|v| v.as_i64())
                    .map(|n| n as i32),
            });
        }

        let api_endpoint = api.api_endpoint.clone();
        let mut accounts = self.apigatewayv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.apis.insert(id.clone(), api);

        Ok(ProvisionResult::new(id.clone())
            .with("ApiId", id)
            .with("ApiEndpoint", api_endpoint))
    }

    pub(super) fn delete_apigwv2_api(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.apigatewayv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.apis.remove(physical_id);
        state.routes.remove(physical_id);
        state.integrations.remove(physical_id);
        state.stages.remove(physical_id);
        state.deployments.remove(physical_id);
        state.authorizers.remove(physical_id);
        state.models.remove(physical_id);
        state.integration_responses.remove(physical_id);
        state.route_responses.remove(physical_id);
        Ok(())
    }

    pub(super) fn create_apigwv2_route(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let api_id = props
            .get("ApiId")
            .and_then(|v| v.as_str())
            .ok_or("ApiId is required")?
            .to_string();
        let route_key = props
            .get("RouteKey")
            .and_then(|v| v.as_str())
            .ok_or("RouteKey is required")?
            .to_string();

        let mut accounts = self.apigatewayv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if !state.apis.contains_key(&api_id) {
            return Err(format!("Api {api_id} not yet provisioned"));
        }
        let id = make_apigwv2_id(10);
        let route = ApiGwV2Route {
            route_id: id.clone(),
            route_key,
            target: props
                .get("Target")
                .and_then(|v| v.as_str())
                .map(String::from),
            authorization_type: props
                .get("AuthorizationType")
                .and_then(|v| v.as_str())
                .map(String::from),
            authorizer_id: props
                .get("AuthorizerId")
                .and_then(|v| v.as_str())
                .map(String::from),
        };
        state
            .routes
            .entry(api_id.clone())
            .or_default()
            .insert(id.clone(), route);

        Ok(ProvisionResult::new(id.clone())
            .with("RouteId", id)
            .with("ApiId", api_id))
    }

    pub(super) fn delete_apigwv2_route(
        &self,
        physical_id: &str,
        attributes: &BTreeMap<String, String>,
    ) -> Result<(), String> {
        let Some(api_id) = attributes.get("ApiId") else {
            return Ok(());
        };
        let mut accounts = self.apigatewayv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if let Some(map) = state.routes.get_mut(api_id) {
            map.remove(physical_id);
        }
        Ok(())
    }

    pub(super) fn create_apigwv2_integration(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let api_id = props
            .get("ApiId")
            .and_then(|v| v.as_str())
            .ok_or("ApiId is required")?
            .to_string();
        let integration_type = props
            .get("IntegrationType")
            .and_then(|v| v.as_str())
            .ok_or("IntegrationType is required")?
            .to_string();

        let mut accounts = self.apigatewayv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if !state.apis.contains_key(&api_id) {
            return Err(format!("Api {api_id} not yet provisioned"));
        }
        let id = make_apigwv2_id(10);
        let integration = ApiGwV2Integration {
            integration_id: id.clone(),
            integration_type,
            integration_uri: props
                .get("IntegrationUri")
                .and_then(|v| v.as_str())
                .map(String::from),
            payload_format_version: props
                .get("PayloadFormatVersion")
                .and_then(|v| v.as_str())
                .map(String::from),
            timeout_in_millis: props.get("TimeoutInMillis").and_then(|v| v.as_i64()),
            connection_type: props
                .get("ConnectionType")
                .and_then(|v| v.as_str())
                .unwrap_or("INTERNET")
                .to_string(),
        };
        state
            .integrations
            .entry(api_id.clone())
            .or_default()
            .insert(id.clone(), integration);

        Ok(ProvisionResult::new(id.clone())
            .with("IntegrationId", id)
            .with("ApiId", api_id))
    }

    pub(super) fn delete_apigwv2_integration(
        &self,
        physical_id: &str,
        attributes: &BTreeMap<String, String>,
    ) -> Result<(), String> {
        let Some(api_id) = attributes.get("ApiId") else {
            return Ok(());
        };
        let mut accounts = self.apigatewayv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if let Some(map) = state.integrations.get_mut(api_id) {
            map.remove(physical_id);
        }
        Ok(())
    }

    pub(super) fn create_apigwv2_integration_response(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let api_id = props
            .get("ApiId")
            .and_then(|v| v.as_str())
            .ok_or("ApiId is required")?
            .to_string();
        let integration_id = props
            .get("IntegrationId")
            .and_then(|v| v.as_str())
            .ok_or("IntegrationId is required")?
            .to_string();
        let key_expr = props
            .get("IntegrationResponseKey")
            .and_then(|v| v.as_str())
            .ok_or("IntegrationResponseKey is required")?
            .to_string();
        let id = make_apigwv2_id(10);
        let body = serde_json::json!({
            "integrationResponseId": id,
            "integrationId": integration_id,
            "integrationResponseKey": key_expr,
            "responseTemplates": props.get("ResponseTemplates").cloned().unwrap_or(serde_json::json!({})),
            "responseParameters": props.get("ResponseParameters").cloned().unwrap_or(serde_json::json!({})),
            "templateSelectionExpression": props.get("TemplateSelectionExpression").and_then(|v| v.as_str()),
            "contentHandlingStrategy": props.get("ContentHandlingStrategy").and_then(|v| v.as_str()),
        });
        let composite_key = format!("{integration_id}/{id}");
        let mut accounts = self.apigatewayv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if !state
            .integrations
            .get(&api_id)
            .map(|m| m.contains_key(&integration_id))
            .unwrap_or(false)
        {
            return Err(format!(
                "Integration {integration_id} not yet provisioned for api {api_id}"
            ));
        }
        state
            .integration_responses
            .entry(api_id.clone())
            .or_default()
            .insert(composite_key.clone(), body);
        Ok(ProvisionResult::new(composite_key.clone())
            .with("IntegrationResponseId", id)
            .with("IntegrationId", integration_id)
            .with("ApiId", api_id))
    }

    pub(super) fn delete_apigwv2_integration_response(
        &self,
        physical_id: &str,
        attributes: &BTreeMap<String, String>,
    ) -> Result<(), String> {
        let Some(api_id) = attributes.get("ApiId") else {
            return Ok(());
        };
        let mut accounts = self.apigatewayv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if let Some(map) = state.integration_responses.get_mut(api_id) {
            map.remove(physical_id);
        }
        Ok(())
    }

    pub(super) fn create_apigwv2_route_response(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let api_id = props
            .get("ApiId")
            .and_then(|v| v.as_str())
            .ok_or("ApiId is required")?
            .to_string();
        let route_id = props
            .get("RouteId")
            .and_then(|v| v.as_str())
            .ok_or("RouteId is required")?
            .to_string();
        let key_expr = props
            .get("RouteResponseKey")
            .and_then(|v| v.as_str())
            .ok_or("RouteResponseKey is required")?
            .to_string();
        let id = make_apigwv2_id(10);
        let body = serde_json::json!({
            "routeResponseId": id,
            "routeId": route_id,
            "routeResponseKey": key_expr,
            "responseModels": props.get("ResponseModels").cloned().unwrap_or(serde_json::json!({})),
            "modelSelectionExpression": props.get("ModelSelectionExpression").and_then(|v| v.as_str()),
            "responseParameters": props.get("ResponseParameters").cloned().unwrap_or(serde_json::json!({})),
        });
        let composite = format!("{route_id}/{id}");
        let mut accounts = self.apigatewayv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if !state
            .routes
            .get(&api_id)
            .map(|m| m.contains_key(&route_id))
            .unwrap_or(false)
        {
            return Err(format!(
                "Route {route_id} not yet provisioned for api {api_id}"
            ));
        }
        state
            .route_responses
            .entry(api_id.clone())
            .or_default()
            .insert(composite.clone(), body);
        Ok(ProvisionResult::new(composite.clone())
            .with("RouteResponseId", id)
            .with("RouteId", route_id)
            .with("ApiId", api_id))
    }

    pub(super) fn delete_apigwv2_route_response(
        &self,
        physical_id: &str,
        attributes: &BTreeMap<String, String>,
    ) -> Result<(), String> {
        let Some(api_id) = attributes.get("ApiId") else {
            return Ok(());
        };
        let mut accounts = self.apigatewayv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if let Some(map) = state.route_responses.get_mut(api_id) {
            map.remove(physical_id);
        }
        Ok(())
    }

    pub(super) fn create_apigwv2_stage(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let api_id = props
            .get("ApiId")
            .and_then(|v| v.as_str())
            .ok_or("ApiId is required")?
            .to_string();
        let stage_name = props
            .get("StageName")
            .and_then(|v| v.as_str())
            .ok_or("StageName is required")?
            .to_string();
        let auto_deploy = props
            .get("AutoDeploy")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let deployment_id = props
            .get("DeploymentId")
            .and_then(|v| v.as_str())
            .map(String::from);

        let stage_variables = props
            .get("StageVariables")
            .and_then(|v| v.as_object())
            .map(|obj| {
                obj.iter()
                    .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                    .collect()
            });

        let access_log_settings = props.get("AccessLogSettings").and_then(|v| {
            let destination_arn = v.get("DestinationArn")?.as_str()?.to_string();
            let format = v.get("Format").and_then(|f| f.as_str().map(String::from));
            Some(fakecloud_apigatewayv2::AccessLogSettings {
                destination_arn,
                format,
            })
        });

        let stage = ApiGwV2Stage {
            stage_name: stage_name.clone(),
            description: props
                .get("Description")
                .and_then(|v| v.as_str())
                .map(String::from),
            deployment_id: deployment_id.clone(),
            auto_deploy,
            created_date: Utc::now(),
            last_updated_date: None,
            web_acl_arn: None,
            stage_variables,
            access_log_settings,
        };

        let mut accounts = self.apigatewayv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if !state.apis.contains_key(&api_id) {
            return Err(format!("Api {api_id} not yet provisioned"));
        }
        if let Some(dep) = &deployment_id {
            if !state
                .deployments
                .get(&api_id)
                .map(|m| m.contains_key(dep))
                .unwrap_or(false)
            {
                return Err(format!(
                    "Deployment {dep} not yet provisioned for api {api_id}"
                ));
            }
        }
        state
            .stages
            .entry(api_id.clone())
            .or_default()
            .insert(stage_name.clone(), stage);

        Ok(ProvisionResult::new(stage_name.clone())
            .with("StageName", stage_name)
            .with("ApiId", api_id))
    }

    pub(super) fn delete_apigwv2_stage(
        &self,
        physical_id: &str,
        attributes: &BTreeMap<String, String>,
    ) -> Result<(), String> {
        let Some(api_id) = attributes.get("ApiId") else {
            return Ok(());
        };
        let mut accounts = self.apigatewayv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if let Some(map) = state.stages.get_mut(api_id) {
            map.remove(physical_id);
        }
        Ok(())
    }

    pub(super) fn create_apigwv2_deployment(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let api_id = props
            .get("ApiId")
            .and_then(|v| v.as_str())
            .ok_or("ApiId is required")?
            .to_string();
        let id = make_apigwv2_id(10);
        let deployment = ApiGwV2Deployment {
            deployment_id: id.clone(),
            description: props
                .get("Description")
                .and_then(|v| v.as_str())
                .map(String::from),
            created_date: Utc::now(),
            auto_deployed: false,
            deployment_status: "DEPLOYED".to_string(),
        };
        let mut accounts = self.apigatewayv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if !state.apis.contains_key(&api_id) {
            return Err(format!("Api {api_id} not yet provisioned"));
        }
        state
            .deployments
            .entry(api_id.clone())
            .or_default()
            .insert(id.clone(), deployment);
        Ok(ProvisionResult::new(id.clone())
            .with("DeploymentId", id)
            .with("ApiId", api_id))
    }

    pub(super) fn delete_apigwv2_deployment(
        &self,
        physical_id: &str,
        attributes: &BTreeMap<String, String>,
    ) -> Result<(), String> {
        let Some(api_id) = attributes.get("ApiId") else {
            return Ok(());
        };
        let mut accounts = self.apigatewayv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if let Some(map) = state.deployments.get_mut(api_id) {
            map.remove(physical_id);
        }
        Ok(())
    }

    pub(super) fn create_apigwv2_authorizer(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let api_id = props
            .get("ApiId")
            .and_then(|v| v.as_str())
            .ok_or("ApiId is required")?
            .to_string();
        let name = props
            .get("Name")
            .and_then(|v| v.as_str())
            .ok_or("Name is required")?
            .to_string();
        let authorizer_type = props
            .get("AuthorizerType")
            .and_then(|v| v.as_str())
            .unwrap_or("REQUEST")
            .to_string();
        let identity_source = props
            .get("IdentitySource")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect::<Vec<String>>()
            });
        let jwt_configuration = props
            .get("JwtConfiguration")
            .and_then(|v| v.as_object())
            .map(|obj| ApiGwV2JwtConfiguration {
                audience: obj.get("Audience").and_then(|v| v.as_array()).map(|a| {
                    a.iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect()
                }),
                issuer: obj.get("Issuer").and_then(|v| v.as_str()).map(String::from),
            });

        let id = make_apigwv2_id(10);
        let auth = ApiGwV2Authorizer {
            authorizer_id: id.clone(),
            name,
            authorizer_type,
            authorizer_uri: props
                .get("AuthorizerUri")
                .and_then(|v| v.as_str())
                .map(String::from),
            identity_source,
            jwt_configuration,
        };
        let mut accounts = self.apigatewayv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if !state.apis.contains_key(&api_id) {
            return Err(format!("Api {api_id} not yet provisioned"));
        }
        state
            .authorizers
            .entry(api_id.clone())
            .or_default()
            .insert(id.clone(), auth);
        Ok(ProvisionResult::new(id.clone())
            .with("AuthorizerId", id)
            .with("ApiId", api_id))
    }

    pub(super) fn delete_apigwv2_authorizer(
        &self,
        physical_id: &str,
        attributes: &BTreeMap<String, String>,
    ) -> Result<(), String> {
        let Some(api_id) = attributes.get("ApiId") else {
            return Ok(());
        };
        let mut accounts = self.apigatewayv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if let Some(map) = state.authorizers.get_mut(api_id) {
            map.remove(physical_id);
        }
        Ok(())
    }

    pub(super) fn create_apigwv2_domain_name(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let domain_name = props
            .get("DomainName")
            .and_then(|v| v.as_str())
            .ok_or("DomainName is required")?
            .to_string();
        let body = serde_json::json!({
            "domainName": domain_name,
            "domainNameConfigurations": props.get("DomainNameConfigurations").cloned().unwrap_or(serde_json::json!([])),
            "mutualTlsAuthentication": props.get("MutualTlsAuthentication").cloned(),
            "apiMappingSelectionExpression": null,
        });
        let mut accounts = self.apigatewayv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.domain_names.insert(domain_name.clone(), body);
        Ok(ProvisionResult::new(domain_name.clone()).with("DomainName", domain_name))
    }

    pub(super) fn delete_apigwv2_domain_name(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.apigatewayv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.domain_names.remove(physical_id);
        state.api_mappings.remove(physical_id);
        Ok(())
    }

    pub(super) fn create_apigwv2_api_mapping(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let domain_name = props
            .get("DomainName")
            .and_then(|v| v.as_str())
            .ok_or("DomainName is required")?
            .to_string();
        let api_id = props
            .get("ApiId")
            .and_then(|v| v.as_str())
            .ok_or("ApiId is required")?
            .to_string();
        let stage = props
            .get("Stage")
            .and_then(|v| v.as_str())
            .ok_or("Stage is required")?
            .to_string();
        let api_mapping_key = props
            .get("ApiMappingKey")
            .and_then(|v| v.as_str())
            .map(String::from);
        let id = make_apigwv2_id(10);
        let body = serde_json::json!({
            "apiMappingId": id,
            "apiId": api_id,
            "stage": stage,
            "apiMappingKey": api_mapping_key,
        });
        let mut accounts = self.apigatewayv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if !state.domain_names.contains_key(&domain_name) {
            return Err(format!("DomainName {domain_name} not yet provisioned"));
        }
        if !state.apis.contains_key(&api_id) {
            return Err(format!("Api {api_id} not yet provisioned"));
        }
        state
            .api_mappings
            .entry(domain_name.clone())
            .or_default()
            .insert(id.clone(), body);
        Ok(ProvisionResult::new(id.clone())
            .with("ApiMappingId", id)
            .with("DomainName", domain_name))
    }

    pub(super) fn delete_apigwv2_api_mapping(
        &self,
        physical_id: &str,
        attributes: &BTreeMap<String, String>,
    ) -> Result<(), String> {
        let Some(domain) = attributes.get("DomainName") else {
            return Ok(());
        };
        let mut accounts = self.apigatewayv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if let Some(map) = state.api_mappings.get_mut(domain) {
            map.remove(physical_id);
        }
        Ok(())
    }

    pub(super) fn create_apigwv2_vpc_link(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("Name")
            .and_then(|v| v.as_str())
            .ok_or("Name is required")?
            .to_string();
        let id = make_apigwv2_id(10);
        let body = serde_json::json!({
            "vpcLinkId": id,
            "name": name,
            "subnetIds": props.get("SubnetIds").cloned().unwrap_or(serde_json::json!([])),
            "securityGroupIds": props.get("SecurityGroupIds").cloned().unwrap_or(serde_json::json!([])),
            "tags": props.get("Tags").cloned().unwrap_or(serde_json::json!({})),
            "vpcLinkStatus": "AVAILABLE",
            "vpcLinkVersion": "V2",
            "createdDate": Utc::now().to_rfc3339(),
        });
        let mut accounts = self.apigatewayv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.vpc_links.insert(id.clone(), body);
        Ok(ProvisionResult::new(id.clone()).with("VpcLinkId", id))
    }

    pub(super) fn delete_apigwv2_vpc_link(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.apigatewayv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.vpc_links.remove(physical_id);
        Ok(())
    }

    pub(super) fn create_apigwv2_model(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let api_id = props
            .get("ApiId")
            .and_then(|v| v.as_str())
            .ok_or("ApiId is required")?
            .to_string();
        let name = props
            .get("Name")
            .and_then(|v| v.as_str())
            .ok_or("Name is required")?
            .to_string();
        let id = make_apigwv2_id(10);
        let body = serde_json::json!({
            "modelId": id,
            "name": name,
            "contentType": props.get("ContentType").and_then(|v| v.as_str()).unwrap_or("application/json"),
            "description": props.get("Description").and_then(|v| v.as_str()),
            "schema": props.get("Schema").map(|v| if let Some(s) = v.as_str() { s.to_string() } else { v.to_string() }),
        });
        let mut accounts = self.apigatewayv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if !state.apis.contains_key(&api_id) {
            return Err(format!("Api {api_id} not yet provisioned"));
        }
        state
            .models
            .entry(api_id.clone())
            .or_default()
            .insert(id.clone(), body);
        Ok(ProvisionResult::new(id.clone())
            .with("ModelId", id)
            .with("ApiId", api_id))
    }

    pub(super) fn delete_apigwv2_model(
        &self,
        physical_id: &str,
        attributes: &BTreeMap<String, String>,
    ) -> Result<(), String> {
        let Some(api_id) = attributes.get("ApiId") else {
            return Ok(());
        };
        let mut accounts = self.apigatewayv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if let Some(map) = state.models.get_mut(api_id) {
            map.remove(physical_id);
        }
        Ok(())
    }

    /// In-place update for AWS::ApiGatewayV2::Api. CFN keeps the same
    /// ApiId across updates; only mutable properties (Name, Description,
    /// CORS, RouteSelectionExpression, etc.) are rewritten.
    pub(super) fn update_apigwv2_api(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let api_id = existing.physical_id.clone();
        let mut accounts = self.apigatewayv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let api = state
            .apis
            .get_mut(&api_id)
            .ok_or_else(|| format!("Api {api_id} no longer exists in state"))?;

        if let Some(s) = props.get("Name").and_then(|v| v.as_str()) {
            api.name = s.to_string();
        }
        if let Some(s) = props.get("ProtocolType").and_then(|v| v.as_str()) {
            api.protocol_type = s.to_string();
        }
        api.description = props
            .get("Description")
            .and_then(|v| v.as_str())
            .map(String::from)
            .or_else(|| api.description.clone());
        if let Some(s) = props
            .get("RouteSelectionExpression")
            .and_then(|v| v.as_str())
        {
            api.route_selection_expression = s.to_string();
        }
        if let Some(s) = props
            .get("ApiKeySelectionExpression")
            .and_then(|v| v.as_str())
        {
            api.api_key_selection_expression = s.to_string();
        }
        if let Some(b) = props
            .get("DisableExecuteApiEndpoint")
            .and_then(|v| v.as_bool())
        {
            api.disable_execute_api_endpoint = b;
        }
        if let Some(s) = props.get("IpAddressType").and_then(|v| v.as_str()) {
            api.ip_address_type = s.to_string();
        }
        if let Some(cors) = props.get("CorsConfiguration").and_then(|v| v.as_object()) {
            api.cors_configuration = Some(ApiGwV2CorsConfiguration {
                allow_credentials: cors.get("AllowCredentials").and_then(|v| v.as_bool()),
                allow_headers: cors
                    .get("AllowHeaders")
                    .and_then(|v| v.as_array())
                    .map(|a| {
                        a.iter()
                            .filter_map(|v| v.as_str().map(String::from))
                            .collect()
                    }),
                allow_methods: cors
                    .get("AllowMethods")
                    .and_then(|v| v.as_array())
                    .map(|a| {
                        a.iter()
                            .filter_map(|v| v.as_str().map(String::from))
                            .collect()
                    }),
                allow_origins: cors
                    .get("AllowOrigins")
                    .and_then(|v| v.as_array())
                    .map(|a| {
                        a.iter()
                            .filter_map(|v| v.as_str().map(String::from))
                            .collect()
                    }),
                expose_headers: cors
                    .get("ExposeHeaders")
                    .and_then(|v| v.as_array())
                    .map(|a| {
                        a.iter()
                            .filter_map(|v| v.as_str().map(String::from))
                            .collect()
                    }),
                max_age: cors
                    .get("MaxAge")
                    .and_then(|v| v.as_i64())
                    .map(|n| n as i32),
            });
        }
        if let Some(obj) = props.get("Tags").and_then(|v| v.as_object()) {
            let tags: BTreeMap<String, String> = obj
                .iter()
                .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                .collect();
            api.tags = Some(tags);
        }

        let api_endpoint = api.api_endpoint.clone();
        Ok(ProvisionResult::new(api_id.clone())
            .with("ApiId", api_id)
            .with("ApiEndpoint", api_endpoint))
    }

    /// In-place update for AWS::ApiGatewayV2::Route. The RouteId is
    /// stable across updates; only Target / AuthorizationType /
    /// AuthorizerId / RouteKey are rewritten.
    pub(super) fn update_apigwv2_route(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let api_id = props
            .get("ApiId")
            .and_then(|v| v.as_str())
            .ok_or("ApiId is required")?
            .to_string();
        let route_id = existing.physical_id.clone();

        let mut accounts = self.apigatewayv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let routes = state
            .routes
            .get_mut(&api_id)
            .ok_or_else(|| format!("Api {api_id} not yet provisioned"))?;
        let route = routes
            .get_mut(&route_id)
            .ok_or_else(|| format!("Route {route_id} not yet provisioned for api {api_id}"))?;
        if let Some(s) = props.get("RouteKey").and_then(|v| v.as_str()) {
            route.route_key = s.to_string();
        }
        if let Some(s) = props.get("Target").and_then(|v| v.as_str()) {
            route.target = Some(s.to_string());
        }
        if let Some(s) = props.get("AuthorizationType").and_then(|v| v.as_str()) {
            route.authorization_type = Some(s.to_string());
        }
        if let Some(s) = props.get("AuthorizerId").and_then(|v| v.as_str()) {
            route.authorizer_id = Some(s.to_string());
        }
        Ok(ProvisionResult::new(route_id.clone())
            .with("RouteId", route_id)
            .with("ApiId", api_id))
    }

    /// In-place update for AWS::ApiGatewayV2::Integration.
    pub(super) fn update_apigwv2_integration(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let api_id = props
            .get("ApiId")
            .and_then(|v| v.as_str())
            .ok_or("ApiId is required")?
            .to_string();
        let integration_id = existing.physical_id.clone();

        let mut accounts = self.apigatewayv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let integrations = state
            .integrations
            .get_mut(&api_id)
            .ok_or_else(|| format!("Api {api_id} not yet provisioned"))?;
        let integ = integrations.get_mut(&integration_id).ok_or_else(|| {
            format!("Integration {integration_id} not yet provisioned for api {api_id}")
        })?;
        if let Some(s) = props.get("IntegrationType").and_then(|v| v.as_str()) {
            integ.integration_type = s.to_string();
        }
        if let Some(s) = props.get("IntegrationUri").and_then(|v| v.as_str()) {
            integ.integration_uri = Some(s.to_string());
        }
        if let Some(s) = props.get("PayloadFormatVersion").and_then(|v| v.as_str()) {
            integ.payload_format_version = Some(s.to_string());
        }
        if let Some(n) = props.get("TimeoutInMillis").and_then(|v| v.as_i64()) {
            integ.timeout_in_millis = Some(n);
        }
        Ok(ProvisionResult::new(integration_id.clone())
            .with("IntegrationId", integration_id)
            .with("ApiId", api_id))
    }

    /// In-place update for AWS::ApiGatewayV2::IntegrationResponse. The
    /// composite physical id `{integration_id}/{response_id}` is stable
    /// across updates; only the embedded response body is rewritten.
    pub(super) fn update_apigwv2_integration_response(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let api_id = props
            .get("ApiId")
            .and_then(|v| v.as_str())
            .ok_or("ApiId is required")?
            .to_string();
        let composite_key = existing.physical_id.clone();
        let (integration_id, response_id) = composite_key
            .split_once('/')
            .map(|(a, b)| (a.to_string(), b.to_string()))
            .ok_or_else(|| format!("Invalid IntegrationResponse physical id: {composite_key}"))?;
        let key_expr = props
            .get("IntegrationResponseKey")
            .and_then(|v| v.as_str())
            .ok_or("IntegrationResponseKey is required")?
            .to_string();
        let body = serde_json::json!({
            "integrationResponseId": response_id,
            "integrationId": integration_id,
            "integrationResponseKey": key_expr,
            "responseTemplates": props.get("ResponseTemplates").cloned().unwrap_or(serde_json::json!({})),
            "responseParameters": props.get("ResponseParameters").cloned().unwrap_or(serde_json::json!({})),
            "templateSelectionExpression": props.get("TemplateSelectionExpression").and_then(|v| v.as_str()),
            "contentHandlingStrategy": props.get("ContentHandlingStrategy").and_then(|v| v.as_str()),
        });
        let mut accounts = self.apigatewayv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let map = state
            .integration_responses
            .get_mut(&api_id)
            .ok_or_else(|| format!("No integration responses found for api {api_id}"))?;
        if !map.contains_key(&composite_key) {
            return Err(format!(
                "IntegrationResponse {composite_key} not yet provisioned for api {api_id}"
            ));
        }
        map.insert(composite_key.clone(), body);
        Ok(ProvisionResult::new(composite_key)
            .with("IntegrationResponseId", response_id)
            .with("IntegrationId", integration_id)
            .with("ApiId", api_id))
    }

    /// In-place update for AWS::ApiGatewayV2::RouteResponse. Composite
    /// physical id `{route_id}/{response_id}` is preserved.
    pub(super) fn update_apigwv2_route_response(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let api_id = props
            .get("ApiId")
            .and_then(|v| v.as_str())
            .ok_or("ApiId is required")?
            .to_string();
        let composite = existing.physical_id.clone();
        let (route_id, response_id) = composite
            .split_once('/')
            .map(|(a, b)| (a.to_string(), b.to_string()))
            .ok_or_else(|| format!("Invalid RouteResponse physical id: {composite}"))?;
        let key_expr = props
            .get("RouteResponseKey")
            .and_then(|v| v.as_str())
            .ok_or("RouteResponseKey is required")?
            .to_string();
        let body = serde_json::json!({
            "routeResponseId": response_id,
            "routeId": route_id,
            "routeResponseKey": key_expr,
            "responseModels": props.get("ResponseModels").cloned().unwrap_or(serde_json::json!({})),
            "modelSelectionExpression": props.get("ModelSelectionExpression").and_then(|v| v.as_str()),
            "responseParameters": props.get("ResponseParameters").cloned().unwrap_or(serde_json::json!({})),
        });
        let mut accounts = self.apigatewayv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let map = state
            .route_responses
            .get_mut(&api_id)
            .ok_or_else(|| format!("No route responses found for api {api_id}"))?;
        if !map.contains_key(&composite) {
            return Err(format!(
                "RouteResponse {composite} not yet provisioned for api {api_id}"
            ));
        }
        map.insert(composite.clone(), body);
        Ok(ProvisionResult::new(composite)
            .with("RouteResponseId", response_id)
            .with("RouteId", route_id)
            .with("ApiId", api_id))
    }

    /// In-place update for AWS::ApiGatewayV2::Stage. StageName is the
    /// physical id and stays stable; description / deployment id /
    /// auto-deploy can change.
    pub(super) fn update_apigwv2_stage(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let api_id = props
            .get("ApiId")
            .and_then(|v| v.as_str())
            .ok_or("ApiId is required")?
            .to_string();
        let stage_name = existing.physical_id.clone();

        let mut accounts = self.apigatewayv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let stages = state
            .stages
            .get_mut(&api_id)
            .ok_or_else(|| format!("Api {api_id} not yet provisioned"))?;
        let stage = stages
            .get_mut(&stage_name)
            .ok_or_else(|| format!("Stage {stage_name} not yet provisioned for api {api_id}"))?;
        if let Some(s) = props.get("Description").and_then(|v| v.as_str()) {
            stage.description = Some(s.to_string());
        }
        if let Some(s) = props.get("DeploymentId").and_then(|v| v.as_str()) {
            stage.deployment_id = Some(s.to_string());
        }
        if let Some(b) = props.get("AutoDeploy").and_then(|v| v.as_bool()) {
            stage.auto_deploy = b;
        }
        stage.last_updated_date = Some(Utc::now());
        Ok(ProvisionResult::new(stage_name.clone())
            .with("StageName", stage_name)
            .with("ApiId", api_id))
    }

    /// In-place update for AWS::ApiGatewayV2::Deployment. CFN allows
    /// editing the Description; the DeploymentId is stable.
    pub(super) fn update_apigwv2_deployment(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let api_id = props
            .get("ApiId")
            .and_then(|v| v.as_str())
            .ok_or("ApiId is required")?
            .to_string();
        let deployment_id = existing.physical_id.clone();
        let mut accounts = self.apigatewayv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let deps = state
            .deployments
            .get_mut(&api_id)
            .ok_or_else(|| format!("Api {api_id} not yet provisioned"))?;
        let dep = deps.get_mut(&deployment_id).ok_or_else(|| {
            format!("Deployment {deployment_id} not yet provisioned for api {api_id}")
        })?;
        if let Some(s) = props.get("Description").and_then(|v| v.as_str()) {
            dep.description = Some(s.to_string());
        }
        Ok(ProvisionResult::new(deployment_id.clone())
            .with("DeploymentId", deployment_id)
            .with("ApiId", api_id))
    }

    /// In-place update for AWS::ApiGatewayV2::Authorizer. Mutates the
    /// stored authorizer in place; the AuthorizerId remains stable.
    pub(super) fn update_apigwv2_authorizer(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let api_id = props
            .get("ApiId")
            .and_then(|v| v.as_str())
            .ok_or("ApiId is required")?
            .to_string();
        let authorizer_id = existing.physical_id.clone();

        let mut accounts = self.apigatewayv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let auths = state
            .authorizers
            .get_mut(&api_id)
            .ok_or_else(|| format!("Api {api_id} not yet provisioned"))?;
        let auth = auths.get_mut(&authorizer_id).ok_or_else(|| {
            format!("Authorizer {authorizer_id} not yet provisioned for api {api_id}")
        })?;
        if let Some(s) = props.get("Name").and_then(|v| v.as_str()) {
            auth.name = s.to_string();
        }
        if let Some(s) = props.get("AuthorizerType").and_then(|v| v.as_str()) {
            auth.authorizer_type = s.to_string();
        }
        if let Some(s) = props.get("AuthorizerUri").and_then(|v| v.as_str()) {
            auth.authorizer_uri = Some(s.to_string());
        }
        if let Some(arr) = props.get("IdentitySource").and_then(|v| v.as_array()) {
            auth.identity_source = Some(
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect(),
            );
        }
        if let Some(obj) = props.get("JwtConfiguration").and_then(|v| v.as_object()) {
            auth.jwt_configuration = Some(ApiGwV2JwtConfiguration {
                audience: obj.get("Audience").and_then(|v| v.as_array()).map(|a| {
                    a.iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect()
                }),
                issuer: obj.get("Issuer").and_then(|v| v.as_str()).map(String::from),
            });
        }
        Ok(ProvisionResult::new(authorizer_id.clone())
            .with("AuthorizerId", authorizer_id)
            .with("ApiId", api_id))
    }

    /// In-place update for AWS::ApiGatewayV2::DomainName. The DomainName
    /// is the physical id and stays stable; configuration / mTLS can
    /// change.
    pub(super) fn update_apigwv2_domain_name(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let domain_name = existing.physical_id.clone();
        let body = serde_json::json!({
            "domainName": domain_name,
            "domainNameConfigurations": props.get("DomainNameConfigurations").cloned().unwrap_or(serde_json::json!([])),
            "mutualTlsAuthentication": props.get("MutualTlsAuthentication").cloned(),
            "apiMappingSelectionExpression": null,
        });
        let mut accounts = self.apigatewayv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if !state.domain_names.contains_key(&domain_name) {
            return Err(format!("DomainName {domain_name} no longer exists"));
        }
        state.domain_names.insert(domain_name.clone(), body);
        Ok(ProvisionResult::new(domain_name.clone()).with("DomainName", domain_name))
    }

    /// In-place update for AWS::ApiGatewayV2::ApiMapping. The ApiMappingId
    /// is the physical id; ApiId / Stage / ApiMappingKey can change.
    pub(super) fn update_apigwv2_api_mapping(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let domain_name = props
            .get("DomainName")
            .and_then(|v| v.as_str())
            .ok_or("DomainName is required")?
            .to_string();
        let api_id = props
            .get("ApiId")
            .and_then(|v| v.as_str())
            .ok_or("ApiId is required")?
            .to_string();
        let stage = props
            .get("Stage")
            .and_then(|v| v.as_str())
            .ok_or("Stage is required")?
            .to_string();
        let api_mapping_key = props
            .get("ApiMappingKey")
            .and_then(|v| v.as_str())
            .map(String::from);
        let id = existing.physical_id.clone();
        let body = serde_json::json!({
            "apiMappingId": id,
            "apiId": api_id,
            "stage": stage,
            "apiMappingKey": api_mapping_key,
        });
        let mut accounts = self.apigatewayv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let map = state
            .api_mappings
            .get_mut(&domain_name)
            .ok_or_else(|| format!("DomainName {domain_name} no longer exists"))?;
        map.insert(id.clone(), body);
        Ok(ProvisionResult::new(id.clone())
            .with("ApiMappingId", id)
            .with("DomainName", domain_name))
    }

    /// In-place update for AWS::ApiGatewayV2::VpcLink. The VpcLinkId is
    /// the physical id; Name / SubnetIds / SecurityGroupIds / Tags can
    /// change.
    pub(super) fn update_apigwv2_vpc_link(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let id = existing.physical_id.clone();
        let mut accounts = self.apigatewayv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let body = state
            .vpc_links
            .get_mut(&id)
            .ok_or_else(|| format!("VpcLink {id} no longer exists"))?;
        if let Some(s) = props.get("Name").and_then(|v| v.as_str()) {
            body["name"] = serde_json::Value::String(s.to_string());
        }
        if let Some(v) = props.get("SubnetIds").cloned() {
            body["subnetIds"] = v;
        }
        if let Some(v) = props.get("SecurityGroupIds").cloned() {
            body["securityGroupIds"] = v;
        }
        if let Some(v) = props.get("Tags").cloned() {
            body["tags"] = v;
        }
        Ok(ProvisionResult::new(id.clone()).with("VpcLinkId", id))
    }

    /// In-place update for AWS::ApiGatewayV2::Model. The ModelId is the
    /// physical id and stays stable; Schema / Description / ContentType
    /// / Name can change.
    pub(super) fn update_apigwv2_model(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let api_id = props
            .get("ApiId")
            .and_then(|v| v.as_str())
            .ok_or("ApiId is required")?
            .to_string();
        let id = existing.physical_id.clone();
        let mut accounts = self.apigatewayv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let map = state
            .models
            .get_mut(&api_id)
            .ok_or_else(|| format!("Api {api_id} not yet provisioned"))?;
        let body = map
            .get_mut(&id)
            .ok_or_else(|| format!("Model {id} not yet provisioned for api {api_id}"))?;
        if let Some(s) = props.get("Name").and_then(|v| v.as_str()) {
            body["name"] = serde_json::Value::String(s.to_string());
        }
        if let Some(s) = props.get("ContentType").and_then(|v| v.as_str()) {
            body["contentType"] = serde_json::Value::String(s.to_string());
        }
        if let Some(s) = props.get("Description").and_then(|v| v.as_str()) {
            body["description"] = serde_json::Value::String(s.to_string());
        }
        if let Some(v) = props.get("Schema") {
            body["schema"] = serde_json::Value::String(if let Some(s) = v.as_str() {
                s.to_string()
            } else {
                v.to_string()
            });
        }
        Ok(ProvisionResult::new(id.clone())
            .with("ModelId", id)
            .with("ApiId", api_id))
    }
}
