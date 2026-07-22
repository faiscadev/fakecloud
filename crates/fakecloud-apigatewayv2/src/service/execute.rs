//! `ApiGatewayV2Service` `execute` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl ApiGatewayV2Service {
    /// Return the stored `MutualTlsAuthentication` block for a custom
    /// domain name, plus a marker that fakecloud accepted the trust
    /// store URI structurally without anchoring it to a real CA.
    ///
    /// Surfaced via
    /// `/_fakecloud/apigatewayv2/domain-names/{name}/mtls-info` so tests
    /// can assert what trust store URI we received and confirm the
    /// expected emulator gap (no external PKI validation).
    pub fn mtls_info(&self, domain_name: &str) -> Option<serde_json::Value> {
        let accounts = self.state.read();
        for (_, state) in accounts.iter() {
            if let Some(domain) = state.domain_names.get(domain_name) {
                let mtls = domain.get("MutualTlsAuthentication").cloned();
                return Some(serde_json::json!({
                    "domain_name": domain_name,
                    "mutual_tls_authentication": mtls,
                    "external_ca_validated": false,
                }));
            }
        }
        None
    }

    /// Determine the action from the HTTP method and path segments.
    /// API Gateway v2 uses REST-style routing:
    ///   POST   /v2/apis              -> CreateApi
    ///   GET    /v2/apis              -> GetApis
    ///   GET    /v2/apis/{api-id}     -> GetApi
    ///   PATCH  /v2/apis/{api-id}     -> UpdateApi
    ///   DELETE /v2/apis/{api-id}     -> DeleteApi
    ///   POST   /v2/apis/{api-id}/routes -> CreateRoute
    ///   GET    /v2/apis/{api-id}/routes -> GetRoutes
    ///   GET    /v2/apis/{api-id}/routes/{route-id} -> GetRoute
    ///   PATCH  /v2/apis/{api-id}/routes/{route-id} -> UpdateRoute
    ///   DELETE /v2/apis/{api-id}/routes/{route-id} -> DeleteRoute
    ///   POST   /v2/apis/{api-id}/integrations -> CreateIntegration
    ///   GET    /v2/apis/{api-id}/integrations -> GetIntegrations
    ///   GET    /v2/apis/{api-id}/integrations/{int-id} -> GetIntegration
    ///   PATCH  /v2/apis/{api-id}/integrations/{int-id} -> UpdateIntegration
    ///   DELETE /v2/apis/{api-id}/integrations/{int-id} -> DeleteIntegration
    ///   POST   /v2/apis/{api-id}/stages -> CreateStage
    ///   GET    /v2/apis/{api-id}/stages -> GetStages
    ///   GET    /v2/apis/{api-id}/stages/{stage-name} -> GetStage
    ///   PATCH  /v2/apis/{api-id}/stages/{stage-name} -> UpdateStage
    ///   DELETE /v2/apis/{api-id}/stages/{stage-name} -> DeleteStage
    ///   POST   /v2/apis/{api-id}/deployments -> CreateDeployment
    ///   GET    /v2/apis/{api-id}/deployments -> GetDeployments
    ///   GET    /v2/apis/{api-id}/deployments/{deployment-id} -> GetDeployment
    ///   POST   /v2/apis/{api-id}/authorizers -> CreateAuthorizer
    ///   GET    /v2/apis/{api-id}/authorizers -> GetAuthorizers
    ///   GET    /v2/apis/{api-id}/authorizers/{auth-id} -> GetAuthorizer
    ///   PATCH  /v2/apis/{api-id}/authorizers/{auth-id} -> UpdateAuthorizer
    ///   DELETE /v2/apis/{api-id}/authorizers/{auth-id} -> DeleteAuthorizer
    pub(super) fn resolve_action(
        req: &AwsRequest,
    ) -> Option<(&'static str, Option<String>, Option<String>)> {
        let segs = &req.path_segments;
        if segs.len() < 2 || segs[0] != "v2" {
            return None;
        }

        // Non-/v2/apis collections.
        let second = segs.get(1).map(|s| s.as_str());
        let m = &req.method;
        let res = segs.get(2).map(|s| s.to_string());
        let sub = segs.get(4).map(|s| s.to_string());

        // For non-/v2/apis collections, the primary identifier (domain name,
        // VPC link id, etc.) lives in segs[2] which we expose as `resource_id`
        // (slot 2 of the tuple). Sub-ids (api mapping id, page id) live in
        // segs[4] which we expose via the `api_id` slot purely as a carrier
        // — handlers always read it as the second-level identifier.
        if second == Some("domainnames") {
            return match (m, segs.len(), segs.get(3).map(|s| s.as_str())) {
                (&Method::POST, 2, _) => Some(("CreateDomainName", None, None)),
                (&Method::GET, 2, _) => Some(("GetDomainNames", None, None)),
                (&Method::GET, 3, _) => Some(("GetDomainName", None, res)),
                (&Method::PATCH, 3, _) => Some(("UpdateDomainName", None, res)),
                (&Method::DELETE, 3, _) => Some(("DeleteDomainName", None, res)),
                (&Method::POST, 4, Some("apimappings")) => Some(("CreateApiMapping", None, res)),
                (&Method::GET, 4, Some("apimappings")) => Some(("GetApiMappings", None, res)),
                (&Method::GET, 5, Some("apimappings")) => Some(("GetApiMapping", sub, res)),
                (&Method::PATCH, 5, Some("apimappings")) => Some(("UpdateApiMapping", sub, res)),
                (&Method::DELETE, 5, Some("apimappings")) => Some(("DeleteApiMapping", sub, res)),
                // Routing rules are nested under a domain name per the Smithy
                // model (/v2/domainnames/{DomainName}/routingrules[/...]).
                (&Method::POST, 4, Some("routingrules")) => Some(("CreateRoutingRule", None, res)),
                (&Method::GET, 4, Some("routingrules")) => Some(("ListRoutingRules", None, res)),
                (&Method::GET, 5, Some("routingrules")) => Some(("GetRoutingRule", sub, res)),
                (&Method::PUT, 5, Some("routingrules")) => Some(("PutRoutingRule", sub, res)),
                (&Method::DELETE, 5, Some("routingrules")) => Some(("DeleteRoutingRule", sub, res)),
                _ => None,
            };
        }

        if second == Some("vpclinks") {
            return match (m, segs.len()) {
                (&Method::POST, 2) => Some(("CreateVpcLink", None, None)),
                (&Method::GET, 2) => Some(("GetVpcLinks", None, None)),
                (&Method::GET, 3) => Some(("GetVpcLink", None, res)),
                (&Method::PATCH, 3) => Some(("UpdateVpcLink", None, res)),
                (&Method::DELETE, 3) => Some(("DeleteVpcLink", None, res)),
                _ => None,
            };
        }

        if second == Some("tags") {
            // /v2/tags/{resource-arn}
            let arn = segs.get(2).map(|s| s.to_string());
            return match *m {
                Method::POST => Some(("TagResource", None, arn)),
                Method::DELETE => Some(("UntagResource", None, arn)),
                Method::GET => Some(("GetTags", None, arn)),
                _ => None,
            };
        }

        if second == Some("portals") {
            return match (m, segs.len(), segs.get(3).map(|s| s.as_str())) {
                (&Method::POST, 2, _) => Some(("CreatePortal", None, None)),
                (&Method::GET, 2, _) => Some(("ListPortals", None, None)),
                (&Method::GET, 3, _) => Some(("GetPortal", None, res)),
                (&Method::PATCH, 3, _) => Some(("UpdatePortal", None, res)),
                (&Method::DELETE, 3, _) => Some(("DeletePortal", None, res)),
                // Smithy: DisablePortal is DELETE /v2/portals/{id}/publish
                // (it "unpublishes" the portal). PublishPortal is POST of the
                // same path.
                (&Method::DELETE, 4, Some("publish")) => Some(("DisablePortal", None, res)),
                (&Method::POST, 4, Some("preview")) => Some(("PreviewPortal", None, res)),
                (&Method::POST, 4, Some("publish")) => Some(("PublishPortal", None, res)),
                _ => None,
            };
        }

        if second == Some("portalproducts") {
            return match (m, segs.len(), segs.get(3).map(|s| s.as_str())) {
                (&Method::POST, 2, _) => Some(("CreatePortalProduct", None, None)),
                (&Method::GET, 2, _) => Some(("ListPortalProducts", None, None)),
                (&Method::GET, 3, _) => Some(("GetPortalProduct", None, res)),
                (&Method::PATCH, 3, _) => Some(("UpdatePortalProduct", None, res)),
                (&Method::DELETE, 3, _) => Some(("DeletePortalProduct", None, res)),
                (&Method::PUT, 4, Some("sharingpolicy")) => {
                    Some(("PutPortalProductSharingPolicy", None, res))
                }
                (&Method::GET, 4, Some("sharingpolicy")) => {
                    Some(("GetPortalProductSharingPolicy", None, res))
                }
                (&Method::DELETE, 4, Some("sharingpolicy")) => {
                    Some(("DeletePortalProductSharingPolicy", None, res))
                }
                (&Method::POST, 4, Some("productpages")) => Some(("CreateProductPage", None, res)),
                (&Method::GET, 4, Some("productpages")) => Some(("ListProductPages", None, res)),
                (&Method::GET, 5, Some("productpages")) => Some(("GetProductPage", sub, res)),
                (&Method::PATCH, 5, Some("productpages")) => Some(("UpdateProductPage", sub, res)),
                (&Method::DELETE, 5, Some("productpages")) => Some(("DeleteProductPage", sub, res)),
                (&Method::POST, 4, Some("productrestendpointpages")) => {
                    Some(("CreateProductRestEndpointPage", None, res))
                }
                (&Method::GET, 4, Some("productrestendpointpages")) => {
                    Some(("ListProductRestEndpointPages", None, res))
                }
                (&Method::GET, 5, Some("productrestendpointpages")) => {
                    Some(("GetProductRestEndpointPage", sub, res))
                }
                (&Method::PATCH, 5, Some("productrestendpointpages")) => {
                    Some(("UpdateProductRestEndpointPage", sub, res))
                }
                (&Method::DELETE, 5, Some("productrestendpointpages")) => {
                    Some(("DeleteProductRestEndpointPage", sub, res))
                }
                _ => None,
            };
        }

        if second != Some("apis") {
            return None;
        }

        // `api_id` is segs[2] (the api identifier) for every action below
        // that has one; `resource_id` is segs[4] (the routes/integrations/
        // stages/... child id). We resolve both once here so the match
        // body only picks the action name.
        // Percent-decode the apis-collection ids: core dispatch splits the raw
        // path without decoding, so the child id in segs[4] arrives URL-encoded
        // (e.g. `%24default` for the `$default` stage). Decoding here restores
        // the literal name handlers compare against stored keys. Server-generated
        // ids (routes/integrations/authorizers/...) contain no encodable chars,
        // so decoding is a no-op for them. This branch is guarded by
        // `second == Some("apis")` above, so it never touches the tags ARN path
        // (which intentionally keeps segments encoded and decodes later).
        let api_id = segs.get(2).map(|s| {
            percent_encoding::percent_decode_str(s)
                .decode_utf8_lossy()
                .into_owned()
        });
        let resource_id = segs.get(4).map(|s| {
            percent_encoding::percent_decode_str(s)
                .decode_utf8_lossy()
                .into_owned()
        });
        let collection = segs.get(3).map(|s| s.as_str());
        let method = &req.method;

        let action = match (method, segs.len(), collection) {
            // /v2/apis
            (&Method::POST, 2, _) => "CreateApi",
            (&Method::PUT, 2, _) => "ImportApi",
            (&Method::GET, 2, _) => "GetApis",
            // /v2/apis/{api-id}
            (&Method::GET, 3, _) => "GetApi",
            (&Method::PATCH, 3, _) => "UpdateApi",
            (&Method::PUT, 3, _) => "ReimportApi",
            (&Method::DELETE, 3, _) => "DeleteApi",
            // /v2/apis/{api-id}/{collection}
            (m, 4, Some(col)) => resolve_collection_action(m, col)?,
            // /v2/apis/{api-id}/{collection}/{resource-id}
            (m, 5, Some(col)) => resolve_resource_action(m, col)?,
            // /v2/apis/{api-id}/{collection}/{resource-id}/{sub}
            (m, 6, Some(col)) => {
                let sub = segs.get(5).map(|s| s.as_str())?;
                match (m.clone(), col, sub) {
                    (Method::POST, "integrations", "integrationresponses") => {
                        "CreateIntegrationResponse"
                    }
                    (Method::GET, "integrations", "integrationresponses") => {
                        "GetIntegrationResponses"
                    }
                    (Method::POST, "routes", "routeresponses") => "CreateRouteResponse",
                    (Method::GET, "routes", "routeresponses") => "GetRouteResponses",
                    (Method::GET, "models", "template") => "GetModelTemplate",
                    (Method::DELETE, "stages", "accesslogsettings") => "DeleteAccessLogSettings",
                    (Method::GET, "exports", _) => "ExportApi",
                    _ => return None,
                }
            }
            // /v2/apis/{api-id}/{collection}/{resource-id}/{sub}/{sub-id}
            (m, 7, Some(col)) => {
                let sub = segs.get(5).map(|s| s.as_str())?;
                match (m.clone(), col, sub) {
                    (Method::GET, "integrations", "integrationresponses") => {
                        "GetIntegrationResponse"
                    }
                    (Method::PATCH, "integrations", "integrationresponses") => {
                        "UpdateIntegrationResponse"
                    }
                    (Method::DELETE, "integrations", "integrationresponses") => {
                        "DeleteIntegrationResponse"
                    }
                    (Method::GET, "routes", "routeresponses") => "GetRouteResponse",
                    (Method::PATCH, "routes", "routeresponses") => "UpdateRouteResponse",
                    (Method::DELETE, "routes", "routeresponses") => "DeleteRouteResponse",
                    (Method::DELETE, "routes", "requestparameters") => {
                        "DeleteRouteRequestParameter"
                    }
                    (Method::DELETE, "stages", "routesettings") => "DeleteRouteSettings",
                    (Method::DELETE, "stages", "cache") => "ResetAuthorizersCache",
                    _ => return None,
                }
            }
            _ => return None,
        };

        Some((action, api_id, resource_id))
    }

    // ─── EXECUTE API ────────────────────────────────────────────────────

    pub(super) async fn handle_execute_api(
        &self,
        mut req: AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut api_id = String::new();
        let mut stage_name = String::new();
        let mut resource_path = String::new();
        let mut matched_route_key = String::new();

        let result: Result<AwsResponse, AwsServiceError> = async {
            // Try custom domain resolution first.
            let (a, s, stage_vars, via_custom_domain) = {
                let accounts = self.state.read();
                let empty = ApiGatewayV2State::new(&req.account_id, &req.region);
                let state = accounts.get(&req.account_id).unwrap_or(&empty);

                if let Some((a, s, new_segs, new_raw_path)) = resolve_custom_domain(&req, state) {
                    req.path_segments = new_segs;
                    req.raw_path = new_raw_path;
                    let stage_vars = state
                        .stages
                        .get(&a)
                        .and_then(|stages| stages.get(&s))
                        .and_then(|st| st.stage_variables.clone())
                        .unwrap_or_default();
                    (a, s, stage_vars, true)
                } else if let Some(a) = host_api_id(&req).filter(|id| state.apis.contains_key(id)) {
                    // Default execute-api endpoint: the API is keyed on the
                    // Host header (`{api-id}.execute-api.<region>...`). Scope
                    // stage/route resolution to that API so two APIs sharing a
                    // stage name don't collide, and honour the `$default`
                    // stage — whose URL omits the stage segment entirely
                    // (`https://{api-id}.execute-api.../items`).
                    let stages = state.stages.get(&a);
                    let first = req.path_segments.first().cloned();
                    let stage_name = match (&first, stages) {
                        (Some(seg), Some(st)) if st.contains_key(seg) => {
                            // Named stage in the first path segment; consume it.
                            req.path_segments.remove(0);
                            seg.clone()
                        }
                        (_, Some(st)) if st.contains_key("$default") => {
                            // `$default` stage serves the path directly; do not
                            // consume a segment.
                            "$default".to_string()
                        }
                        _ => {
                            return Err(AwsServiceError::aws_error(
                                StatusCode::NOT_FOUND,
                                "NotFoundException",
                                format!("Stage not found for API {}", a),
                            ));
                        }
                    };
                    let stage_vars = stages
                        .and_then(|st| st.get(&stage_name))
                        .and_then(|st| st.stage_variables.clone())
                        .unwrap_or_default();
                    (a, stage_name, stage_vars, false)
                } else {
                    // Execute API format: /{stage}/{path...}
                    if req.path_segments.is_empty() {
                        return Err(AwsServiceError::aws_error(
                            StatusCode::NOT_FOUND,
                            "NotFoundException",
                            "Stage not specified",
                        ));
                    }
                    let s = req.path_segments[0].clone();
                    // Strip the stage segment so route matching uses the resource path.
                    req.path_segments.remove(0);
                    let stage_vars = state
                        .stages
                        .iter()
                        .find_map(|(_, stages)| stages.get(&s))
                        .and_then(|st| st.stage_variables.clone())
                        .unwrap_or_default();
                    // Find which API has this stage (sort by API ID for deterministic resolution)
                    let mut stage_entries: Vec<_> = state
                        .stages
                        .iter()
                        .filter_map(|(api_id, stages)| {
                            stages.get(&s).map(|stage| (api_id.clone(), stage.clone()))
                        })
                        .collect();
                    stage_entries.sort_by(|a, b| a.0.cmp(&b.0));
                    let (a, _) = stage_entries.into_iter().next().ok_or_else(|| {
                        AwsServiceError::aws_error(
                            StatusCode::NOT_FOUND,
                            "NotFoundException",
                            format!("Stage not found: {}", s),
                        )
                    })?;
                    (a, s, stage_vars, false)
                }
            };

            api_id = a;
            stage_name = s;

            // Enforce `disableExecuteApiEndpoint`: when set, the default
            // `execute-api` endpoint returns 403. Custom-domain traffic is
            // unaffected — that's the whole point of disabling the default
            // endpoint.
            if !via_custom_domain {
                let disabled = {
                    let accounts = self.state.read();
                    accounts
                        .get(&req.account_id)
                        .and_then(|st| st.apis.get(&api_id))
                        .map(|api| api.disable_execute_api_endpoint)
                        .unwrap_or(false)
                };
                if disabled {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::FORBIDDEN,
                        "ForbiddenException",
                        "The execute-api endpoint is disabled for this API",
                    ));
                }
            }

            resource_path = if req.path_segments.is_empty() {
                "/".to_string()
            } else {
                format!("/{}", req.path_segments.join("/"))
            };

            let (routes, cors_config) = {
                let accounts = self.state.read();
                let empty = ApiGatewayV2State::new(&req.account_id, &req.region);
                let state = accounts.get(&req.account_id).unwrap_or(&empty);

                let routes = state
                    .routes
                    .get(&api_id)
                    .map(|r| r.values().cloned().collect())
                    .unwrap_or_default();

                let cors_config = state
                    .apis
                    .get(&api_id)
                    .and_then(|api| api.cors_configuration.clone());

                (routes, cors_config)
            };

            // Handle CORS preflight requests
            if let Some(ref cors_cfg) = cors_config {
                if cors::is_preflight_request(&req) {
                    return Ok(cors::handle_preflight(cors_cfg, &req));
                }
            }

            // WAFv2 inspection: when the matched stage's ARN is associated
            // with a WebACL and the service was wired with WAF state,
            // evaluate the request before route match / authorizer /
            // integration. Block / Captcha / Challenge short-circuit;
            // Count is recorded but lets the request continue.
            if let Some(resp) = self.evaluate_waf(&req, &api_id, &stage_name) {
                return Ok(resp);
            }

            // Match the request against routes
            let router = Router::new(routes);
            let route_match = router
                .match_route(req.method.as_str(), &resource_path)
                .ok_or_else(|| {
                    AwsServiceError::aws_error(
                        StatusCode::NOT_FOUND,
                        "NotFoundException",
                        format!("No route matches: {} {}", req.method, resource_path),
                    )
                })?;

            matched_route_key = route_match.route.route_key.clone();

            // Authorizer enforcement
            let authorizer_info = self
                .enforce_authorizer(&req, &api_id, &stage_name, &route_match.route)
                .await?;

            // Get the integration for this route
            let integration_id = route_match
                .route
                .target
                .as_ref()
                .and_then(|target| target.strip_prefix("integrations/"))
                .ok_or_else(|| {
                    AwsServiceError::aws_error(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "InternalError",
                        "Route has no integration",
                    )
                })?;

            let mut integration = {
                let accounts = self.state.read();
                let empty = ApiGatewayV2State::new(&req.account_id, &req.region);
                let state = accounts.get(&req.account_id).unwrap_or(&empty);
                state
                    .integrations
                    .get(&api_id)
                    .and_then(|integrations| integrations.get(integration_id))
                    .ok_or_else(|| {
                        AwsServiceError::aws_error(
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "InternalError",
                            format!("Integration not found: {}", integration_id),
                        )
                    })?
                    .clone()
            };

            // Substitute stage variables into the integration URI before dispatch.
            if let Some(ref uri) = integration.integration_uri {
                let substituted = substitute_stage_variables(uri, &stage_vars);
                if substituted != *uri {
                    integration.integration_uri = Some(substituted);
                }
            }

            // Handle based on integration type
            let mut response = match integration.integration_type.as_str() {
                "AWS_PROXY" => {
                    let delivery = self.delivery.as_ref().ok_or_else(|| {
                        AwsServiceError::aws_error(
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "InternalError",
                            "Lambda delivery not configured",
                        )
                    })?;

                    let integration_uri =
                        integration.integration_uri.as_ref().ok_or_else(|| {
                            AwsServiceError::aws_error(
                                StatusCode::INTERNAL_SERVER_ERROR,
                                "InternalError",
                                "Integration has no URI",
                            )
                        })?;

                    if is_lambda_arn(integration_uri) {
                        // Honour the integration's payloadFormatVersion:
                        // "1.0" sends the REST-shaped envelope, "2.0" (the
                        // default) sends the HTTP-API envelope.
                        let event = if integration
                            .payload_format_version
                            .as_deref()
                            .map(|v| v == "1.0")
                            .unwrap_or(false)
                        {
                            lambda_proxy::construct_event_v1(
                                &req,
                                &route_match.route.route_key,
                                &stage_name,
                                route_match.path_parameters,
                                authorizer_info,
                            )
                        } else {
                            lambda_proxy::construct_event(
                                &req,
                                &route_match.route.route_key,
                                &stage_name,
                                route_match.path_parameters,
                                authorizer_info,
                            )
                        };
                        lambda_proxy::invoke_lambda(delivery, integration_uri, event).await?
                    } else {
                        dispatch_aws_service_integration(delivery, integration_uri, &req)?
                    }
                }
                "HTTP_PROXY" => {
                    // HTTP proxy integration
                    let target_url = integration.integration_uri.as_ref().ok_or_else(|| {
                        AwsServiceError::aws_error(
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "InternalError",
                            "Integration has no URI",
                        )
                    })?;

                    // Substitute `{proxy}` / `{var}` path placeholders from the
                    // matched route into the backend URL.
                    let mut url = target_url.clone();
                    for (name, value) in &route_match.path_parameters {
                        url = url.replace(&format!("{{{name}}}"), value);
                    }
                    // Apply `requestParameters` parameter mappings + append the
                    // client query string, then forward with the integration's
                    // method (`integrationMethod`) when configured.
                    let (url, headers) = apply_request_parameters(
                        &url,
                        integration.request_parameters.as_ref(),
                        &req,
                    );
                    let method = integration
                        .integration_method
                        .as_deref()
                        .and_then(|m| {
                            http::Method::from_bytes(m.to_ascii_uppercase().as_bytes()).ok()
                        })
                        .unwrap_or_else(|| req.method.clone());
                    http_proxy::forward_request(
                        &url,
                        &method,
                        &headers,
                        &req.body,
                        integration.timeout_in_millis,
                    )
                    .await?
                }
                "MOCK" => {
                    // Mock integration
                    mock::create_mock_response()
                }
                _ => {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::NOT_IMPLEMENTED,
                        "NotImplemented",
                        format!(
                            "Integration type not supported: {}",
                            integration.integration_type
                        ),
                    ));
                }
            };

            // Add CORS headers if CORS is configured
            if let Some(ref cors_cfg) = cors_config {
                response = cors::add_cors_headers(response, cors_cfg);
            }

            Ok(response)
        }
        .await;

        let status_code = match &result {
            Ok(resp) => resp.status.as_u16(),
            Err(err) => err.status().as_u16(),
        };

        if !api_id.is_empty() {
            self.record_request(&req, &api_id, &stage_name, &resource_path, status_code);
        }

        self.emit_access_log(&req, &api_id, &stage_name, &matched_route_key, status_code);

        result
    }
}

/// Extract the API id from the execute-api `Host` header
/// (`{api-id}.execute-api.<region>.amazonaws.com`). Returns `None` when
/// the header is absent/empty so the caller falls back to stage scanning.
fn host_api_id(req: &AwsRequest) -> Option<String> {
    let host = req.headers.get("host").and_then(|v| v.to_str().ok())?;
    let id = host.split('.').next()?;
    if id.is_empty() || !host.contains("execute-api") {
        return None;
    }
    Some(id.to_string())
}

/// Resolve a `requestParameters` mapping value: a `$request.*` source
/// expression pulled from the incoming request, or a literal.
fn resolve_param_value(spec: &str, req: &AwsRequest) -> Option<String> {
    if let Some(name) = spec.strip_prefix("$request.header.") {
        return req
            .headers
            .get(name)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
    }
    if let Some(name) = spec.strip_prefix("$request.querystring.") {
        return req.query_params.get(name).cloned();
    }
    if spec.starts_with("$request.") {
        // Unsupported source expression (e.g. $request.path.*) — drop it
        // rather than forward a literal `$request...` string.
        return None;
    }
    // Static value: accept both the plain HTTP-API form and the
    // single-quoted REST-style form.
    let literal = if spec.len() >= 2 && spec.starts_with('\'') && spec.ends_with('\'') {
        &spec[1..spec.len() - 1]
    } else {
        spec
    };
    Some(literal.to_string())
}

/// Apply an HTTP-API integration's `requestParameters` parameter mappings
/// to the outgoing request, returning the final URL (path + query) and the
/// header map to forward. Supported keys follow the AWS format
/// `<action>:<location>.<name>` where action ∈ {overwrite, append, remove}
/// and location ∈ {header, querystring, path}.
fn apply_request_parameters(
    base_url: &str,
    mappings: Option<&std::collections::BTreeMap<String, String>>,
    req: &AwsRequest,
) -> (String, http::HeaderMap) {
    let mut headers = req.headers.clone();

    // Split the base URL into `scheme://authority`, path, and query.
    let (prefix, mut path, base_query) = match base_url.parse::<http::Uri>() {
        Ok(u) if u.authority().is_some() => {
            let prefix = format!(
                "{}://{}",
                u.scheme_str().unwrap_or("http"),
                u.authority().map(|a| a.as_str()).unwrap_or("")
            );
            (
                prefix,
                u.path().to_string(),
                u.query().unwrap_or("").to_string(),
            )
        }
        _ => (base_url.to_string(), String::new(), String::new()),
    };

    // Seed the query params from the URL's own query plus the client's.
    let mut query: Vec<(String, String)> = Vec::new();
    for raw in [base_query.as_str(), req.raw_query.as_str()] {
        for pair in raw.split('&').filter(|s| !s.is_empty()) {
            let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
            query.push((k.to_string(), v.to_string()));
        }
    }

    if let Some(mappings) = mappings {
        for (key, spec) in mappings {
            let Some((action, target)) = key.split_once(':') else {
                continue;
            };
            let (location, name) = match target.split_once('.') {
                Some((l, n)) => (l, n),
                None => (target, ""),
            };
            let value = if action == "remove" {
                None
            } else {
                resolve_param_value(spec, req)
            };
            match (action, location) {
                ("overwrite", "header") | ("append", "header") => {
                    if let (Ok(hn), Some(v)) = (
                        http::HeaderName::from_bytes(name.as_bytes()),
                        value
                            .as_deref()
                            .and_then(|v| http::HeaderValue::from_str(v).ok()),
                    ) {
                        if action == "overwrite" {
                            headers.insert(hn, v);
                        } else {
                            headers.append(hn, v);
                        }
                    }
                }
                ("remove", "header") => {
                    headers.remove(name);
                }
                ("overwrite", "querystring") => {
                    query.retain(|(k, _)| k != name);
                    if let Some(v) = value {
                        query.push((name.to_string(), v));
                    }
                }
                ("append", "querystring") => {
                    if let Some(v) = value {
                        query.push((name.to_string(), v));
                    }
                }
                ("remove", "querystring") => {
                    query.retain(|(k, _)| k != name);
                }
                ("overwrite", "path") => {
                    if let Some(v) = value {
                        path = if v.starts_with('/') {
                            v
                        } else {
                            format!("/{v}")
                        };
                    }
                }
                _ => {}
            }
        }
    }

    let query_string = query
        .iter()
        .map(|(k, v)| {
            if v.is_empty() {
                k.clone()
            } else {
                format!("{k}={v}")
            }
        })
        .collect::<Vec<_>>()
        .join("&");
    let url = if query_string.is_empty() {
        format!("{prefix}{path}")
    } else {
        format!("{prefix}{path}?{query_string}")
    };
    (url, headers)
}

#[cfg(test)]
mod request_param_tests {
    use super::*;
    use bytes::Bytes;
    use http::HeaderMap;
    use std::collections::HashMap;

    fn req_with(headers: &[(&str, &str)], query: &[(&str, &str)]) -> AwsRequest {
        let mut hm = HeaderMap::new();
        for (k, v) in headers {
            hm.insert(
                http::HeaderName::from_bytes(k.as_bytes()).unwrap(),
                v.parse().unwrap(),
            );
        }
        let qp: HashMap<String, String> = query
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        let raw_query = query
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join("&");
        AwsRequest {
            service: "apigateway".to_string(),
            action: String::new(),
            region: "us-east-1".to_string(),
            account_id: "123456789012".to_string(),
            request_id: "rid".to_string(),
            headers: hm,
            query_params: qp,
            body: Bytes::new(),
            body_stream: parking_lot::Mutex::new(None),
            path_segments: vec![],
            raw_path: "/items".to_string(),
            raw_query,
            method: http::Method::GET,
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        }
    }

    #[test]
    fn host_api_id_only_for_execute_api_host() {
        let req = req_with(
            &[("host", "abc123.execute-api.us-east-1.amazonaws.com")],
            &[],
        );
        assert_eq!(host_api_id(&req).as_deref(), Some("abc123"));
        let custom = req_with(&[("host", "api.example.com")], &[]);
        assert_eq!(host_api_id(&custom), None);
        assert_eq!(host_api_id(&req_with(&[], &[])), None);
    }

    #[test]
    fn request_parameters_overwrite_header_and_query() {
        let req = req_with(&[("x-user", "orig")], &[("a", "1")]);
        let mut mappings = std::collections::BTreeMap::new();
        mappings.insert("overwrite:header.x-user".to_string(), "'admin'".to_string());
        mappings.insert("append:querystring.trace".to_string(), "'on'".to_string());
        mappings.insert(
            "overwrite:header.x-from".to_string(),
            "$request.header.x-user".to_string(),
        );
        let (url, headers) =
            apply_request_parameters("http://backend.local/base", Some(&mappings), &req);
        // Static overwrite wins.
        assert_eq!(headers.get("x-user").unwrap(), "admin");
        // Source-expression header copies the ORIGINAL client value.
        assert_eq!(headers.get("x-from").unwrap(), "orig");
        // Query carries the client param plus the appended one.
        assert!(url.contains("a=1"), "url: {url}");
        assert!(url.contains("trace=on"), "url: {url}");
    }

    #[test]
    fn request_parameters_overwrite_path() {
        let req = req_with(&[], &[]);
        let mut mappings = std::collections::BTreeMap::new();
        mappings.insert("overwrite:path".to_string(), "'/rewritten'".to_string());
        let (url, _) = apply_request_parameters("http://backend.local/orig", Some(&mappings), &req);
        assert_eq!(url, "http://backend.local/rewritten");
    }
}
