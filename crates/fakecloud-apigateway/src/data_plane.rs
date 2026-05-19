//! Execute API (data plane) handler for API Gateway v1.
//!
//! Incoming unsigned HTTP requests that didn't match a control-plane
//! REST route land here. The first path segment is treated as the
//! stage name; the rest is matched against the resource tree of any
//! REST API that has the stage deployed. The matching method's
//! integration is then invoked.
//!
//! Resource matching uses AWS's path-parameter syntax: `{var}` matches
//! a single segment, `{var+}` greedily matches the rest of the path.
//! Method matching tries the exact verb first (`POST`, `GET`, …); if no
//! method-specific integration is configured, falls back to `ANY`
//! (registered via `x-amazon-apigateway-any-method`), matching real
//! REST API behavior. Methods configured for `OPTIONS` handle CORS
//! preflights.

use http::{Method, StatusCode};
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap};
use std::time::Instant;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::lambda_proxy;
use crate::service::helpers::response_key;
use crate::service::ApiGatewayService;
use crate::state::{AuthEffect, Authorizer, CachedAuthorizerResult, Integration};

/// Default `authorizerResultTtlInSeconds` per AWS docs.
const DEFAULT_AUTHORIZER_TTL_SECS: i64 = 300;

/// Resolved data-plane match: which API hosts this request, the
/// integration to invoke, the path-parameter bindings, the resource
/// path the integration was registered against, the stage variables,
/// and the method-level auth config (type + optional authorizer).
struct DataPlaneMatch {
    api_id: String,
    integration: Integration,
    path_params: BTreeMap<String, String>,
    resource_path: String,
    stage_vars: BTreeMap<String, String>,
    authorization_type: String,
    authorizer: Option<Authorizer>,
    /// Method-level request parameter declarations (`method.request.*`).
    /// `true` = required, `false` = optional.
    request_parameters: BTreeMap<String, bool>,
    /// Content-type → model name mapping for request body validation.
    request_models: BTreeMap<String, String>,
    /// Optional validator ID. When set, the data plane validates request
    /// parameters and/or body per the validator's configuration before
    /// invoking the integration.
    request_validator_id: Option<String>,
    /// Whether the matched method has `apiKeyRequired = true`. When set,
    /// the data plane enforces the `x-api-key` header + the associated
    /// usage plan's throttle/quota before invoking the integration.
    api_key_required: bool,
    /// WebACL ARN attached to the matched stage (`Stage.web_acl_arn`).
    /// Optional both because most stages don't have a WebACL and
    /// because the stage may not exist (data plane handles miss
    /// elsewhere).
    stage_web_acl_arn: Option<String>,
    /// Binary media types configured on the matched `RestApi`.
    /// Used by Lambda proxy `encode_body` and the HTTP backend
    /// response path to decide whether a payload is binary.
    binary_media_types: Vec<String>,
}

/// Outcome of authorizer evaluation. `claims`/`context` are merged into
/// `requestContext.authorizer` of the proxy event when present.
struct AuthorizerOutcome {
    principal_id: String,
    context: serde_json::Value,
    claims: Option<serde_json::Value>,
}

pub async fn handle(
    service: &ApiGatewayService,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    if req.path_segments.is_empty() {
        return Err(not_found(format!(
            "No matching API for path {}",
            req.raw_path
        )));
    }

    // ── Custom domain resolution ──
    // When the Host header matches a DomainName's regionalDomainName,
    // look up BasePathMappings to determine the target API + stage.
    // The longest matching basePath prefix wins (AWS behavior).
    let (stage_name, remaining, custom_domain_api_hint) = match resolve_custom_domain(service, req)
    {
        Some(triple) => triple,
        None => {
            let stage = req.path_segments[0].clone();
            let rest = req.path_segments[1..].to_vec();
            (Some(stage), rest, None)
        }
    };
    let stage_name = stage_name.unwrap_or_else(|| req.path_segments[0].clone());

    // Find the API/stage pair that owns this request.
    let DataPlaneMatch {
        api_id,
        integration,
        path_params,
        resource_path,
        stage_vars,
        authorization_type,
        authorizer,
        request_parameters,
        request_models,
        request_validator_id,
        api_key_required,
        stage_web_acl_arn,
        binary_media_types,
    } = {
        let accounts = service.state_handle().read();
        let state = match accounts.get(&req.account_id) {
            Some(s) => s,
            None => {
                return Err(not_found(format!(
                    "No matching API for path {}",
                    req.raw_path
                )))
            }
        };
        let mut found: Option<DataPlaneMatch> = None;
        for (api_id, api_stages) in &state.stages {
            // When custom domain resolved an explicit API, skip others.
            if let Some(ref hint) = custom_domain_api_hint {
                if api_id != hint {
                    continue;
                }
            }
            let Some(_stage) = api_stages.get(&stage_name) else {
                continue;
            };
            let resources = match state.resources.get(api_id) {
                Some(r) => r,
                None => continue,
            };
            for resource in resources.values() {
                if let Some(params) = match_resource_path(&resource.path, &remaining) {
                    let exact_key = format!(
                        "{api_id}/{}/{}",
                        resource.id,
                        req.method.as_str().to_uppercase()
                    );
                    let any_key = format!("{api_id}/{}/ANY", resource.id);
                    let (integration_opt, method_lookup_key) = match (
                        state.integrations.get(&exact_key),
                        state.integrations.get(&any_key),
                    ) {
                        (Some(i), _) => (Some(i.clone()), exact_key.clone()),
                        (None, Some(i)) => (Some(i.clone()), any_key.clone()),
                        (None, None) => (None, exact_key.clone()),
                    };
                    if let Some(integration) = integration_opt {
                        // Look up the matching method record so we can
                        // pick up its authorizer config. Fall back to
                        // ANY when no method-specific record exists, so
                        // catch-all routes still get authorized.
                        let method_record = state
                            .methods
                            .get(&method_lookup_key)
                            .or_else(|| state.methods.get(&any_key))
                            .cloned();
                        let authorization_type = method_record
                            .as_ref()
                            .map(|m| m.authorization_type.clone())
                            .unwrap_or_else(|| "NONE".to_string());
                        let authorizer = method_record
                            .as_ref()
                            .and_then(|m| m.authorizer_id.clone())
                            .and_then(|aid| {
                                state
                                    .authorizers
                                    .get(api_id)
                                    .and_then(|m| m.get(&aid))
                                    .cloned()
                            });
                        let stage_vars = api_stages
                            .get(&stage_name)
                            .map(|s| s.variables.clone())
                            .unwrap_or_default();
                        let api_key_required = method_record
                            .as_ref()
                            .map(|m| m.api_key_required)
                            .unwrap_or(false);
                        let request_parameters = method_record
                            .as_ref()
                            .map(|m| m.request_parameters.clone())
                            .unwrap_or_default();
                        let request_models = method_record
                            .as_ref()
                            .map(|m| m.request_models.clone())
                            .unwrap_or_default();
                        let request_validator_id = method_record
                            .as_ref()
                            .and_then(|m| m.request_validator_id.clone());
                        let stage_web_acl_arn = api_stages
                            .get(&stage_name)
                            .and_then(|s| s.web_acl_arn.clone());
                        let binary_media_types = state
                            .apis
                            .get(api_id)
                            .map(|api| api.binary_media_types.clone())
                            .unwrap_or_default();
                        found = Some(DataPlaneMatch {
                            api_id: api_id.clone(),
                            integration,
                            path_params: params,
                            resource_path: resource.path.clone(),
                            stage_vars,
                            authorization_type,
                            authorizer,
                            request_parameters,
                            request_models,
                            request_validator_id,
                            api_key_required,
                            stage_web_acl_arn,
                            binary_media_types,
                        });
                        break;
                    }
                }
            }
            if found.is_some() {
                break;
            }
        }
        match found {
            Some(x) => x,
            None => {
                return Err(not_found(format!(
                    "No matching API for path {}",
                    req.raw_path
                )))
            }
        }
    };

    // WAFv2 inspection: when the matched stage's ARN is associated
    // with a WebACL and the service was wired with WAF state,
    // evaluate the request before the authorizer. Block / Captcha /
    // Challenge short-circuit; Count is recorded but lets the request
    // fall through. The `stage_web_acl_arn` field on the stage is a
    // hint cached from AssociateWebACL — we still hit the WAFv2
    // association table for the actual lookup, since that's the
    // source of truth.
    let _ = &stage_web_acl_arn;
    let stage_arn = stage_resource_arn(&req.region, &api_id, &stage_name);
    if let Some(resp) = evaluate_waf(service, req, &stage_arn) {
        service.record_request(&req.account_id, &api_id, &stage_name, req, resp.status);
        return Ok(resp);
    }

    // Run the authorizer (when configured) before touching the
    // integration. AWS rejects with 401/403 here without ever invoking
    // the backend; mirror that semantics so caching and observability
    // in tests reflect a real auth failure rather than a bad upstream.
    let auth_outcome = match enforce_authorizer(
        service,
        req,
        &api_id,
        &stage_name,
        &resource_path,
        &authorization_type,
        authorizer.as_ref(),
    )
    .await
    {
        Ok(out) => out,
        Err(err) => {
            // Consult any configured gateway response template for the
            // failure category (UNAUTHORIZED for 401, ACCESS_DENIED for
            // 403) so customers can override the status code and body.
            // If the specific category has no override, fall back to
            // DEFAULT_4XX — AWS treats it as the catch-all for any 4xx
            // response that isn't otherwise customized.
            let response_type = match err.status() {
                StatusCode::UNAUTHORIZED => "UNAUTHORIZED",
                StatusCode::FORBIDDEN => "ACCESS_DENIED",
                _ => "DEFAULT_4XX",
            };
            let overridden = apply_gateway_response_override(
                service,
                &req.account_id,
                &api_id,
                response_type,
                &err,
            )
            .or_else(|| {
                if response_type == "DEFAULT_4XX" {
                    None
                } else {
                    apply_gateway_response_override(
                        service,
                        &req.account_id,
                        &api_id,
                        "DEFAULT_4XX",
                        &err,
                    )
                }
            });
            let recorded_status = overridden
                .as_ref()
                .map(|r| r.status)
                .unwrap_or_else(|| err.status());
            service.record_request(&req.account_id, &api_id, &stage_name, req, recorded_status);
            return match overridden {
                Some(resp) => Ok(resp),
                None => Err(err),
            };
        }
    };

    // Usage plan enforcement: the matched method opts in via
    // `apiKeyRequired = true`. The caller must present a known + enabled
    // `x-api-key`; if the key is associated with a usage plan that lists
    // this `(api_id, stage_name)` in `apiStages`, throttle + quota are
    // enforced. Plans without throttle/quota fall through unchanged.
    if api_key_required {
        // Effective method on the request — for ANY-method matches, the
        // caller's verb still drives method-level throttle lookups. The
        // method path AWS uses in `apiStages[].throttle` keys is
        // `<resource_path>/<HTTP_METHOD>` (e.g. `/items/GET`).
        let method_path = format!("{}/{}", resource_path, req.method.as_str().to_uppercase());
        if let Err(err) = enforce_usage_plan(service, req, &api_id, &stage_name, &method_path) {
            service.record_request(&req.account_id, &api_id, &stage_name, req, err.status());
            return Err(err);
        }
    }

    // Request validator enforcement: when the matched method references a
    // validator, check required parameters and/or validate the request body
    // against the declared model before invoking the integration.
    if let Some(validator_id) = &request_validator_id {
        if let Err(err) = enforce_request_validator(
            service,
            req,
            &api_id,
            &stage_name,
            validator_id,
            &request_parameters,
            &request_models,
            &path_params,
        ) {
            service.record_request(&req.account_id, &api_id, &stage_name, req, err.status());
            return Err(err);
        }
    }

    let mut vtl_ctx = crate::vtl::build_context(
        req,
        &api_id,
        &stage_name,
        &resource_path,
        &path_params,
        &stage_vars,
    );

    let result: Result<AwsResponse, AwsServiceError> = match integration.integration_type.as_str() {
        "AWS_PROXY" => {
            let function_arn = match integration.uri.as_deref() {
                Some(uri) => extract_lambda_arn(uri).ok_or_else(|| {
                    bad_gateway("AWS_PROXY integration uri must reference a Lambda function ARN")
                })?,
                None => {
                    return Err(bad_gateway("AWS_PROXY integration missing uri"));
                }
            };
            let mut event = lambda_proxy::construct_event(
                req,
                &api_id,
                &stage_name,
                &resource_path,
                path_params,
                stage_vars,
                &binary_media_types,
            );
            if let Some(out) = &auth_outcome {
                inject_authorizer_into_event(&mut event, out);
            }
            let delivery = service
                .delivery()
                .ok_or_else(|| bad_gateway("Lambda delivery not configured"))?;
            lambda_proxy::invoke_lambda(delivery, &function_arn, event).await
        }
        "HTTP_PROXY" => http_proxy(req, &integration, None).await,
        "HTTP" => {
            if integration.connection_type.as_deref() == Some("VPC_LINK") {
                vpc_link_proxy(req, &integration, service).await
            } else {
                // Apply request template before sending to backend.
                let transformed_body = apply_request_template(req, &integration, &mut vtl_ctx);
                let backend_resp = http_proxy(req, &integration, transformed_body).await?;
                // Apply response template after receiving from backend.
                apply_response_template(
                    backend_resp,
                    &integration,
                    req,
                    &mut vtl_ctx,
                    &api_id,
                    &resource_path,
                    &stage_name,
                    &path_params,
                    &stage_vars,
                    service,
                )
                .await
            }
        }
        "MOCK" => {
            mock_response(
                req,
                &integration,
                &mut vtl_ctx,
                &api_id,
                &resource_path,
                &stage_name,
                service,
            )
            .await
        }
        "AWS" => {
            let uri = integration
                .uri
                .as_deref()
                .ok_or_else(|| bad_gateway("AWS integration missing uri"))?;
            aws_direct_integration(req, uri, service).await
        }
        other => Err(bad_gateway(format!(
            "Integration type '{other}' not supported in fakecloud's data plane",
        ))),
    };

    // Record after the integration runs so introspection sees the real
    // outcome (e.g. 502 from a failed Lambda invoke or HTTP backend).
    let recorded_status = match &result {
        Ok(r) => r.status,
        Err(e) => e.status(),
    };
    service.record_request(&req.account_id, &api_id, &stage_name, req, recorded_status);

    result
}

fn not_found(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::NOT_FOUND, "NotFoundException", msg.into())
}

fn bad_gateway(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_GATEWAY, "BadGatewayException", msg.into())
}

fn unauthorized(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::UNAUTHORIZED,
        "UnauthorizedException",
        msg.into(),
    )
}

fn forbidden(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::FORBIDDEN, "AccessDeniedException", msg.into())
}

/// `ForbiddenException` matches the wire shape AWS returns for an
/// API-key check failure (missing key / unknown key / disabled key).
fn api_key_forbidden() -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::FORBIDDEN, "ForbiddenException", "Forbidden")
}

/// `LimitExceededException` is the wire shape AWS uses when throttle or
/// quota tripped at the data plane.
fn limit_exceeded() -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::TOO_MANY_REQUESTS,
        "LimitExceededException",
        "Limit Exceeded",
    )
}

/// Resolve the header name an authorizer's `identitySource` points at.
/// AWS uses `method.request.header.<Name>` (e.g.
/// `method.request.header.Authorization`). Bare names also work for
/// callers that store just `Authorization`. Defaults to `Authorization`
/// when nothing was configured.
fn header_name_from_identity_source(source: Option<&str>) -> String {
    let raw = source.unwrap_or("Authorization").trim();
    // `identitySource` may contain comma-separated entries; use the
    // first one (matches AWS's behaviour for primary-key caching).
    let first = raw.split(',').next().unwrap_or(raw).trim();
    if let Some(stripped) = first.strip_prefix("method.request.header.") {
        stripped.to_string()
    } else if first.is_empty() {
        "Authorization".to_string()
    } else {
        first.to_string()
    }
}

fn extract_header_value(req: &AwsRequest, header_name: &str) -> Option<String> {
    req.headers
        .get(header_name)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
}

/// Strip an optional `Bearer ` prefix from a TOKEN-authorizer header
/// value before forwarding the raw token to the Lambda. AWS leaves the
/// prefix in place, but Lambdas commonly receive it stripped too;
/// preserve verbatim to match real behaviour.
fn raw_token(value: &str) -> &str {
    value
}

async fn enforce_authorizer(
    service: &ApiGatewayService,
    req: &AwsRequest,
    api_id: &str,
    stage: &str,
    resource_path: &str,
    authorization_type: &str,
    authorizer: Option<&Authorizer>,
) -> Result<Option<AuthorizerOutcome>, AwsServiceError> {
    match authorization_type {
        // Methods that haven't opted into authorization pass through.
        // `AWS_IAM` is treated as "no method-level authorizer" here —
        // SigV4 enforcement is handled upstream by the request signer.
        "" | "NONE" | "AWS_IAM" => Ok(None),
        "CUSTOM" | "TOKEN" | "REQUEST" => {
            let authorizer = authorizer.ok_or_else(|| {
                forbidden("Method requires a custom authorizer but none is configured")
            })?;
            run_lambda_authorizer(service, req, api_id, stage, resource_path, authorizer).await
        }
        "COGNITO_USER_POOLS" => {
            let authorizer = authorizer.ok_or_else(|| {
                forbidden("Method requires Cognito authorization but no authorizer is attached")
            })?;
            run_cognito_authorizer(service, req, authorizer).await
        }
        other => Err(forbidden(format!(
            "Unsupported authorizationType '{other}'"
        ))),
    }
}

/// Invoke a TOKEN/REQUEST authorizer Lambda and translate its policy
/// into an Allow/Deny outcome. Caches successful evaluations by
/// `<authorizerId>|<token>` for `authorizerResultTtlInSeconds`.
async fn run_lambda_authorizer(
    service: &ApiGatewayService,
    req: &AwsRequest,
    api_id: &str,
    stage: &str,
    resource_path: &str,
    authorizer: &Authorizer,
) -> Result<Option<AuthorizerOutcome>, AwsServiceError> {
    // For TOKEN authorizers AWS treats the value of the configured
    // identity-source header as the cache key. For REQUEST authorizers
    // the cache key concatenates all configured sources; we keep the
    // simpler one-header model here because that's what real-world
    // configurations use 95% of the time.
    let header_name = header_name_from_identity_source(authorizer.identity_source.as_deref());
    let token_value = extract_header_value(req, &header_name).ok_or_else(|| {
        unauthorized(format!(
            "Missing required identity source header '{header_name}'"
        ))
    })?;
    if token_value.trim().is_empty() {
        return Err(unauthorized(format!(
            "Empty identity source header '{header_name}'"
        )));
    }

    let cache_key = format!("{}|{}", authorizer.id, token_value);
    if let Some(cached) = lookup_cached_auth(service, &req.account_id, &cache_key) {
        return interpret_cached(cached);
    }

    let auth_uri = authorizer
        .authorizer_uri
        .as_deref()
        .ok_or_else(|| bad_gateway("Authorizer is missing authorizerUri; cannot invoke Lambda"))?;
    let function_arn = extract_lambda_arn(auth_uri)
        .ok_or_else(|| bad_gateway("authorizerUri must reference a Lambda function ARN"))?;
    let method_arn = build_method_arn(req, api_id, stage, resource_path);

    let event = match authorizer.authorizer_type.as_str() {
        "TOKEN" => {
            json!({
                "type": "TOKEN",
                "methodArn": method_arn,
                "authorizationToken": raw_token(&token_value),
            })
        }
        // Default to REQUEST shape for any non-TOKEN Lambda authorizer.
        _ => {
            let mut headers = serde_json::Map::new();
            for (k, v) in req.headers.iter() {
                if let Ok(s) = v.to_str() {
                    headers.insert(
                        k.as_str().to_string(),
                        serde_json::Value::String(s.to_string()),
                    );
                }
            }
            let mut query = serde_json::Map::new();
            for (k, v) in &req.query_params {
                query.insert(k.clone(), serde_json::Value::String(v.clone()));
            }
            json!({
                "type": "REQUEST",
                "methodArn": method_arn,
                "resource": resource_path,
                "path": req.raw_path,
                "httpMethod": req.method.as_str(),
                "headers": headers,
                "queryStringParameters": query,
                "requestContext": {
                    "apiId": api_id,
                    "stage": stage,
                    "path": req.raw_path,
                    "httpMethod": req.method.as_str(),
                },
            })
        }
    };

    let delivery = service
        .delivery()
        .ok_or_else(|| bad_gateway("Lambda delivery not configured"))?;
    let response_bytes = delivery
        .invoke_lambda(&function_arn, &event.to_string())
        .await
        .ok_or_else(|| bad_gateway("Lambda delivery not configured"))?
        .map_err(|e| forbidden(format!("Authorizer Lambda failed: {e}")))?;
    let response: serde_json::Value = serde_json::from_slice(&response_bytes)
        .map_err(|e| forbidden(format!("Authorizer returned invalid JSON: {e}")))?;

    let principal_id = response
        .get("principalId")
        .and_then(|v| v.as_str())
        .unwrap_or("user")
        .to_string();
    let context = response
        .get("context")
        .cloned()
        .unwrap_or_else(|| serde_json::Value::Object(serde_json::Map::new()));
    let effect = parse_policy_effect(&response, &method_arn);

    let ttl = authorizer
        .authorizer_result_ttl_in_seconds
        .map(|v| v as i64)
        .unwrap_or(DEFAULT_AUTHORIZER_TTL_SECS);
    cache_auth_result(
        service,
        &req.account_id,
        cache_key,
        CachedAuthorizerResult {
            principal_id: principal_id.clone(),
            effect,
            context: context.clone(),
            claims: None,
            expires_at: chrono::Utc::now() + chrono::Duration::seconds(ttl),
        },
    );

    match effect {
        AuthEffect::Allow => Ok(Some(AuthorizerOutcome {
            principal_id,
            context,
            claims: None,
        })),
        AuthEffect::Deny => Err(forbidden("User is not authorized to access this resource")),
    }
}

/// Walk `policyDocument.Statement` and resolve to a single Allow/Deny
/// effect. Multiple matching Allow statements collapse to Allow; any
/// Deny short-circuits to Deny (mirroring the IAM policy combinator).
fn parse_policy_effect(response: &serde_json::Value, method_arn: &str) -> AuthEffect {
    let Some(stmts) = response
        .get("policyDocument")
        .and_then(|p| p.get("Statement"))
        .and_then(|s| s.as_array())
    else {
        return AuthEffect::Deny;
    };
    let mut allow = false;
    for stmt in stmts {
        let effect = stmt.get("Effect").and_then(|v| v.as_str()).unwrap_or("");
        // Resource matching: explicit ARN match, wildcard `*`, or
        // missing Resource (treat as `*`).
        let matches = match stmt.get("Resource") {
            Some(serde_json::Value::String(s)) => arn_matches(s, method_arn),
            Some(serde_json::Value::Array(arr)) => arr
                .iter()
                .filter_map(|v| v.as_str())
                .any(|s| arn_matches(s, method_arn)),
            _ => true,
        };
        if !matches {
            continue;
        }
        match effect {
            "Deny" => return AuthEffect::Deny,
            "Allow" => allow = true,
            _ => {}
        }
    }
    if allow {
        AuthEffect::Allow
    } else {
        AuthEffect::Deny
    }
}

/// Glob-match a policy resource expression (`arn:...:*` etc) against a
/// concrete method ARN. `*` matches any sequence inside a single
/// segment; `?` matches a single character.
fn arn_matches(pattern: &str, target: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    let mut p_chars = pattern.chars().peekable();
    let mut t_chars = target.chars().peekable();
    loop {
        match (p_chars.peek().copied(), t_chars.peek().copied()) {
            (None, None) => return true,
            (None, Some(_)) => return false,
            (Some('*'), _) => {
                p_chars.next();
                if p_chars.peek().is_none() {
                    return true;
                }
                while t_chars.peek().is_some() {
                    if arn_matches(
                        &p_chars.clone().collect::<String>(),
                        &t_chars.clone().collect::<String>(),
                    ) {
                        return true;
                    }
                    t_chars.next();
                }
                return false;
            }
            (Some('?'), Some(_)) => {
                p_chars.next();
                t_chars.next();
            }
            (Some(a), Some(b)) if a == b => {
                p_chars.next();
                t_chars.next();
            }
            _ => return false,
        }
    }
}

fn build_method_arn(req: &AwsRequest, api_id: &str, stage: &str, resource_path: &str) -> String {
    let trimmed = resource_path.trim_start_matches('/');
    format!(
        "arn:aws:execute-api:{}:{}:{}/{}/{}/{}",
        req.region,
        req.account_id,
        api_id,
        stage,
        req.method.as_str().to_uppercase(),
        trimmed,
    )
}

async fn run_cognito_authorizer(
    service: &ApiGatewayService,
    req: &AwsRequest,
    authorizer: &Authorizer,
) -> Result<Option<AuthorizerOutcome>, AwsServiceError> {
    let header_name = header_name_from_identity_source(authorizer.identity_source.as_deref());
    let token_value = extract_header_value(req, &header_name)
        .ok_or_else(|| unauthorized(format!("Missing required JWT in header '{header_name}'")))?;
    let token = token_value
        .strip_prefix("Bearer ")
        .or_else(|| token_value.strip_prefix("bearer "))
        .unwrap_or(&token_value)
        .trim();
    if token.is_empty() {
        return Err(unauthorized("Empty Authorization header"));
    }

    let cache_key = format!("{}|{}", authorizer.id, token);
    if let Some(cached) = lookup_cached_auth(service, &req.account_id, &cache_key) {
        return interpret_cached(cached);
    }

    let pool_arn = authorizer
        .provider_arns
        .first()
        .ok_or_else(|| forbidden("Cognito authorizer has no providerARNs configured"))?;
    let delivery = service
        .delivery()
        .ok_or_else(|| unauthorized("Cognito JWT verifier not configured"))?;
    let claims = delivery
        .verify_cognito_jwt(&req.account_id, pool_arn, token)
        .map_err(|e| unauthorized(format!("Invalid JWT: {e}")))?;

    let principal_id = claims
        .get("sub")
        .and_then(|v| v.as_str())
        .unwrap_or("user")
        .to_string();
    let ttl = authorizer
        .authorizer_result_ttl_in_seconds
        .map(|v| v as i64)
        .unwrap_or(DEFAULT_AUTHORIZER_TTL_SECS);
    cache_auth_result(
        service,
        &req.account_id,
        cache_key,
        CachedAuthorizerResult {
            principal_id: principal_id.clone(),
            effect: AuthEffect::Allow,
            context: serde_json::Value::Object(serde_json::Map::new()),
            claims: Some(claims.clone()),
            expires_at: chrono::Utc::now() + chrono::Duration::seconds(ttl),
        },
    );

    Ok(Some(AuthorizerOutcome {
        principal_id,
        context: serde_json::Value::Object(serde_json::Map::new()),
        claims: Some(claims),
    }))
}

fn lookup_cached_auth(
    service: &ApiGatewayService,
    account_id: &str,
    key: &str,
) -> Option<CachedAuthorizerResult> {
    let now = chrono::Utc::now();
    let mut accounts = service.state_handle().write();
    let state = accounts.get_or_create(account_id);
    if let Some(cached) = state.authorizer_cache.get(key) {
        if cached.expires_at > now {
            return Some(cached.clone());
        }
    }
    state.authorizer_cache.remove(key);
    None
}

fn cache_auth_result(
    service: &ApiGatewayService,
    account_id: &str,
    key: String,
    entry: CachedAuthorizerResult,
) {
    let mut accounts = service.state_handle().write();
    let state = accounts.get_or_create(account_id);
    state.authorizer_cache.insert(key, entry);
}

fn interpret_cached(
    cached: CachedAuthorizerResult,
) -> Result<Option<AuthorizerOutcome>, AwsServiceError> {
    match cached.effect {
        AuthEffect::Allow => Ok(Some(AuthorizerOutcome {
            principal_id: cached.principal_id,
            context: cached.context,
            claims: cached.claims,
        })),
        AuthEffect::Deny => Err(forbidden("User is not authorized to access this resource")),
    }
}

/// Apply the customer-configured gateway response template (if any) for
/// `response_type`. Returns `None` when no override is registered, in
/// which case the caller should propagate the original `AwsServiceError`
/// unchanged. AWS allows overriding the HTTP status code and the
/// response body via `responseTemplates` keyed by content type; we honor
/// both and substitute `$context.error.messageString` /
/// `$context.error.responseType` so the standard AWS-recommended template
/// `{"message":$context.error.messageString}` renders correctly.
fn apply_gateway_response_override(
    service: &ApiGatewayService,
    account_id: &str,
    api_id: &str,
    response_type: &str,
    err: &AwsServiceError,
) -> Option<AwsResponse> {
    let accounts = service.state_handle().read();
    let state = accounts.get(account_id)?;
    let value = state.gateway_responses.get(api_id)?.get(response_type)?;
    // `statusCode` may be a string or numeric per AWS docs; accept both
    // and reject anything that doesn't fit a u16 instead of silently
    // truncating it.
    let status_code = value
        .get("statusCode")
        .and_then(|v| {
            v.as_str().and_then(|s| s.parse::<u16>().ok()).or_else(|| {
                v.as_u64()
                    .filter(|n| *n <= u16::MAX as u64)
                    .map(|n| n as u16)
            })
        })
        .and_then(|n| StatusCode::from_u16(n).ok())
        .unwrap_or_else(|| err.status());
    let templates = value.get("responseTemplates").and_then(|v| v.as_object());
    let template = templates
        .and_then(|t| t.get("application/json").and_then(|v| v.as_str()))
        .map(|s| s.to_string());
    let body = match template {
        Some(t) => render_error_template(&t, response_type, &err.message()),
        // Default body matches AWS's built-in shape for an UNAUTHORIZED /
        // ACCESS_DENIED response.
        None => format!("{{\"message\":\"{}\"}}", escape_json(&err.message())),
    };
    Some(AwsResponse {
        status: status_code,
        content_type: "application/json".to_string(),
        body: bytes::Bytes::from(body.into_bytes()).into(),
        headers: http::HeaderMap::new(),
    })
}

/// Substitute the two `$context.error.*` variables AWS exposes in
/// gateway response templates. Anything else is left verbatim — full VTL
/// rendering belongs to integration request/response transforms, not
/// here.
fn render_error_template(template: &str, response_type: &str, message: &str) -> String {
    let escaped = escape_json(message);
    template
        .replace("$context.error.messageString", &format!("\"{escaped}\""))
        .replace("$context.error.message", &escaped)
        .replace("$context.error.responseType", response_type)
}

fn escape_json(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out
}

fn inject_authorizer_into_event(event: &mut serde_json::Value, outcome: &AuthorizerOutcome) {
    let Some(req_ctx) = event
        .get_mut("requestContext")
        .and_then(|v| v.as_object_mut())
    else {
        return;
    };
    let mut auth_obj = serde_json::Map::new();
    auth_obj.insert(
        "principalId".to_string(),
        serde_json::Value::String(outcome.principal_id.clone()),
    );
    if let serde_json::Value::Object(ctx) = &outcome.context {
        for (k, v) in ctx {
            auth_obj.insert(k.clone(), v.clone());
        }
    }
    if let Some(claims) = &outcome.claims {
        auth_obj.insert("claims".to_string(), claims.clone());
    }
    req_ctx.insert(
        "authorizer".to_string(),
        serde_json::Value::Object(auth_obj),
    );
}

/// Match `template` (e.g. `/items/{id}/parts`) against `path_segments`
/// (e.g. `["items", "42", "parts"]`). Returns the path-parameter map on
/// match, or `None` if the path doesn't fit the template.
fn match_resource_path(
    template: &str,
    path_segments: &[String],
) -> Option<BTreeMap<String, String>> {
    // Root resource: only an empty (or missing) remaining path matches.
    if template == "/" {
        return if path_segments.is_empty() {
            Some(BTreeMap::new())
        } else {
            None
        };
    }
    let template_segments: Vec<&str> = template.split('/').filter(|s| !s.is_empty()).collect();
    let mut params = BTreeMap::new();
    let mut t = 0;
    let mut p = 0;
    while t < template_segments.len() {
        let seg = template_segments[t];
        if seg.starts_with('{') && seg.ends_with('}') {
            let inner = seg.trim_start_matches('{').trim_end_matches('}');
            if let Some(name) = inner.strip_suffix('+') {
                // Greedy match — consume the remainder.
                if p >= path_segments.len() {
                    return None;
                }
                params.insert(name.to_string(), path_segments[p..].join("/"));
                return Some(params);
            }
            if p >= path_segments.len() {
                return None;
            }
            params.insert(inner.to_string(), path_segments[p].clone());
            p += 1;
        } else {
            if p >= path_segments.len() || path_segments[p] != seg {
                return None;
            }
            p += 1;
        }
        t += 1;
    }
    if p == path_segments.len() {
        Some(params)
    } else {
        None
    }
}

/// Pull the function ARN out of an AWS_PROXY integration URI of the
/// shape `arn:aws:apigateway:<region>:lambda:path/<api-version>/functions/<arn>/invocations`.
fn extract_lambda_arn(uri: &str) -> Option<String> {
    if !uri.contains(":lambda:path/") {
        return None;
    }
    let prefix = uri.split("/functions/").nth(1)?;
    let arn = prefix.trim_end_matches("/invocations");
    Some(arn.to_string())
}

async fn http_proxy(
    req: &AwsRequest,
    integration: &Integration,
    body_override: Option<bytes::Bytes>,
) -> Result<AwsResponse, AwsServiceError> {
    let url = integration
        .uri
        .as_ref()
        .ok_or_else(|| bad_gateway("HTTP integration missing uri"))?;
    let method = match req.method {
        Method::GET => reqwest::Method::GET,
        Method::POST => reqwest::Method::POST,
        Method::PUT => reqwest::Method::PUT,
        Method::DELETE => reqwest::Method::DELETE,
        Method::PATCH => reqwest::Method::PATCH,
        Method::HEAD => reqwest::Method::HEAD,
        Method::OPTIONS => reqwest::Method::OPTIONS,
        _ => reqwest::Method::GET,
    };
    let client = reqwest::Client::new();
    let mut builder = client.request(method, url);
    for (k, v) in req.headers.iter() {
        if let Ok(s) = v.to_str() {
            builder = builder.header(k.as_str(), s);
        }
    }
    let body = body_override.as_ref().unwrap_or(&req.body);
    if !body.is_empty() {
        builder = builder.body(body.clone().to_vec());
    }
    let resp = builder
        .send()
        .await
        .map_err(|e| bad_gateway(format!("backend HTTP failure: {e}")))?;
    let status = StatusCode::from_u16(resp.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
    let mut headers = http::HeaderMap::new();
    for (k, v) in resp.headers().iter() {
        if let (Ok(name), Ok(val)) = (
            http::HeaderName::from_bytes(k.as_str().as_bytes()),
            http::HeaderValue::from_bytes(v.as_bytes()),
        ) {
            // `append` preserves multi-value headers like multiple
            // `Set-Cookie` lines that the backend may emit.
            headers.append(name, val);
        }
    }
    let content_type = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/octet-stream")
        .to_string();
    let body = resp
        .bytes()
        .await
        .map_err(|e| bad_gateway(format!("backend body read failure: {e}")))?;
    Ok(AwsResponse {
        status,
        content_type,
        headers,
        body: bytes::Bytes::from(body.to_vec()).into(),
    })
}

/// Resolve a VPC_LINK integration by looking up the VpcLink's
/// `targetArns`, finding the first NLB/ALB that has a bound port in
/// the ELBv2 dataplane, and forwarding the request there.
async fn vpc_link_proxy(
    req: &AwsRequest,
    integration: &Integration,
    service: &ApiGatewayService,
) -> Result<AwsResponse, AwsServiceError> {
    let connection_id = integration
        .connection_id
        .as_deref()
        .ok_or_else(|| bad_gateway("VPC_LINK integration missing connectionId"))?;

    let target_arns: Vec<String> = {
        let accounts = service.state_handle().read();
        let state = accounts
            .get(&req.account_id)
            .ok_or_else(|| bad_gateway("VPC_LINK: account not found in API Gateway state"))?;
        let vpc_link = state
            .vpc_links
            .get(connection_id)
            .ok_or_else(|| bad_gateway(format!("VPC_LINK not found: {connection_id}")))?;
        let target_arns = vpc_link
            .get("targetArns")
            .and_then(|v| v.as_array())
            .ok_or_else(|| bad_gateway("VPC_LINK missing targetArns"))?;
        if target_arns.is_empty() {
            return Err(bad_gateway("VPC_LINK targetArns is empty"));
        }
        target_arns
            .iter()
            .filter_map(|v| v.as_str().map(|s| s.to_string()))
            .collect()
    };

    let elbv2 = service
        .elbv2_state
        .as_ref()
        .ok_or_else(|| bad_gateway("VPC_LINK not available: ELBv2 state not wired"))?;
    let port = {
        let elbv2_read = elbv2.read();
        let elbv2_account = elbv2_read
            .get(&req.account_id)
            .ok_or_else(|| bad_gateway("VPC_LINK: account not found in ELBv2 state"))?;
        let mut bound_port = None;
        for arn in &target_arns {
            if let Some(lb) = elbv2_account.load_balancers.get(arn) {
                if let Some(port) = lb.bound_port {
                    bound_port = Some(port);
                    break;
                }
            }
        }
        bound_port.ok_or_else(|| {
            bad_gateway("VPC_LINK: none of the target NLBs/ALBs have an active dataplane port")
        })?
    };

    // Build the backend URL from the integration URI, replacing the
    // host with the local ELBv2 dataplane endpoint.
    let original_url = integration.uri.as_deref().unwrap_or("http://localhost/");
    let path_and_query = http::Uri::try_from(original_url)
        .ok()
        .and_then(|u| u.path_and_query().map(|p| p.as_str().to_string()))
        .unwrap_or_else(|| req.raw_path.clone());
    let backend_url = format!("http://127.0.0.1:{port}{path_and_query}");

    let mut proxy_integration = integration.clone();
    proxy_integration.uri = Some(backend_url);
    http_proxy(req, &proxy_integration, None).await
}

/// Apply the integration's `requestTemplates` to the request body.
/// Returns `Some(transformed_body)` when a template matched the request
/// content type, or `None` to leave the body unchanged.
fn apply_request_template(
    req: &AwsRequest,
    integration: &Integration,
    vtl_ctx: &mut crate::vtl::Context,
) -> Option<bytes::Bytes> {
    let content_type = req
        .headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/json");
    let normalized = content_type
        .split(';')
        .next()
        .unwrap_or(content_type)
        .trim();
    let template = integration.request_templates.get(normalized)?;
    let rendered = crate::vtl::render(template, vtl_ctx);
    Some(bytes::Bytes::from(rendered))
}

/// Apply the integration response's `responseTemplates` to the backend
/// response body. Returns a new `AwsResponse` with the rendered template.
#[allow(clippy::too_many_arguments)]
async fn apply_response_template(
    backend_resp: AwsResponse,
    integration: &Integration,
    req: &AwsRequest,
    vtl_ctx: &mut crate::vtl::Context,
    api_id: &str,
    resource_path: &str,
    _stage_name: &str,
    _path_params: &BTreeMap<String, String>,
    _stage_vars: &BTreeMap<String, String>,
    service: &ApiGatewayService,
) -> Result<AwsResponse, AwsServiceError> {
    let status_code = backend_resp.status.as_u16().to_string();
    let key = response_key(
        api_id,
        resource_path,
        &integration.http_method,
        &status_code,
    );
    let accounts = service.state_handle().read();
    let state = accounts.get(&req.account_id);
    let resp_template = state.and_then(|st| {
        st.integration_responses
            .get(&key)
            .and_then(|v| v.get("responseTemplates"))
            .and_then(|t| t.as_object())
    });
    let content_type = backend_resp
        .content_type
        .as_str()
        .split(';')
        .next()
        .unwrap_or(&backend_resp.content_type)
        .trim();
    let template = resp_template
        .and_then(|t| t.get(content_type))
        .and_then(|v| v.as_str());
    let body = if let Some(template) = template {
        // Inject $input with the backend response body so the template
        // can reference it.
        let body_str = String::from_utf8_lossy(backend_resp.body.expect_bytes()).to_string();
        let body_json: Value = serde_json::from_str(&body_str).unwrap_or(Value::Null);
        vtl_ctx.set("input", json!({"body": body_str, "json": body_json}));
        crate::vtl::render(template, vtl_ctx).into_bytes()
    } else {
        backend_resp.body.expect_bytes().to_vec()
    };
    Ok(AwsResponse {
        status: backend_resp.status,
        content_type: backend_resp.content_type,
        headers: backend_resp.headers,
        body: bytes::Bytes::from(body).into(),
    })
}

/// Build a MOCK integration response by looking up the integration
/// response for the method and rendering its `responseTemplates`.
#[allow(clippy::too_many_arguments)]
async fn mock_response(
    req: &AwsRequest,
    integration: &Integration,
    vtl_ctx: &mut crate::vtl::Context,
    api_id: &str,
    resource_path: &str,
    _stage_name: &str,
    service: &ApiGatewayService,
) -> Result<AwsResponse, AwsServiceError> {
    let method = integration.http_method.as_str();
    // Default to 200 when no explicit status code is configured.
    let default_status = "200";
    let key = response_key(api_id, resource_path, method, default_status);
    let accounts = service.state_handle().read();
    let state = accounts.get(&req.account_id);
    // Try the 200 response first; if absent scan for any integration
    // response registered for this method and use its configured status.
    let resp_record = state.and_then(|st| {
        st.integration_responses.get(&key).or_else(|| {
            let prefix = format!("{api_id}/{resource_path}/{method}/");
            st.integration_responses
                .iter()
                .find(|(k, _)| k.starts_with(&prefix))
                .map(|(_, v)| v)
        })
    });
    let (status, resp_templates) = if let Some(record) = resp_record {
        let status = record
            .get("statusCode")
            .and_then(|v| v.as_str())
            .unwrap_or(default_status);
        let templates = record.get("responseTemplates").and_then(|v| v.as_object());
        (status, templates)
    } else {
        (default_status, None)
    };
    let status = status
        .parse::<u16>()
        .ok()
        .and_then(|n| StatusCode::from_u16(n).ok())
        .unwrap_or(StatusCode::OK);
    let content_type = "application/json";
    let body = if let Some(templates) = resp_templates {
        templates
            .get(content_type)
            .and_then(|v| v.as_str())
            .map(|t| crate::vtl::render(t, vtl_ctx))
            .unwrap_or_default()
    } else {
        String::new()
    };
    Ok(AwsResponse {
        status,
        content_type: content_type.to_string(),
        headers: http::HeaderMap::new(),
        body: bytes::Bytes::from(body).into(),
    })
}

/// Dispatch an `AWS` direct service integration to the corresponding
/// fakecloud service handler. The integration URI follows the API Gateway
/// format: `arn:aws:apigateway:{region}:{service}:action/{Action}` or
/// `arn:aws:apigateway:{region}:{service}:path/{path}`.
async fn aws_direct_integration(
    req: &AwsRequest,
    uri: &str,
    service: &ApiGatewayService,
) -> Result<AwsResponse, AwsServiceError> {
    let registry = service.registry().ok_or_else(|| {
        bad_gateway("AWS direct integration not available: service registry not wired")
    })?;
    let registry = registry.get().ok_or_else(|| {
        bad_gateway("AWS direct integration not available: service registry not yet populated")
    })?;

    let parts: Vec<&str> = uri.split(':').collect();
    if parts.len() < 6 || parts[0] != "arn" || parts[1] != "aws" || parts[2] != "apigateway" {
        return Err(bad_gateway(format!(
            "AWS integration uri not in expected ARN format: {uri}"
        )));
    }
    let target_service = parts[4];
    let action_or_path = parts[5];

    let target = registry.get(target_service).ok_or_else(|| {
        bad_gateway(format!(
            "AWS integration target service '{target_service}' not registered"
        ))
    })?;

    let mut dispatch_req = AwsRequest {
        service: target_service.to_string(),
        action: req.action.clone(),
        region: req.region.clone(),
        account_id: req.account_id.clone(),
        request_id: uuid::Uuid::new_v4().to_string(),
        headers: req.headers.clone(),
        query_params: req.query_params.clone(),
        body: req.body.clone(),
        body_stream: parking_lot::Mutex::new(None),
        path_segments: req.path_segments.clone(),
        raw_path: req.raw_path.clone(),
        raw_query: req.raw_query.clone(),
        method: req.method.clone(),
        is_query_protocol: false,
        access_key_id: req.access_key_id.clone(),
        principal: req.principal.clone(),
    };

    if let Some(action) = action_or_path.strip_prefix("action/") {
        dispatch_req.action = action.to_string();
    } else if let Some(path) = action_or_path.strip_prefix("path/") {
        dispatch_req.raw_path = format!("/{path}");
        dispatch_req.path_segments = path.split('/').map(|s| s.to_string()).collect();
    } else {
        return Err(bad_gateway(format!(
            "AWS integration uri must contain action/ or path/ segment: {uri}"
        )));
    }

    target.handle(dispatch_req).await
}

// ── Usage plan throttle + quota ──

/// In-memory throttle + quota state. Buckets are keyed by
/// `(account_id, plan_id, key_id, method_override_path)` — the trailing
/// segment is empty when the plan-level throttle is in effect, or the
/// `apiStages[].throttle` map key (e.g. `/items/GET`) when a method
/// override applies. Counters add a period-window string to the same
/// `(account, plan, key)` tuple so each window meters independently.
/// Lives in `ApiGatewayService::meters`; not persisted across restarts.
#[derive(Default)]
pub struct UsageMeters {
    pub buckets: HashMap<(String, String, String, String), TokenBucket>,
    pub counters: HashMap<(String, String, String, String), u64>,
}

/// Hand-rolled token bucket. AWS's API Gateway throttle is documented
/// as a refilling token bucket with `rateLimit` tokens/sec sustained
/// rate and `burstLimit` capacity.
#[derive(Debug, Clone)]
pub struct TokenBucket {
    pub rate_per_sec: f64,
    pub burst: f64,
    pub tokens: f64,
    pub last_refill: Instant,
}

impl TokenBucket {
    pub fn new(rate_per_sec: f64, burst: f64) -> Self {
        Self {
            rate_per_sec,
            burst,
            tokens: burst,
            last_refill: Instant::now(),
        }
    }

    /// Try to consume one token. Returns `true` on success. Refills the
    /// bucket up to `burst` based on elapsed wall-clock time first.
    pub fn try_acquire(&mut self, now: Instant) -> bool {
        self.try_acquire_with(now, 1.0)
    }

    pub fn try_acquire_with(&mut self, now: Instant, cost: f64) -> bool {
        let elapsed = now
            .saturating_duration_since(self.last_refill)
            .as_secs_f64();
        if elapsed > 0.0 {
            self.tokens = (self.tokens + elapsed * self.rate_per_sec).min(self.burst);
            self.last_refill = now;
        }
        if self.tokens >= cost {
            self.tokens -= cost;
            true
        } else {
            false
        }
    }
}

/// Resolve the usage plan that matches `(api_id, stage_name)` for an
/// API key. If no plan matches, the request is unmetered (the key is
/// known but not associated with a plan that targets this stage).
fn enforce_usage_plan(
    service: &ApiGatewayService,
    req: &AwsRequest,
    api_id: &str,
    stage_name: &str,
    method_path: &str,
) -> Result<(), AwsServiceError> {
    let presented = req
        .headers
        .get("x-api-key")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());
    let presented = match presented {
        Some(s) if !s.trim().is_empty() => s,
        _ => return Err(api_key_forbidden()),
    };

    // Resolve key + the active usage plan under a single read lock.
    // `plan = None` when the key isn't associated with any plan that
    // targets this stage; in that case the request is unmetered.
    let (key_id, plan): (String, Option<UsagePlanSnapshot>) = {
        let accounts = service.state_handle().read();
        let state = accounts
            .get(&req.account_id)
            .ok_or_else(api_key_forbidden)?;
        let key = state
            .api_keys
            .values()
            .find(|k| k.value == presented)
            .cloned();
        let key = match key {
            Some(k) if k.enabled => k,
            _ => return Err(api_key_forbidden()),
        };
        let plan = first_matching_plan(state, &key.id, api_id, stage_name, method_path);
        (key.id, plan)
    };
    let Some(plan) = plan else {
        return Ok(());
    };

    let mut meters = service.meters.lock();

    // Throttle. A method-level override (`apiStages[].throttle[<path>]`)
    // takes precedence over the plan-level throttle. The bucket key
    // includes the method override path so each `(plan, key, method)`
    // triple meters independently — matching AWS's documented behavior
    // that method-level limits are evaluated separately from the plan
    // overall throttle.
    if let Some((rate, burst, method_override)) = plan.throttle {
        let bucket_key = (
            req.account_id.clone(),
            plan.id.clone(),
            key_id.clone(),
            method_override.unwrap_or_default(),
        );
        let bucket = meters
            .buckets
            .entry(bucket_key)
            .or_insert_with(|| TokenBucket::new(rate, burst));
        // Keep config fresh in case the plan was updated.
        bucket.rate_per_sec = rate;
        bucket.burst = burst;
        if !bucket.try_acquire(Instant::now()) {
            return Err(limit_exceeded());
        }
    }

    // Quota.
    if let Some((limit, period, offset)) = plan.quota {
        let window = current_quota_window(chrono::Utc::now(), period, offset);
        let counter_key = (
            req.account_id.clone(),
            plan.id.clone(),
            key_id.clone(),
            window,
        );
        let entry = meters.counters.entry(counter_key).or_insert(0);
        if *entry >= limit {
            return Err(limit_exceeded());
        }
        *entry += 1;
    }

    Ok(())
}

#[derive(Debug, Clone)]
struct UsagePlanSnapshot {
    id: String,
    /// Effective throttle for the request:
    /// `(rateLimit_per_sec, burstLimit, method_override_path)`. The
    /// third value is `Some(path)` when an `apiStages[].throttle[path]`
    /// entry overrode the plan-level limits, and `None` when the
    /// plan-level throttle (or no throttle) is in effect. Carrying the
    /// path through to the meter key keeps method-level buckets
    /// segregated from plan-level ones, matching AWS's docs.
    throttle: Option<(f64, f64, Option<String>)>,
    /// `(limit, period, offset_days)` when configured.
    quota: Option<(u64, QuotaPeriod, i64)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum QuotaPeriod {
    Day,
    Week,
    Month,
}

fn parse_quota_period(s: &str) -> Option<QuotaPeriod> {
    match s {
        "DAY" => Some(QuotaPeriod::Day),
        "WEEK" => Some(QuotaPeriod::Week),
        "MONTH" => Some(QuotaPeriod::Month),
        _ => None,
    }
}

/// AWS picks any matching plan when multiple plans associate the same
/// key; we pick the first per `BTreeMap` iteration order, which is
/// deterministic by `usagePlanId`. `method_path` (e.g. `/items/GET`)
/// drives selection of the per-method throttle override under the
/// matched `apiStages[]` entry.
fn first_matching_plan(
    state: &crate::state::ApiGatewayState,
    key_id: &str,
    api_id: &str,
    stage_name: &str,
    method_path: &str,
) -> Option<UsagePlanSnapshot> {
    for (plan_id, keys) in &state.usage_plan_keys {
        if !keys.contains_key(key_id) {
            continue;
        }
        let Some(plan) = state.usage_plans.get(plan_id) else {
            continue;
        };
        let matched_stage = plan.api_stages.iter().find(|stage_entry| {
            let api = stage_entry.get("apiId").and_then(|v| v.as_str());
            let stage = stage_entry.get("stage").and_then(|v| v.as_str());
            matches!((api, stage), (Some(a), Some(s)) if a == api_id && s == stage_name)
        });
        let Some(matched_stage) = matched_stage else {
            continue;
        };
        return Some(snapshot_plan(plan, matched_stage, method_path));
    }
    None
}

fn snapshot_plan(
    plan: &crate::state::UsagePlan,
    matched_stage: &serde_json::Value,
    method_path: &str,
) -> UsagePlanSnapshot {
    let plan_throttle = plan.throttle.as_ref().and_then(parse_throttle);
    // Method-level override under the matched apiStage entry. AWS keys
    // these by `<resource_path>/<HTTP_METHOD>`. We try the exact path
    // first, then the AWS catch-all `/*/*` if no exact match.
    let method_throttle = matched_stage
        .get("throttle")
        .and_then(|t| t.as_object())
        .and_then(|map| {
            let exact = map.get(method_path);
            let wildcard = map.get("/*/*");
            exact
                .or(wildcard)
                .and_then(parse_throttle)
                .map(|(rate, burst)| (rate, burst, Some(method_path.to_string())))
        });
    let throttle =
        method_throttle.or_else(|| plan_throttle.map(|(rate, burst)| (rate, burst, None)));
    let quota = plan.quota.as_ref().and_then(|q| {
        let limit = q.get("limit").and_then(|v| v.as_u64())?;
        let period_str = q.get("period").and_then(|v| v.as_str())?;
        let period = parse_quota_period(period_str)?;
        let offset = q.get("offset").and_then(|v| v.as_i64()).unwrap_or(0);
        Some((limit, period, offset))
    });
    UsagePlanSnapshot {
        id: plan.id.clone(),
        throttle,
        quota,
    }
}

/// Parse a `{rateLimit, burstLimit}` JSON object into a `(rate, burst)`
/// pair. Returns `None` for missing fields, non-numeric values, or
/// non-positive limits — AWS treats those as "no throttle configured".
fn parse_throttle(t: &serde_json::Value) -> Option<(f64, f64)> {
    let rate = t.get("rateLimit").and_then(|v| v.as_f64())?;
    let burst = t
        .get("burstLimit")
        .and_then(|v| v.as_f64().or_else(|| v.as_i64().map(|n| n as f64)))?;
    if rate <= 0.0 || burst <= 0.0 {
        return None;
    }
    Some((rate, burst))
}

/// Compute the canonical window key for a quota period. AWS resets
/// quota counters at the start of the period (UTC midnight for DAY,
/// Sunday for WEEK, first-of-month for MONTH); `offset` shifts the
/// boundary by N days. The returned string is purely a counter key.
#[allow(clippy::too_many_arguments)]
fn enforce_request_validator(
    service: &ApiGatewayService,
    req: &AwsRequest,
    api_id: &str,
    _stage_name: &str,
    validator_id: &str,
    request_parameters: &BTreeMap<String, bool>,
    request_models: &BTreeMap<String, String>,
    path_params: &BTreeMap<String, String>,
) -> Result<(), AwsServiceError> {
    let accounts = service.state_handle().read();
    let state = accounts
        .get(&req.account_id)
        .ok_or_else(|| bad_request("Request validation failed"))?;

    let validator = state
        .request_validators
        .get(api_id)
        .and_then(|m| m.get(validator_id))
        .ok_or_else(|| bad_request("Request validation failed"))?;

    let validate_body = validator
        .get("validateRequestBody")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let validate_params = validator
        .get("validateRequestParameters")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    if validate_params {
        for (param_key, required) in request_parameters {
            if !*required {
                continue;
            }
            let present = if let Some(name) = param_key.strip_prefix("method.request.querystring.")
            {
                req.query_params
                    .get(name)
                    .map(|v| !v.is_empty())
                    .unwrap_or(false)
            } else if let Some(name) = param_key.strip_prefix("method.request.path.") {
                path_params
                    .get(name)
                    .map(|v| !v.is_empty())
                    .unwrap_or(false)
            } else if let Some(name) = param_key.strip_prefix("method.request.header.") {
                req.headers
                    .get(name)
                    .and_then(|v| v.to_str().ok())
                    .map(|v| !v.trim().is_empty())
                    .unwrap_or(false)
            } else {
                // Unknown parameter source — skip.
                true
            };
            if !present {
                return Err(bad_request(format!(
                    "Missing required request parameter: {}",
                    param_key
                )));
            }
        }
    }

    if validate_body {
        let content_type = req
            .headers
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("application/json");
        // Normalize content-type by stripping charset suffix.
        let normalized = content_type
            .split(';')
            .next()
            .unwrap_or(content_type)
            .trim();

        let model_name = match request_models.get(normalized) {
            Some(name) => name,
            None => match request_models.get("$default") {
                Some(name) => name,
                None => {
                    // No model for this content type and no $default — skip body validation.
                    return Ok(());
                }
            },
        };

        let model = state
            .models
            .get(api_id)
            .and_then(|m| m.get(model_name))
            .ok_or_else(|| bad_request("Request body does not match model schema"))?;

        let schema_str = model
            .schema
            .as_deref()
            .ok_or_else(|| bad_request("Request body does not match model schema"))?;
        let schema: serde_json::Value = serde_json::from_str(schema_str)
            .map_err(|_| bad_request("Request body does not match model schema"))?;

        let body_json: serde_json::Value = serde_json::from_slice(&req.body)
            .map_err(|_| bad_request("Request body does not match model schema"))?;

        drop(accounts);

        if let Err(e) = crate::model_validation::validate(&schema, &body_json) {
            return Err(bad_request(format!(
                "Request body does not match model schema for content type {}: {}",
                normalized, e
            )));
        }
    }

    Ok(())
}

fn bad_request(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "BadRequestException", msg.into())
}

fn current_quota_window(
    now: chrono::DateTime<chrono::Utc>,
    period: QuotaPeriod,
    offset: i64,
) -> String {
    use chrono::Datelike;
    let date = now.date_naive() - chrono::Duration::days(offset);
    match period {
        QuotaPeriod::Day => format!("D:{date}"),
        QuotaPeriod::Week => {
            // ISO week: Monday-based. AWS docs aren't specific about
            // anchor day; ISO week is stable + locale-free.
            let iso = date.iso_week();
            format!("W:{}-{:02}", iso.year(), iso.week())
        }
        QuotaPeriod::Month => format!("M:{}-{:02}", date.year(), date.month()),
    }
}

// ─── WAFv2 inspection ──────────────────────────────────────────────

/// Build the resource ARN that callers use when associating a WebACL
/// with an API Gateway v1 stage:
/// `arn:aws:apigateway:<region>::/restapis/<api>/stages/<stage>`.
fn stage_resource_arn(region: &str, api_id: &str, stage_name: &str) -> String {
    format!("arn:aws:apigateway:{region}::/restapis/{api_id}/stages/{stage_name}",)
}

/// Run the WAFv2 evaluator for one API Gateway v1 request. Returns
/// `Some(response)` for a terminal action (`Block` / `Captcha` /
/// `Challenge`); returns `None` for `Allow` / `Count` / `NoAcl`.
fn evaluate_waf(
    service: &ApiGatewayService,
    req: &AwsRequest,
    resource_arn: &str,
) -> Option<AwsResponse> {
    let waf_state = service.waf_state.as_ref()?;
    let limiter = service.waf_rate_limiter.as_ref()?;
    let ctx = build_waf_context(req);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    let decision = fakecloud_wafv2::evaluate_request(waf_state, resource_arn, &ctx, limiter, now);
    record_count_rules(service, &decision);
    decision_to_response(decision)
}

fn build_waf_context(req: &AwsRequest) -> fakecloud_wafv2::RequestContext {
    let headers: Vec<(String, String)> = req
        .headers
        .iter()
        .filter_map(|(k, v)| {
            v.to_str()
                .ok()
                .map(|s| (k.as_str().to_lowercase(), s.to_string()))
        })
        .collect();
    let source_ip = headers
        .iter()
        .find(|(k, _)| k == "x-forwarded-for")
        .and_then(|(_, v)| v.split(',').next().map(str::trim))
        .and_then(|s| s.parse::<std::net::IpAddr>().ok());
    let mut ctx =
        fakecloud_wafv2::RequestContext::new(req.method.as_str(), &req.raw_path, &req.raw_query)
            .with_headers(headers)
            .with_body(req.body.as_ref());
    if let Some(ip) = source_ip {
        ctx = ctx.with_source_ip(ip);
    }
    ctx
}

fn record_count_rules(service: &ApiGatewayService, decision: &fakecloud_wafv2::Decision) {
    let rules = decision.count_rules();
    if rules.is_empty() {
        return;
    }
    let Some(arn) = decision.web_acl_arn() else {
        return;
    };
    let mut metrics = service.waf_count_metrics.lock();
    for rule in rules {
        let key = format!("{arn}|{rule}");
        *metrics.entry(key).or_insert(0) += 1;
    }
}

fn decision_to_response(decision: fakecloud_wafv2::Decision) -> Option<AwsResponse> {
    use fakecloud_wafv2::Decision;
    let (status, message) = match decision {
        Decision::NoAcl | Decision::Allow { .. } => return None,
        Decision::Block { status, .. } => (
            StatusCode::from_u16(status).unwrap_or(StatusCode::FORBIDDEN),
            "Forbidden".to_string(),
        ),
        // CAPTCHA / Challenge interstitials are out of scope for this
        // batch; surface a 403 with a discoverable description so
        // tests can distinguish from a plain Block.
        Decision::Captcha { .. } => (StatusCode::FORBIDDEN, "WAF requires CAPTCHA".to_string()),
        Decision::Challenge { .. } => (StatusCode::FORBIDDEN, "WAF requires challenge".to_string()),
    };
    let body = json!({"message": message});
    let mut resp = AwsResponse::json_value(status, body);
    // Match the ALB shape: real AWS returns plain JSON, not the
    // amz-json-1.1 content-type the JSON-protocol services use.
    resp.content_type = "application/json".to_string();
    Some(resp)
}

/// When the request's `Host` header matches a DomainName's
/// `regionalDomainName`, look up BasePathMappings for that domain and
/// return the resolved `(stage, remaining_path_segments, api_id_hint)`.
/// The longest matching `basePath` prefix wins — AWS semantics.
/// Returns `None` when no custom domain matches, letting the caller fall
/// back to the default stage-in-first-segment behaviour.
fn resolve_custom_domain(
    service: &ApiGatewayService,
    req: &AwsRequest,
) -> Option<(Option<String>, Vec<String>, Option<String>)> {
    let host = req
        .headers
        .get("host")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if host.is_empty() {
        return None;
    }

    let accounts = service.state_handle().read();
    let state = accounts.get(&req.account_id)?;

    // Find domain whose regionalDomainName matches the Host header.
    let domain_entry = state.domain_names.iter().find(|(_name, value)| {
        value
            .get("regionalDomainName")
            .and_then(Value::as_str)
            .is_some_and(|rdn| rdn.eq_ignore_ascii_case(host))
    });
    let (domain_name, _domain_value) = domain_entry?;

    let mappings = state.base_path_mappings.get(domain_name)?;

    // Find the longest matching basePath prefix.
    // AWS sorts by basePath length descending; `(none)` = empty prefix.
    let mut best: Option<(&String, &Value, usize /* prefix len in segments */)> = None;
    for (bp, value) in mappings {
        let prefix = if bp == "(none)" { "" } else { bp.as_str() };
        let prefix_segments: Vec<&str> = prefix.split('/').filter(|s| !s.is_empty()).collect();
        if prefix_segments.len() <= req.path_segments.len()
            && prefix_segments
                .iter()
                .enumerate()
                .all(|(i, &seg)| req.path_segments.get(i).map(|s| s == seg).unwrap_or(false))
        {
            let candidate_len = prefix_segments.len();
            match best {
                None => best = Some((bp, value, candidate_len)),
                Some((_, _, len)) if candidate_len > len => best = Some((bp, value, candidate_len)),
                _ => {}
            }
        }
    }

    if let Some((_bp, value, prefix_len)) = best {
        let stage = value.get("stage").and_then(Value::as_str).map(String::from);
        let api_id = value
            .get("restApiId")
            .and_then(Value::as_str)
            .map(String::from);
        let remaining = req.path_segments[prefix_len..].to_vec();
        return Some((stage, remaining, api_id));
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{
        ApiGatewayState, Authorizer as StateAuthorizer, Integration as StateIntegration,
        Method as StateMethod, Resource as StateResource, RestApi, SharedApiGatewayState,
        Stage as StateStage,
    };
    use async_trait::async_trait;
    use bytes::Bytes;
    use chrono::Utc;
    use fakecloud_core::delivery::{CognitoJwtVerifier, DeliveryBus, LambdaDelivery};
    use fakecloud_core::multi_account::MultiAccountState;
    use fakecloud_core::service::AwsService;
    use http::HeaderMap;
    use std::collections::HashMap;
    use std::pin::Pin;
    use std::sync::Arc;

    #[test]
    fn match_root_only_for_empty_path() {
        assert!(match_resource_path("/", &[]).is_some());
        assert!(match_resource_path("/", &["x".to_string()]).is_none());
    }

    #[test]
    fn match_exact_segments() {
        let r = match_resource_path("/items", &["items".to_string()]).unwrap();
        assert!(r.is_empty());
        assert!(match_resource_path("/items", &["items".to_string(), "x".to_string()]).is_none());
        assert!(match_resource_path("/items", &["other".to_string()]).is_none());
    }

    #[test]
    fn match_param_segment() {
        let r =
            match_resource_path("/items/{id}", &["items".to_string(), "42".to_string()]).unwrap();
        assert_eq!(r.get("id"), Some(&"42".to_string()));
    }

    #[test]
    fn match_greedy_segment() {
        let r = match_resource_path(
            "/proxy/{path+}",
            &[
                "proxy".to_string(),
                "a".to_string(),
                "b".to_string(),
                "c".to_string(),
            ],
        )
        .unwrap();
        assert_eq!(r.get("path"), Some(&"a/b/c".to_string()));
    }

    // ── custom domain resolution tests ──

    #[test]
    fn resolve_custom_domain_no_host_falls_through() {
        let state = build_state("NONE", None);
        let service = ApiGatewayService::new(state);
        let req = make_request(HeaderMap::new());
        assert!(resolve_custom_domain(&service, &req).is_none());
    }

    #[test]
    fn resolve_custom_domain_routes_via_base_path_mapping() {
        let state = build_state("NONE", None);
        {
            let mut accounts = state.write();
            let s = accounts.get_or_create(TEST_ACCOUNT);
            s.domain_names.insert(
                "api.example.com".to_string(),
                json!({"regionalDomainName": "api.example.com.fakecloud"}),
            );
            let mut mappings = BTreeMap::new();
            mappings.insert(
                "v1".to_string(),
                json!({"restApiId": TEST_API_ID, "stage": "prod"}),
            );
            s.base_path_mappings
                .insert("api.example.com".to_string(), mappings);
        }
        let service = ApiGatewayService::new(state);
        let mut headers = HeaderMap::new();
        headers.insert("host", "api.example.com.fakecloud".parse().unwrap());
        let mut req = make_request(headers);
        req.path_segments = vec!["v1".to_string(), "items".to_string()];

        let (stage, remaining, api) = resolve_custom_domain(&service, &req).unwrap();
        assert_eq!(stage, Some("prod".to_string()));
        assert_eq!(remaining, vec!["items".to_string()]);
        assert_eq!(api, Some(TEST_API_ID.to_string()));
    }

    #[test]
    fn resolve_custom_domain_longest_prefix_wins() {
        let state = build_state("NONE", None);
        {
            let mut accounts = state.write();
            let s = accounts.get_or_create(TEST_ACCOUNT);
            s.domain_names.insert(
                "api.example.com".to_string(),
                json!({"regionalDomainName": "api.example.com.fakecloud"}),
            );
            let mut mappings = BTreeMap::new();
            mappings.insert(
                "v1".to_string(),
                json!({"restApiId": TEST_API_ID, "stage": "prod"}),
            );
            mappings.insert(
                "v1/internal".to_string(),
                json!({"restApiId": "other123", "stage": "dev"}),
            );
            s.base_path_mappings
                .insert("api.example.com".to_string(), mappings);
        }
        let service = ApiGatewayService::new(state);
        let mut headers = HeaderMap::new();
        headers.insert("host", "api.example.com.fakecloud".parse().unwrap());
        let mut req = make_request(headers);
        req.path_segments = vec![
            "v1".to_string(),
            "internal".to_string(),
            "items".to_string(),
        ];

        let (stage, remaining, api) = resolve_custom_domain(&service, &req).unwrap();
        assert_eq!(stage, Some("dev".to_string()));
        assert_eq!(remaining, vec!["items".to_string()]);
        assert_eq!(api, Some("other123".to_string()));
    }

    #[test]
    fn resolve_custom_domain_none_base_path_matches_root() {
        let state = build_state("NONE", None);
        {
            let mut accounts = state.write();
            let s = accounts.get_or_create(TEST_ACCOUNT);
            s.domain_names.insert(
                "api.example.com".to_string(),
                json!({"regionalDomainName": "api.example.com.fakecloud"}),
            );
            let mut mappings = BTreeMap::new();
            mappings.insert(
                "(none)".to_string(),
                json!({"restApiId": TEST_API_ID, "stage": "prod"}),
            );
            s.base_path_mappings
                .insert("api.example.com".to_string(), mappings);
        }
        let service = ApiGatewayService::new(state);
        let mut headers = HeaderMap::new();
        headers.insert("host", "api.example.com.fakecloud".parse().unwrap());
        let mut req = make_request(headers);
        req.path_segments = vec!["items".to_string()];

        let (stage, remaining, api) = resolve_custom_domain(&service, &req).unwrap();
        assert_eq!(stage, Some("prod".to_string()));
        assert_eq!(remaining, vec!["items".to_string()]);
        assert_eq!(api, Some(TEST_API_ID.to_string()));
    }

    #[test]
    fn resolve_custom_domain_exact_base_path_leaves_empty_remaining() {
        let state = build_state("NONE", None);
        {
            let mut accounts = state.write();
            let s = accounts.get_or_create(TEST_ACCOUNT);
            s.domain_names.insert(
                "api.example.com".to_string(),
                json!({"regionalDomainName": "api.example.com.fakecloud"}),
            );
            let mut mappings = BTreeMap::new();
            mappings.insert(
                "v1".to_string(),
                json!({"restApiId": TEST_API_ID, "stage": "prod"}),
            );
            s.base_path_mappings
                .insert("api.example.com".to_string(), mappings);
        }
        let service = ApiGatewayService::new(state);
        let mut headers = HeaderMap::new();
        headers.insert("host", "api.example.com.fakecloud".parse().unwrap());
        let mut req = make_request(headers);
        // Request path is exactly the base path "v1" — no trailing segments.
        req.path_segments = vec!["v1".to_string()];

        let (stage, remaining, api) = resolve_custom_domain(&service, &req).unwrap();
        assert_eq!(stage, Some("prod".to_string()));
        assert!(remaining.is_empty());
        assert_eq!(api, Some(TEST_API_ID.to_string()));
    }

    #[test]
    fn extract_lambda_arn_from_uri() {
        let uri = "arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/arn:aws:lambda:us-east-1:000000000000:function:my-fn/invocations";
        assert_eq!(
            extract_lambda_arn(uri),
            Some("arn:aws:lambda:us-east-1:000000000000:function:my-fn".to_string())
        );
    }

    // ── data-plane authorizer enforcement tests ──

    const TEST_ACCOUNT: &str = "000000000000";
    const TEST_REGION: &str = "us-east-1";
    const TEST_API_ID: &str = "abc123";
    const RES_ID: &str = "items0001";
    const AUTH_ID: &str = "auth000001";
    const FN_ARN: &str = "arn:aws:lambda:us-east-1:000000000000:function:authorizer";
    const BACKEND_ARN: &str = "arn:aws:lambda:us-east-1:000000000000:function:backend";
    const COGNITO_ARN: &str = "arn:aws:cognito-idp:us-east-1:000000000000:userpool/us-east-1_pool1";

    /// Lambda stub that returns a fixed JSON response for authorizer
    /// invocations and a generic 200 proxy response for the backend.
    /// `expectations` records how many times each function was invoked.
    struct StubLambda {
        responses: parking_lot::Mutex<HashMap<String, Vec<u8>>>,
        invocations: parking_lot::Mutex<Vec<(String, String)>>,
    }

    impl StubLambda {
        fn new() -> Self {
            Self {
                responses: parking_lot::Mutex::new(HashMap::new()),
                invocations: parking_lot::Mutex::new(Vec::new()),
            }
        }

        fn set(&self, arn: &str, body: serde_json::Value) {
            self.responses
                .lock()
                .insert(arn.to_string(), body.to_string().into_bytes());
        }

        fn invocation_count(&self, arn: &str) -> usize {
            self.invocations
                .lock()
                .iter()
                .filter(|(a, _)| a == arn)
                .count()
        }

        fn last_payload(&self, arn: &str) -> Option<String> {
            self.invocations
                .lock()
                .iter()
                .rev()
                .find(|(a, _)| a == arn)
                .map(|(_, p)| p.clone())
        }
    }

    impl LambdaDelivery for StubLambda {
        fn invoke_lambda(
            &self,
            function_arn: &str,
            payload: &str,
        ) -> Pin<Box<dyn std::future::Future<Output = Result<Vec<u8>, String>> + Send>> {
            self.invocations
                .lock()
                .push((function_arn.to_string(), payload.to_string()));
            let resp = self
                .responses
                .lock()
                .get(function_arn)
                .cloned()
                .unwrap_or_else(|| {
                    serde_json::json!({"statusCode": 200, "body": "ok"})
                        .to_string()
                        .into_bytes()
                });
            Box::pin(async move { Ok(resp) })
        }
    }

    /// JWT verifier stub that returns a fixed claims object for valid
    /// tokens and a fixed error for invalid ones. Tests register the
    /// outcome explicitly so we don't need real RSA in unit tests.
    struct StubJwtVerifier {
        valid_token: String,
        claims: serde_json::Value,
    }

    impl CognitoJwtVerifier for StubJwtVerifier {
        fn verify_token(
            &self,
            _account_id: &str,
            _user_pool_arn: &str,
            token: &str,
        ) -> Result<serde_json::Value, String> {
            if token == self.valid_token {
                Ok(self.claims.clone())
            } else {
                Err("invalid signature".to_string())
            }
        }
    }

    fn build_state(
        authorization_type: &str,
        authorizer: Option<StateAuthorizer>,
    ) -> SharedApiGatewayState {
        let mut state = ApiGatewayState::new(TEST_ACCOUNT, TEST_REGION);
        state.apis.insert(
            TEST_API_ID.to_string(),
            RestApi {
                id: TEST_API_ID.to_string(),
                name: "test".to_string(),
                description: None,
                version: None,
                created_date: Utc::now(),
                api_key_source: "HEADER".to_string(),
                endpoint_configuration: serde_json::json!({}),
                policy: None,
                binary_media_types: vec![],
                minimum_compression_size: None,
                disable_execute_api_endpoint: false,
                root_resource_id: "root".to_string(),
                tags: BTreeMap::new(),
                import_source: None,
            },
        );
        let mut resources = BTreeMap::new();
        resources.insert(
            RES_ID.to_string(),
            StateResource {
                id: RES_ID.to_string(),
                parent_id: Some("root".to_string()),
                path_part: Some("items".to_string()),
                path: "/items".to_string(),
            },
        );
        state.resources.insert(TEST_API_ID.to_string(), resources);
        let key = format!("{TEST_API_ID}/{RES_ID}/GET");
        state.methods.insert(
            key.clone(),
            StateMethod {
                rest_api_id: TEST_API_ID.to_string(),
                resource_id: RES_ID.to_string(),
                http_method: "GET".to_string(),
                authorization_type: authorization_type.to_string(),
                authorizer_id: authorizer.as_ref().map(|a| a.id.clone()),
                api_key_required: false,
                operation_name: None,
                request_parameters: BTreeMap::new(),
                request_models: BTreeMap::new(),
                request_validator_id: None,
                authorization_scopes: vec![],
            },
        );
        state.integrations.insert(
            key,
            StateIntegration {
                rest_api_id: TEST_API_ID.to_string(),
                resource_id: RES_ID.to_string(),
                http_method: "GET".to_string(),
                integration_type: "AWS_PROXY".to_string(),
                integration_http_method: Some("POST".to_string()),
                uri: Some(format!(
                    "arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/{BACKEND_ARN}/invocations"
                )),
                credentials: None,
                request_parameters: BTreeMap::new(),
                request_templates: BTreeMap::new(),
                passthrough_behavior: "WHEN_NO_MATCH".to_string(),
                timeout_in_millis: None,
                cache_namespace: None,
                cache_key_parameters: vec![],
                content_handling: None,
                connection_type: None,
                connection_id: None,
                tls_config: None,
            },
        );
        if let Some(auth) = authorizer {
            state
                .authorizers
                .entry(TEST_API_ID.to_string())
                .or_default()
                .insert(auth.id.clone(), auth);
        }
        let mut stages = BTreeMap::new();
        stages.insert(
            "prod".to_string(),
            StateStage {
                stage_name: "prod".to_string(),
                deployment_id: "dep1".to_string(),
                description: None,
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
            },
        );
        state.stages.insert(TEST_API_ID.to_string(), stages);

        let mut mas: MultiAccountState<ApiGatewayState> =
            MultiAccountState::new(TEST_ACCOUNT, TEST_REGION, "http://localhost:4566");
        *mas.get_or_create(TEST_ACCOUNT) = state;
        Arc::new(parking_lot::RwLock::new(mas))
    }

    fn make_request(headers: HeaderMap) -> AwsRequest {
        AwsRequest {
            service: "apigateway".to_string(),
            action: String::new(),
            method: Method::GET,
            raw_path: "/prod/items".to_string(),
            raw_query: String::new(),
            path_segments: vec!["prod".to_string(), "items".to_string()],
            query_params: HashMap::new(),
            headers,
            body: Bytes::new(),
            body_stream: parking_lot::Mutex::new(None),
            account_id: TEST_ACCOUNT.to_string(),
            region: TEST_REGION.to_string(),
            request_id: "rid".to_string(),
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        }
    }

    fn token_authorizer() -> StateAuthorizer {
        StateAuthorizer {
            id: AUTH_ID.to_string(),
            name: "tok".to_string(),
            authorizer_type: "TOKEN".to_string(),
            provider_arns: vec![],
            auth_type: None,
            authorizer_uri: Some(format!(
                "arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/{FN_ARN}/invocations"
            )),
            authorizer_credentials: None,
            identity_source: Some("method.request.header.Authorization".to_string()),
            identity_validation_expression: None,
            authorizer_result_ttl_in_seconds: Some(300),
        }
    }

    fn request_authorizer() -> StateAuthorizer {
        let mut a = token_authorizer();
        a.authorizer_type = "REQUEST".to_string();
        a.identity_source = Some("method.request.header.X-Custom".to_string());
        a
    }

    fn cognito_authorizer() -> StateAuthorizer {
        StateAuthorizer {
            id: AUTH_ID.to_string(),
            name: "cog".to_string(),
            authorizer_type: "COGNITO_USER_POOLS".to_string(),
            provider_arns: vec![COGNITO_ARN.to_string()],
            auth_type: None,
            authorizer_uri: None,
            authorizer_credentials: None,
            identity_source: Some("method.request.header.Authorization".to_string()),
            identity_validation_expression: None,
            authorizer_result_ttl_in_seconds: Some(300),
        }
    }

    fn build_service(
        state: SharedApiGatewayState,
        lambda: Arc<StubLambda>,
        verifier: Option<Arc<dyn CognitoJwtVerifier>>,
    ) -> ApiGatewayService {
        let mut bus = DeliveryBus::new().with_lambda(lambda);
        if let Some(v) = verifier {
            bus = bus.with_cognito_jwt_verifier(v);
        }
        ApiGatewayService::new(state).with_delivery(Arc::new(bus))
    }

    #[tokio::test]
    async fn request_passes_when_authorization_type_none() {
        let state = build_state("NONE", None);
        let lambda = Arc::new(StubLambda::new());
        lambda.set(
            BACKEND_ARN,
            serde_json::json!({"statusCode": 200, "body": "ok"}),
        );
        let service = build_service(state, lambda.clone(), None);
        let resp = handle(&service, &make_request(HeaderMap::new()))
            .await
            .expect("request must succeed");
        assert_eq!(resp.status, StatusCode::OK);
        assert_eq!(lambda.invocation_count(BACKEND_ARN), 1);
    }

    #[tokio::test]
    async fn request_blocked_by_token_authorizer_returning_deny() {
        let state = build_state("CUSTOM", Some(token_authorizer()));
        let lambda = Arc::new(StubLambda::new());
        lambda.set(
            FN_ARN,
            serde_json::json!({
                "principalId": "user-1",
                "policyDocument": {
                    "Version": "2012-10-17",
                    "Statement": [{"Effect": "Deny", "Action": "execute-api:Invoke", "Resource": "*"}]
                }
            }),
        );
        let service = build_service(state, lambda.clone(), None);
        let mut headers = HeaderMap::new();
        headers.insert("authorization", "tok-deny".parse().unwrap());
        let result = handle(&service, &make_request(headers)).await;
        let err = match result {
            Ok(_) => panic!("Deny must surface as error"),
            Err(e) => e,
        };
        assert_eq!(err.status(), StatusCode::FORBIDDEN);
        assert_eq!(lambda.invocation_count(FN_ARN), 1);
        assert_eq!(lambda.invocation_count(BACKEND_ARN), 0);
    }

    #[tokio::test]
    async fn request_allowed_by_token_authorizer_returning_allow() {
        let state = build_state("CUSTOM", Some(token_authorizer()));
        let lambda = Arc::new(StubLambda::new());
        lambda.set(
            FN_ARN,
            serde_json::json!({
                "principalId": "user-1",
                "policyDocument": {
                    "Version": "2012-10-17",
                    "Statement": [{"Effect": "Allow", "Action": "execute-api:Invoke", "Resource": "*"}]
                },
                "context": {"role": "admin"}
            }),
        );
        lambda.set(
            BACKEND_ARN,
            serde_json::json!({"statusCode": 200, "body": "ok"}),
        );
        let service = build_service(state, lambda.clone(), None);
        let mut headers = HeaderMap::new();
        headers.insert("authorization", "tok-allow".parse().unwrap());
        let resp = handle(&service, &make_request(headers))
            .await
            .expect("Allow must let request through");
        assert_eq!(resp.status, StatusCode::OK);
        assert_eq!(lambda.invocation_count(BACKEND_ARN), 1);
        // Backend received the authorizer context in requestContext.
        let payload: serde_json::Value =
            serde_json::from_str(&lambda.last_payload(BACKEND_ARN).unwrap()).unwrap();
        assert_eq!(payload["requestContext"]["authorizer"]["role"], "admin");
        assert_eq!(
            payload["requestContext"]["authorizer"]["principalId"],
            "user-1"
        );
    }

    #[tokio::test]
    async fn request_blocked_by_token_authorizer_when_token_missing() {
        let state = build_state("CUSTOM", Some(token_authorizer()));
        let lambda = Arc::new(StubLambda::new());
        let service = build_service(state, lambda.clone(), None);
        let result = handle(&service, &make_request(HeaderMap::new())).await;
        let err = match result {
            Ok(_) => panic!("missing identity source must 401"),
            Err(e) => e,
        };
        assert_eq!(err.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(lambda.invocation_count(FN_ARN), 0);
        assert_eq!(lambda.invocation_count(BACKEND_ARN), 0);
    }

    #[tokio::test]
    async fn cognito_authorizer_rejects_invalid_jwt_signature() {
        let state = build_state("COGNITO_USER_POOLS", Some(cognito_authorizer()));
        let lambda = Arc::new(StubLambda::new());
        let verifier: Arc<dyn CognitoJwtVerifier> = Arc::new(StubJwtVerifier {
            valid_token: "valid-jwt".to_string(),
            claims: serde_json::json!({"sub": "u-1"}),
        });
        let service = build_service(state, lambda.clone(), Some(verifier));
        let mut headers = HeaderMap::new();
        headers.insert("authorization", "Bearer tampered".parse().unwrap());
        let result = handle(&service, &make_request(headers)).await;
        let err = match result {
            Ok(_) => panic!("tampered JWT must 401"),
            Err(e) => e,
        };
        assert_eq!(err.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(lambda.invocation_count(BACKEND_ARN), 0);
    }

    #[tokio::test]
    async fn cognito_authorizer_accepts_valid_jwt_from_pool() {
        let state = build_state("COGNITO_USER_POOLS", Some(cognito_authorizer()));
        let lambda = Arc::new(StubLambda::new());
        lambda.set(
            BACKEND_ARN,
            serde_json::json!({"statusCode": 200, "body": "ok"}),
        );
        let claims = serde_json::json!({"sub": "u-1", "email": "a@b.c"});
        let verifier: Arc<dyn CognitoJwtVerifier> = Arc::new(StubJwtVerifier {
            valid_token: "valid-jwt".to_string(),
            claims: claims.clone(),
        });
        let service = build_service(state, lambda.clone(), Some(verifier));
        let mut headers = HeaderMap::new();
        headers.insert("authorization", "Bearer valid-jwt".parse().unwrap());
        let resp = handle(&service, &make_request(headers))
            .await
            .expect("valid JWT lets request through");
        assert_eq!(resp.status, StatusCode::OK);
        let payload: serde_json::Value =
            serde_json::from_str(&lambda.last_payload(BACKEND_ARN).unwrap()).unwrap();
        assert_eq!(payload["requestContext"]["authorizer"]["claims"], claims);
    }

    // ── Usage plan throttle + quota tests ──

    /// Force `api_key_required = true` on the matched method and seed a
    /// usage plan + key + plan-key association. Returns the API-key
    /// value the caller should send in `x-api-key`.
    fn install_api_key_plan(
        state: &SharedApiGatewayState,
        plan_id: &str,
        throttle: Option<serde_json::Value>,
        quota: Option<serde_json::Value>,
    ) -> String {
        use crate::state::{ApiKey, UsagePlan};
        let mut accounts = state.write();
        let st = accounts.get_or_create(TEST_ACCOUNT);
        // Flip the matched method to require an API key.
        let mkey = format!("{TEST_API_ID}/{RES_ID}/GET");
        if let Some(m) = st.methods.get_mut(&mkey) {
            m.api_key_required = true;
        }
        let key_value = "test-key-value-1".to_string();
        let key_id = "key0001".to_string();
        st.api_keys.insert(
            key_id.clone(),
            ApiKey {
                id: key_id.clone(),
                value: key_value.clone(),
                name: "k".to_string(),
                description: None,
                enabled: true,
                created_date: Utc::now(),
                last_updated_date: Utc::now(),
                stage_keys: vec![],
                tags: BTreeMap::new(),
                customer_id: None,
            },
        );
        st.usage_plans.insert(
            plan_id.to_string(),
            UsagePlan {
                id: plan_id.to_string(),
                name: "p".to_string(),
                description: None,
                api_stages: vec![serde_json::json!({
                    "apiId": TEST_API_ID,
                    "stage": "prod",
                })],
                throttle,
                quota,
                product_code: None,
                tags: BTreeMap::new(),
            },
        );
        let mut plan_keys = BTreeMap::new();
        plan_keys.insert(
            key_id,
            serde_json::json!({"id": "key0001", "type": "API_KEY", "value": key_value}),
        );
        st.usage_plan_keys.insert(plan_id.to_string(), plan_keys);
        key_value
    }

    #[tokio::test]
    async fn missing_api_key_header_returns_403_forbidden() {
        let state = build_state("NONE", None);
        let _ = install_api_key_plan(
            &state,
            "plan-a",
            Some(serde_json::json!({"rateLimit": 100.0, "burstLimit": 100})),
            None,
        );
        let lambda = Arc::new(StubLambda::new());
        let service = build_service(state, lambda.clone(), None);
        let err = match handle(&service, &make_request(HeaderMap::new())).await {
            Err(e) => e,
            Ok(_) => panic!("missing key must 403"),
        };
        assert_eq!(err.status(), StatusCode::FORBIDDEN);
        assert_eq!(lambda.invocation_count(BACKEND_ARN), 0);
    }

    #[tokio::test]
    async fn unknown_api_key_returns_403_forbidden() {
        let state = build_state("NONE", None);
        let _ = install_api_key_plan(
            &state,
            "plan-a",
            Some(serde_json::json!({"rateLimit": 100.0, "burstLimit": 100})),
            None,
        );
        let lambda = Arc::new(StubLambda::new());
        let service = build_service(state, lambda.clone(), None);
        let mut headers = HeaderMap::new();
        headers.insert("x-api-key", "not-a-real-key".parse().unwrap());
        let err = match handle(&service, &make_request(headers)).await {
            Err(e) => e,
            Ok(_) => panic!("unknown key must 403"),
        };
        assert_eq!(err.status(), StatusCode::FORBIDDEN);
        assert_eq!(lambda.invocation_count(BACKEND_ARN), 0);
    }

    #[tokio::test]
    async fn disabled_api_key_returns_403_forbidden() {
        let state = build_state("NONE", None);
        let key_value = install_api_key_plan(&state, "plan-a", None, None);
        // Disable the key after installing it.
        {
            let mut accounts = state.write();
            let st = accounts.get_or_create(TEST_ACCOUNT);
            for k in st.api_keys.values_mut() {
                k.enabled = false;
            }
        }
        let lambda = Arc::new(StubLambda::new());
        let service = build_service(state, lambda.clone(), None);
        let mut headers = HeaderMap::new();
        headers.insert("x-api-key", key_value.parse().unwrap());
        let err = match handle(&service, &make_request(headers)).await {
            Err(e) => e,
            Ok(_) => panic!("disabled key must 403"),
        };
        assert_eq!(err.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn key_without_matching_plan_passes_unmetered() {
        // Key exists but no usage plan associates it with this stage —
        // request must succeed without throttle/quota enforcement.
        let state = build_state("NONE", None);
        let key_value = {
            use crate::state::ApiKey;
            let mut accounts = state.write();
            let st = accounts.get_or_create(TEST_ACCOUNT);
            let mkey = format!("{TEST_API_ID}/{RES_ID}/GET");
            if let Some(m) = st.methods.get_mut(&mkey) {
                m.api_key_required = true;
            }
            let v = "loose-key".to_string();
            st.api_keys.insert(
                "k1".to_string(),
                ApiKey {
                    id: "k1".to_string(),
                    value: v.clone(),
                    name: "k".to_string(),
                    description: None,
                    enabled: true,
                    created_date: Utc::now(),
                    last_updated_date: Utc::now(),
                    stage_keys: vec![],
                    tags: BTreeMap::new(),
                    customer_id: None,
                },
            );
            v
        };
        let lambda = Arc::new(StubLambda::new());
        lambda.set(
            BACKEND_ARN,
            serde_json::json!({"statusCode": 200, "body": "ok"}),
        );
        let service = build_service(state, lambda.clone(), None);
        let mut headers = HeaderMap::new();
        headers.insert("x-api-key", key_value.parse().unwrap());
        let resp = handle(&service, &make_request(headers))
            .await
            .expect("known key without plan must pass through");
        assert_eq!(resp.status, StatusCode::OK);
        assert_eq!(lambda.invocation_count(BACKEND_ARN), 1);
    }

    #[tokio::test]
    async fn second_request_returns_429_when_throttle_burst_is_one() {
        // 1 RPS / burst=1: the bucket grants exactly one request per
        // refill window. Fire two back-to-back so the second hits 429
        // before any token has had time to drip back in.
        let state = build_state("NONE", None);
        let key_value = install_api_key_plan(
            &state,
            "plan-tight",
            Some(serde_json::json!({"rateLimit": 1.0, "burstLimit": 1})),
            None,
        );
        let lambda = Arc::new(StubLambda::new());
        lambda.set(
            BACKEND_ARN,
            serde_json::json!({"statusCode": 200, "body": "ok"}),
        );
        let service = build_service(state, lambda.clone(), None);
        let mut headers = HeaderMap::new();
        headers.insert("x-api-key", key_value.parse().unwrap());

        let first = handle(&service, &make_request(headers.clone()))
            .await
            .expect("first request consumes the only token");
        assert_eq!(first.status, StatusCode::OK);

        let err = match handle(&service, &make_request(headers)).await {
            Err(e) => e,
            Ok(_) => panic!("second request must trip throttle"),
        };
        assert_eq!(err.status(), StatusCode::TOO_MANY_REQUESTS);
        // Backend invoked exactly once — second call shorted at the
        // throttle gate before reaching the integration.
        assert_eq!(lambda.invocation_count(BACKEND_ARN), 1);
    }

    #[tokio::test]
    async fn quota_blocks_second_request_when_limit_is_one() {
        let state = build_state("NONE", None);
        let key_value = install_api_key_plan(
            &state,
            "plan-quota",
            // Generous throttle so the rate gate doesn't trip first.
            Some(serde_json::json!({"rateLimit": 100.0, "burstLimit": 100})),
            Some(serde_json::json!({"limit": 1, "period": "DAY", "offset": 0})),
        );
        let lambda = Arc::new(StubLambda::new());
        lambda.set(
            BACKEND_ARN,
            serde_json::json!({"statusCode": 200, "body": "ok"}),
        );
        let service = build_service(state, lambda.clone(), None);
        let mut headers = HeaderMap::new();
        headers.insert("x-api-key", key_value.parse().unwrap());

        let first = handle(&service, &make_request(headers.clone()))
            .await
            .expect("first request consumes the only quota token");
        assert_eq!(first.status, StatusCode::OK);

        let err = match handle(&service, &make_request(headers)).await {
            Err(e) => e,
            Ok(_) => panic!("second request must trip quota"),
        };
        assert_eq!(err.status(), StatusCode::TOO_MANY_REQUESTS);
    }

    /// Plan throttle is generous (100/100) but the apiStages entry
    /// pins `/items/GET` to 1/1. The second request must trip the
    /// method-level bucket even though the plan-level rate would have
    /// allowed it — mirroring AWS's documented per-method overrides.
    #[tokio::test]
    async fn method_level_throttle_override_takes_precedence_over_plan() {
        use crate::state::{ApiKey, UsagePlan};
        let state = build_state("NONE", None);
        let key_value = "method-key".to_string();
        let plan_id = "plan-method-override".to_string();
        {
            let mut accounts = state.write();
            let st = accounts.get_or_create(TEST_ACCOUNT);
            let mkey = format!("{TEST_API_ID}/{RES_ID}/GET");
            if let Some(m) = st.methods.get_mut(&mkey) {
                m.api_key_required = true;
            }
            st.api_keys.insert(
                "k1".to_string(),
                ApiKey {
                    id: "k1".to_string(),
                    value: key_value.clone(),
                    name: "k".to_string(),
                    description: None,
                    enabled: true,
                    created_date: Utc::now(),
                    last_updated_date: Utc::now(),
                    stage_keys: vec![],
                    tags: BTreeMap::new(),
                    customer_id: None,
                },
            );
            st.usage_plans.insert(
                plan_id.clone(),
                UsagePlan {
                    id: plan_id.clone(),
                    name: "p".to_string(),
                    description: None,
                    api_stages: vec![serde_json::json!({
                        "apiId": TEST_API_ID,
                        "stage": "prod",
                        "throttle": {
                            "/items/GET": {"rateLimit": 1.0, "burstLimit": 1}
                        }
                    })],
                    // Plan-level limits intentionally generous so a
                    // failure here proves the method-level bucket was
                    // consulted.
                    throttle: Some(serde_json::json!({"rateLimit": 100.0, "burstLimit": 100})),
                    quota: None,
                    product_code: None,
                    tags: BTreeMap::new(),
                },
            );
            let mut plan_keys = BTreeMap::new();
            plan_keys.insert(
                "k1".to_string(),
                serde_json::json!({"id": "k1", "type": "API_KEY", "value": key_value}),
            );
            st.usage_plan_keys.insert(plan_id.clone(), plan_keys);
        }
        let lambda = Arc::new(StubLambda::new());
        lambda.set(
            BACKEND_ARN,
            serde_json::json!({"statusCode": 200, "body": "ok"}),
        );
        let service = build_service(state, lambda.clone(), None);
        let mut headers = HeaderMap::new();
        headers.insert("x-api-key", key_value.parse().unwrap());

        let first = handle(&service, &make_request(headers.clone()))
            .await
            .expect("first request consumes the only method-level token");
        assert_eq!(first.status, StatusCode::OK);

        let err = match handle(&service, &make_request(headers)).await {
            Err(e) => e,
            Ok(_) => panic!("second request must trip method-level throttle"),
        };
        assert_eq!(err.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(lambda.invocation_count(BACKEND_ARN), 1);
    }

    /// `/*/*` wildcard under apiStages.throttle applies to any method
    /// path that lacks a more specific entry. Same shape as the exact
    /// override test, but keyed under the catch-all instead of
    /// `/items/GET`.
    #[tokio::test]
    async fn method_level_throttle_wildcard_catchall_applies() {
        use crate::state::{ApiKey, UsagePlan};
        let state = build_state("NONE", None);
        let key_value = "wildcard-key".to_string();
        let plan_id = "plan-wildcard".to_string();
        {
            let mut accounts = state.write();
            let st = accounts.get_or_create(TEST_ACCOUNT);
            let mkey = format!("{TEST_API_ID}/{RES_ID}/GET");
            if let Some(m) = st.methods.get_mut(&mkey) {
                m.api_key_required = true;
            }
            st.api_keys.insert(
                "k2".to_string(),
                ApiKey {
                    id: "k2".to_string(),
                    value: key_value.clone(),
                    name: "k".to_string(),
                    description: None,
                    enabled: true,
                    created_date: Utc::now(),
                    last_updated_date: Utc::now(),
                    stage_keys: vec![],
                    tags: BTreeMap::new(),
                    customer_id: None,
                },
            );
            st.usage_plans.insert(
                plan_id.clone(),
                UsagePlan {
                    id: plan_id.clone(),
                    name: "p".to_string(),
                    description: None,
                    api_stages: vec![serde_json::json!({
                        "apiId": TEST_API_ID,
                        "stage": "prod",
                        "throttle": {
                            "/*/*": {"rateLimit": 1.0, "burstLimit": 1}
                        }
                    })],
                    throttle: None,
                    quota: None,
                    product_code: None,
                    tags: BTreeMap::new(),
                },
            );
            let mut plan_keys = BTreeMap::new();
            plan_keys.insert(
                "k2".to_string(),
                serde_json::json!({"id": "k2", "type": "API_KEY", "value": key_value}),
            );
            st.usage_plan_keys.insert(plan_id.clone(), plan_keys);
        }
        let lambda = Arc::new(StubLambda::new());
        lambda.set(
            BACKEND_ARN,
            serde_json::json!({"statusCode": 200, "body": "ok"}),
        );
        let service = build_service(state, lambda.clone(), None);
        let mut headers = HeaderMap::new();
        headers.insert("x-api-key", key_value.parse().unwrap());

        let first = handle(&service, &make_request(headers.clone()))
            .await
            .expect("first request consumes the only wildcard token");
        assert_eq!(first.status, StatusCode::OK);

        let err = match handle(&service, &make_request(headers)).await {
            Err(e) => e,
            Ok(_) => panic!("second request must trip wildcard throttle"),
        };
        assert_eq!(err.status(), StatusCode::TOO_MANY_REQUESTS);
    }

    #[test]
    fn token_bucket_grants_initial_burst_then_refills() {
        let mut bucket = TokenBucket::new(10.0, 2.0);
        let t0 = Instant::now();
        // Burst of 2 -> two acquires succeed back-to-back.
        assert!(bucket.try_acquire(t0));
        assert!(bucket.try_acquire(t0));
        assert!(!bucket.try_acquire(t0));
        // After 200ms at 10 RPS, ~2 tokens have refilled.
        let t1 = t0 + std::time::Duration::from_millis(200);
        assert!(bucket.try_acquire(t1));
    }

    #[test]
    fn token_bucket_caps_at_burst() {
        let mut bucket = TokenBucket::new(1.0, 3.0);
        let t0 = Instant::now();
        // Long idle period — tokens must not exceed `burst`.
        let t1 = t0 + std::time::Duration::from_secs(60);
        // Drain up to burst.
        assert!(bucket.try_acquire(t1));
        assert!(bucket.try_acquire(t1));
        assert!(bucket.try_acquire(t1));
        assert!(!bucket.try_acquire(t1));
    }

    #[test]
    fn quota_window_strings_change_at_period_boundaries() {
        use chrono::TimeZone;
        let day1 = chrono::Utc.with_ymd_and_hms(2026, 5, 3, 23, 59, 0).unwrap();
        let day2 = chrono::Utc.with_ymd_and_hms(2026, 5, 4, 0, 1, 0).unwrap();
        assert_ne!(
            current_quota_window(day1, QuotaPeriod::Day, 0),
            current_quota_window(day2, QuotaPeriod::Day, 0)
        );
        // Same day -> same window.
        let day1_morning = chrono::Utc.with_ymd_and_hms(2026, 5, 3, 0, 1, 0).unwrap();
        assert_eq!(
            current_quota_window(day1, QuotaPeriod::Day, 0),
            current_quota_window(day1_morning, QuotaPeriod::Day, 0)
        );
        // Month boundary.
        let april = chrono::Utc.with_ymd_and_hms(2026, 4, 30, 12, 0, 0).unwrap();
        let may = chrono::Utc.with_ymd_and_hms(2026, 5, 1, 12, 0, 0).unwrap();
        assert_ne!(
            current_quota_window(april, QuotaPeriod::Month, 0),
            current_quota_window(may, QuotaPeriod::Month, 0)
        );
    }

    #[tokio::test]
    async fn token_authorizer_cache_short_circuits_second_invocation() {
        // Two requests with the same identity-source value must hit the
        // authorizer Lambda once; the cached Allow result feeds the
        // second call directly.
        let state = build_state("CUSTOM", Some(token_authorizer()));
        let lambda = Arc::new(StubLambda::new());
        lambda.set(
            FN_ARN,
            serde_json::json!({
                "principalId": "u",
                "policyDocument": {
                    "Version": "2012-10-17",
                    "Statement": [{"Effect": "Allow", "Resource": "*"}]
                }
            }),
        );
        lambda.set(
            BACKEND_ARN,
            serde_json::json!({"statusCode": 200, "body": "ok"}),
        );
        let service = build_service(state, lambda.clone(), None);
        let mut headers = HeaderMap::new();
        headers.insert("authorization", "tok-cache".parse().unwrap());

        for _ in 0..2 {
            let resp = handle(&service, &make_request(headers.clone()))
                .await
                .expect("Allow must let request through");
            assert_eq!(resp.status, StatusCode::OK);
        }
        // Authorizer Lambda invoked exactly once across both requests
        // (cache TTL is 300s by default in this fixture).
        assert_eq!(lambda.invocation_count(FN_ARN), 1);
        // Backend Lambda invoked twice — caching only applies to the
        // authorizer decision, not to the integration.
        assert_eq!(lambda.invocation_count(BACKEND_ARN), 2);
    }

    #[tokio::test]
    async fn unauthorized_gateway_response_template_overrides_status_and_body() {
        // Customer registers a gateway response template that maps
        // UNAUTHORIZED to HTTP 418 with a custom JSON body. A request
        // missing the identity-source header must surface that override
        // instead of the default 401.
        let state = build_state("CUSTOM", Some(token_authorizer()));
        {
            let mut accounts = state.write();
            let st = accounts.get_or_create(TEST_ACCOUNT);
            let mut by_type = BTreeMap::new();
            by_type.insert(
                "UNAUTHORIZED".to_string(),
                serde_json::json!({
                    "responseType": "UNAUTHORIZED",
                    "statusCode": "418",
                    "responseTemplates": {
                        "application/json": "{\"reason\":$context.error.messageString}"
                    }
                }),
            );
            st.gateway_responses
                .insert(TEST_API_ID.to_string(), by_type);
        }
        let lambda = Arc::new(StubLambda::new());
        let service = build_service(state, lambda.clone(), None);
        let resp = handle(&service, &make_request(HeaderMap::new()))
            .await
            .expect("override must surface as a successful AwsResponse");
        assert_eq!(resp.status, StatusCode::IM_A_TEAPOT);
        let body_bytes = match &resp.body {
            fakecloud_core::service::ResponseBody::Bytes(b) => b.clone(),
            _ => panic!("override body should be inline bytes"),
        };
        let body: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
        assert!(body["reason"].as_str().unwrap().contains("Authorization"));
        // Authorizer Lambda never invoked — request shorted at the
        // missing identity source check.
        assert_eq!(lambda.invocation_count(FN_ARN), 0);
        assert_eq!(lambda.invocation_count(BACKEND_ARN), 0);
    }

    #[tokio::test]
    async fn default_4xx_gateway_response_template_falls_back_for_unauthorized() {
        // No UNAUTHORIZED-specific override is registered, but
        // DEFAULT_4XX is. AWS treats DEFAULT_4XX as the catch-all for
        // any uncustomized 4xx, so the missing-token 401 must adopt the
        // fallback's status and body.
        let state = build_state("CUSTOM", Some(token_authorizer()));
        {
            let mut accounts = state.write();
            let st = accounts.get_or_create(TEST_ACCOUNT);
            let mut by_type = BTreeMap::new();
            by_type.insert(
                "DEFAULT_4XX".to_string(),
                serde_json::json!({
                    "responseType": "DEFAULT_4XX",
                    "statusCode": 451,
                    "responseTemplates": {
                        "application/json": "{\"fallback\":$context.error.messageString}"
                    }
                }),
            );
            st.gateway_responses
                .insert(TEST_API_ID.to_string(), by_type);
        }
        let lambda = Arc::new(StubLambda::new());
        let service = build_service(state, lambda.clone(), None);
        let resp = handle(&service, &make_request(HeaderMap::new()))
            .await
            .expect("DEFAULT_4XX fallback must surface as a successful AwsResponse");
        assert_eq!(resp.status, StatusCode::UNAVAILABLE_FOR_LEGAL_REASONS);
        let body_bytes = match &resp.body {
            fakecloud_core::service::ResponseBody::Bytes(b) => b.clone(),
            _ => panic!("override body should be inline bytes"),
        };
        let body: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
        assert!(body["fallback"].as_str().unwrap().contains("Authorization"));
    }

    #[tokio::test]
    async fn request_authorizer_evaluates_full_request_event() {
        let state = build_state("CUSTOM", Some(request_authorizer()));
        let lambda = Arc::new(StubLambda::new());
        lambda.set(
            FN_ARN,
            serde_json::json!({
                "principalId": "u",
                "policyDocument": {
                    "Statement": [{"Effect": "Allow", "Resource": "*"}]
                }
            }),
        );
        lambda.set(
            BACKEND_ARN,
            serde_json::json!({"statusCode": 200, "body": "ok"}),
        );
        let service = build_service(state, lambda.clone(), None);
        let mut headers = HeaderMap::new();
        headers.insert("x-custom", "secret".parse().unwrap());
        let resp = handle(&service, &make_request(headers))
            .await
            .expect("REQUEST authorizer Allow must succeed");
        assert_eq!(resp.status, StatusCode::OK);
        let payload: serde_json::Value =
            serde_json::from_str(&lambda.last_payload(FN_ARN).unwrap()).unwrap();
        assert_eq!(payload["type"], "REQUEST");
        assert_eq!(payload["headers"]["x-custom"], "secret");
        assert_eq!(payload["httpMethod"], "GET");
        assert!(payload["methodArn"].as_str().unwrap().contains("/items"));
    }

    // ── Request validator tests ──

    fn install_validator_and_model(
        state: &SharedApiGatewayState,
        validator_id: &str,
        validate_params: bool,
        validate_body: bool,
    ) {
        use crate::state::Model;
        let mut accounts = state.write();
        let st = accounts.get_or_create(TEST_ACCOUNT);
        // Register validator
        let mut validators = std::collections::BTreeMap::new();
        validators.insert(
            validator_id.to_string(),
            serde_json::json!({
                "id": validator_id,
                "name": "test-validator",
                "validateRequestParameters": validate_params,
                "validateRequestBody": validate_body,
            }),
        );
        st.request_validators
            .insert(TEST_API_ID.to_string(), validators);
        // Register model
        let mut models = std::collections::BTreeMap::new();
        models.insert(
            "ItemModel".to_string(),
            Model {
                id: "model1".to_string(),
                name: "ItemModel".to_string(),
                description: None,
                schema: Some(r#"{"type":"object","required":["name"],"properties":{"name":{"type":"string"},"count":{"type":"integer"}}}"#.to_string()),
                content_type: "application/json".to_string(),
            },
        );
        st.models.insert(TEST_API_ID.to_string(), models);
    }

    #[tokio::test]
    async fn missing_required_query_parameter_returns_400() {
        let state = build_state("NONE", None);
        {
            let mut accounts = state.write();
            let st = accounts.get_or_create(TEST_ACCOUNT);
            let mkey = format!("{TEST_API_ID}/{RES_ID}/GET");
            if let Some(m) = st.methods.get_mut(&mkey) {
                m.request_validator_id = Some("val1".to_string());
                m.request_parameters
                    .insert("method.request.querystring.name".to_string(), true);
            }
        }
        install_validator_and_model(&state, "val1", true, false);
        let lambda = Arc::new(StubLambda::new());
        let service = build_service(state, lambda.clone(), None);
        let err = match handle(
            &service,
            &AwsRequest {
                query_params: std::collections::HashMap::new(),
                ..make_request(HeaderMap::new())
            },
        )
        .await
        {
            Err(e) => e,
            Ok(_) => panic!("missing query param must 400"),
        };
        assert_eq!(err.status(), StatusCode::BAD_REQUEST);
        assert!(err.message().contains("Missing required request parameter"));
        assert_eq!(lambda.invocation_count(BACKEND_ARN), 0);
    }

    #[tokio::test]
    async fn missing_required_header_returns_400() {
        let state = build_state("NONE", None);
        {
            let mut accounts = state.write();
            let st = accounts.get_or_create(TEST_ACCOUNT);
            let mkey = format!("{TEST_API_ID}/{RES_ID}/GET");
            if let Some(m) = st.methods.get_mut(&mkey) {
                m.request_validator_id = Some("val1".to_string());
                m.request_parameters
                    .insert("method.request.header.X-Required".to_string(), true);
            }
        }
        install_validator_and_model(&state, "val1", true, false);
        let lambda = Arc::new(StubLambda::new());
        let service = build_service(state, lambda.clone(), None);
        let err = match handle(&service, &make_request(HeaderMap::new())).await {
            Err(e) => e,
            Ok(_) => panic!("missing header must 400"),
        };
        assert_eq!(err.status(), StatusCode::BAD_REQUEST);
        assert!(err.message().contains("Missing required request parameter"));
        assert_eq!(lambda.invocation_count(BACKEND_ARN), 0);
    }

    #[tokio::test]
    async fn present_required_parameters_passes_validation() {
        let state = build_state("NONE", None);
        {
            let mut accounts = state.write();
            let st = accounts.get_or_create(TEST_ACCOUNT);
            let mkey = format!("{TEST_API_ID}/{RES_ID}/GET");
            if let Some(m) = st.methods.get_mut(&mkey) {
                m.request_validator_id = Some("val1".to_string());
                m.request_parameters
                    .insert("method.request.querystring.name".to_string(), true);
            }
        }
        install_validator_and_model(&state, "val1", true, false);
        let lambda = Arc::new(StubLambda::new());
        lambda.set(
            BACKEND_ARN,
            serde_json::json!({"statusCode": 200, "body": "ok"}),
        );
        let service = build_service(state, lambda.clone(), None);
        let mut req = make_request(HeaderMap::new());
        req.query_params
            .insert("name".to_string(), "test".to_string());
        let resp = handle(&service, &req)
            .await
            .expect("present params must pass");
        assert_eq!(resp.status, StatusCode::OK);
        assert_eq!(lambda.invocation_count(BACKEND_ARN), 1);
    }

    #[tokio::test]
    async fn invalid_body_returns_400() {
        let state = build_state("NONE", None);
        {
            let mut accounts = state.write();
            let st = accounts.get_or_create(TEST_ACCOUNT);
            let mkey = format!("{TEST_API_ID}/{RES_ID}/GET");
            if let Some(m) = st.methods.get_mut(&mkey) {
                m.request_validator_id = Some("val1".to_string());
                m.request_models
                    .insert("application/json".to_string(), "ItemModel".to_string());
            }
        }
        install_validator_and_model(&state, "val1", false, true);
        let lambda = Arc::new(StubLambda::new());
        let service = build_service(state, lambda.clone(), None);
        let mut req = make_request(HeaderMap::new());
        req.body = bytes::Bytes::from(r#"{"count": 42}"#);
        let err = match handle(&service, &req).await {
            Err(e) => e,
            Ok(_) => panic!("invalid body must 400"),
        };
        assert_eq!(err.status(), StatusCode::BAD_REQUEST);
        assert!(err
            .message()
            .contains("Request body does not match model schema"));
        assert_eq!(lambda.invocation_count(BACKEND_ARN), 0);
    }

    #[tokio::test]
    async fn valid_body_passes_validation() {
        let state = build_state("NONE", None);
        {
            let mut accounts = state.write();
            let st = accounts.get_or_create(TEST_ACCOUNT);
            let mkey = format!("{TEST_API_ID}/{RES_ID}/GET");
            if let Some(m) = st.methods.get_mut(&mkey) {
                m.request_validator_id = Some("val1".to_string());
                m.request_models
                    .insert("application/json".to_string(), "ItemModel".to_string());
            }
        }
        install_validator_and_model(&state, "val1", false, true);
        let lambda = Arc::new(StubLambda::new());
        lambda.set(
            BACKEND_ARN,
            serde_json::json!({"statusCode": 200, "body": "ok"}),
        );
        let service = build_service(state, lambda.clone(), None);
        let mut req = make_request(HeaderMap::new());
        req.body = bytes::Bytes::from(r#"{"name": "hello", "count": 42}"#);
        let resp = handle(&service, &req).await.expect("valid body must pass");
        assert_eq!(resp.status, StatusCode::OK);
        assert_eq!(lambda.invocation_count(BACKEND_ARN), 1);
    }

    #[tokio::test]
    async fn missing_model_for_content_type_skips_validation() {
        let state = build_state("NONE", None);
        {
            let mut accounts = state.write();
            let st = accounts.get_or_create(TEST_ACCOUNT);
            let mkey = format!("{TEST_API_ID}/{RES_ID}/GET");
            if let Some(m) = st.methods.get_mut(&mkey) {
                m.request_validator_id = Some("val1".to_string());
                // No requestModels registered
            }
        }
        install_validator_and_model(&state, "val1", false, true);
        let lambda = Arc::new(StubLambda::new());
        lambda.set(
            BACKEND_ARN,
            serde_json::json!({"statusCode": 200, "body": "ok"}),
        );
        let service = build_service(state, lambda.clone(), None);
        let mut req = make_request(HeaderMap::new());
        req.body = bytes::Bytes::from(r#"{}"#);
        let resp = handle(&service, &req)
            .await
            .expect("missing model skips validation");
        assert_eq!(resp.status, StatusCode::OK);
        assert_eq!(lambda.invocation_count(BACKEND_ARN), 1);
    }

    #[tokio::test]
    async fn blank_required_query_parameter_returns_400() {
        let state = build_state("NONE", None);
        {
            let mut accounts = state.write();
            let st = accounts.get_or_create(TEST_ACCOUNT);
            let mkey = format!("{TEST_API_ID}/{RES_ID}/GET");
            if let Some(m) = st.methods.get_mut(&mkey) {
                m.request_validator_id = Some("val1".to_string());
                m.request_parameters
                    .insert("method.request.querystring.name".to_string(), true);
            }
        }
        install_validator_and_model(&state, "val1", true, false);
        let lambda = Arc::new(StubLambda::new());
        let service = build_service(state, lambda.clone(), None);
        let mut req = make_request(HeaderMap::new());
        req.query_params.insert("name".to_string(), "".to_string());
        let err = match handle(&service, &req).await {
            Err(e) => e,
            Ok(_) => panic!("blank query param must 400"),
        };
        assert_eq!(err.status(), StatusCode::BAD_REQUEST);
        assert!(err.message().contains("Missing required request parameter"));
        assert_eq!(lambda.invocation_count(BACKEND_ARN), 0);
    }

    #[tokio::test]
    async fn blank_required_header_returns_400() {
        let state = build_state("NONE", None);
        {
            let mut accounts = state.write();
            let st = accounts.get_or_create(TEST_ACCOUNT);
            let mkey = format!("{TEST_API_ID}/{RES_ID}/GET");
            if let Some(m) = st.methods.get_mut(&mkey) {
                m.request_validator_id = Some("val1".to_string());
                m.request_parameters
                    .insert("method.request.header.X-Required".to_string(), true);
            }
        }
        install_validator_and_model(&state, "val1", true, false);
        let lambda = Arc::new(StubLambda::new());
        let service = build_service(state, lambda.clone(), None);
        let mut headers = HeaderMap::new();
        headers.insert("X-Required", "".parse().unwrap());
        let err = match handle(&service, &make_request(headers)).await {
            Err(e) => e,
            Ok(_) => panic!("blank header must 400"),
        };
        assert_eq!(err.status(), StatusCode::BAD_REQUEST);
        assert!(err.message().contains("Missing required request parameter"));
        assert_eq!(lambda.invocation_count(BACKEND_ARN), 0);
    }

    #[tokio::test]
    async fn default_model_used_when_no_exact_match() {
        let state = build_state("NONE", None);
        {
            let mut accounts = state.write();
            let st = accounts.get_or_create(TEST_ACCOUNT);
            let mkey = format!("{TEST_API_ID}/{RES_ID}/GET");
            if let Some(m) = st.methods.get_mut(&mkey) {
                m.request_validator_id = Some("val1".to_string());
                m.request_models
                    .insert("$default".to_string(), "ItemModel".to_string());
            }
        }
        install_validator_and_model(&state, "val1", false, true);
        let lambda = Arc::new(StubLambda::new());
        lambda.set(
            BACKEND_ARN,
            serde_json::json!({"statusCode": 200, "body": "ok"}),
        );
        let service = build_service(state, lambda.clone(), None);
        let mut req = make_request(HeaderMap::new());
        req.body = bytes::Bytes::from(r#"{"name": "hello", "count": 42}"#);
        let resp = handle(&service, &req)
            .await
            .expect("$default model should validate");
        assert_eq!(resp.status, StatusCode::OK);
        assert_eq!(lambda.invocation_count(BACKEND_ARN), 1);
    }

    #[tokio::test]
    async fn vpc_link_proxy_resolves_target_nlb_and_attempts_connection() {
        // Seed API Gateway state with a VpcLink pointing at an NLB ARN.
        let mut state = ApiGatewayState::new(TEST_ACCOUNT, TEST_REGION);
        let vpc_link_id = "vpclink001";
        state.vpc_links.insert(
            vpc_link_id.to_string(),
            serde_json::json!({
                "id": vpc_link_id,
                "name": "link",
                "targetArns": ["arn:aws:elasticloadbalancing:us-east-1:000000000000:loadbalancer/app/my-nlb/50dc6c495c0c9188"],
                "status": "AVAILABLE"
            }),
        );
        let apigw_state: SharedApiGatewayState = {
            let mut mas =
                MultiAccountState::new(TEST_ACCOUNT, TEST_REGION, "http://localhost:4566");
            *mas.get_or_create(TEST_ACCOUNT) = state;
            Arc::new(parking_lot::RwLock::new(mas))
        };

        // Seed ELBv2 state with a load balancer that has a bound port.
        let mut elbv2_accounts = fakecloud_elbv2::Elbv2Accounts::new();
        let elbv2_state = elbv2_accounts.get_or_create(TEST_ACCOUNT);
        let lb_arn = "arn:aws:elasticloadbalancing:us-east-1:000000000000:loadbalancer/app/my-nlb/50dc6c495c0c9188";
        let mut lb: fakecloud_elbv2::LoadBalancer = serde_json::from_value(serde_json::json!({
            "arn": lb_arn,
            "name": "my-nlb",
            "dns_name": "my-nlb-123.elb.us-east-1.amazonaws.com",
            "canonical_hosted_zone_id": "Z35SXDOTRQ7X7K",
            "created_time": "2024-01-01T00:00:00Z",
            "scheme": "internal",
            "vpc_id": "vpc-123",
            "state_code": "active",
            "lb_type": "application",
            "availability_zones": [],
            "security_groups": [],
            "ip_address_type": "ipv4",
            "tags": [],
            "attributes": {}
        }))
        .unwrap();
        lb.bound_port = Some(54321);
        elbv2_state.load_balancers.insert(lb_arn.to_string(), lb);
        let shared_elbv2 = Arc::new(parking_lot::RwLock::new(elbv2_accounts));

        let service = ApiGatewayService::new(apigw_state).with_elbv2(shared_elbv2);

        let integration = Integration {
            rest_api_id: "api1".to_string(),
            resource_id: "res1".to_string(),
            http_method: "GET".to_string(),
            integration_type: "HTTP".to_string(),
            integration_http_method: Some("GET".to_string()),
            uri: Some("http://backend.internal/items".to_string()),
            credentials: None,
            request_parameters: BTreeMap::new(),
            request_templates: BTreeMap::new(),
            passthrough_behavior: "WHEN_NO_MATCH".to_string(),
            timeout_in_millis: None,
            cache_namespace: None,
            cache_key_parameters: vec![],
            content_handling: None,
            connection_type: Some("VPC_LINK".to_string()),
            connection_id: Some(vpc_link_id.to_string()),
            tls_config: None,
        };

        let req = make_request(HeaderMap::new());
        let result = vpc_link_proxy(&req, &integration, &service).await;
        let err = match result {
            Err(e) => e,
            Ok(_) => panic!("VPC_LINK to port 54321 with no server must fail"),
        };
        // No actual HTTP server is listening on port 54321, so the
        // proxy step must fail with a backend HTTP error.
        assert_eq!(err.status(), StatusCode::BAD_GATEWAY);
        let msg = err.message();
        assert!(
            msg.contains("backend HTTP failure") || msg.contains("connection refused"),
            "expected backend connection error, got: {msg}"
        );
    }

    // ── AWS direct integration tests ──

    struct StubAwsService {
        name: String,
        last_request: parking_lot::Mutex<Option<AwsRequest>>,
    }

    #[async_trait]
    impl AwsService for StubAwsService {
        fn service_name(&self) -> &str {
            &self.name
        }
        fn supported_actions(&self) -> &[&str] {
            &["PutItem"]
        }
        async fn handle(&self, request: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
            *self.last_request.lock() = Some(request);
            Ok(AwsResponse::ok_json(serde_json::json!({"ok": true})))
        }
    }

    #[tokio::test]
    async fn aws_direct_integration_dispatches_action_to_service() {
        let stub = Arc::new(StubAwsService {
            name: "dynamodb".to_string(),
            last_request: parking_lot::Mutex::new(None),
        });
        let mut registry = fakecloud_core::registry::ServiceRegistry::new();
        registry.register(stub.clone());
        let registry_arc = Arc::new(registry);
        let registry_handle = Arc::new(std::sync::OnceLock::new());
        let _ = registry_handle.set(registry_arc);

        let state = build_state("NONE", None);
        let service = ApiGatewayService::new(state).with_registry(registry_handle);

        let mut req = make_request(HeaderMap::new());
        req.body = bytes::Bytes::from(r#"{"TableName":"t","Item":{"id":{"S":"1"}}}"#);

        let resp = aws_direct_integration(
            &req,
            "arn:aws:apigateway:us-east-1:dynamodb:action/PutItem",
            &service,
        )
        .await
        .expect("dispatch must succeed");
        assert_eq!(resp.status, StatusCode::OK);

        let locked = stub.last_request.lock();
        let dispatched = locked.as_ref().expect("stub must have received a request");
        assert_eq!(dispatched.action, "PutItem");
        assert_eq!(dispatched.service, "dynamodb");
        assert_eq!(dispatched.account_id, TEST_ACCOUNT);
        assert_eq!(dispatched.region, TEST_REGION);
    }

    #[tokio::test]
    async fn aws_direct_integration_path_prefix_routes_to_raw_path() {
        let stub = Arc::new(StubAwsService {
            name: "sqs".to_string(),
            last_request: parking_lot::Mutex::new(None),
        });
        let mut registry = fakecloud_core::registry::ServiceRegistry::new();
        registry.register(stub.clone());
        let registry_arc = Arc::new(registry);
        let registry_handle = Arc::new(std::sync::OnceLock::new());
        let _ = registry_handle.set(registry_arc);

        let state = build_state("NONE", None);
        let service = ApiGatewayService::new(state).with_registry(registry_handle);

        let mut req = make_request(HeaderMap::new());
        req.method = Method::POST;
        req.body = bytes::Bytes::from("Action=SendMessage&QueueUrl=http://q");

        let resp = aws_direct_integration(&req, "arn:aws:apigateway:us-east-1:sqs:path/", &service)
            .await
            .expect("dispatch must succeed");
        assert_eq!(resp.status, StatusCode::OK);

        let locked = stub.last_request.lock();
        let dispatched = locked.as_ref().expect("stub must have received a request");
        assert_eq!(dispatched.raw_path, "/");
        assert_eq!(dispatched.path_segments, vec![""]); // path/ splits to [""]
    }
}
