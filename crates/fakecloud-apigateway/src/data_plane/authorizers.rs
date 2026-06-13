//! apigateway data_plane `authorizers` concerns (audit-2026-05-19).

use super::*;

pub(super) async fn enforce_authorizer(
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
pub(super) async fn run_lambda_authorizer(
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

/// Evaluate an authorizer for `TestInvokeAuthorizer`. Unlike the live
/// request path (`run_lambda_authorizer` / `run_cognito_authorizer`)
/// this never enforces — it returns the authorizer's *actual* output
/// (the policy + principalId + context that a Lambda authorizer
/// returned, or the verified JWT claims for a Cognito authorizer) so the
/// caller can inspect a misconfigured authorizer instead of always
/// seeing a canned Allow (bug-audit 2026-06-13, 1.7).
///
/// `headers` are the request headers supplied in the TestInvokeAuthorizer
/// body; `synthetic` is an `AwsRequest` carrying those headers so the
/// shared identity-source / Lambda-event helpers can be reused.
pub(crate) async fn test_invoke_authorizer_eval(
    service: &ApiGatewayService,
    synthetic: &AwsRequest,
    api_id: &str,
    authorizer: &Authorizer,
) -> Result<serde_json::Value, AwsServiceError> {
    let header_name = header_name_from_identity_source(authorizer.identity_source.as_deref());
    let token_value = extract_header_value(synthetic, &header_name);

    match authorizer.authorizer_type.as_str() {
        "COGNITO_USER_POOLS" => {
            let token_value = token_value.ok_or_else(|| {
                unauthorized(format!("Missing required JWT in header '{header_name}'"))
            })?;
            let token = token_value
                .strip_prefix("Bearer ")
                .or_else(|| token_value.strip_prefix("bearer "))
                .unwrap_or(&token_value)
                .trim();
            if token.is_empty() {
                return Ok(json!({
                    "clientStatus": 401,
                    "log": "TestInvokeAuthorizer: empty JWT",
                    "latency": 0,
                    "principalId": "",
                    "authorization": {},
                    "claims": {},
                }));
            }
            let pool_arn = authorizer
                .provider_arns
                .first()
                .ok_or_else(|| forbidden("Cognito authorizer has no providerARNs configured"))?;
            let delivery = service
                .delivery()
                .ok_or_else(|| bad_gateway("Cognito JWT verifier not configured"))?;
            match delivery.verify_cognito_jwt(&synthetic.account_id, pool_arn, token) {
                Ok(claims) => {
                    let principal = claims
                        .get("sub")
                        .and_then(|v| v.as_str())
                        .unwrap_or("user")
                        .to_string();
                    Ok(json!({
                        "clientStatus": 200,
                        "log": "TestInvokeAuthorizer ok",
                        "latency": 0,
                        "principalId": principal,
                        "authorization": {},
                        "claims": claims,
                    }))
                }
                Err(e) => Ok(json!({
                    "clientStatus": 403,
                    "log": format!("TestInvokeAuthorizer: invalid JWT: {e}"),
                    "latency": 0,
                    "principalId": "",
                    "authorization": {},
                    "claims": {},
                })),
            }
        }
        // TOKEN / REQUEST / CUSTOM are Lambda authorizers.
        _ => {
            let token_value = match token_value.filter(|v| !v.trim().is_empty()) {
                Some(v) => v,
                None => {
                    // Missing required identity source -> AWS returns 401
                    // with no policy.
                    return Ok(json!({
                        "clientStatus": 401,
                        "log": "TestInvokeAuthorizer: identity source not found",
                        "latency": 0,
                        "principalId": "",
                        "authorization": {},
                        "claims": {},
                    }));
                }
            };
            let auth_uri = authorizer.authorizer_uri.as_deref().ok_or_else(|| {
                bad_gateway("Authorizer is missing authorizerUri; cannot invoke Lambda")
            })?;
            let function_arn = extract_lambda_arn(auth_uri)
                .ok_or_else(|| bad_gateway("authorizerUri must reference a Lambda function ARN"))?;
            // TestInvokeAuthorizer has no concrete method; build a wildcard
            // method ARN against the API so the returned policy can be
            // evaluated for clientStatus.
            let method_arn = format!(
                "arn:aws:execute-api:{}:{}:{}/*/*/*",
                synthetic.region, synthetic.account_id, api_id,
            );
            let event = if authorizer.authorizer_type == "TOKEN" {
                json!({
                    "type": "TOKEN",
                    "methodArn": method_arn,
                    "authorizationToken": raw_token(&token_value),
                })
            } else {
                let mut headers = serde_json::Map::new();
                for (k, v) in synthetic.headers.iter() {
                    if let Ok(s) = v.to_str() {
                        headers.insert(k.as_str().to_string(), json!(s));
                    }
                }
                json!({
                    "type": "REQUEST",
                    "methodArn": method_arn,
                    "headers": headers,
                    "requestContext": { "apiId": api_id },
                })
            };

            let delivery = service
                .delivery()
                .ok_or_else(|| bad_gateway("Lambda delivery not configured"))?;
            let response_bytes = delivery
                .invoke_lambda(&function_arn, &event.to_string())
                .await
                .ok_or_else(|| bad_gateway("Lambda delivery not configured"))?
                .map_err(|e| bad_gateway(format!("Authorizer Lambda failed: {e}")))?;
            let response: serde_json::Value = serde_json::from_slice(&response_bytes)
                .map_err(|e| bad_gateway(format!("Authorizer returned invalid JSON: {e}")))?;

            let principal_id = response
                .get("principalId")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let context = response
                .get("context")
                .cloned()
                .unwrap_or_else(|| json!({}));
            let policy_doc = response
                .get("policyDocument")
                .cloned()
                .unwrap_or_else(|| json!({}));
            let effect = parse_policy_effect(&response, &method_arn);
            // AWS reports clientStatus 200 for an authorizer that ran and
            // returned a policy (Allow or Deny); 403 only when no policy
            // was produced at all.
            let client_status = if policy_doc.get("Statement").is_some() {
                200
            } else {
                403
            };
            Ok(json!({
                "clientStatus": client_status,
                "log": match effect {
                    AuthEffect::Allow => "TestInvokeAuthorizer: Allow",
                    AuthEffect::Deny => "TestInvokeAuthorizer: Deny",
                },
                "latency": 0,
                "principalId": principal_id,
                "policy": serde_json::to_string(&policy_doc).unwrap_or_default(),
                "authorization": context,
                "claims": {},
            }))
        }
    }
}

/// Walk `policyDocument.Statement` and resolve to a single Allow/Deny
/// effect. Multiple matching Allow statements collapse to Allow; any
/// Deny short-circuits to Deny (mirroring the IAM policy combinator).
pub(super) fn parse_policy_effect(response: &serde_json::Value, method_arn: &str) -> AuthEffect {
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
        // missing Resource (treat as `*`). Invalid Resource types
        // (number, bool, null) are NOT treated as wildcard — that would
        // let a malformed policy authorize requests instead of failing
        // safe.
        let matches = match stmt.get("Resource") {
            Some(serde_json::Value::String(s)) => arn_matches(s, method_arn),
            Some(serde_json::Value::Array(arr)) => arr
                .iter()
                .filter_map(|v| v.as_str())
                .any(|s| arn_matches(s, method_arn)),
            // Missing Resource is interpreted as `*` (AWS default).
            None => true,
            // Wrong type — refuse to match. Caller sees Deny.
            _ => false,
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
pub(super) fn arn_matches(pattern: &str, target: &str) -> bool {
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

pub(super) fn build_method_arn(
    req: &AwsRequest,
    api_id: &str,
    stage: &str,
    resource_path: &str,
) -> String {
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

pub(super) async fn run_cognito_authorizer(
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

pub(super) fn lookup_cached_auth(
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

pub(super) fn cache_auth_result(
    service: &ApiGatewayService,
    account_id: &str,
    key: String,
    entry: CachedAuthorizerResult,
) {
    let mut accounts = service.state_handle().write();
    let state = accounts.get_or_create(account_id);
    state.authorizer_cache.insert(key, entry);
}

pub(super) fn interpret_cached(
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
pub(super) fn apply_gateway_response_override(
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
pub(super) fn render_error_template(template: &str, response_type: &str, message: &str) -> String {
    let escaped = escape_json(message);
    template
        .replace("$context.error.messageString", &format!("\"{escaped}\""))
        .replace("$context.error.message", &escaped)
        .replace("$context.error.responseType", response_type)
}

pub(super) fn escape_json(s: &str) -> String {
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

pub(super) fn inject_authorizer_into_event(
    event: &mut serde_json::Value,
    outcome: &AuthorizerOutcome,
) {
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
