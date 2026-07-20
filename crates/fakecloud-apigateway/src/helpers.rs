use super::*;

pub(crate) fn not_found(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::NOT_FOUND, "NotFoundException", msg.into())
}

pub(crate) fn bad_request(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "BadRequestException", msg.into())
}

pub(crate) fn conflict(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::CONFLICT, "ConflictException", msg.into())
}

/// PatchOperation.value is modeled as a STRING in the API Gateway API, so
/// clients (CLI/SDK/Terraform) send booleans as `"true"`/`"false"`. Accept both
/// the native JSON boolean and its string form so e.g. disabling an API key
/// (`{"op":"replace","path":"/enabled","value":"false"}`) is not silently
/// dropped.
pub(crate) fn patch_bool(value: &Value) -> Option<bool> {
    value
        .as_bool()
        .or_else(|| value.as_str().map(|s| s == "true"))
}

/// Like [`patch_bool`] but for integer-valued patch fields (e.g.
/// `/timeoutInMillis`, `/authorizerResultTtlInSeconds`) that arrive as a quoted
/// string per the STRING-typed PatchOperation.value model.
pub(crate) fn patch_i32(value: &Value) -> Option<i32> {
    value
        .as_i64()
        .or_else(|| value.as_str().and_then(|s| s.parse::<i64>().ok()))
        .map(|v| v as i32)
}

pub(crate) fn ok(value: Value) -> Result<AwsResponse, AwsServiceError> {
    Ok(AwsResponse::ok_json(strip_nulls_deep(value)))
}

pub(crate) fn ok_status(status: StatusCode, value: Value) -> Result<AwsResponse, AwsServiceError> {
    Ok(AwsResponse::json(
        status,
        serde_json::to_vec(&strip_nulls_deep(value)).unwrap(),
    ))
}

/// Recursively strip null fields from objects (and from objects nested in
/// arrays/maps). AWS clients typed-decode optional members and reject
/// `null` for non-nullable types, so it's safer to omit absent fields
/// than to send them as `null`.
pub(crate) fn strip_nulls_deep(value: Value) -> Value {
    match value {
        Value::Object(map) => {
            let mut out = serde_json::Map::new();
            for (k, v) in map {
                if v.is_null() {
                    continue;
                }
                out.insert(k, strip_nulls_deep(v));
            }
            Value::Object(out)
        }
        Value::Array(arr) => Value::Array(arr.into_iter().map(strip_nulls_deep).collect()),
        other => other,
    }
}

/// The SDK-type descriptors API Gateway exposes via GetSdkTypes, shared
/// with GetSdkType so a single id lookup returns a real descriptor.
pub(crate) fn sdk_types() -> Vec<Value> {
    vec![
        serde_json::json!({
            "id": "android",
            "friendlyName": "Android",
            "description": "Android SDK with a Java client",
            "configurationProperties": []
        }),
        serde_json::json!({
            "id": "javascript",
            "friendlyName": "JavaScript",
            "description": "JavaScript SDK",
            "configurationProperties": []
        }),
        serde_json::json!({
            "id": "ios-objectivec",
            "friendlyName": "iOS (Objective-C)",
            "description": "iOS SDK in Objective-C",
            "configurationProperties": []
        }),
        serde_json::json!({
            "id": "ios-swift",
            "friendlyName": "iOS (Swift)",
            "description": "iOS SDK in Swift",
            "configurationProperties": []
        }),
        serde_json::json!({
            "id": "java",
            "friendlyName": "Java",
            "description": "Java SDK",
            "configurationProperties": []
        }),
        serde_json::json!({
            "id": "ruby",
            "friendlyName": "Ruby",
            "description": "Ruby SDK",
            "configurationProperties": []
        }),
    ]
}

pub(crate) fn ok_no_content() -> Result<AwsResponse, AwsServiceError> {
    Ok(AwsResponse {
        status: StatusCode::ACCEPTED,
        content_type: "application/json".to_string(),
        body: bytes::Bytes::new().into(),
        headers: http::HeaderMap::new(),
    })
}

pub(crate) fn method_key(api: &str, res: &str, m: &str) -> String {
    format!("{api}/{res}/{}", m.to_uppercase())
}

pub(crate) fn response_key(api: &str, res: &str, m: &str, code: &str) -> String {
    format!("{api}/{res}/{}/{}", m.to_uppercase(), code)
}

pub(crate) fn request_account(req: &AwsRequest) -> String {
    req.account_id.clone()
}

/// AWS REST-JSON omits optional fields whose runtime value is unset.
/// Smithy clients reject `null` for typed members (string, integer, …).
/// Strip null members from a JSON object before serializing it.
pub(crate) fn strip_nulls(mut value: Value) -> Value {
    if let Some(map) = value.as_object_mut() {
        let keys: Vec<String> = map
            .iter()
            .filter(|(_, v)| v.is_null())
            .map(|(k, _)| k.clone())
            .collect();
        for k in keys {
            map.remove(&k);
        }
    }
    value
}

pub(crate) fn rest_api_to_json(api: &RestApi) -> Value {
    strip_nulls(json!({
        "id": api.id,
        "name": api.name,
        "description": api.description,
        "version": api.version,
        "createdDate": api.created_date.timestamp(),
        "apiKeySource": api.api_key_source,
        "endpointConfiguration": api.endpoint_configuration,
        "policy": api.policy,
        "binaryMediaTypes": api.binary_media_types,
        "minimumCompressionSize": api.minimum_compression_size,
        "disableExecuteApiEndpoint": api.disable_execute_api_endpoint,
        "rootResourceId": api.root_resource_id,
        "tags": api.tags,
    }))
}

pub(crate) fn resource_to_json(r: &Resource, methods: BTreeMap<String, Value>) -> Value {
    let mut v = strip_nulls(json!({
        "id": r.id,
        "parentId": r.parent_id,
        "pathPart": r.path_part,
        "path": r.path,
    }));
    if !methods.is_empty() {
        v["resourceMethods"] = json!(methods);
    }
    v
}

pub(crate) fn method_to_json(m: &ApiMethod) -> Value {
    strip_nulls(json!({
        "httpMethod": m.http_method,
        "authorizationType": m.authorization_type,
        "authorizerId": m.authorizer_id,
        "apiKeyRequired": m.api_key_required,
        "operationName": m.operation_name,
        "requestParameters": m.request_parameters,
        "requestModels": m.request_models,
        "requestValidatorId": m.request_validator_id,
        "authorizationScopes": m.authorization_scopes,
    }))
}

pub(crate) fn integration_to_json(i: &Integration) -> Value {
    strip_nulls(json!({
        "type": i.integration_type,
        "httpMethod": i.integration_http_method,
        "uri": i.uri,
        "credentials": i.credentials,
        "requestParameters": i.request_parameters,
        "requestTemplates": i.request_templates,
        "passthroughBehavior": i.passthrough_behavior,
        "timeoutInMillis": i.timeout_in_millis,
        "cacheNamespace": i.cache_namespace,
        "cacheKeyParameters": i.cache_key_parameters,
        "contentHandling": i.content_handling,
        "connectionType": i.connection_type,
        "connectionId": i.connection_id,
        "tlsConfig": i.tls_config,
    }))
}

pub(crate) fn deployment_to_json(d: &Deployment) -> Value {
    strip_nulls(json!({
        "id": d.id,
        "description": d.description,
        "createdDate": d.created_date.timestamp(),
        "apiSummary": d.api_summary,
    }))
}

pub(crate) fn stage_to_json(s: &Stage) -> Value {
    strip_nulls(json!({
        "stageName": s.stage_name,
        "deploymentId": s.deployment_id,
        "description": s.description,
        "cacheClusterEnabled": s.cache_cluster_enabled,
        "cacheClusterSize": s.cache_cluster_size,
        "variables": s.variables,
        "methodSettings": s.method_settings,
        "createdDate": s.created_date.timestamp(),
        "lastUpdatedDate": s.last_updated_date.timestamp(),
        "tracingEnabled": s.tracing_enabled,
        "webAclArn": s.web_acl_arn,
        "canarySettings": s.canary_settings,
        "accessLogSettings": s.access_log_settings,
        "tags": s.tags,
    }))
}

pub(crate) fn model_to_json(m: &Model) -> Value {
    strip_nulls(json!({
        "id": m.id,
        "name": m.name,
        "description": m.description,
        "schema": m.schema,
        "contentType": m.content_type,
    }))
}

pub(crate) fn authorizer_to_json(a: &Authorizer) -> Value {
    strip_nulls(json!({
        "id": a.id,
        "name": a.name,
        "type": a.authorizer_type,
        "providerARNs": a.provider_arns,
        "authType": a.auth_type,
        "authorizerUri": a.authorizer_uri,
        "authorizerCredentials": a.authorizer_credentials,
        "identitySource": a.identity_source,
        "identityValidationExpression": a.identity_validation_expression,
        "authorizerResultTtlInSeconds": a.authorizer_result_ttl_in_seconds,
    }))
}

pub(crate) fn api_key_to_json(k: &ApiKey, include_value: bool) -> Value {
    let mut v = strip_nulls(json!({
        "id": k.id,
        "name": k.name,
        "description": k.description,
        "enabled": k.enabled,
        "createdDate": k.created_date.timestamp(),
        "lastUpdatedDate": k.last_updated_date.timestamp(),
        "stageKeys": k.stage_keys,
        "tags": k.tags,
        "customerId": k.customer_id,
    }));
    if include_value {
        v["value"] = Value::String(k.value.clone());
    }
    v
}

pub(crate) fn usage_plan_to_json(p: &UsagePlan) -> Value {
    strip_nulls(json!({
        "id": p.id,
        "name": p.name,
        "description": p.description,
        "apiStages": p.api_stages,
        "throttle": p.throttle,
        "quota": p.quota,
        "productCode": p.product_code,
        "tags": p.tags,
    }))
}

pub(crate) fn is_mutating_method(method: &Method) -> bool {
    matches!(
        *method,
        Method::POST | Method::PUT | Method::PATCH | Method::DELETE
    )
}

pub(crate) fn methods_for_resource(
    state: &ApiGatewayState,
    api_id: &str,
    res_id: &str,
) -> BTreeMap<String, Value> {
    let mut out = BTreeMap::new();
    let prefix = format!("{api_id}/{res_id}/");
    for (key, m) in &state.methods {
        if let Some(rest) = key.strip_prefix(&prefix) {
            out.insert(rest.to_string(), method_to_json(m));
        }
    }
    out
}

/// Recompute every resource's full `path` from its parent chain. Paths are
/// stored (not derived on read) and drive both `resource_to_json` and the
/// data-plane router, so any `pathPart` rename or reparent must refresh them
/// for the resource itself AND all of its descendants.
pub(crate) fn recompute_resource_paths(resources: &mut BTreeMap<String, Resource>) {
    let snapshot: BTreeMap<String, (Option<String>, Option<String>)> = resources
        .iter()
        .map(|(id, r)| (id.clone(), (r.parent_id.clone(), r.path_part.clone())))
        .collect();
    for (id, r) in resources.iter_mut() {
        r.path = compute_resource_path(id, &snapshot);
    }
}

fn compute_resource_path(
    id: &str,
    snapshot: &BTreeMap<String, (Option<String>, Option<String>)>,
) -> String {
    let mut parts: Vec<String> = Vec::new();
    let mut cur = Some(id.to_string());
    let max_depth = snapshot.len() + 1;
    let mut steps = 0;
    while let Some(cid) = cur {
        steps += 1;
        if steps > max_depth {
            break; // cycle guard
        }
        match snapshot.get(&cid) {
            Some((parent, part)) => {
                if let Some(p) = part {
                    parts.push(p.clone());
                }
                cur = parent.clone();
            }
            None => break,
        }
    }
    parts.reverse();
    if parts.is_empty() {
        "/".to_string()
    } else {
        format!("/{}", parts.join("/"))
    }
}

/// True if `candidate` is `target` itself or a descendant of `target`.
/// Reparenting `target` under such a node would create a cycle.
pub(crate) fn is_self_or_descendant(
    resources: &BTreeMap<String, Resource>,
    target: &str,
    candidate: &str,
) -> bool {
    let mut cur = Some(candidate.to_string());
    let max_depth = resources.len() + 1;
    let mut steps = 0;
    while let Some(cid) = cur {
        if cid == target {
            return true;
        }
        steps += 1;
        if steps > max_depth {
            break;
        }
        cur = resources.get(&cid).and_then(|r| r.parent_id.clone());
    }
    false
}

pub(crate) fn tags_from(body: &Value) -> BTreeMap<String, String> {
    body.get("tags")
        .and_then(Value::as_object)
        .map(|m| {
            m.iter()
                .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                .collect()
        })
        .unwrap_or_default()
}

pub(crate) fn apply_patch_operations(req: &AwsRequest, mut on: impl FnMut(&str, &str, &Value)) {
    let body = req.json_body();
    let ops = body.get("patchOperations").and_then(Value::as_array);
    if let Some(arr) = ops {
        for entry in arr {
            let op = entry.get("op").and_then(Value::as_str).unwrap_or("");
            let path = entry.get("path").and_then(Value::as_str).unwrap_or("");
            let value = entry.get("value").cloned().unwrap_or(Value::Null);
            on(op, path, &value);
        }
    }
}

pub(crate) fn apply_rest_api_patch(api: &mut RestApi, op: &str, path: &str, value: &Value) {
    let is_binary_media_path =
        path == "/binaryMediaTypes" || path.starts_with("/binaryMediaTypes/");
    let is_endpoint_path =
        path == "/endpointConfiguration" || path.starts_with("/endpointConfiguration/");
    let remove_ok = is_binary_media_path || is_endpoint_path || path == "/minimumCompressionSize";
    if op != "replace" && op != "add" && !(op == "remove" && remove_ok) {
        return;
    }
    if is_endpoint_path {
        apply_endpoint_config_patch(api, op, path, value);
        return;
    }
    match path {
        "/minimumCompressionSize" => {
            if op == "remove" {
                api.minimum_compression_size = None;
            } else if let Some(n) = value
                .as_i64()
                .or_else(|| value.as_str().and_then(|s| s.parse().ok()))
            {
                api.minimum_compression_size = Some(n);
            }
        }
        "/name" => {
            if let Some(s) = value.as_str() {
                api.name = s.to_string();
            }
        }
        "/description" => api.description = value.as_str().map(String::from),
        "/version" => api.version = value.as_str().map(String::from),
        "/policy" => api.policy = value.as_str().map(String::from),
        "/disableExecuteApiEndpoint" => {
            if let Some(b) = patch_bool(value) {
                api.disable_execute_api_endpoint = b;
            }
        }
        "/apiKeySource" => {
            if let Some(s) = value.as_str() {
                api.api_key_source = s.to_string();
            }
        }
        "/binaryMediaTypes" => {
            if op == "remove" {
                api.binary_media_types.clear();
            } else if let Some(arr) = value.as_array() {
                api.binary_media_types = arr
                    .iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect();
            }
        }
        _ => {
            if let Some(suffix) = path.strip_prefix("/binaryMediaTypes/") {
                let mt = suffix.replace("~1", "/").replace("~0", "~");
                if op == "add" || op == "replace" {
                    if !api.binary_media_types.contains(&mt) {
                        api.binary_media_types.push(mt);
                    }
                } else if op == "remove" {
                    api.binary_media_types.retain(|m| m != &mt);
                }
            }
        }
    }
}

/// Apply a patch op to a REST API's `endpointConfiguration`. Supports the whole
/// object (`/endpointConfiguration`) and the `types` / `vpcEndpointIds` arrays
/// (indexed: `/endpointConfiguration/types/0`, or the whole array).
fn apply_endpoint_config_patch(api: &mut RestApi, op: &str, path: &str, value: &Value) {
    // Ensure the stored value is an object we can mutate.
    if !api.endpoint_configuration.is_object() {
        api.endpoint_configuration = json!({});
    }
    if path == "/endpointConfiguration" {
        if op == "remove" {
            api.endpoint_configuration = json!({});
        } else if value.is_object() {
            api.endpoint_configuration = value.clone();
        }
        return;
    }
    let Some(rest) = path.strip_prefix("/endpointConfiguration/") else {
        return;
    };
    let obj = api
        .endpoint_configuration
        .as_object_mut()
        .expect("endpoint_configuration coerced to object above");
    // rest is e.g. "types", "types/0", "vpcEndpointIds", "vpcEndpointIds/1".
    let (field, index) = match rest.split_once('/') {
        Some((f, i)) => (f, i.parse::<usize>().ok()),
        None => (rest, None),
    };
    match index {
        None => {
            // Whole array replace/remove.
            if op == "remove" {
                obj.remove(field);
            } else {
                obj.insert(field.to_string(), value.clone());
            }
        }
        Some(idx) => {
            let arr = obj
                .entry(field.to_string())
                .or_insert_with(|| Value::Array(Vec::new()));
            if let Some(items) = arr.as_array_mut() {
                match op {
                    "remove" => {
                        if idx < items.len() {
                            items.remove(idx);
                        }
                    }
                    "add" => {
                        if idx >= items.len() {
                            items.push(value.clone());
                        } else {
                            items.insert(idx, value.clone());
                        }
                    }
                    _ => {
                        // replace
                        if idx < items.len() {
                            items[idx] = value.clone();
                        } else {
                            items.push(value.clone());
                        }
                    }
                }
            }
        }
    }
}

pub(crate) fn apply_method_patch(m: &mut ApiMethod, op: &str, path: &str, value: &Value) {
    if op != "replace" && op != "add" && op != "remove" {
        return;
    }
    match path {
        "/authorizationType" => {
            if let Some(s) = value.as_str() {
                m.authorization_type = s.to_string();
            }
        }
        "/authorizerId" => m.authorizer_id = value.as_str().map(String::from),
        "/apiKeyRequired" => {
            if let Some(b) = patch_bool(value) {
                m.api_key_required = b;
            }
        }
        "/operationName" => m.operation_name = value.as_str().map(String::from),
        // Previously dropped (bug-hunt 2026-06-24, 1.11): request validation +
        // Cognito authorization scopes. AWS patches map members at
        // `/requestParameters/<name>` (bool) and `/requestModels/<contentType>`
        // (model name).
        "/requestValidatorId" => m.request_validator_id = value.as_str().map(String::from),
        _ if path.starts_with("/requestParameters/") => {
            let key = path.trim_start_matches("/requestParameters/").to_string();
            if op == "remove" {
                m.request_parameters.remove(&key);
            } else {
                let required = value
                    .as_bool()
                    .or_else(|| value.as_str().map(|s| s == "true"))
                    .unwrap_or(false);
                m.request_parameters.insert(key, required);
            }
        }
        _ if path.starts_with("/requestModels/") => {
            let key = path.trim_start_matches("/requestModels/").to_string();
            if op == "remove" {
                m.request_models.remove(&key);
            } else if let Some(v) = value.as_str() {
                m.request_models.insert(key, v.to_string());
            }
        }
        "/authorizationScopes" if op == "add" => {
            if let Some(s) = value.as_str() {
                if !m.authorization_scopes.iter().any(|x| x == s) {
                    m.authorization_scopes.push(s.to_string());
                }
            }
        }
        _ if path.starts_with("/authorizationScopes/") && op == "remove" => {
            let target = path.trim_start_matches("/authorizationScopes/");
            m.authorization_scopes.retain(|x| x != target);
        }
        _ => {}
    }
}

pub(crate) fn build_path_segments(stage: &str, path: &str) -> Vec<String> {
    let mut segs = vec![stage.to_string()];
    for p in path.trim_start_matches('/').split('/') {
        if !p.is_empty() {
            segs.push(p.to_string());
        }
    }
    segs
}

pub(crate) fn serializable_headers(headers: &http::HeaderMap) -> serde_json::Map<String, Value> {
    let mut out = serde_json::Map::new();
    for (k, v) in headers.iter() {
        if let Ok(s) = v.to_str() {
            out.insert(k.as_str().to_string(), Value::String(s.to_string()));
        }
    }
    out
}

pub(crate) fn extract_string_map(body: &Value, key: &str) -> BTreeMap<String, String> {
    body.get(key)
        .and_then(Value::as_object)
        .map(|m| {
            m.iter()
                .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                .collect()
        })
        .unwrap_or_default()
}

pub(crate) fn snapshot_api(
    accounts: &fakecloud_core::multi_account::MultiAccountState<crate::state::ApiGatewayState>,
    account: &str,
    api_id: &str,
) -> Value {
    let Some(state) = accounts.get(account) else {
        return Value::Null;
    };
    // ApiSummary maps each resource path to its set of methods. This is
    // close enough to the AWS shape that SDKs round-trip it cleanly,
    // and the snapshot lets us interpret traffic deterministically per
    // deployment even if the live API is mutated afterwards.
    let mut summary = serde_json::Map::new();
    if let Some(resources) = state.resources.get(api_id) {
        for resource in resources.values() {
            let prefix = format!("{api_id}/{}/", resource.id);
            let mut methods = serde_json::Map::new();
            for (key, m) in &state.methods {
                if let Some(rest) = key.strip_prefix(&prefix) {
                    methods.insert(
                        rest.to_string(),
                        json!({
                            "authorizationType": m.authorization_type,
                            "apiKeyRequired": m.api_key_required,
                        }),
                    );
                }
            }
            if !methods.is_empty() {
                summary.insert(resource.path.clone(), Value::Object(methods));
            }
        }
    }
    Value::Object(summary)
}
