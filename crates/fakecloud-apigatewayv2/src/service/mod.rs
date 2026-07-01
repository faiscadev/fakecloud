use async_trait::async_trait;
use http::{Method, StatusCode};
use serde_json::json;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::Mutex as AsyncMutex;

use fakecloud_core::delivery::DeliveryBus;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};

/// Apigwv2-local replacement for `fakecloud_core::validation::validate_required`.
/// The shared helper emits `ValidationException`, but every operation in
/// the apigwv2 Smithy model declares `BadRequestException` as the
/// client-error code instead.
fn validate_required(field: &str, value: &serde_json::Value) -> Result<(), AwsServiceError> {
    if value.is_null() {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "BadRequestException",
            format!("{} is required", field),
        ));
    }
    Ok(())
}
use fakecloud_persistence::SnapshotStore;

use crate::state::{
    ApiGatewayV2Snapshot, ApiGatewayV2State, ApiRequest, Authorizer, Deployment, HttpApi,
    Integration, Route, SharedApiGatewayV2State, Stage, APIGATEWAYV2_SNAPSHOT_SCHEMA_VERSION,
};
use crate::{cors, http_proxy, lambda_proxy, mock, router::Router};
use lambda_proxy::AuthorizerInfo;

const SUPPORTED: &[&str] = &[
    "CreateApi",
    "GetApi",
    "GetApis",
    "UpdateApi",
    "DeleteApi",
    "CreateRoute",
    "GetRoute",
    "GetRoutes",
    "UpdateRoute",
    "DeleteRoute",
    "CreateIntegration",
    "GetIntegration",
    "GetIntegrations",
    "UpdateIntegration",
    "DeleteIntegration",
    "CreateStage",
    "GetStage",
    "GetStages",
    "UpdateStage",
    "DeleteStage",
    "CreateDeployment",
    "GetDeployment",
    "GetDeployments",
    "CreateAuthorizer",
    "GetAuthorizer",
    "GetAuthorizers",
    "UpdateAuthorizer",
    "DeleteAuthorizer",
    "CreateDomainName",
    "GetDomainName",
    "GetDomainNames",
    "UpdateDomainName",
    "DeleteDomainName",
    "CreateApiMapping",
    "GetApiMapping",
    "GetApiMappings",
    "UpdateApiMapping",
    "DeleteApiMapping",
    "CreateModel",
    "GetModel",
    "GetModels",
    "UpdateModel",
    "DeleteModel",
    "GetModelTemplate",
    "CreateIntegrationResponse",
    "GetIntegrationResponse",
    "GetIntegrationResponses",
    "UpdateIntegrationResponse",
    "DeleteIntegrationResponse",
    "CreateRouteResponse",
    "GetRouteResponse",
    "GetRouteResponses",
    "UpdateRouteResponse",
    "DeleteRouteResponse",
    "CreateRoutingRule",
    "GetRoutingRule",
    "PutRoutingRule",
    "DeleteRoutingRule",
    "ListRoutingRules",
    "CreateVpcLink",
    "GetVpcLink",
    "GetVpcLinks",
    "UpdateVpcLink",
    "DeleteVpcLink",
    "TagResource",
    "UntagResource",
    "GetTags",
    "CreatePortal",
    "GetPortal",
    "ListPortals",
    "UpdatePortal",
    "DeletePortal",
    "DisablePortal",
    "PreviewPortal",
    "PublishPortal",
    "CreatePortalProduct",
    "GetPortalProduct",
    "ListPortalProducts",
    "UpdatePortalProduct",
    "DeletePortalProduct",
    "PutPortalProductSharingPolicy",
    "GetPortalProductSharingPolicy",
    "DeletePortalProductSharingPolicy",
    "CreateProductPage",
    "GetProductPage",
    "ListProductPages",
    "UpdateProductPage",
    "DeleteProductPage",
    "CreateProductRestEndpointPage",
    "GetProductRestEndpointPage",
    "ListProductRestEndpointPages",
    "UpdateProductRestEndpointPage",
    "DeleteProductRestEndpointPage",
    "ImportApi",
    "ReimportApi",
    "ExportApi",
    "DeleteCorsConfiguration",
    "DeleteAccessLogSettings",
    "DeleteRouteRequestParameter",
    "DeleteRouteSettings",
    "DeleteDeployment",
    "UpdateDeployment",
    "ResetAuthorizersCache",
];

pub struct ApiGatewayV2Service {
    pub(crate) state: SharedApiGatewayV2State,
    delivery: Option<Arc<DeliveryBus>>,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<AsyncMutex<()>>,
    /// WAFv2 inspection wiring. When set together with
    /// `waf_rate_limiter`, the data plane evaluates each request
    /// against the WebACL associated with the matched stage's ARN
    /// before authorizer / integration dispatch.
    pub(crate) waf_state: Option<fakecloud_wafv2::SharedWafv2State>,
    pub(crate) waf_rate_limiter: Option<Arc<fakecloud_wafv2::RateLimiter>>,
    /// Per-(WebACL ARN, rule name) Count-action match counter. Keyed
    /// by `"<acl-arn>|<rule-name>"`.
    pub(crate) waf_count_metrics: Arc<parking_lot::Mutex<std::collections::BTreeMap<String, u64>>>,
}

mod apis;
mod authorizers;
mod deployments;
mod execute;
mod integrations;
mod management;
mod routes;
mod stages;
mod waf;

impl ApiGatewayV2Service {
    pub fn new(state: SharedApiGatewayV2State) -> Self {
        Self {
            state,
            delivery: None,
            snapshot_store: None,
            snapshot_lock: Arc::new(AsyncMutex::new(())),
            waf_state: None,
            waf_rate_limiter: None,
            waf_count_metrics: Arc::new(parking_lot::Mutex::new(std::collections::BTreeMap::new())),
        }
    }

    pub fn with_delivery(mut self, delivery: Arc<DeliveryBus>) -> Self {
        self.delivery = Some(delivery);
        self
    }

    pub fn with_snapshot_store(mut self, store: Arc<dyn SnapshotStore>) -> Self {
        self.snapshot_store = Some(store);
        self
    }

    async fn save_snapshot(&self) {
        save_apigatewayv2_snapshot(
            &self.state,
            self.snapshot_store.clone(),
            &self.snapshot_lock,
        )
        .await;
    }

    /// Build a hook that persists the current API Gateway v2 state when invoked,
    /// or `None` in memory mode (no snapshot store). The CloudFormation
    /// provisioner mutates `state` directly and uses this to write a
    /// CFN-provisioned resource through to disk, the same way a direct mutating
    /// API call would.
    pub fn snapshot_hook(&self) -> Option<fakecloud_persistence::SnapshotHook> {
        let store = self.snapshot_store.clone()?;
        let state = self.state.clone();
        let lock = self.snapshot_lock.clone();
        Some(Arc::new(move || {
            let state = state.clone();
            let store = store.clone();
            let lock = lock.clone();
            Box::pin(async move {
                save_apigatewayv2_snapshot(&state, Some(store), &lock).await;
            })
        }))
    }
}

/// Persist the current API Gateway v2 state as a snapshot. Cloned + serialized
/// under the snapshot lock. Noop when `store` is `None` (memory mode). Shared by
/// `ApiGatewayV2Service::save_snapshot` and the CloudFormation provisioner's
/// post-provision persist hook so both route through the same
/// serialize-and-write path.
pub async fn save_apigatewayv2_snapshot(
    state: &SharedApiGatewayV2State,
    store: Option<Arc<dyn SnapshotStore>>,
    lock: &AsyncMutex<()>,
) {
    let Some(store) = store else {
        return;
    };
    let _guard = lock.lock().await;
    let snapshot = ApiGatewayV2Snapshot {
        schema_version: APIGATEWAYV2_SNAPSHOT_SCHEMA_VERSION,
        state: None,
        accounts: Some(state.read().clone()),
    };
    let join = tokio::task::spawn_blocking(move || -> std::io::Result<()> {
        let bytes = serde_json::to_vec(&snapshot)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
        store.save(&bytes)
    })
    .await;
    match join {
        Ok(Ok(())) => {}
        Ok(Err(err)) => tracing::error!(%err, "failed to write apigatewayv2 snapshot"),
        Err(err) => tracing::error!(%err, "apigatewayv2 snapshot task panicked"),
    }
}

#[async_trait]
impl AwsService for ApiGatewayV2Service {
    fn service_name(&self) -> &str {
        "apigateway"
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        // Check if this is a management API request or an execute API request
        // Management API: /v2/* (apis, domainnames, vpclinks, routingrules,
        // tags, portals, portalproducts)
        // Execute API: /{stage}/{path}
        if req.path_segments.first().map(|s| s.as_str()) == Some("v2") {
            return self.handle_management_api(req).await;
        }

        // Execute API
        self.handle_execute_api(req).await
    }

    fn supported_actions(&self) -> &[&str] {
        SUPPORTED
    }
}

impl ApiGatewayV2Service {
    fn update_api(
        &self,
        req: &AwsRequest,
        api_id: Option<&str>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let api_id = api_id.ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "BadRequestException",
                "API ID is required",
            )
        })?;

        let body = req.json_body();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        let api = state.apis.get_mut(api_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                format!("API not found: {}", api_id),
            )
        })?;

        if let Some(name) = body["name"].as_str() {
            api.name = name.to_string();
        }

        if let Some(description) = body["description"].as_str() {
            api.description = Some(description.to_string());
        }

        if let Some(cors) = body.get("corsConfiguration") {
            api.cors_configuration = Some(serde_json::from_value(cors.clone()).map_err(|e| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    format!("Invalid corsConfiguration: {}", e),
                )
            })?);
        }

        // Previously dropped (bug-hunt 2026-06-24, 1.11): WebSocket route /
        // API-key selection expressions, IP address type, and the
        // execute-api-endpoint toggle could not be changed.
        if let Some(v) = body["routeSelectionExpression"].as_str() {
            api.route_selection_expression = v.to_string();
        }
        if let Some(v) = body["apiKeySelectionExpression"].as_str() {
            api.api_key_selection_expression = v.to_string();
        }
        if let Some(v) = body["ipAddressType"].as_str() {
            api.ip_address_type = v.to_string();
        }
        if let Some(b) = body["disableExecuteApiEndpoint"].as_bool() {
            api.disable_execute_api_endpoint = b;
        }
        if let Some(v) = body["version"].as_str() {
            api.version = Some(v.to_string());
        }

        Ok(AwsResponse::ok_json(json!(api)))
    }
}

// ─── WAFv2 inspection helpers ─────────────────────────────────────

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
    resp.content_type = "application/json".to_string();
    Some(resp)
}

/// Parse an API Gateway v2 identity-source expression and extract
/// the corresponding value from the request.
fn extract_identity_source_value(req: &AwsRequest, source: &str) -> Option<String> {
    if let Some(header_name) = source.strip_prefix("$request.header.") {
        req.headers
            .get(header_name)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string())
    } else if let Some(param_name) = source.strip_prefix("$request.querystring.") {
        req.query_params.get(param_name).cloned()
    } else {
        None
    }
}

/// Map a Cognito issuer URL (`https://cognito-idp.<region>.amazonaws.com/<pool-id>`)
/// to the corresponding user-pool ARN.
fn issuer_to_pool_arn(account_id: &str, region: &str, issuer: &str) -> Option<String> {
    let pool_id = issuer.rsplit_once('/')?.1;
    Some(format!(
        "arn:aws:cognito-idp:{}:{}:userpool/{}",
        region, account_id, pool_id
    ))
}

/// Pull a Lambda function ARN out of an `authorizerUri` value.
fn extract_lambda_arn(uri: &str) -> Option<String> {
    // Expected: arn:aws:apigateway:<region>:lambda:path/2015-03-31/functions/<arn>/invocations
    let suffix = uri.strip_prefix("arn:aws:apigateway:")?;
    let rest = suffix.split_once("lambda:path/2015-03-31/functions/")?.1;
    let arn = rest.strip_suffix("/invocations")?;
    Some(arn.to_string())
}

/// Build the method ARN used in Lambda-authorizer policy documents.
/// AWS format: `arn:aws:execute-api:<region>:<account-id>:<api-id>/<stage>/<method>/<path>`.
/// `stage` is passed in explicitly because the execute-api path
/// (`/{stage}/{path}`) and the custom-domain path
/// (`/{base-path}/{path}` -> resolved stage) need different sources of
/// truth — falling back to `req.path_segments[0]` for custom domains
/// would surface the route's first path segment as the stage.
fn build_method_arn(req: &AwsRequest, api_id: &str, stage: &str) -> String {
    let segments = if req.path_segments.first().map(|s| s.as_str()) == Some(stage) {
        // execute-api: stage IS path_segments[0], so the remaining
        // segments are the resource path.
        req.path_segments
            .iter()
            .skip(1)
            .map(|s| s.as_str())
            .collect::<Vec<_>>()
    } else {
        // custom-domain: path_segments already contains only the
        // resource path; stage was stripped before this fn ran.
        req.path_segments
            .iter()
            .map(|s| s.as_str())
            .collect::<Vec<_>>()
    };
    let path = segments.join("/");
    format!(
        "arn:aws:execute-api:{}:{}:{}/{}/{}/{}",
        req.region,
        req.account_id,
        api_id,
        stage,
        req.method.as_str(),
        path
    )
}

/// Walk `policyDocument.Statement` and resolve to a single Allow/Deny
/// effect. Multiple matching Allow statements collapse to Allow; any
/// Deny short-circuits to Deny.
fn parse_policy_effect(response: &serde_json::Value, method_arn: &str) -> crate::state::AuthEffect {
    let Some(stmts) = response
        .get("policyDocument")
        .and_then(|p| p.get("Statement"))
        .and_then(|s| s.as_array())
    else {
        return crate::state::AuthEffect::Deny;
    };
    let mut allow = false;
    for stmt in stmts {
        let effect = stmt.get("Effect").and_then(|v| v.as_str()).unwrap_or("");
        let matches = match stmt.get("Resource") {
            Some(serde_json::Value::String(s)) => arn_matches(s, method_arn),
            Some(serde_json::Value::Array(arr)) => arr
                .iter()
                .filter_map(|v| v.as_str())
                .any(|s| arn_matches(s, method_arn)),
            _ => false,
        };
        if !matches {
            continue;
        }
        match effect {
            "Deny" => return crate::state::AuthEffect::Deny,
            "Allow" => allow = true,
            _ => {}
        }
    }
    if allow {
        crate::state::AuthEffect::Allow
    } else {
        crate::state::AuthEffect::Deny
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
            (Some('*'), _) => {
                p_chars.next();
                // Try matching * against 0, 1, 2, ... chars in target.
                // Simple recursive backtracking (patterns are tiny).
                let rest_p: String = p_chars.collect();
                let mut rest_t: String = t_chars.collect();
                loop {
                    if arn_matches(&rest_p, &rest_t) {
                        return true;
                    }
                    if rest_t.is_empty() {
                        return false;
                    }
                    rest_t.remove(0);
                }
            }
            (Some('?'), Some(_)) => {
                p_chars.next();
                t_chars.next();
            }
            (Some(pc), Some(tc)) if pc == tc => {
                p_chars.next();
                t_chars.next();
            }
            _ => return false,
        }
    }
}

/// Replace `${stageVariables.<name>}` placeholders in `uri` with values
/// from `stage_variables`. Unknown names are left as-is.
fn substitute_stage_variables(uri: &str, stage_variables: &BTreeMap<String, String>) -> String {
    let mut result = uri.to_string();
    for (k, v) in stage_variables {
        let placeholder = format!("${{stageVariables.{}}}", k);
        result = result.replace(&placeholder, v);
    }
    result
}

/// When the request Host header matches a custom domain name, look up
/// the ApiMapping for the base path and return `(api_id, stage_name,
/// remaining_path_segments, resource_path)`.
///
/// The returned `remaining_path_segments` is what should be used for
/// route matching (the base path prefix is stripped). `resource_path`
/// is the display path recorded in request history.
fn resolve_custom_domain(
    req: &AwsRequest,
    state: &ApiGatewayV2State,
) -> Option<(String, String, Vec<String>, String)> {
    let host = req.headers.get("host").and_then(|v| v.to_str().ok())?;

    // Only consider hosts that don't look like the default execute-api endpoint.
    if host.contains(".execute-api.") {
        return None;
    }

    let domain = state.domain_names.get(host)?;
    let domain_name = domain["DomainName"].as_str()?;

    let mappings = state.api_mappings.get(domain_name)?;
    if mappings.is_empty() {
        return None;
    }

    // Find the mapping whose ApiMappingKey matches the longest prefix of the path.
    let raw_path = &req.raw_path;
    let mut best: Option<(&str, &str, Vec<String>, String)> = None;

    for mapping in mappings.values() {
        let key = mapping["ApiMappingKey"].as_str().unwrap_or("");
        let api_id = mapping["ApiId"].as_str()?;
        let stage = mapping["Stage"].as_str()?;

        let (stripped_path, remaining) = if key.is_empty() {
            (raw_path.to_string(), raw_path.to_string())
        } else {
            let prefix = format!("/{}/", key);
            let prefix_root = format!("/{}", key);
            if *raw_path == *prefix_root || raw_path.starts_with(&prefix) {
                let rest = &raw_path[prefix_root.len()..];
                (rest.to_string(), rest.to_string())
            } else {
                continue;
            }
        };

        let segs: Vec<String> = if stripped_path.is_empty() || stripped_path == "/" {
            vec![]
        } else {
            stripped_path
                .split('/')
                .skip(1)
                .map(|s| s.to_string())
                .collect()
        };

        let best_key_len = best.as_ref().map(|(k, _, _, _)| k.len()).unwrap_or(0);
        if key.len() >= best_key_len {
            best = Some((api_id, stage, segs, remaining));
        }
    }

    best.map(|(api_id, stage, segs, resource_path)| {
        // Custom-domain strip can leave an empty resource_path when the
        // request exactly matches the base path. Normalize to "/" so
        // downstream route matching sees a valid path instead of an
        // empty string.
        let resource_path = if resource_path.is_empty() {
            "/".to_string()
        } else {
            resource_path
        };
        (api_id.to_string(), stage.to_string(), segs, resource_path)
    })
}

/// Returns true when `uri` is a Lambda function ARN.
fn is_lambda_arn(uri: &str) -> bool {
    uri.starts_with("arn:aws:lambda:") && uri.contains(":function:")
}

/// Dispatch a non-Lambda AWS_PROXY integration to the appropriate
/// AWS service via the delivery bus. Supported targets:
///   - SQS queue ARN
///   - SNS topic ARN
///   - StepFunctions state-machine ARN
fn dispatch_aws_service_integration(
    delivery: &DeliveryBus,
    integration_uri: &str,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    if integration_uri.starts_with("arn:aws:sqs:") {
        let message = String::from_utf8_lossy(&req.body);
        delivery.send_to_sqs(integration_uri, &message, &std::collections::HashMap::new());
        return Ok(AwsResponse::ok_json(json!({
            "statusCode": 200,
            "body": json!({"MessageId": uuid::Uuid::new_v4().to_string()}).to_string()
        })));
    }

    if integration_uri.starts_with("arn:aws:sns:") {
        let message = String::from_utf8_lossy(&req.body);
        let subject = req
            .headers
            .get("x-amz-sns-subject")
            .and_then(|v| v.to_str().ok());
        delivery.publish_to_sns(integration_uri, &message, subject);
        return Ok(AwsResponse::ok_json(json!({
            "statusCode": 200,
            "body": json!({"MessageId": uuid::Uuid::new_v4().to_string()}).to_string()
        })));
    }

    if integration_uri.starts_with("arn:aws:states:") && integration_uri.contains(":stateMachine:")
    {
        let input = String::from_utf8_lossy(&req.body);
        let execution_name = format!("apigw-{}-{}", req.request_id, uuid::Uuid::new_v4().simple());
        delivery.start_stepfunctions_execution(integration_uri, &input);
        return Ok(AwsResponse::ok_json(json!({
            "statusCode": 200,
            "body": json!({
                "executionArn": format!("{}/execution/{}", integration_uri, execution_name),
                "startDate": chrono::Utc::now().to_rfc3339()
            }).to_string()
        })));
    }

    Err(AwsServiceError::aws_error(
        StatusCode::NOT_IMPLEMENTED,
        "NotImplemented",
        format!(
            "AWS_PROXY integration target not supported: {}",
            integration_uri
        ),
    ))
}

#[path = "../service_helpers.rs"]
mod service_helpers;
pub(crate) use service_helpers::*;

/// Normalize the JSON body of an API Gateway v2 management request so all
/// downstream handlers can read camelCase keys (`name`, `protocolType`, …)
/// regardless of the wire shape sent by the client. Smithy's `@jsonName`
/// trait on every member of every operation in this service is just
/// "PascalCase with the first letter lowercased," so we recursively
/// lowercase the first ASCII letter of each object key. Already-camelCase
/// keys are unchanged.
///
/// Only the request body is rewritten; query params and path segments
/// are left alone (they're not affected by `@jsonName`).
fn normalize_request_body_keys(mut req: AwsRequest) -> AwsRequest {
    if req.body.is_empty() {
        return req;
    }
    let parsed: serde_json::Value = match serde_json::from_slice(&req.body) {
        Ok(v) => v,
        Err(_) => return req,
    };
    let normalized = lowercase_first_letter_keys(parsed);
    if let Ok(bytes) = serde_json::to_vec(&normalized) {
        req.body = bytes::Bytes::from(bytes);
    }
    req
}

/// Members whose values are maps with *arbitrary, user-supplied keys*
/// (`map<string, ...>` in the Smithy model). The `@jsonName` first-letter
/// normalization applies to modeled member names only — it must NOT rewrite
/// the keys of these maps (content types, request-parameter expressions,
/// route keys, tag names, ...), or a request like
/// `requestModels: {"application/json": "M"}` would gain a corrupt
/// `Application/json` sibling. Their *values* are still normalized so that a
/// nested modeled struct (e.g. `requestParameters`'
/// `ParameterConstraints.required`) keeps working in either case.
const ARBITRARY_KEY_MAPS: &[&str] = &[
    "requestModels",
    "responseModels",
    "requestParameters",
    "responseParameters",
    "requestTemplates",
    "responseTemplates",
    "routeSettings",
    "stageVariables",
    "tags",
    "variables",
];

/// Whether `field` (in either case) names an arbitrary-key map member.
fn is_arbitrary_key_map(field: &str) -> bool {
    let mut chars = field.chars();
    let camel = match chars.next() {
        Some(c) => c.to_ascii_lowercase().to_string() + chars.as_str(),
        None => return false,
    };
    ARBITRARY_KEY_MAPS.contains(&camel.as_str())
}

/// Walk the body and, for every *modeled* map key that starts with an
/// uppercase ASCII letter, insert a sibling entry whose first letter is
/// lowercased (and vice versa for camelCase keys). This is purely additive —
/// the original key is preserved — so handlers that read either case keep
/// working through the same body. Keys of arbitrary-key maps (see
/// `ARBITRARY_KEY_MAPS`) are left exactly as sent.
fn lowercase_first_letter_keys(v: serde_json::Value) -> serde_json::Value {
    normalize_member_keys(v, false)
}

/// `keys_are_arbitrary` is true when the current object's own keys are
/// user-supplied (a `map<string, ...>` value) and must be preserved verbatim.
fn normalize_member_keys(v: serde_json::Value, keys_are_arbitrary: bool) -> serde_json::Value {
    match v {
        serde_json::Value::Object(map) => {
            let mut out = serde_json::Map::with_capacity(map.len() * 2);
            for (k, val) in map {
                // A child is an arbitrary-key map iff *this* member name says
                // so — regardless of the current level being arbitrary.
                let child_arbitrary = is_arbitrary_key_map(&k);
                let normalized = normalize_member_keys(val, child_arbitrary);
                if !keys_are_arbitrary {
                    let alt_key = swap_first_letter_case(&k);
                    if alt_key != k && !out.contains_key(&alt_key) {
                        out.insert(alt_key, normalized.clone());
                    }
                }
                out.insert(k, normalized);
            }
            serde_json::Value::Object(out)
        }
        serde_json::Value::Array(items) => serde_json::Value::Array(
            items
                .into_iter()
                .map(|x| normalize_member_keys(x, false))
                .collect(),
        ),
        other => other,
    }
}

/// Parse a JSON array of strings into `Vec<String>`, or `None` when the
/// value is not an array.
pub(crate) fn parse_string_array(v: &serde_json::Value) -> Option<Vec<String>> {
    v.as_array().map(|arr| {
        arr.iter()
            .filter_map(|x| x.as_str().map(|s| s.to_string()))
            .collect()
    })
}

/// Parse a JSON object of `string -> string` into a `BTreeMap`, or `None`
/// when the value is not an object.
pub(crate) fn parse_string_map(v: &serde_json::Value) -> Option<BTreeMap<String, String>> {
    v.as_object().map(|obj| {
        obj.iter()
            .filter_map(|(k, val)| val.as_str().map(|s| (k.clone(), s.to_string())))
            .collect()
    })
}

/// Parse a JSON object of `string -> Value` into a `BTreeMap`, preserving
/// each value verbatim, or `None` when the value is not an object.
pub(crate) fn parse_value_map(
    v: &serde_json::Value,
) -> Option<BTreeMap<String, serde_json::Value>> {
    v.as_object().map(|obj| {
        obj.iter()
            .map(|(k, val)| (k.clone(), val.clone()))
            .collect()
    })
}

fn swap_first_letter_case(s: &str) -> String {
    let mut chars = s.chars();
    match chars.next() {
        Some(c) if c.is_ascii_uppercase() => {
            let mut out = String::with_capacity(s.len());
            out.push(c.to_ascii_lowercase());
            out.extend(chars);
            out
        }
        Some(c) if c.is_ascii_lowercase() => {
            let mut out = String::with_capacity(s.len());
            out.push(c.to_ascii_uppercase());
            out.extend(chars);
            out
        }
        _ => s.to_string(),
    }
}

#[cfg(test)]
#[path = "../service_tests.rs"]
mod tests;
