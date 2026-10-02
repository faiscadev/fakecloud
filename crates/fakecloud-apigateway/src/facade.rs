//! Combined v1+v2 facade.
//!
//! Real AWS uses one SigV4 service identifier (`apigateway`) for both
//! API Gateway v1 (REST APIs, `/restapis/...`) and API Gateway v2
//! (HTTP APIs, `/v2/...`), distinguished only by URL. fakecloud's
//! service registry is keyed by SigV4 service name, so we wrap both
//! handlers behind a single registered `"apigateway"` entry that
//! routes by URL prefix.
//!
//! Deployed APIs are invoked on the execute-api host
//! (`{api-id}.execute-api.<region>.amazonaws.com/{stage}/{path}`). Clients
//! that can't set that host reach the same data plane through path-style
//! invocation URLs, which the facade rewrites to the canonical form (see
//! [`ApiGatewayFacade::rewrite_path_style_invocation`]):
//!
//! - `/restapis/{api-id}/{stage}/_user_request_/{path}` (LocalStack's
//!   long-standing form),
//! - `/_aws/execute-api/{api-id}/{stage}/{path}` (LocalStack's current form),
//! - `/restapis/{api-id}/{stage}/{path}` when it is not a control-plane route
//!   and `{stage}` is deployed on that REST API.

use async_trait::async_trait;
use std::collections::HashSet;
use std::sync::Arc;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};

use crate::ApiGatewayService;

/// Marker segment of LocalStack's path-style invocation URL
/// `/restapis/{api-id}/{stage}/_user_request_/{path}`.
const USER_REQUEST_MARKER: &str = "_user_request_";

/// Looks up whether any API Gateway v2 API in `account` has a stage named
/// `stage`. Lets the facade pick v1 for a plain-host data-plane request whose
/// stage only a REST API defines.
pub type V2StageLookup = Arc<dyn Fn(&str, &str) -> bool + Send + Sync>;

/// Looks up whether `host` is an API Gateway v2 custom domain name in
/// `account`. Custom-domain traffic is routed by its mappings, never by
/// guessing from the first path segment.
pub type V2DomainLookup = Arc<dyn Fn(&str, &str) -> bool + Send + Sync>;

const V1_CONTROL_PREFIXES: &[&str] = &[
    "restapis",
    "apikeys",
    "usageplans",
    "vpclinks",
    "domainnames",
    "domainnameaccessassociations",
    "rejectdomainnameaccessassociations",
    "clientcertificates",
    "sdktypes",
    "tags",
    "account",
];

pub struct ApiGatewayFacade {
    v1: Arc<ApiGatewayService>,
    v2: Arc<dyn AwsService>,
    v2_has_stage: Option<V2StageLookup>,
    v2_has_domain: Option<V2DomainLookup>,
    actions: Vec<&'static str>,
}

impl ApiGatewayFacade {
    pub fn new(v1: Arc<ApiGatewayService>, v2: Arc<dyn AwsService>) -> Self {
        // Combine the two services' supported_actions slices into one
        // de-duplicated list and leak it — the conformance audit
        // already inspects the v1 and v2 crate sources separately, so
        // this slice is only used by runtime introspection.
        let mut set: HashSet<&str> = HashSet::new();
        for &a in v1.supported_actions() {
            set.insert(a);
        }
        for &a in v2.supported_actions() {
            set.insert(a);
        }
        let mut sorted: Vec<&str> = set.into_iter().collect();
        sorted.sort();
        let leaked: Vec<&'static str> = sorted
            .into_iter()
            .map(|s| Box::leak(s.to_string().into_boxed_str()) as &'static str)
            .collect();
        Self {
            v1,
            v2,
            v2_has_stage: None,
            v2_has_domain: None,
            actions: leaked,
        }
    }

    /// Give the facade a view of v2 stages so a plain-host data-plane request
    /// (`http://localhost:4566/{stage}/{path}`) reaches a REST API when only
    /// v1 defines that stage.
    pub fn with_v2_stage_lookup(mut self, lookup: V2StageLookup) -> Self {
        self.v2_has_stage = Some(lookup);
        self
    }

    /// Give the facade a view of v2 custom domain names, so traffic for an
    /// HTTP API custom domain is never captured by a REST API stage name.
    pub fn with_v2_domain_lookup(mut self, lookup: V2DomainLookup) -> Self {
        self.v2_has_domain = Some(lookup);
        self
    }

    /// Whether the `Host` names a v1 custom domain (its name or its regional
    /// domain name), which the v1 data plane resolves via base path mappings.
    fn host_is_v1_custom_domain(&self, req: &AwsRequest) -> bool {
        let Some(host) = fakecloud_core::protocol::normalized_host_from_headers(&req.headers)
        else {
            return false;
        };
        let accounts = self.v1.state_handle().read();
        let Some(state) = accounts.get(&req.account_id) else {
            return false;
        };
        state
            .domain_names
            .iter()
            .any(|(name, value)| crate::data_plane::domain_matches_host(name, value, &host))
    }

    fn host_is_v2_custom_domain(&self, req: &AwsRequest) -> bool {
        let Some(lookup) = self.v2_has_domain.as_ref() else {
            return false;
        };
        fakecloud_core::protocol::normalized_host_from_headers(&req.headers)
            .is_some_and(|h| lookup(&req.account_id, &h))
    }

    /// Rewrite a path-style invocation URL to the canonical execute-api form:
    /// the API id moves into the `Host` header and the path becomes
    /// `/{stage}/{path}`, exactly as a request to the execute-api endpoint
    /// arrives. Returns whether the request was rewritten.
    fn rewrite_path_style_invocation(&self, req: &mut AwsRequest) -> bool {
        let raw = fakecloud_core::path::split_raw_path_segments(&req.raw_path);
        let seg = |i: usize| raw.get(i).map(String::as_str);
        // (api id, stage, number of leading segments the invocation prefix
        // occupies; the stage is the last of them except for `_user_request_`).
        let (api_id, stage, prefix_len) = match (seg(0), seg(1), seg(2), seg(3)) {
            (Some("_aws"), Some("execute-api"), Some(api), Some(stage)) => (api, stage, 4),
            (Some("restapis"), Some(api), Some(stage), Some(USER_REQUEST_MARKER)) => {
                (api, stage, 4)
            }
            (Some("restapis"), Some(api), Some(stage), _)
                if crate::dispatch::resolve(&req.method, &req.path_segments, &req.query_params)
                    .is_none()
                    && self.v1_stage_exists(&req.account_id, api, stage) =>
            {
                (api, stage, 3)
            }
            _ => return false,
        };
        let (api_id, stage) = (api_id.to_string(), stage.to_string());
        // Keep the remainder of the wire path verbatim (encoding, trailing
        // slash); fall back to re-joining segments for a path with empty
        // segments, which the prefix match can't anchor on.
        let prefix = format!("/{}", raw[..prefix_len].join("/"));
        let tail = match req.raw_path.strip_prefix(&prefix) {
            Some(tail) if tail.is_empty() || tail.starts_with('/') => tail.to_string(),
            _ => raw[prefix_len..]
                .iter()
                .map(|s| format!("/{s}"))
                .collect::<String>(),
        };
        req.raw_path = format!("/{stage}{tail}");
        req.path_segments = fakecloud_core::path::split_path_segments(&req.raw_path);
        let host = format!("{api_id}.execute-api.{}.amazonaws.com", req.region);
        if let Ok(value) = http::HeaderValue::from_str(&host) {
            req.headers.insert(http::header::HOST, value);
        }
        true
    }

    fn v1_stage_exists(&self, account_id: &str, api_id: &str, stage: &str) -> bool {
        let accounts = self.v1.state_handle().read();
        accounts
            .get(account_id)
            .and_then(|state| state.stages.get(api_id))
            .is_some_and(|stages| stages.contains_key(stage))
    }

    /// A plain-host data-plane request (`/{stage}/{path}` with no execute-api
    /// host to name the API) belongs to v1 when a REST API defines that stage
    /// and no v2 API does. When both do, v2 keeps the request, as before.
    fn plain_host_stage_owned_by_v1(&self, req: &AwsRequest) -> bool {
        // An execute-api host names the API itself, and a custom domain is
        // routed by its own mappings: neither is a stage-name guess.
        if host_is_execute_api(req)
            || self.host_is_v1_custom_domain(req)
            || self.host_is_v2_custom_domain(req)
        {
            return false;
        }
        let Some(stage) = req.path_segments.first() else {
            return false;
        };
        let v1_has = {
            let accounts = self.v1.state_handle().read();
            accounts
                .get(&req.account_id)
                .is_some_and(|state| state.stages.values().any(|s| s.contains_key(stage)))
        };
        if !v1_has {
            return false;
        }
        let v2_has = self
            .v2_has_stage
            .as_ref()
            .is_some_and(|lookup| lookup(&req.account_id, stage));
        !v2_has
    }

    fn route_v1(req: &AwsRequest) -> bool {
        req.path_segments
            .first()
            .map(|s| V1_CONTROL_PREFIXES.contains(&s.as_str()))
            .unwrap_or(false)
    }

    fn route_v2_control(req: &AwsRequest) -> bool {
        req.path_segments
            .first()
            .map(|s| s == "v2")
            .unwrap_or(false)
    }

    /// Returns true when the data-plane request targets an API ID that
    /// exists in v1 state. AWS keys the execute-api host on the API ID
    /// (`{api-id}.execute-api.<region>.amazonaws.com`); fakecloud
    /// surfaces it via the `Host` header. Stage-name lookups would
    /// misroute traffic when v1 and v2 share a stage name.
    fn data_plane_owned_by_v1(&self, req: &AwsRequest) -> bool {
        let Some(host) = req.headers.get("host").and_then(|v| v.to_str().ok()) else {
            return false;
        };
        let Some(api_id) = host.split('.').next() else {
            return false;
        };
        if api_id.is_empty() {
            return false;
        }
        let accounts = self.v1.state_handle().read();
        let Some(state) = accounts.get(&req.account_id) else {
            return false;
        };
        state.apis.contains_key(api_id)
    }
}

/// Whether the request's `Host` is an execute-api endpoint
/// (`{api-id}.execute-api...`), which names the API being invoked.
fn host_is_execute_api(req: &AwsRequest) -> bool {
    req.headers
        .get("host")
        .and_then(|v| v.to_str().ok())
        .and_then(|h| h.split('.').nth(1))
        .is_some_and(|label| label == "execute-api")
}

#[async_trait]
impl AwsService for ApiGatewayFacade {
    fn service_name(&self) -> &str {
        "apigateway"
    }

    fn supported_actions(&self) -> &[&str] {
        &self.actions
    }

    async fn handle(&self, mut req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        // An execute-api host (or a path-style invocation URL, rewritten to
        // one) is always data plane: the control plane lives on the
        // `apigateway` endpoint, so a stage named like a control-plane
        // collection (`account`, `tags`, ...) must not be read as one.
        if self.rewrite_path_style_invocation(&mut req) || host_is_execute_api(&req) {
            return if self.data_plane_owned_by_v1(&req) {
                self.v1.handle_data_plane(req).await
            } else {
                self.v2.handle(req).await
            };
        }
        if Self::route_v2_control(&req) {
            return self.v2.handle(req).await;
        }
        if Self::route_v1(&req) {
            return self.v1.handle(req).await;
        }
        if self.data_plane_owned_by_v1(&req) {
            return self.v1.handle(req).await;
        }
        // A REST API custom domain is served by v1's base path mappings
        // (unless an HTTP API claims the same domain name).
        if self.host_is_v1_custom_domain(&req) && !self.host_is_v2_custom_domain(&req) {
            return self.v1.handle_data_plane(req).await;
        }
        if self.plain_host_stage_owned_by_v1(&req) {
            return self.v1.handle_data_plane(req).await;
        }
        // Default fallback for unsigned execute calls — v2 was the
        // original handler and remains the default until v1 has
        // matching state.
        self.v2.handle(req).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{ApiGatewayState, SharedApiGatewayState, Stage};
    use bytes::Bytes;
    use chrono::Utc;
    use fakecloud_core::multi_account::MultiAccountState;
    use http::{HeaderMap, Method};
    use std::collections::{BTreeMap, HashMap};

    const ACCOUNT: &str = "123456789012";
    const REGION: &str = "us-east-1";
    const API: &str = "abc123";

    struct NoV2;

    #[async_trait]
    impl AwsService for NoV2 {
        fn service_name(&self) -> &str {
            "apigatewayv2"
        }
        fn supported_actions(&self) -> &[&str] {
            &[]
        }
        async fn handle(&self, _req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
            unreachable!("not used by these tests")
        }
    }

    fn stage(name: &str) -> Stage {
        Stage {
            stage_name: name.to_string(),
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
        }
    }

    /// A facade whose v1 state has REST API `abc123` deployed to `prod`.
    fn facade() -> ApiGatewayFacade {
        let state: SharedApiGatewayState =
            Arc::new(parking_lot::RwLock::new(
                MultiAccountState::<ApiGatewayState>::new(ACCOUNT, REGION, ""),
            ));
        state
            .write()
            .get_or_create(ACCOUNT)
            .stages
            .entry(API.to_string())
            .or_default()
            .insert("prod".to_string(), stage("prod"));
        ApiGatewayFacade::new(Arc::new(ApiGatewayService::new(state)), Arc::new(NoV2))
    }

    fn request(method: Method, raw_path: &str) -> AwsRequest {
        let mut headers = HeaderMap::new();
        headers.insert("host", "localhost:4566".parse().unwrap());
        AwsRequest {
            service: "apigateway".to_string(),
            action: String::new(),
            method,
            raw_path: raw_path.to_string(),
            raw_query: String::new(),
            path_segments: fakecloud_core::path::split_path_segments(raw_path),
            query_params: HashMap::new(),
            headers,
            body: Bytes::new(),
            body_stream: parking_lot::Mutex::new(None),
            account_id: ACCOUNT.to_string(),
            region: REGION.to_string(),
            request_id: "rid".to_string(),
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        }
    }

    fn host(req: &AwsRequest) -> &str {
        req.headers.get("host").unwrap().to_str().unwrap()
    }

    #[test]
    fn user_request_url_is_rewritten_to_execute_api_form() {
        let f = facade();
        let mut req = request(
            Method::GET,
            "/restapis/abc123/prod/_user_request_/items/a%2Fb/",
        );
        assert!(f.rewrite_path_style_invocation(&mut req));
        // The tail is kept verbatim: encoding and trailing slash.
        assert_eq!(req.raw_path, "/prod/items/a%2Fb/");
        assert_eq!(req.path_segments, vec!["prod", "items", "a/b"]);
        assert_eq!(host(&req), "abc123.execute-api.us-east-1.amazonaws.com");
        assert!(host_is_execute_api(&req));
    }

    #[test]
    fn aws_execute_api_url_is_rewritten() {
        let f = facade();
        let mut req = request(Method::POST, "/_aws/execute-api/abc123/prod");
        assert!(f.rewrite_path_style_invocation(&mut req));
        assert_eq!(req.raw_path, "/prod");
        assert_eq!(host(&req), "abc123.execute-api.us-east-1.amazonaws.com");
    }

    #[test]
    fn bare_restapis_url_needs_a_deployed_stage_and_no_control_route() {
        let f = facade();
        let mut req = request(Method::GET, "/restapis/abc123/prod/items");
        assert!(f.rewrite_path_style_invocation(&mut req));
        assert_eq!(req.raw_path, "/prod/items");

        // No such stage: left for the control plane (which 404s).
        let mut req = request(Method::GET, "/restapis/abc123/dev/items");
        assert!(!f.rewrite_path_style_invocation(&mut req));
        assert_eq!(req.raw_path, "/restapis/abc123/dev/items");

        // Control-plane routes always win.
        for (method, path) in [
            (Method::GET, "/restapis/abc123/stages/prod"),
            (Method::GET, "/restapis/abc123/resources"),
            (Method::GET, "/restapis/abc123"),
            (Method::POST, "/restapis/abc123/deployments"),
        ] {
            let mut req = request(method, path);
            assert!(!f.rewrite_path_style_invocation(&mut req), "{path}");
        }
    }

    #[test]
    fn plain_host_stage_routes_to_v1_only_when_unambiguous() {
        let f = facade();
        let req = request(Method::GET, "/prod/items");
        // No v2 lookup wired: v1 is the only owner of `prod`.
        assert!(f.plain_host_stage_owned_by_v1(&req));
        // Unknown stage stays with v2.
        assert!(!f.plain_host_stage_owned_by_v1(&request(Method::GET, "/dev/items")));

        let f = facade().with_v2_stage_lookup(Arc::new(|_, stage| stage == "prod"));
        assert!(!f.plain_host_stage_owned_by_v1(&req), "v2 shares the stage");

        let f = facade().with_v2_stage_lookup(Arc::new(|_, _| false));
        assert!(f.plain_host_stage_owned_by_v1(&req));
        // An execute-api host names the API itself; no stage guessing.
        // A v2 custom domain routes by its API mappings, not by stage name.
        let f = facade()
            .with_v2_stage_lookup(Arc::new(|_, _| false))
            .with_v2_domain_lookup(Arc::new(|_, host| host == "api.example.com"));
        let mut custom = request(Method::GET, "/prod/items");
        custom
            .headers
            .insert("host", "api.example.com:4566".parse().unwrap());
        assert!(!f.plain_host_stage_owned_by_v1(&custom));
        assert!(f.host_is_v2_custom_domain(&custom));
        assert!(f.plain_host_stage_owned_by_v1(&req));
        let mut pinned = request(Method::GET, "/prod/items");
        pinned.headers.insert(
            "host",
            "zzz.execute-api.us-east-1.amazonaws.com".parse().unwrap(),
        );
        assert!(!f.plain_host_stage_owned_by_v1(&pinned));
    }

    #[test]
    fn v1_custom_domain_is_recognized_and_not_stage_guessed() {
        let f = facade();
        f.v1.state_handle()
            .write()
            .get_or_create(ACCOUNT)
            .domain_names
            .insert(
                "rest.example.com".to_string(),
                serde_json::json!({"regionalDomainName": "d-abc.execute-api.us-east-1.amazonaws.com"}),
            );
        let mut req = request(Method::GET, "/prod/items");
        req.headers
            .insert("host", "rest.example.com:4566".parse().unwrap());
        assert!(f.host_is_v1_custom_domain(&req));
        assert!(!f.plain_host_stage_owned_by_v1(&req));
        assert!(!f.host_is_v1_custom_domain(&request(Method::GET, "/prod/items")));
    }
}
