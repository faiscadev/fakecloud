//! API Gateway v1 (REST APIs) service implementation.

use async_trait::async_trait;
use http::{Method, StatusCode};
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use tokio::sync::Mutex as AsyncMutex;

use fakecloud_core::delivery::DeliveryBus;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};
use fakecloud_persistence::SnapshotStore;

use crate::dispatch::{resolve, ResolvedAction};
use crate::state::{
    make_id, ApiGatewaySnapshot, ApiGatewayState, ApiKey, Authorizer, Deployment, Integration,
    Method as ApiMethod, Model, Resource, RestApi, SharedApiGatewayState, Stage, UsagePlan,
    APIGATEWAY_SNAPSHOT_SCHEMA_VERSION,
};

pub const SUPPORTED_ACTIONS: &[&str] = &[
    "GetAccount",
    "UpdateAccount",
    "CreateRestApi",
    "GetRestApi",
    "GetRestApis",
    "DeleteRestApi",
    "UpdateRestApi",
    "PutRestApi",
    "ImportRestApi",
    "CreateResource",
    "GetResource",
    "GetResources",
    "DeleteResource",
    "UpdateResource",
    "PutMethod",
    "GetMethod",
    "DeleteMethod",
    "UpdateMethod",
    "PutMethodResponse",
    "GetMethodResponse",
    "DeleteMethodResponse",
    "UpdateMethodResponse",
    "PutIntegration",
    "GetIntegration",
    "DeleteIntegration",
    "UpdateIntegration",
    "PutIntegrationResponse",
    "GetIntegrationResponse",
    "DeleteIntegrationResponse",
    "UpdateIntegrationResponse",
    "TestInvokeMethod",
    "TestInvokeAuthorizer",
    "CreateDeployment",
    "GetDeployment",
    "GetDeployments",
    "DeleteDeployment",
    "UpdateDeployment",
    "CreateStage",
    "GetStage",
    "GetStages",
    "DeleteStage",
    "UpdateStage",
    "FlushStageCache",
    "FlushStageAuthorizersCache",
    "CreateModel",
    "GetModel",
    "GetModels",
    "DeleteModel",
    "UpdateModel",
    "GetModelTemplate",
    "CreateRequestValidator",
    "GetRequestValidator",
    "GetRequestValidators",
    "DeleteRequestValidator",
    "UpdateRequestValidator",
    "CreateAuthorizer",
    "GetAuthorizer",
    "GetAuthorizers",
    "DeleteAuthorizer",
    "UpdateAuthorizer",
    "CreateApiKey",
    "GetApiKey",
    "GetApiKeys",
    "DeleteApiKey",
    "UpdateApiKey",
    "CreateUsagePlan",
    "GetUsagePlan",
    "GetUsagePlans",
    "DeleteUsagePlan",
    "UpdateUsagePlan",
    "CreateUsagePlanKey",
    "GetUsagePlanKey",
    "GetUsagePlanKeys",
    "DeleteUsagePlanKey",
    "GetUsage",
    "UpdateUsage",
    "CreateVpcLink",
    "GetVpcLink",
    "GetVpcLinks",
    "DeleteVpcLink",
    "UpdateVpcLink",
    "CreateDomainName",
    "GetDomainName",
    "GetDomainNames",
    "DeleteDomainName",
    "UpdateDomainName",
    "CreateDomainNameAccessAssociation",
    "DeleteDomainNameAccessAssociation",
    "GetDomainNameAccessAssociations",
    "RejectDomainNameAccessAssociation",
    "ImportApiKeys",
    "ImportDocumentationParts",
    "CreateBasePathMapping",
    "GetBasePathMapping",
    "GetBasePathMappings",
    "DeleteBasePathMapping",
    "UpdateBasePathMapping",
    "GenerateClientCertificate",
    "GetClientCertificate",
    "GetClientCertificates",
    "DeleteClientCertificate",
    "UpdateClientCertificate",
    "CreateDocumentationPart",
    "GetDocumentationPart",
    "GetDocumentationParts",
    "DeleteDocumentationPart",
    "UpdateDocumentationPart",
    "CreateDocumentationVersion",
    "GetDocumentationVersion",
    "GetDocumentationVersions",
    "DeleteDocumentationVersion",
    "UpdateDocumentationVersion",
    "PutGatewayResponse",
    "GetGatewayResponse",
    "GetGatewayResponses",
    "DeleteGatewayResponse",
    "UpdateGatewayResponse",
    "GetExport",
    "GetSdk",
    "GetSdkType",
    "GetSdkTypes",
    "TagResource",
    "UntagResource",
    "GetTags",
];

pub struct ApiGatewayService {
    pub(crate) state: SharedApiGatewayState,
    delivery: Option<Arc<DeliveryBus>>,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<AsyncMutex<()>>,
    /// In-memory throttle + quota counters keyed per
    /// `(account_id, plan_id, key_id)`. Token-bucket and per-period
    /// counters are inherently transient — AWS itself doesn't expose
    /// these to control-plane reads, and quotas reset on a wall-clock
    /// boundary. Restart resetting the buckets is acceptable.
    pub(crate) meters: Arc<parking_lot::Mutex<crate::data_plane::UsageMeters>>,
    /// WAFv2 inspection wiring. When set together with
    /// `waf_rate_limiter`, the data plane evaluates each request
    /// against the WebACL associated with the matched stage's ARN
    /// before invoking the authorizer or integration.
    pub(crate) waf_state: Option<fakecloud_wafv2::SharedWafv2State>,
    pub(crate) waf_rate_limiter: Option<Arc<fakecloud_wafv2::RateLimiter>>,
    /// Per-(WebACL ARN, rule name) Count-action match counter. Keyed
    /// by `"<acl-arn>|<rule-name>"`; exposed via the admin endpoint
    /// for tests and future metrics scraping.
    pub(crate) waf_count_metrics: Arc<parking_lot::Mutex<BTreeMap<String, u64>>>,
    /// ELBv2 state for resolving VPC_LINK target NLB/ALB bound ports.
    pub(crate) elbv2_state: Option<fakecloud_elbv2::SharedElbv2State>,
    /// Deferred-fill handle to the central [`ServiceRegistry`]. Populated
    /// after all services have been registered so AWS direct integrations
    /// can dispatch to other fakecloud service handlers.
    pub(crate) registry:
        Option<Arc<std::sync::OnceLock<Arc<fakecloud_core::registry::ServiceRegistry>>>>,
}

impl ApiGatewayService {
    pub fn new(state: SharedApiGatewayState) -> Self {
        Self {
            state,
            delivery: None,
            snapshot_store: None,
            snapshot_lock: Arc::new(AsyncMutex::new(())),
            meters: Arc::new(parking_lot::Mutex::new(
                crate::data_plane::UsageMeters::default(),
            )),
            waf_state: None,
            waf_rate_limiter: None,
            waf_count_metrics: Arc::new(parking_lot::Mutex::new(BTreeMap::new())),
            elbv2_state: None,
            registry: None,
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

    /// Wire the shared WAFv2 state + rate limiter so the data plane
    /// can evaluate the WebACL associated with the matched stage's
    /// ARN on every execute-API request. Pass the same
    /// `Wafv2Service::rate_limiter()` instance every dataplane uses
    /// so `RateBasedStatement` counters stay consistent.
    pub fn with_waf(
        mut self,
        state: fakecloud_wafv2::SharedWafv2State,
        rate_limiter: Arc<fakecloud_wafv2::RateLimiter>,
    ) -> Self {
        self.waf_state = Some(state);
        self.waf_rate_limiter = Some(rate_limiter);
        self
    }

    /// Snapshot of the WAF Count-action metrics. Keyed by
    /// `"<webacl-arn>|<rule-name>"`. Returns an empty map when WAF
    /// inspection is disabled or no Count rules have fired.
    pub fn waf_count_metrics_snapshot(&self) -> BTreeMap<String, u64> {
        self.waf_count_metrics.lock().clone()
    }

    pub fn with_elbv2(mut self, state: fakecloud_elbv2::SharedElbv2State) -> Self {
        self.elbv2_state = Some(state);
        self
    }

    pub fn with_registry(
        mut self,
        registry: Arc<std::sync::OnceLock<Arc<fakecloud_core::registry::ServiceRegistry>>>,
    ) -> Self {
        self.registry = Some(registry);
        self
    }

    pub(crate) fn delivery(&self) -> Option<&Arc<DeliveryBus>> {
        self.delivery.as_ref()
    }

    pub(crate) fn registry(
        &self,
    ) -> Option<&Arc<std::sync::OnceLock<Arc<fakecloud_core::registry::ServiceRegistry>>>> {
        self.registry.as_ref()
    }

    pub fn state_handle(&self) -> &SharedApiGatewayState {
        &self.state
    }

    pub(crate) fn record_request(
        &self,
        account_id: &str,
        api_id: &str,
        stage: &str,
        req: &AwsRequest,
        status: StatusCode,
    ) {
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(account_id);
        state.request_history.push(crate::state::ApiRequest {
            api_id: api_id.to_string(),
            stage: stage.to_string(),
            method: req.method.as_str().to_string(),
            path: req.raw_path.clone(),
            status: status.as_u16(),
            created_at: chrono::Utc::now(),
        });
        if state.request_history.len() > 1000 {
            let drop = state.request_history.len() - 1000;
            state.request_history.drain(..drop);
        }
    }

    async fn save_snapshot(&self) {
        save_apigateway_snapshot(
            &self.state,
            self.snapshot_store.clone(),
            &self.snapshot_lock,
        )
        .await;
    }

    /// Build a hook that persists the current API Gateway state when invoked, or
    /// `None` in memory mode (no snapshot store). The CloudFormation provisioner
    /// mutates `state` directly and uses this to write a CFN-provisioned
    /// resource through to disk, the same way a direct mutating API call would.
    pub fn snapshot_hook(&self) -> Option<fakecloud_persistence::SnapshotHook> {
        let store = self.snapshot_store.clone()?;
        let state = self.state.clone();
        let lock = self.snapshot_lock.clone();
        Some(Arc::new(move || {
            let state = state.clone();
            let store = store.clone();
            let lock = lock.clone();
            Box::pin(async move {
                save_apigateway_snapshot(&state, Some(store), &lock).await;
            })
        }))
    }
}

/// Persist the current API Gateway state as a snapshot. Cloned + serialized
/// under the snapshot lock. Noop when `store` is `None` (memory mode). Shared by
/// `ApiGatewayService::save_snapshot` and the CloudFormation provisioner's
/// post-provision persist hook so both route through the same
/// serialize-and-write path.
pub async fn save_apigateway_snapshot(
    state: &SharedApiGatewayState,
    store: Option<Arc<dyn SnapshotStore>>,
    lock: &AsyncMutex<()>,
) {
    let Some(store) = store else {
        return;
    };
    let _guard = lock.lock().await;
    let snapshot = ApiGatewaySnapshot {
        schema_version: APIGATEWAY_SNAPSHOT_SCHEMA_VERSION,
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
        Ok(Err(err)) => tracing::error!(%err, "failed to write apigateway snapshot"),
        Err(err) => tracing::error!(%err, "apigateway snapshot task panicked"),
    }
}

#[async_trait]
impl fakecloud_core::service::AwsService for ApiGatewayService {
    fn service_name(&self) -> &str {
        "apigatewayv1"
    }

    fn supported_actions(&self) -> &[&str] {
        SUPPORTED_ACTIONS
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        // Identify whether this is a control-plane request (matches one
        // of the known REST routes) or a data-plane execute call.
        if let Some(resolved) = resolve(&req.method, &req.path_segments, &req.query_params) {
            let res = self.handle_control(&req, resolved).await;
            if res.is_ok() && is_mutating_method(&req.method) {
                self.save_snapshot().await;
            }
            return res;
        }
        // Fallback: data-plane invocation.
        crate::data_plane::handle(self, &req).await
    }
}

// ── Integration handlers ──

// ── Deployment handlers ──

// ── Stage handlers ──

// ── Models ──

// ── Request validators ──

// ── Authorizers ──

// ── API keys ──

// ── Usage plans + keys ──

// ── VPC links / domain names / base path mappings / client certs ──
// All accept arbitrary JSON, store it under the keyed map, and return
// the same payload — that's enough for SDK round-trip tests.

// ── Documentation parts/versions, gateway responses, export, sdk, tags ──

#[path = "service_rest_api_resources.rs"]
mod service_rest_api_resources;

#[path = "service_integrations.rs"]
mod service_integrations;

#[path = "service_deployments.rs"]
mod service_deployments;

#[path = "service_stages.rs"]
mod service_stages;

#[path = "service_models.rs"]
mod service_models;

#[path = "service_request_validators.rs"]
mod service_request_validators;

#[path = "service_authorizers.rs"]
mod service_authorizers;

#[path = "service_api_keys.rs"]
mod service_api_keys;

#[path = "service_usage_plans.rs"]
mod service_usage_plans;

#[path = "service_vpc_links_etc.rs"]
mod service_vpc_links_etc;

#[path = "service_extras.rs"]
mod service_extras;

#[path = "helpers.rs"]
pub(crate) mod helpers;
pub(crate) use helpers::*;

#[cfg(test)]
mod snapshot_hook_tests {
    use super::ApiGatewayService;
    use std::sync::Arc;

    fn svc() -> ApiGatewayService {
        let state = Arc::new(parking_lot::RwLock::new(
            fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
        ));
        ApiGatewayService::new(state)
    }

    /// No snapshot store (memory mode) -> no persist hook for the CFN provisioner.
    #[test]
    fn snapshot_hook_is_none_without_store() {
        let svc = svc();
        assert!(svc.snapshot_hook().is_none());
    }

    #[tokio::test]
    async fn snapshot_hook_fires_with_store() {
        let store: Arc<dyn fakecloud_persistence::SnapshotStore> =
            Arc::new(fakecloud_persistence::MemorySnapshotStore::new());
        let svc = svc().with_snapshot_store(store);
        let hook = svc
            .snapshot_hook()
            .expect("hook present when a store is set");
        hook().await;
    }
}
