//! Route 53 REST-XML service implementation.

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::Utc;
use http::{HeaderMap, StatusCode};
use parking_lot::RwLock;
use tokio::sync::Mutex as AsyncMutex;
use uuid::Uuid;

use fakecloud_aws::arn::Arn;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError, ResponseBody};
use fakecloud_persistence::SnapshotStore;

use crate::model::{
    AssociateVpcRequest, ChangeCidrCollectionRequest, ChangeResourceRecordSetsRequest,
    ChangeTagsForResourceRequest, CreateCidrCollectionRequest, CreateHealthCheckRequest,
    CreateHostedZoneRequest, CreateKeySigningKeyRequest, CreateQueryLoggingConfigRequest,
    CreateReusableDelegationSetRequest, CreateTrafficPolicyInstanceRequest,
    CreateTrafficPolicyRequest, CreateTrafficPolicyVersionRequest, HealthCheckConfig,
    ListTagsForResourcesRequest, ResourceRecordSet, UpdateHealthCheckRequest,
    UpdateHostedZoneCommentRequest, UpdateHostedZoneFeaturesRequest,
    UpdateTrafficPolicyCommentRequest, UpdateTrafficPolicyInstanceRequest, VpcAuthorizationRequest,
    VPC,
};
use crate::router::{route, Route};
use crate::state::{
    AccountState, HealthCheckStatus, Route53Accounts, Route53Snapshot, SharedRoute53State,
    StoredChange, StoredCidrCollection, StoredHealthCheck, StoredHostedZone, StoredKeySigningKey,
    StoredQueryLoggingConfig, StoredReusableDelegationSet, StoredTrafficPolicy,
    StoredTrafficPolicyInstance, ROUTE53_SNAPSHOT_SCHEMA_VERSION,
};
use crate::xml_io;

pub(crate) const DEFAULT_ACCOUNT: &str = "000000000000";
pub(crate) const NS: &str = crate::NAMESPACE;
const XML_DECL: &str = r#"<?xml version="1.0" encoding="UTF-8"?>"#;

const SUPPORTED_ACTIONS: &[&str] = &[
    "CreateHostedZone",
    "GetHostedZone",
    "DeleteHostedZone",
    "ListHostedZones",
    "ListHostedZonesByName",
    "GetHostedZoneCount",
    "UpdateHostedZoneComment",
    "UpdateHostedZoneFeatures",
    "GetHostedZoneLimit",
    "ChangeResourceRecordSets",
    "ListResourceRecordSets",
    "GetChange",
    "TestDNSAnswer",
    "CreateHealthCheck",
    "GetHealthCheck",
    "UpdateHealthCheck",
    "DeleteHealthCheck",
    "ListHealthChecks",
    "GetHealthCheckCount",
    "GetHealthCheckStatus",
    "GetHealthCheckLastFailureReason",
    "GetCheckerIpRanges",
    "CreateTrafficPolicy",
    "CreateTrafficPolicyVersion",
    "GetTrafficPolicy",
    "UpdateTrafficPolicyComment",
    "DeleteTrafficPolicy",
    "ListTrafficPolicies",
    "ListTrafficPolicyVersions",
    "CreateTrafficPolicyInstance",
    "GetTrafficPolicyInstance",
    "UpdateTrafficPolicyInstance",
    "DeleteTrafficPolicyInstance",
    "ListTrafficPolicyInstances",
    "ListTrafficPolicyInstancesByHostedZone",
    "ListTrafficPolicyInstancesByPolicy",
    "GetTrafficPolicyInstanceCount",
    "GetDNSSEC",
    "EnableHostedZoneDNSSEC",
    "DisableHostedZoneDNSSEC",
    "CreateKeySigningKey",
    "DeleteKeySigningKey",
    "ActivateKeySigningKey",
    "DeactivateKeySigningKey",
    "CreateQueryLoggingConfig",
    "GetQueryLoggingConfig",
    "DeleteQueryLoggingConfig",
    "ListQueryLoggingConfigs",
    "CreateCidrCollection",
    "ChangeCidrCollection",
    "DeleteCidrCollection",
    "ListCidrCollections",
    "ListCidrLocations",
    "ListCidrBlocks",
    "AssociateVPCWithHostedZone",
    "DisassociateVPCFromHostedZone",
    "CreateVPCAssociationAuthorization",
    "DeleteVPCAssociationAuthorization",
    "ListVPCAssociationAuthorizations",
    "ListHostedZonesByVPC",
    "CreateReusableDelegationSet",
    "GetReusableDelegationSet",
    "DeleteReusableDelegationSet",
    "ListReusableDelegationSets",
    "GetReusableDelegationSetLimit",
    "ListGeoLocations",
    "GetGeoLocation",
    "GetAccountLimit",
    "ChangeTagsForResource",
    "ListTagsForResource",
    "ListTagsForResources",
];

pub struct Route53Service {
    pub(crate) state: SharedRoute53State,
    /// Optional CloudWatch Logs state. When wired, every TestDNSAnswer
    /// call against a zone with an active QueryLoggingConfig appends a
    /// query log record to the configured log group, mirroring real
    /// Route 53's query logging delivery.
    pub(crate) logs_state: Option<fakecloud_logs::SharedLogsState>,
    /// Optional ELBv2 state. When wired, alias records targeting an
    /// ELB DNS name (e.g. `my-lb-123.us-east-1.elb.amazonaws.com`)
    /// resolve to the load balancer's actual A/AAAA addresses pulled
    /// from `LoadBalancerAddress`.
    pub(crate) elbv2_state: Option<fakecloud_elbv2::SharedElbv2State>,
    /// Optional CloudFront state. When wired, alias records targeting
    /// a CloudFront `<id>.cloudfront.net` domain resolve only when the
    /// distribution exists.
    pub(crate) cloudfront_state: Option<fakecloud_cloudfront::SharedCloudFrontState>,
    /// Optional S3 state. When wired, alias records targeting an S3
    /// website endpoint (`<bucket>.s3-website-<region>.amazonaws.com`)
    /// or virtual-hosted bucket endpoint resolve only when the bucket
    /// exists.
    pub(crate) s3_state: Option<fakecloud_s3::SharedS3State>,
    pub(crate) snapshot_store: Option<Arc<dyn SnapshotStore>>,
    pub(crate) snapshot_lock: Arc<AsyncMutex<()>>,
}

mod cidr;
mod dnssec;
mod health_checks;
mod hosted_zones;
mod query_logging;
mod records;
mod traffic_policies;

impl Route53Service {
    pub fn new(state: SharedRoute53State) -> Self {
        Self {
            state,
            logs_state: None,
            elbv2_state: None,
            cloudfront_state: None,
            s3_state: None,
            snapshot_store: None,
            snapshot_lock: Arc::new(AsyncMutex::new(())),
        }
    }

    pub fn with_snapshot_store(mut self, store: Arc<dyn SnapshotStore>) -> Self {
        self.snapshot_store = Some(store);
        self
    }

    /// Persist current state as a snapshot. Held across the
    /// clone-serialize-write sequence to prevent stale-last writes, with serde
    /// + file I/O offloaded to the blocking pool.
    async fn save_snapshot(&self) {
        save_route53_snapshot(
            &self.state,
            self.snapshot_store.clone(),
            &self.snapshot_lock,
        )
        .await;
    }

    /// Build a hook that persists the current Route 53 state when invoked, or
    /// `None` in memory mode. The CloudFormation provisioner mutates `state`
    /// directly and uses this to write a CFN-provisioned resource through to
    /// disk, the same way a direct mutating API call would.
    pub fn snapshot_hook(&self) -> Option<fakecloud_persistence::SnapshotHook> {
        let store = self.snapshot_store.clone()?;
        let state = self.state.clone();
        let lock = self.snapshot_lock.clone();
        Some(Arc::new(move || {
            let state = state.clone();
            let store = store.clone();
            let lock = lock.clone();
            Box::pin(async move {
                save_route53_snapshot(&state, Some(store), &lock).await;
            })
        }))
    }

    /// Admin-endpoint flavour of [`set_health_check_status`](Self::set_health_check_status)
    /// that persists the override so it survives a restart.
    pub async fn set_health_check_status_persistent(
        &self,
        id: &str,
        status: HealthCheckStatus,
        reason: Option<String>,
    ) -> bool {
        let changed = self.set_health_check_status(id, status, reason);
        if changed {
            self.save_snapshot().await;
        }
        changed
    }

    /// Wire CloudWatch Logs so `TestDNSAnswer` calls against a zone
    /// with an active QueryLoggingConfig forward their query record
    /// into the configured log group.
    pub fn with_logs(mut self, logs: fakecloud_logs::SharedLogsState) -> Self {
        self.logs_state = Some(logs);
        self
    }

    /// Wire ELBv2 state so `TestDNSAnswer` resolves alias records that
    /// point at an ELB DNS name to the load balancer's real addresses.
    pub fn with_elbv2(mut self, elbv2: fakecloud_elbv2::SharedElbv2State) -> Self {
        self.elbv2_state = Some(elbv2);
        self
    }

    /// Wire CloudFront state so `TestDNSAnswer` only resolves alias
    /// records that point at an existing CloudFront distribution.
    pub fn with_cloudfront(mut self, cf: fakecloud_cloudfront::SharedCloudFrontState) -> Self {
        self.cloudfront_state = Some(cf);
        self
    }

    /// Wire S3 state so `TestDNSAnswer` only resolves alias records
    /// that point at an existing S3 bucket / website endpoint.
    pub fn with_s3(mut self, s3: fakecloud_s3::SharedS3State) -> Self {
        self.s3_state = Some(s3);
        self
    }

    pub fn shared_state(&self) -> SharedRoute53State {
        Arc::clone(&self.state)
    }
}

impl Default for Route53Service {
    fn default() -> Self {
        Self::new(Arc::new(RwLock::new(Route53Accounts::new())))
    }
}

/// Bundle of DNSSEC RRSIG fields returned by
/// `Route53Service::sign_rrset_with_zone_ksk`. Carries enough context
/// (algorithm, key tag, validity window, signer) for an admin caller
/// to assemble a full RRSIG wire record.
#[derive(Debug, Clone)]
pub struct DnssecSignature {
    pub signature_b64: String,
    pub algorithm: u8,
    pub key_tag: u16,
    pub signer_name: String,
    pub inception: u32,
    pub expiration: u32,
    pub labels: u8,
    pub original_ttl: u32,
    pub rrset_type: String,
}

/// Actions that mutate persisted Route 53 state and therefore must trigger a
/// snapshot write. Read-only actions (Get*/List*/Test*) are excluded.
const MUTATING_ACTIONS: &[&str] = &[
    "CreateHostedZone",
    "DeleteHostedZone",
    "UpdateHostedZoneComment",
    "UpdateHostedZoneFeatures",
    "ChangeResourceRecordSets",
    "CreateHealthCheck",
    "UpdateHealthCheck",
    "DeleteHealthCheck",
    "CreateTrafficPolicy",
    "CreateTrafficPolicyVersion",
    "DeleteTrafficPolicy",
    "UpdateTrafficPolicyComment",
    "CreateTrafficPolicyInstance",
    "UpdateTrafficPolicyInstance",
    "DeleteTrafficPolicyInstance",
    "EnableHostedZoneDNSSEC",
    "DisableHostedZoneDNSSEC",
    "CreateKeySigningKey",
    "DeleteKeySigningKey",
    "ActivateKeySigningKey",
    "DeactivateKeySigningKey",
    "CreateQueryLoggingConfig",
    "DeleteQueryLoggingConfig",
    "CreateCidrCollection",
    "ChangeCidrCollection",
    "DeleteCidrCollection",
    "AssociateVPCWithHostedZone",
    "DisassociateVPCFromHostedZone",
    "CreateVPCAssociationAuthorization",
    "DeleteVPCAssociationAuthorization",
    "CreateReusableDelegationSet",
    "DeleteReusableDelegationSet",
    "ChangeTagsForResource",
];

/// Persist the current Route 53 state as a snapshot. Offloads the serde +
/// blocking file write to the Tokio blocking pool. Noop when `store` is `None`
/// (memory mode). Shared by `Route53Service::save_snapshot` and the
/// CloudFormation provisioner persist hook so both route through the same
/// serialize-and-write path.
pub async fn save_route53_snapshot(
    state: &SharedRoute53State,
    store: Option<Arc<dyn SnapshotStore>>,
    lock: &AsyncMutex<()>,
) {
    let Some(store) = store else {
        return;
    };
    let _guard = lock.lock().await;
    let snapshot = Route53Snapshot {
        schema_version: ROUTE53_SNAPSHOT_SCHEMA_VERSION,
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
        Ok(Err(err)) => tracing::error!(%err, "failed to write route53 snapshot"),
        Err(err) => tracing::error!(%err, "route53 snapshot task panicked"),
    }
}

#[async_trait]
impl AwsService for Route53Service {
    fn service_name(&self) -> &str {
        "route53"
    }

    fn supported_actions(&self) -> &[&str] {
        SUPPORTED_ACTIONS
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let resolved = match route(&req.method, &req.raw_path, &req.raw_query) {
            Some(r) => r,
            None => {
                return Err(aws_error(
                    StatusCode::NOT_FOUND,
                    "InvalidArgument",
                    format!("Unknown Route 53 route: {} {}", req.method, req.raw_path),
                ));
            }
        };

        let mutates = MUTATING_ACTIONS.contains(&resolved.action);
        let result = match resolved.action {
            "CreateHostedZone" => self.create_hosted_zone(&req),
            "GetHostedZone" => self.get_hosted_zone(&resolved),
            "DeleteHostedZone" => self.delete_hosted_zone(&resolved),
            "ListHostedZones" => self.list_hosted_zones(&req),
            "ListHostedZonesByName" => self.list_hosted_zones_by_name(&req),
            "GetHostedZoneCount" => self.get_hosted_zone_count(),
            "UpdateHostedZoneComment" => self.update_hosted_zone_comment(&req, &resolved),
            "UpdateHostedZoneFeatures" => self.update_hosted_zone_features(&req, &resolved),
            "GetHostedZoneLimit" => self.get_hosted_zone_limit(&resolved),
            "ChangeResourceRecordSets" => self.change_resource_record_sets(&req, &resolved),
            "ListResourceRecordSets" => self.list_resource_record_sets(&req, &resolved),
            "GetChange" => self.get_change(&resolved),
            "TestDNSAnswer" => self.test_dns_answer(&req),
            "CreateHealthCheck" => self.create_health_check(&req),
            "GetHealthCheck" => self.get_health_check(&resolved),
            "UpdateHealthCheck" => self.update_health_check(&req, &resolved),
            "DeleteHealthCheck" => self.delete_health_check(&resolved),
            "ListHealthChecks" => self.list_health_checks(&req),
            "GetHealthCheckCount" => self.get_health_check_count(),
            "GetHealthCheckStatus" => self.get_health_check_status(&resolved),
            "GetHealthCheckLastFailureReason" => {
                self.get_health_check_last_failure_reason(&resolved)
            }
            "GetCheckerIpRanges" => self.get_checker_ip_ranges(),
            "CreateTrafficPolicy" => self.create_traffic_policy(&req),
            "CreateTrafficPolicyVersion" => self.create_traffic_policy_version(&req, &resolved),
            "GetTrafficPolicy" => self.get_traffic_policy(&resolved),
            "UpdateTrafficPolicyComment" => self.update_traffic_policy_comment(&req, &resolved),
            "DeleteTrafficPolicy" => self.delete_traffic_policy(&resolved),
            "ListTrafficPolicies" => self.list_traffic_policies(&req),
            "ListTrafficPolicyVersions" => self.list_traffic_policy_versions(&req, &resolved),
            "CreateTrafficPolicyInstance" => self.create_traffic_policy_instance(&req),
            "GetTrafficPolicyInstance" => self.get_traffic_policy_instance(&resolved),
            "UpdateTrafficPolicyInstance" => self.update_traffic_policy_instance(&req, &resolved),
            "DeleteTrafficPolicyInstance" => self.delete_traffic_policy_instance(&resolved),
            "ListTrafficPolicyInstances" => self.list_traffic_policy_instances(&req),
            "ListTrafficPolicyInstancesByHostedZone" => {
                self.list_traffic_policy_instances_by_hosted_zone(&req)
            }
            "ListTrafficPolicyInstancesByPolicy" => {
                self.list_traffic_policy_instances_by_policy(&req)
            }
            "GetTrafficPolicyInstanceCount" => self.get_traffic_policy_instance_count(),
            "GetDNSSEC" => self.get_dnssec(&resolved),
            "EnableHostedZoneDNSSEC" => self.enable_hosted_zone_dnssec(&resolved),
            "DisableHostedZoneDNSSEC" => self.disable_hosted_zone_dnssec(&resolved),
            "CreateKeySigningKey" => self.create_key_signing_key(&req),
            "DeleteKeySigningKey" => self.delete_key_signing_key(&resolved),
            "ActivateKeySigningKey" => self.activate_key_signing_key(&resolved),
            "DeactivateKeySigningKey" => self.deactivate_key_signing_key(&resolved),
            "CreateQueryLoggingConfig" => self.create_query_logging_config(&req),
            "GetQueryLoggingConfig" => self.get_query_logging_config(&resolved),
            "DeleteQueryLoggingConfig" => self.delete_query_logging_config(&resolved),
            "ListQueryLoggingConfigs" => self.list_query_logging_configs(&req),
            "CreateCidrCollection" => self.create_cidr_collection(&req),
            "ChangeCidrCollection" => self.change_cidr_collection(&req, &resolved),
            "DeleteCidrCollection" => self.delete_cidr_collection(&resolved),
            "ListCidrCollections" => self.list_cidr_collections(&req),
            "ListCidrLocations" => self.list_cidr_locations(&req, &resolved),
            "ListCidrBlocks" => self.list_cidr_blocks(&req, &resolved),
            "AssociateVPCWithHostedZone" => self.associate_vpc_with_hosted_zone(&req, &resolved),
            "DisassociateVPCFromHostedZone" => {
                self.disassociate_vpc_from_hosted_zone(&req, &resolved)
            }
            "CreateVPCAssociationAuthorization" => {
                self.create_vpc_association_authorization(&req, &resolved)
            }
            "DeleteVPCAssociationAuthorization" => {
                self.delete_vpc_association_authorization(&req, &resolved)
            }
            "ListVPCAssociationAuthorizations" => {
                self.list_vpc_association_authorizations(&req, &resolved)
            }
            "ListHostedZonesByVPC" => self.list_hosted_zones_by_vpc(&req),
            "CreateReusableDelegationSet" => self.create_reusable_delegation_set(&req),
            "GetReusableDelegationSet" => self.get_reusable_delegation_set(&resolved),
            "DeleteReusableDelegationSet" => self.delete_reusable_delegation_set(&resolved),
            "ListReusableDelegationSets" => self.list_reusable_delegation_sets(&req),
            "GetReusableDelegationSetLimit" => self.get_reusable_delegation_set_limit(&resolved),
            "ListGeoLocations" => self.list_geo_locations(&req),
            "GetGeoLocation" => self.get_geo_location(&req),
            "GetAccountLimit" => self.get_account_limit(&resolved),
            "ChangeTagsForResource" => self.change_tags_for_resource(&req, &resolved),
            "ListTagsForResource" => self.list_tags_for_resource(&resolved),
            "ListTagsForResources" => self.list_tags_for_resources(&req, &resolved),
            other => Err(aws_error(
                StatusCode::NOT_IMPLEMENTED,
                "InvalidAction",
                format!("Route 53 action {other} is not implemented yet"),
            )),
        };
        if mutates && matches!(result.as_ref(), Ok(resp) if resp.status.is_success()) {
            self.save_snapshot().await;
        }
        // Route 53 is a REST-XML service whose error wire format wraps the
        // error in `<ErrorResponse>` (unlike S3's bare `<Error>`). The shared
        // dispatcher renders Rest-protocol errors in the S3 shape, so the AWS
        // SDK can't parse the code and reports `UnknownError` — which broke the
        // provider's post-destroy `GetHostedZone` check (it expects
        // `NoSuchHostedZone`). Render the route53-shaped error body here.
        match result {
            Ok(resp) => Ok(resp),
            Err(err) => Ok(route53_error_response(&err, &req.request_id)),
        }
    }
}

/// Render an [`AwsServiceError`] as a Route 53 REST-XML `<ErrorResponse>`
/// document so the AWS SDK can extract the error code.
fn route53_error_response(err: &AwsServiceError, request_id: &str) -> AwsResponse {
    let body = format!(
        "{XML_DECL}<ErrorResponse xmlns=\"{NS}\">\
         <Error><Type>Sender</Type><Code>{}</Code><Message>{}</Message></Error>\
         <RequestId>{}</RequestId></ErrorResponse>",
        esc(err.code()),
        esc(&err.message()),
        esc(request_id),
    );
    let mut headers = HeaderMap::new();
    if let Ok(v) = http::HeaderValue::from_str(err.code()) {
        headers.insert("x-amz-error-code", v);
    }
    xml_response(err.status(), body, headers)
}

// ─── Hosted Zone handlers ────────────────────────────────────────────

impl Route53Service {}

// ─── Resource Record Set handlers ────────────────────────────────────

impl Route53Service {}

// ─── Change tracking + DNS test ──────────────────────────────────────

impl Route53Service {}

// ─── Health Check handlers ───────────────────────────────────────────

impl Route53Service {}

// ─── Traffic Policy handlers ─────────────────────────────────────────

impl Route53Service {}

// ─── Traffic Policy Instance handlers ────────────────────────────────

impl Route53Service {}

// ─── DNSSEC + KSK handlers ───────────────────────────────────────────

impl Route53Service {}

// ─── Query Logging handlers ──────────────────────────────────────────

impl Route53Service {}

// ─── CIDR Collection handlers ────────────────────────────────────────

impl Route53Service {}

// ─── Helpers ─────────────────────────────────────────────────────────

#[path = "../helpers.rs"]
mod helpers;
use helpers::*;

/// Bundle of cross-service shared state references passed into the
/// routing-policy resolver so alias targets can resolve to the actual
/// underlying ELB / CloudFront / S3 endpoints. Each field is optional —
/// when a service isn't wired (unit-test, persistence-only build) the
/// resolver falls back to a deterministic synthetic IP so existing
/// tests stay stable.
pub(crate) struct AliasLookup<'a> {
    pub(crate) elbv2: Option<&'a fakecloud_elbv2::SharedElbv2State>,
    pub(crate) cloudfront: Option<&'a fakecloud_cloudfront::SharedCloudFrontState>,
    pub(crate) s3: Option<&'a fakecloud_s3::SharedS3State>,
}

/// Resolve a TestDNSAnswer query against the candidate RRsets honoring
/// the routing policy fields each set may carry. Mirrors real Route 53.
///
/// * Failover (PRIMARY+SECONDARY) — primary if healthy, otherwise secondary.
/// * Multi-value answer — up to 8 healthy values combined.
/// * Weighted — pick proportional to weight, deterministically keyed by subnet.
/// * Latency — pick the record whose region matches the client's region.
/// * Geolocation — match country/continent against record's GeoLocation.
/// * Alias targets — resolve into the wired ELB/CloudFront/S3 state when
///   present and fall back to a deterministic synthetic IP otherwise.
/// * Default (no routing fields) — first healthy record's values.
fn resolve_routing_policy(
    candidates: &[&crate::model::ResourceRecordSet],
    health_checks: &std::collections::BTreeMap<String, crate::state::StoredHealthCheck>,
    edns0_subnet: Option<&str>,
    alias_lookup: &AliasLookup<'_>,
) -> Vec<String> {
    let rr_values = |r: &crate::model::ResourceRecordSet| -> Vec<String> {
        if let Some(alias) = r.alias_target.as_ref() {
            return resolve_alias_target(alias, &r.record_type, alias_lookup);
        }
        r.resource_records
            .as_ref()
            .map(|rr| rr.resource_record.iter().map(|x| x.value.clone()).collect())
            .unwrap_or_default()
    };
    fn is_healthy(
        r: &crate::model::ResourceRecordSet,
        health_checks: &std::collections::BTreeMap<String, crate::state::StoredHealthCheck>,
    ) -> bool {
        match r.health_check_id.as_ref() {
            None => true,
            Some(id) => health_checks
                .get(id)
                .map(|hc| matches!(hc.status, crate::state::HealthCheckStatus::Success))
                .unwrap_or(true),
        }
    }
    fn subnet_hash(subnet: Option<&str>) -> u64 {
        subnet
            .map(|s| {
                let mut h: u64 = 0xcbf29ce484222325;
                for b in s.bytes() {
                    h ^= b as u64;
                    h = h.wrapping_mul(0x100000001b3);
                }
                h
            })
            .unwrap_or(0)
    }

    if candidates.iter().any(|r| r.failover.is_some()) {
        let primary = candidates
            .iter()
            .find(|r| r.failover.as_deref() == Some("PRIMARY"));
        let secondary = candidates
            .iter()
            .find(|r| r.failover.as_deref() == Some("SECONDARY"));
        if let Some(p) = primary {
            if is_healthy(p, health_checks) {
                return rr_values(p);
            }
        }
        if let Some(s) = secondary {
            // Only fall through to secondary when it's actually healthy;
            // returning an unhealthy secondary while the primary is also
            // unhealthy hands clients a known-broken endpoint instead
            // of the documented "return primary as last resort".
            if is_healthy(s, health_checks) {
                return rr_values(s);
            }
        }
        return primary.map(|p| rr_values(p)).unwrap_or_default();
    }

    if candidates
        .iter()
        .any(|r| r.multi_value_answer == Some(true))
    {
        return candidates
            .iter()
            .filter(|r| is_healthy(r, health_checks))
            .flat_map(|r| rr_values(r))
            .take(8)
            .collect();
    }

    if candidates.iter().any(|r| r.weight.is_some()) {
        let healthy: Vec<&&crate::model::ResourceRecordSet> = candidates
            .iter()
            .filter(|r| is_healthy(r, health_checks) && r.weight.is_some())
            .collect();
        if healthy.is_empty() {
            return candidates.first().map(|r| rr_values(r)).unwrap_or_default();
        }
        let total: i64 = healthy.iter().map(|r| r.weight.unwrap_or(0)).sum();
        if total == 0 {
            // All healthy candidates have weight 0 — uniform random
            // pick among them rather than always returning the first
            // candidate (which may be unhealthy).
            let idx = (subnet_hash(edns0_subnet) as usize) % healthy.len();
            return rr_values(healthy[idx]);
        }
        let mut pick = (subnet_hash(edns0_subnet) % total as u64) as i64;
        for r in &healthy {
            let w = r.weight.unwrap_or(0);
            if pick < w {
                return rr_values(r);
            }
            pick -= w;
        }
        return rr_values(healthy[0]);
    }

    // Latency-based: pick the record whose region matches the client subnet's
    // inferred region. Fall back to the closest region by hash distance.
    if candidates
        .iter()
        .any(|r| r.region.is_some() && r.geo_location.is_none())
    {
        let healthy: Vec<&&crate::model::ResourceRecordSet> = candidates
            .iter()
            .filter(|r| is_healthy(r, health_checks))
            .collect();
        if healthy.is_empty() {
            return Vec::new();
        }
        let client_region = infer_region_from_subnet(edns0_subnet);
        if let Some(r) = healthy
            .iter()
            .find(|r| r.region.as_deref() == Some(client_region.as_str()))
        {
            return rr_values(r);
        }
        // No exact region match — fall back to a deterministic pick.
        let idx = (subnet_hash(edns0_subnet) as usize) % healthy.len();
        return rr_values(healthy[idx]);
    }

    // Geolocation: match country/continent of client against record GeoLocation.
    // A record with GeoLocation { country_code: "*" } (or no GeoLocation) acts
    // as the default. Real Route 53 uses a record with no GeoLocation as the
    // default; we match either form.
    if candidates.iter().any(|r| r.geo_location.is_some()) {
        let healthy: Vec<&&crate::model::ResourceRecordSet> = candidates
            .iter()
            .filter(|r| is_healthy(r, health_checks))
            .collect();
        if healthy.is_empty() {
            return Vec::new();
        }
        let (client_country, client_continent) = infer_geo_from_subnet(edns0_subnet);
        // 1) Exact country match.
        if let Some(r) = healthy.iter().find(|r| {
            r.geo_location
                .as_ref()
                .and_then(|g| g.country_code.as_deref())
                .map(|c| c == client_country)
                .unwrap_or(false)
        }) {
            return rr_values(r);
        }
        // 2) Continent match.
        if let Some(r) = healthy.iter().find(|r| {
            r.geo_location
                .as_ref()
                .and_then(|g| g.continent_code.as_deref())
                .map(|c| c == client_continent)
                .unwrap_or(false)
        }) {
            return rr_values(r);
        }
        // 3) Default record: GeoLocation with country_code="*" or no GeoLocation.
        if let Some(r) = healthy.iter().find(|r| {
            r.geo_location
                .as_ref()
                .and_then(|g| g.country_code.as_deref())
                .map(|c| c == "*")
                .unwrap_or(false)
        }) {
            return rr_values(r);
        }
        if let Some(r) = healthy.iter().find(|r| r.geo_location.is_none()) {
            return rr_values(r);
        }
        // 4) Last resort: first healthy.
        return rr_values(healthy[0]);
    }

    candidates
        .iter()
        .find(|r| is_healthy(r, health_checks))
        .or_else(|| candidates.first())
        .map(|r| rr_values(r))
        .unwrap_or_default()
}

/// Resolve an alias target to record values. Real Route 53 follows the
/// alias to the underlying ELB / CloudFront / S3 endpoint and returns
/// A / AAAA records. For fakecloud's TestDNSAnswer we cross-call into
/// the appropriate crate's state when wired (`with_elbv2`,
/// `with_cloudfront`, `with_s3`) and fall back to a deterministic
/// synthetic IP per DNS name when the target service isn't wired or
/// the resource isn't found. Non-A/AAAA aliases surface the alias
/// hostname directly, matching real Route 53 behaviour for CNAME-style
/// aliases.
fn resolve_alias_target(
    alias: &crate::model::AliasTarget,
    record_type: &str,
    lookup: &AliasLookup<'_>,
) -> Vec<String> {
    let name = alias.dns_name.trim_end_matches('.');
    if record_type == "A" || record_type == "AAAA" {
        // 1) Try ELB lookup: `<name>-<suffix>.<region>.elb.amazonaws.com`.
        if let Some(addrs) = lookup_elbv2_addresses(name, record_type, lookup.elbv2) {
            return addrs;
        }
        // 2) CloudFront: `<id>.cloudfront.net` — only resolve when the
        //    distribution is known.
        if let Some(addrs) = lookup_cloudfront_addresses(name, record_type, lookup.cloudfront) {
            return addrs;
        }
        // 3) S3: `<bucket>.s3-website-<region>.amazonaws.com` /
        //    `<bucket>.s3.<region>.amazonaws.com` — only resolve when
        //    the bucket exists.
        if let Some(addrs) = lookup_s3_addresses(name, record_type, lookup.s3) {
            return addrs;
        }
        // 4) Fallback: deterministic IP derived from the name. Keeps
        //    pre-cross-call test fixtures stable and gives unit tests
        //    a stable value when state isn't wired.
        return synthetic_alias_addresses(name, record_type);
    }
    // Non-A/AAAA aliases (CNAME, etc.) surface the alias hostname directly.
    vec![name.to_string()]
}

fn lookup_elbv2_addresses(
    name: &str,
    record_type: &str,
    state: Option<&fakecloud_elbv2::SharedElbv2State>,
) -> Option<Vec<String>> {
    // ELB DNS names look like:
    //   `<scheme>-?<lb-name>-<suffix>.<region>.elb.amazonaws.com`
    // (`internal-` prefix for private LBs). Match the suffix and pull
    // addresses straight off the matching `LoadBalancer`.
    if !name.ends_with(".elb.amazonaws.com") {
        return None;
    }
    let state = state?;
    let lower = name.to_ascii_lowercase();
    let st = state.read();
    for (_acct, account) in st.iter() {
        for lb in account.load_balancers.values() {
            if lb.dns_name.trim_end_matches('.').to_ascii_lowercase() == lower {
                let mut out: Vec<String> = Vec::new();
                for az in &lb.availability_zones {
                    for addr in &az.load_balancer_addresses {
                        if record_type == "AAAA" {
                            if let Some(v6) = addr.ipv6_address.as_ref() {
                                if !v6.is_empty() {
                                    out.push(v6.clone());
                                }
                            }
                        } else if let Some(v4) = addr.ip_address.as_ref() {
                            if !v4.is_empty() {
                                out.push(v4.clone());
                            }
                        } else if let Some(v4) = addr.private_ipv4_address.as_ref() {
                            if !v4.is_empty() {
                                out.push(v4.clone());
                            }
                        }
                    }
                }
                if out.is_empty() {
                    // LB exists but has no concrete addresses recorded;
                    // synthesise one off the DNS name so the alias still
                    // resolves to something stable.
                    return Some(synthetic_alias_addresses(name, record_type));
                }
                return Some(out);
            }
        }
    }
    None
}

fn lookup_cloudfront_addresses(
    name: &str,
    record_type: &str,
    state: Option<&fakecloud_cloudfront::SharedCloudFrontState>,
) -> Option<Vec<String>> {
    if !name.ends_with(".cloudfront.net") {
        return None;
    }
    let state = state?;
    let lower = name.to_ascii_lowercase();
    let id = lower.trim_end_matches(".cloudfront.net");
    let st = state.read();
    for account in st.accounts.values() {
        if account.distributions.values().any(|d| {
            d.id.eq_ignore_ascii_case(id)
                || d.domain_name.trim_end_matches('.').to_ascii_lowercase() == lower
        }) {
            return Some(synthetic_alias_addresses(name, record_type));
        }
        if account
            .streaming_distributions
            .values()
            .any(|d| d.domain_name.trim_end_matches('.').to_ascii_lowercase() == lower)
        {
            return Some(synthetic_alias_addresses(name, record_type));
        }
    }
    None
}

fn lookup_s3_addresses(
    name: &str,
    record_type: &str,
    state: Option<&fakecloud_s3::SharedS3State>,
) -> Option<Vec<String>> {
    let lower = name.to_ascii_lowercase();
    // Pattern: `<bucket>.s3-website[-.]<region>.amazonaws.com` or
    //          `<bucket>.s3[.<region>].amazonaws.com`.
    let bucket = if let Some(idx) = lower.find(".s3-website") {
        &lower[..idx]
    } else if let Some(idx) = lower.find(".s3.") {
        &lower[..idx]
    } else {
        let idx = lower.find(".s3-")?;
        &lower[..idx]
    };
    if bucket.is_empty() {
        return None;
    }
    let state = state?;
    let st = state.read();
    for (_acct, s3) in st.iter() {
        if s3.buckets.contains_key(bucket) {
            return Some(synthetic_alias_addresses(name, record_type));
        }
    }
    None
}

/// Build a deterministic IPv4 (or IPv6) address from a DNS name. Used
/// as the fallback when an alias target's underlying service either
/// isn't wired into the Route 53 service or doesn't know about the
/// resource. Addresses sit inside the IETF documentation ranges
/// (`198.51.x.x` and `2001:db8::/32`) so they never overlap real
/// production endpoints.
fn synthetic_alias_addresses(name: &str, record_type: &str) -> Vec<String> {
    let h = {
        let mut h: u64 = 0xcbf29ce484222325;
        for b in name.bytes() {
            h ^= b as u64;
            h = h.wrapping_mul(0x100000001b3);
        }
        h
    };
    if record_type == "AAAA" {
        let a = (h & 0xffff) as u16;
        let b = ((h >> 16) & 0xffff) as u16;
        let c = ((h >> 32) & 0xffff) as u16;
        let d = ((h >> 48) & 0xffff) as u16;
        return vec![format!("2001:db8:{a:x}:{b:x}::{c:x}:{d:x}")];
    }
    let oct2 = ((h >> 16) & 0xff) as u8;
    let oct3 = ((h >> 8) & 0xff) as u8;
    let oct4 = (h & 0xff) as u8;
    vec![format!(
        "198.51.{}.{oct4}",
        ((oct2 as u16) << 8 | oct3 as u16) % 256
    )]
}

/// Infer an AWS region from a client subnet IP. Used by latency-based
/// routing. Real Route 53 uses BGP-derived geolocation; fakecloud uses a
/// deterministic /8-prefix mapping that is stable across calls so tests can
/// pin a request to a specific region.
fn infer_region_from_subnet(subnet: Option<&str>) -> String {
    let Some(s) = subnet else {
        return "us-east-1".to_string();
    };
    let first_octet: u32 = s
        .split('.')
        .next()
        .and_then(|o| o.parse().ok())
        .unwrap_or(0);
    match first_octet {
        0..=63 => "us-east-1".to_string(),
        64..=127 => "us-west-2".to_string(),
        128..=159 => "eu-west-1".to_string(),
        160..=191 => "eu-central-1".to_string(),
        192..=223 => "ap-southeast-1".to_string(),
        _ => "ap-northeast-1".to_string(),
    }
}

/// Infer (country_code, continent_code) from a client subnet IP. Used by
/// geolocation routing. Stable mapping keyed by the first octet so tests
/// can pin a request to a specific country.
fn infer_geo_from_subnet(subnet: Option<&str>) -> (String, String) {
    let Some(s) = subnet else {
        return ("US".to_string(), "NA".to_string());
    };
    let first_octet: u32 = s
        .split('.')
        .next()
        .and_then(|o| o.parse().ok())
        .unwrap_or(0);
    match first_octet {
        0..=63 => ("US".to_string(), "NA".to_string()),
        64..=127 => ("CA".to_string(), "NA".to_string()),
        128..=159 => ("GB".to_string(), "EU".to_string()),
        160..=191 => ("DE".to_string(), "EU".to_string()),
        192..=223 => ("SG".to_string(), "AS".to_string()),
        _ => ("JP".to_string(), "AS".to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::HealthCheckConfig;
    use crate::state::{HealthCheckStatus, Route53Accounts, StoredHealthCheck};

    fn svc_with_health_check(id: &str) -> Route53Service {
        let state = Arc::new(RwLock::new(Route53Accounts::default()));
        {
            let mut s = state.write();
            let account = s.accounts.entry(DEFAULT_ACCOUNT.to_string()).or_default();
            account.health_checks.insert(
                id.to_string(),
                StoredHealthCheck {
                    id: id.to_string(),
                    caller_reference: "ref".to_string(),
                    version: 1,
                    config: HealthCheckConfig::default(),
                    created_time: Utc::now(),
                    status: HealthCheckStatus::Success,
                    last_failure_reason: None,
                },
            );
        }
        Route53Service::new(state)
    }

    #[test]
    fn set_health_check_status_flips_status_and_failure_reason() {
        let svc = svc_with_health_check("hc-1");
        assert!(svc.set_health_check_status(
            "hc-1",
            HealthCheckStatus::Failure,
            Some("Endpoint timed out".to_string()),
        ));
        let st = svc.state.read();
        let hc = &st.accounts.get(DEFAULT_ACCOUNT).unwrap().health_checks["hc-1"];
        assert_eq!(hc.status, HealthCheckStatus::Failure);
        assert_eq!(
            hc.last_failure_reason.as_deref().unwrap(),
            "Endpoint timed out"
        );
    }

    #[test]
    fn set_health_check_status_returns_false_for_unknown_id() {
        let svc = svc_with_health_check("hc-1");
        assert!(!svc.set_health_check_status("ghost", HealthCheckStatus::Failure, None,));
    }

    fn rrset(value: &str) -> crate::model::ResourceRecordSet {
        crate::model::ResourceRecordSet {
            name: "x.example.com.".to_string(),
            record_type: "A".to_string(),
            ttl: Some(60),
            resource_records: Some(crate::model::ResourceRecords {
                resource_record: vec![crate::model::ResourceRecord {
                    value: value.to_string(),
                }],
            }),
            ..Default::default()
        }
    }

    #[test]
    fn list_resource_record_sets_uses_reversed_dns_order() {
        // Regression: Route 53 orders record sets by reversed-label DNS name
        // (then type), not plain forward ASCII. The apex (`example.com.`)
        // sorts first under `com.example`, ahead of `a.example.com.`.
        fn named(name: &str) -> crate::model::ResourceRecordSet {
            crate::model::ResourceRecordSet {
                name: name.to_string(),
                record_type: "A".to_string(),
                ttl: Some(60),
                resource_records: Some(crate::model::ResourceRecords {
                    resource_record: vec![crate::model::ResourceRecord {
                        value: "1.2.3.4".to_string(),
                    }],
                }),
                ..Default::default()
            }
        }
        let (svc, zid) = svc_with_zone(vec![
            named("z.example.com."),
            named("a.example.com."),
            named("example.com."),
            named("b.sub.example.com."),
        ]);
        let route = Route {
            action: "ListResourceRecordSets",
            id: Some(zid.clone()),
            second_id: None,
        };
        let req = AwsRequest {
            service: "route53".to_string(),
            action: "ListResourceRecordSets".to_string(),
            region: "us-east-1".to_string(),
            account_id: DEFAULT_ACCOUNT.to_string(),
            request_id: "rid".to_string(),
            headers: HeaderMap::new(),
            query_params: std::collections::HashMap::new(),
            body: Bytes::new(),
            body_stream: parking_lot::Mutex::new(None),
            path_segments: vec![
                "2013-04-01".into(),
                "hostedzone".into(),
                zid.clone(),
                "rrset".into(),
            ],
            raw_path: format!("/2013-04-01/hostedzone/{zid}/rrset"),
            raw_query: String::new(),
            method: http::Method::GET,
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        };
        let resp = svc.list_resource_record_sets(&req, &route).unwrap();
        let body = std::str::from_utf8(resp.body.expect_bytes())
            .unwrap()
            .to_string();
        // Collect <Name> values in document order.
        let mut names = Vec::new();
        let mut rest = body.as_str();
        while let Some(start) = rest.find("<Name>") {
            rest = &rest[start + "<Name>".len()..];
            let end = rest.find("</Name>").unwrap();
            names.push(rest[..end].to_string());
            rest = &rest[end + "</Name>".len()..];
        }
        assert_eq!(
            names,
            vec![
                "example.com.".to_string(),
                "a.example.com.".to_string(),
                "b.sub.example.com.".to_string(),
                "z.example.com.".to_string(),
            ]
        );
    }

    fn empty_lookup() -> AliasLookup<'static> {
        AliasLookup {
            elbv2: None,
            cloudfront: None,
            s3: None,
        }
    }

    #[test]
    fn routing_policy_failover_picks_secondary_when_primary_unhealthy() {
        let mut p = rrset("1.1.1.1");
        p.failover = Some("PRIMARY".to_string());
        p.health_check_id = Some("hc-down".to_string());
        let mut s = rrset("2.2.2.2");
        s.failover = Some("SECONDARY".to_string());
        let mut hcs = std::collections::BTreeMap::new();
        hcs.insert(
            "hc-down".to_string(),
            StoredHealthCheck {
                id: "hc-down".to_string(),
                caller_reference: "r".to_string(),
                version: 1,
                config: HealthCheckConfig::default(),
                created_time: Utc::now(),
                status: HealthCheckStatus::Failure,
                last_failure_reason: Some("connection refused".to_string()),
            },
        );
        let answers = resolve_routing_policy(&[&p, &s], &hcs, None, &empty_lookup());
        assert_eq!(answers, vec!["2.2.2.2".to_string()]);
    }

    #[test]
    fn routing_policy_multivalue_returns_only_healthy() {
        let mut a = rrset("1.1.1.1");
        a.multi_value_answer = Some(true);
        a.health_check_id = Some("hc-down".to_string());
        let mut b = rrset("2.2.2.2");
        b.multi_value_answer = Some(true);
        let mut c = rrset("3.3.3.3");
        c.multi_value_answer = Some(true);
        let mut hcs = std::collections::BTreeMap::new();
        hcs.insert(
            "hc-down".to_string(),
            StoredHealthCheck {
                id: "hc-down".to_string(),
                caller_reference: "r".to_string(),
                version: 1,
                config: HealthCheckConfig::default(),
                created_time: Utc::now(),
                status: HealthCheckStatus::Failure,
                last_failure_reason: None,
            },
        );
        let answers = resolve_routing_policy(&[&a, &b, &c], &hcs, None, &empty_lookup());
        assert_eq!(answers, vec!["2.2.2.2".to_string(), "3.3.3.3".to_string()]);
    }

    #[test]
    fn routing_policy_weighted_picks_deterministically_by_subnet() {
        let mut a = rrset("1.1.1.1");
        a.weight = Some(10);
        let mut b = rrset("2.2.2.2");
        b.weight = Some(90);
        let hcs = std::collections::BTreeMap::new();
        // Same subnet should always produce the same answer.
        let one = resolve_routing_policy(&[&a, &b], &hcs, Some("203.0.113.5"), &empty_lookup());
        let two = resolve_routing_policy(&[&a, &b], &hcs, Some("203.0.113.5"), &empty_lookup());
        assert_eq!(one, two);
        assert_eq!(one.len(), 1);
    }

    #[test]
    fn routing_policy_default_returns_first_records() {
        let a = rrset("1.1.1.1");
        let hcs = std::collections::BTreeMap::new();
        let answers = resolve_routing_policy(&[&a], &hcs, None, &empty_lookup());
        assert_eq!(answers, vec!["1.1.1.1".to_string()]);
    }

    #[test]
    fn routing_policy_latency_picks_record_matching_inferred_region() {
        let mut a = rrset("1.1.1.1");
        a.region = Some("us-east-1".to_string());
        let mut b = rrset("2.2.2.2");
        b.region = Some("eu-west-1".to_string());
        let hcs = std::collections::BTreeMap::new();
        // First-octet 0..=63 -> us-east-1 in `infer_region_from_subnet`.
        let us = resolve_routing_policy(&[&a, &b], &hcs, Some("10.0.0.1"), &empty_lookup());
        assert_eq!(us, vec!["1.1.1.1".to_string()]);
        // First-octet 128..=159 -> eu-west-1.
        let eu = resolve_routing_policy(&[&a, &b], &hcs, Some("130.0.0.1"), &empty_lookup());
        assert_eq!(eu, vec!["2.2.2.2".to_string()]);
    }

    #[test]
    fn routing_policy_geolocation_uses_country_then_continent_then_default() {
        let mut us = rrset("1.1.1.1");
        us.geo_location = Some(crate::model::GeoLocation {
            continent_code: None,
            country_code: Some("US".to_string()),
            subdivision_code: None,
        });
        let mut eu_default = rrset("2.2.2.2");
        eu_default.geo_location = Some(crate::model::GeoLocation {
            continent_code: Some("EU".to_string()),
            country_code: None,
            subdivision_code: None,
        });
        let mut star = rrset("9.9.9.9");
        star.geo_location = Some(crate::model::GeoLocation {
            continent_code: None,
            country_code: Some("*".to_string()),
            subdivision_code: None,
        });
        let hcs = std::collections::BTreeMap::new();
        // 0..=63 -> US country.
        let r1 = resolve_routing_policy(
            &[&us, &eu_default, &star],
            &hcs,
            Some("10.0.0.1"),
            &empty_lookup(),
        );
        assert_eq!(r1, vec!["1.1.1.1".to_string()]);
        // 128..=159 -> GB country (no exact GB record), continent EU matches.
        let r2 = resolve_routing_policy(
            &[&us, &eu_default, &star],
            &hcs,
            Some("130.0.0.1"),
            &empty_lookup(),
        );
        assert_eq!(r2, vec!["2.2.2.2".to_string()]);
        // 192..=223 -> SG / AS — falls back to the `*` default record.
        let r3 = resolve_routing_policy(
            &[&us, &eu_default, &star],
            &hcs,
            Some("200.0.0.1"),
            &empty_lookup(),
        );
        assert_eq!(r3, vec!["9.9.9.9".to_string()]);
    }

    #[test]
    fn alias_target_falls_back_to_synthetic_ip_when_state_not_wired() {
        let alias = crate::model::AliasTarget {
            hosted_zone_id: "Z3DZXE0EXAMPLE".to_string(),
            dns_name: "my-lb-1234567890.us-east-1.elb.amazonaws.com.".to_string(),
            evaluate_target_health: false,
        };
        let answers = resolve_alias_target(&alias, "A", &empty_lookup());
        assert_eq!(answers.len(), 1);
        // Synthetic fallback uses 198.51.x.x (documentation range).
        assert!(
            answers[0].starts_with("198.51."),
            "expected documentation IPv4, got {}",
            answers[0]
        );
    }

    #[test]
    fn alias_target_resolves_elbv2_load_balancer_to_real_addresses() {
        let elbv2_state: fakecloud_elbv2::SharedElbv2State = std::sync::Arc::new(
            parking_lot::RwLock::new(fakecloud_elbv2::Elbv2Accounts::new()),
        );
        {
            let mut st = elbv2_state.write();
            let account = st.get_or_create("000000000000");
            account.load_balancers.insert(
                "lb-1".to_string(),
                fakecloud_elbv2::LoadBalancer {
                    arn: "arn:aws:elasticloadbalancing:us-east-1:000000000000:loadbalancer/app/my-lb/abcdef".to_string(),
                    name: "my-lb".to_string(),
                    dns_name: "my-lb-9999999999.us-east-1.elb.amazonaws.com".to_string(),
                    canonical_hosted_zone_id: "Z35SXDOTRQ7X7K".to_string(),
                    created_time: Utc::now(),
                    scheme: "internet-facing".to_string(),
                    vpc_id: "vpc-1".to_string(),
                    state_code: "active".to_string(),
                    state_reason: None,
                    lb_type: "application".to_string(),
                    availability_zones: vec![fakecloud_elbv2::AvailabilityZone {
                        zone_name: "us-east-1a".to_string(),
                        subnet_id: "subnet-1".to_string(),
                        outpost_id: None,
                        load_balancer_addresses: vec![fakecloud_elbv2::LoadBalancerAddress {
                            ip_address: Some("203.0.113.10".to_string()),
                            allocation_id: None,
                            private_ipv4_address: None,
                            ipv6_address: Some("2001:db8::1".to_string()),
                            ipv4_prefix: None,
                            ipv6_prefix: None,
                        }],
                        source_nat_ipv6_prefixes: vec![],
                    }],
                    security_groups: vec![],
                    ip_address_type: "ipv4".to_string(),
                    customer_owned_ipv4_pool: None,
                    enforce_security_group_inbound_rules_on_private_link_traffic: None,
                    enable_prefix_for_ipv6_source_nat: None,
                    ipv4_ipam_pool_id: None,
                    tags: vec![],
                    attributes: std::collections::BTreeMap::new(),
                    minimum_capacity_units: None,
                    bound_port: None,
                },
            );
        }
        let lookup = AliasLookup {
            elbv2: Some(&elbv2_state),
            cloudfront: None,
            s3: None,
        };
        let alias = crate::model::AliasTarget {
            hosted_zone_id: "Z35SXDOTRQ7X7K".to_string(),
            dns_name: "my-lb-9999999999.us-east-1.elb.amazonaws.com.".to_string(),
            evaluate_target_health: false,
        };
        let v4 = resolve_alias_target(&alias, "A", &lookup);
        assert_eq!(v4, vec!["203.0.113.10".to_string()]);
        let v6 = resolve_alias_target(&alias, "AAAA", &lookup);
        assert_eq!(v6, vec!["2001:db8::1".to_string()]);
    }

    #[test]
    fn get_change_starts_pending_then_flips_after_threshold_reads() {
        use crate::state::StoredChange;
        let state = Arc::new(RwLock::new(Route53Accounts::default()));
        {
            let mut s = state.write();
            let account = s.accounts.entry(DEFAULT_ACCOUNT.to_string()).or_default();
            account.changes.insert(
                "C123".to_string(),
                StoredChange::pending("C123".to_string(), Utc::now(), Some("test".to_string())),
            );
        }
        let svc = Route53Service::new(state);
        let route =
            crate::router::route(&http::Method::GET, "/2013-04-01/change/C123", "").unwrap();

        for i in 1..=5 {
            svc.get_change(&route).unwrap();
            let st = svc.state.read();
            let stored = &st.accounts.get(DEFAULT_ACCOUNT).unwrap().changes["C123"];
            if i < 5 {
                assert_eq!(stored.status, "PENDING", "read {i}: expected PENDING");
            } else {
                assert_eq!(stored.status, "INSYNC", "read {i}: expected INSYNC");
            }
        }
    }

    #[test]
    fn get_change_unknown_id_returns_404() {
        let svc = Route53Service::new(Arc::new(RwLock::new(Route53Accounts::default())));
        let route =
            crate::router::route(&http::Method::GET, "/2013-04-01/change/CGHOST", "").unwrap();
        let err = match svc.get_change(&route) {
            Err(e) => e,
            Ok(_) => panic!("expected NoSuchChange"),
        };
        assert_eq!(err.code(), "NoSuchChange");
    }

    #[test]
    fn set_health_check_status_preserves_existing_reason_when_none() {
        let svc = svc_with_health_check("hc-1");
        svc.set_health_check_status(
            "hc-1",
            HealthCheckStatus::Failure,
            Some("connect() timed out".to_string()),
        );
        // Flipping back to Success must not clobber the historical
        // failure reason — GetHealthCheckLastFailureReason still returns
        // it after recovery, mirroring real Route 53.
        svc.set_health_check_status("hc-1", HealthCheckStatus::Success, None);
        let st = svc.state.read();
        let hc = &st.accounts.get(DEFAULT_ACCOUNT).unwrap().health_checks["hc-1"];
        assert_eq!(hc.status, HealthCheckStatus::Success);
        assert_eq!(
            hc.last_failure_reason.as_deref().unwrap(),
            "connect() timed out"
        );
    }

    #[test]
    fn get_health_check_status_returns_success_by_default() {
        let svc = svc_with_health_check("hc-default");
        let route = crate::router::route(
            &http::Method::GET,
            "/2013-04-01/healthcheck/hc-default/status",
            "",
        )
        .unwrap();
        let resp = svc.get_health_check_status(&route).unwrap();
        let body = std::str::from_utf8(resp.body.expect_bytes())
            .unwrap()
            .to_string();
        assert!(
            body.contains("<Status>Success: HTTP Status Code 200</Status>"),
            "body: {body}"
        );
    }

    #[test]
    fn get_health_check_status_reflects_state_failure() {
        let svc = svc_with_health_check("hc-down");
        {
            let mut state = svc.state.write();
            let hc = state
                .accounts
                .get_mut(DEFAULT_ACCOUNT)
                .unwrap()
                .health_checks
                .get_mut("hc-down")
                .unwrap();
            hc.status = HealthCheckStatus::Failure;
            hc.last_failure_reason = Some("test".to_string());
        }
        let route = crate::router::route(
            &http::Method::GET,
            "/2013-04-01/healthcheck/hc-down/status",
            "",
        )
        .unwrap();
        let resp = svc.get_health_check_status(&route).unwrap();
        let body = std::str::from_utf8(resp.body.expect_bytes())
            .unwrap()
            .to_string();
        assert!(
            body.contains("<Status>Failure: test</Status>"),
            "body: {body}"
        );
    }

    #[test]
    fn get_health_check_status_failure_without_reason_renders_canned_descriptor() {
        let svc = svc_with_health_check("hc-bare");
        {
            let mut state = svc.state.write();
            let hc = state
                .accounts
                .get_mut(DEFAULT_ACCOUNT)
                .unwrap()
                .health_checks
                .get_mut("hc-bare")
                .unwrap();
            hc.status = HealthCheckStatus::Failure;
            hc.last_failure_reason = None;
        }
        let route = crate::router::route(
            &http::Method::GET,
            "/2013-04-01/healthcheck/hc-bare/status",
            "",
        )
        .unwrap();
        let resp = svc.get_health_check_status(&route).unwrap();
        let body = std::str::from_utf8(resp.body.expect_bytes())
            .unwrap()
            .to_string();
        assert!(
            body.contains("<Status>Failure: Endpoint unreachable</Status>"),
            "body: {body}"
        );
    }

    #[test]
    fn get_health_check_status_renders_timeout_dns_insufficient_unknown() {
        let svc = svc_with_health_check("hc-flavours");
        let cases = [
            (
                HealthCheckStatus::Timeout,
                None,
                "<Status>Failure: Connection timed out</Status>",
            ),
            (
                HealthCheckStatus::Timeout,
                Some("custom timeout msg".to_string()),
                "<Status>Failure: custom timeout msg</Status>",
            ),
            (
                HealthCheckStatus::DnsError,
                None,
                "<Status>Failure: DNS resolution failed</Status>",
            ),
            (
                HealthCheckStatus::InsufficientDataPoints,
                None,
                "<Status>InsufficientDataPoints</Status>",
            ),
            (HealthCheckStatus::Unknown, None, "<Status>Unknown</Status>"),
        ];
        for (status, reason, expected) in cases {
            {
                let mut state = svc.state.write();
                let hc = state
                    .accounts
                    .get_mut(DEFAULT_ACCOUNT)
                    .unwrap()
                    .health_checks
                    .get_mut("hc-flavours")
                    .unwrap();
                hc.status = status;
                hc.last_failure_reason = reason.clone();
            }
            let route = crate::router::route(
                &http::Method::GET,
                "/2013-04-01/healthcheck/hc-flavours/status",
                "",
            )
            .unwrap();
            let resp = svc.get_health_check_status(&route).unwrap();
            let body = std::str::from_utf8(resp.body.expect_bytes())
                .unwrap()
                .to_string();
            assert!(
                body.contains(expected),
                "status={status:?} reason={reason:?} body: {body}"
            );
        }
    }

    #[test]
    fn set_health_check_status_records_reason_for_timeout_and_dns_error() {
        let svc = svc_with_health_check("hc-flav");
        assert!(svc.set_health_check_status(
            "hc-flav",
            HealthCheckStatus::Timeout,
            Some("upstream silent".to_string()),
        ));
        {
            let st = svc.state.read();
            let hc = &st.accounts.get(DEFAULT_ACCOUNT).unwrap().health_checks["hc-flav"];
            assert_eq!(hc.status, HealthCheckStatus::Timeout);
            assert_eq!(
                hc.last_failure_reason.as_deref().unwrap(),
                "upstream silent"
            );
        }
        assert!(svc.set_health_check_status(
            "hc-flav",
            HealthCheckStatus::DnsError,
            Some("NXDOMAIN".to_string()),
        ));
        {
            let st = svc.state.read();
            let hc = &st.accounts.get(DEFAULT_ACCOUNT).unwrap().health_checks["hc-flav"];
            assert_eq!(hc.status, HealthCheckStatus::DnsError);
            assert_eq!(hc.last_failure_reason.as_deref().unwrap(), "NXDOMAIN");
        }
        // InsufficientDataPoints / Unknown must not clobber prior reason
        // even when a reason is supplied (those flavours aren't
        // failure-flavoured).
        assert!(svc.set_health_check_status(
            "hc-flav",
            HealthCheckStatus::InsufficientDataPoints,
            Some("ignored".to_string()),
        ));
        {
            let st = svc.state.read();
            let hc = &st.accounts.get(DEFAULT_ACCOUNT).unwrap().health_checks["hc-flav"];
            assert_eq!(hc.status, HealthCheckStatus::InsufficientDataPoints);
            assert_eq!(hc.last_failure_reason.as_deref().unwrap(), "NXDOMAIN");
        }
    }

    // ─── TestDNSAnswer routing-policy E2E tests (U2) ──────────────────

    fn svc_with_zone(records: Vec<crate::model::ResourceRecordSet>) -> (Route53Service, String) {
        let state = Arc::new(RwLock::new(Route53Accounts::default()));
        let zone_id = "Z123ABC".to_string();
        {
            let mut s = state.write();
            let account = s.accounts.entry(DEFAULT_ACCOUNT.to_string()).or_default();
            account.hosted_zones.insert(
                zone_id.clone(),
                crate::state::StoredHostedZone {
                    id: zone_id.clone(),
                    name: "example.com.".to_string(),
                    caller_reference: "ref".to_string(),
                    comment: None,
                    private_zone: false,
                    features: None,
                    vpcs: vec![],
                    delegation_set_id: None,
                    name_servers: vec![],
                    created_time: Utc::now(),
                    resource_record_sets: records,
                },
            );
        }
        (Route53Service::new(state), zone_id)
    }

    fn req_for_dns(zone_id: &str, name: &str, rtype: &str, edns0: Option<&str>) -> AwsRequest {
        let mut params: std::collections::HashMap<String, String> =
            std::collections::HashMap::new();
        params.insert("hostedzoneid".to_string(), zone_id.to_string());
        params.insert("recordname".to_string(), name.to_string());
        params.insert("recordtype".to_string(), rtype.to_string());
        if let Some(s) = edns0 {
            params.insert("edns0clientsubnetip".to_string(), s.to_string());
        }
        let raw_path = "/2013-04-01/testdnsanswer".to_string();
        let segs: Vec<String> = raw_path
            .trim_start_matches('/')
            .split('/')
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .collect();
        AwsRequest {
            service: "route53".to_string(),
            action: "TestDNSAnswer".to_string(),
            region: "us-east-1".to_string(),
            account_id: DEFAULT_ACCOUNT.to_string(),
            request_id: "test".to_string(),
            headers: HeaderMap::new(),
            query_params: params,
            body: Bytes::new(),
            body_stream: parking_lot::Mutex::new(None),
            path_segments: segs,
            raw_path,
            raw_query: String::new(),
            method: http::Method::GET,
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        }
    }

    #[tokio::test]
    async fn get_missing_hosted_zone_returns_errorresponse_wrapper() {
        // Route 53's REST-XML errors must be wrapped in <ErrorResponse> (not
        // S3's bare <Error>) so the AWS SDK can read the code; otherwise the
        // provider's post-destroy GetHostedZone check sees "UnknownError".
        let (svc, _) = svc_with_zone(vec![]);
        let req = AwsRequest {
            service: "route53".to_string(),
            action: "GetHostedZone".to_string(),
            region: "us-east-1".to_string(),
            account_id: DEFAULT_ACCOUNT.to_string(),
            request_id: "rid-1".to_string(),
            headers: HeaderMap::new(),
            query_params: std::collections::HashMap::new(),
            body: Bytes::new(),
            body_stream: parking_lot::Mutex::new(None),
            path_segments: vec!["2013-04-01".into(), "hostedzone".into(), "ZMISSING".into()],
            raw_path: "/2013-04-01/hostedzone/ZMISSING".to_string(),
            raw_query: String::new(),
            method: http::Method::GET,
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        };
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::NOT_FOUND);
        let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
        assert!(body.contains("<ErrorResponse"), "missing wrapper: {body}");
        assert!(
            body.contains("<Code>NoSuchHostedZone</Code>"),
            "missing code: {body}"
        );
    }

    fn extract_record_data(body: &str) -> Vec<String> {
        let mut out = Vec::new();
        let mut rest = body;
        while let Some(start) = rest.find("<RecordDataEntry>") {
            rest = &rest[start + "<RecordDataEntry>".len()..];
            let Some(end) = rest.find("</RecordDataEntry>") else {
                break;
            };
            out.push(rest[..end].to_string());
            rest = &rest[end + "</RecordDataEntry>".len()..];
        }
        out
    }

    #[test]
    fn test_dns_answer_simple_returns_records() {
        let r = rrset("1.2.3.4");
        let (svc, zid) = svc_with_zone(vec![r]);
        let req = req_for_dns(&zid, "x.example.com", "A", None);
        let resp = svc.test_dns_answer(&req).unwrap();
        let body = std::str::from_utf8(resp.body.expect_bytes())
            .unwrap()
            .to_string();
        assert_eq!(extract_record_data(&body), vec!["1.2.3.4".to_string()]);
    }

    #[test]
    fn test_dns_answer_weighted_picks_proportional_to_weight() {
        let mut a = rrset("1.1.1.1");
        a.weight = Some(1);
        a.set_identifier = Some("light".to_string());
        let mut b = rrset("9.9.9.9");
        b.weight = Some(99);
        b.set_identifier = Some("heavy".to_string());
        let (svc, zid) = svc_with_zone(vec![a, b]);
        // Sweep distinct subnets to exercise the weighted distribution.
        let mut heavy = 0usize;
        let mut light = 0usize;
        for i in 0..200 {
            let subnet = format!("203.0.113.{}", i % 200);
            let req = req_for_dns(&zid, "x.example.com", "A", Some(&subnet));
            let resp = svc.test_dns_answer(&req).unwrap();
            let body = std::str::from_utf8(resp.body.expect_bytes())
                .unwrap()
                .to_string();
            let data = extract_record_data(&body);
            assert_eq!(data.len(), 1);
            if data[0] == "9.9.9.9" {
                heavy += 1;
            } else if data[0] == "1.1.1.1" {
                light += 1;
            }
        }
        // With 99:1 weight ratio, heavy should dominate by a large margin.
        assert!(
            heavy > 10 * light,
            "expected heavy-weighted record to dominate, got heavy={heavy} light={light}"
        );
    }

    #[test]
    fn test_dns_answer_failover_uses_primary_when_healthy() {
        let mut p = rrset("10.0.0.1");
        p.failover = Some("PRIMARY".to_string());
        p.health_check_id = Some("hc-up".to_string());
        let mut s = rrset("10.0.0.2");
        s.failover = Some("SECONDARY".to_string());
        let (svc, zid) = svc_with_zone(vec![p, s]);
        // Seed the health check as healthy.
        {
            let mut st = svc.state.write();
            let acct = st.accounts.get_mut(DEFAULT_ACCOUNT).unwrap();
            acct.health_checks.insert(
                "hc-up".to_string(),
                StoredHealthCheck {
                    id: "hc-up".to_string(),
                    caller_reference: "r".to_string(),
                    version: 1,
                    config: HealthCheckConfig::default(),
                    created_time: Utc::now(),
                    status: HealthCheckStatus::Success,
                    last_failure_reason: None,
                },
            );
        }
        let req = req_for_dns(&zid, "x.example.com", "A", None);
        let resp = svc.test_dns_answer(&req).unwrap();
        let body = std::str::from_utf8(resp.body.expect_bytes())
            .unwrap()
            .to_string();
        assert_eq!(extract_record_data(&body), vec!["10.0.0.1".to_string()]);
    }

    #[test]
    fn test_dns_answer_failover_falls_back_to_secondary_when_primary_unhealthy() {
        let mut p = rrset("10.0.0.1");
        p.failover = Some("PRIMARY".to_string());
        p.health_check_id = Some("hc-flip".to_string());
        let mut s = rrset("10.0.0.2");
        s.failover = Some("SECONDARY".to_string());
        let (svc, zid) = svc_with_zone(vec![p, s]);
        {
            let mut st = svc.state.write();
            let acct = st.accounts.get_mut(DEFAULT_ACCOUNT).unwrap();
            acct.health_checks.insert(
                "hc-flip".to_string(),
                StoredHealthCheck {
                    id: "hc-flip".to_string(),
                    caller_reference: "r".to_string(),
                    version: 1,
                    config: HealthCheckConfig::default(),
                    created_time: Utc::now(),
                    status: HealthCheckStatus::Success,
                    last_failure_reason: None,
                },
            );
        }
        // Flip primary's health to failure via the public helper.
        assert!(svc.set_health_check_status(
            "hc-flip",
            HealthCheckStatus::Failure,
            Some("simulated outage".to_string()),
        ));
        let req = req_for_dns(&zid, "x.example.com", "A", None);
        let resp = svc.test_dns_answer(&req).unwrap();
        let body = std::str::from_utf8(resp.body.expect_bytes())
            .unwrap()
            .to_string();
        assert_eq!(extract_record_data(&body), vec!["10.0.0.2".to_string()]);
    }

    #[test]
    fn test_dns_answer_multivalue_returns_up_to_8_healthy() {
        // Case 1: 10 records, 6 healthy → 6 returned.
        let mut records = Vec::new();
        for i in 0..6 {
            let mut r = rrset(&format!("10.0.0.{i}"));
            r.multi_value_answer = Some(true);
            r.set_identifier = Some(format!("h-{i}"));
            records.push(r);
        }
        for i in 6..10 {
            let mut r = rrset(&format!("10.0.0.{i}"));
            r.multi_value_answer = Some(true);
            r.set_identifier = Some(format!("u-{i}"));
            r.health_check_id = Some("hc-down".to_string());
            records.push(r);
        }
        let (svc, zid) = svc_with_zone(records);
        {
            let mut st = svc.state.write();
            let acct = st.accounts.get_mut(DEFAULT_ACCOUNT).unwrap();
            acct.health_checks.insert(
                "hc-down".to_string(),
                StoredHealthCheck {
                    id: "hc-down".to_string(),
                    caller_reference: "r".to_string(),
                    version: 1,
                    config: HealthCheckConfig::default(),
                    created_time: Utc::now(),
                    status: HealthCheckStatus::Failure,
                    last_failure_reason: None,
                },
            );
        }
        let req = req_for_dns(&zid, "x.example.com", "A", None);
        let resp = svc.test_dns_answer(&req).unwrap();
        let body = std::str::from_utf8(resp.body.expect_bytes())
            .unwrap()
            .to_string();
        assert_eq!(extract_record_data(&body).len(), 6);

        // Case 2: 12 healthy → cap at 8.
        let mut records = Vec::new();
        for i in 0..12 {
            let mut r = rrset(&format!("10.0.1.{i}"));
            r.multi_value_answer = Some(true);
            r.set_identifier = Some(format!("h-{i}"));
            records.push(r);
        }
        let (svc2, zid2) = svc_with_zone(records);
        let req2 = req_for_dns(&zid2, "x.example.com", "A", None);
        let resp2 = svc2.test_dns_answer(&req2).unwrap();
        let body2 = std::str::from_utf8(resp2.body.expect_bytes())
            .unwrap()
            .to_string();
        assert_eq!(extract_record_data(&body2).len(), 8);
    }

    #[test]
    fn test_dns_answer_geolocation_matches_country_to_record() {
        let mut us = rrset("1.0.0.1");
        us.set_identifier = Some("us".to_string());
        us.geo_location = Some(crate::model::GeoLocation {
            country_code: Some("US".to_string()),
            ..Default::default()
        });
        let mut default = rrset("9.0.0.9");
        default.set_identifier = Some("default".to_string());
        default.geo_location = Some(crate::model::GeoLocation {
            country_code: Some("*".to_string()),
            ..Default::default()
        });
        let (svc, zid) = svc_with_zone(vec![us, default]);

        // EDNS0 IP `10.x.y.z` infers country "US" → should hit the US record.
        let req = req_for_dns(&zid, "x.example.com", "A", Some("10.0.0.1"));
        let resp = svc.test_dns_answer(&req).unwrap();
        let body = std::str::from_utf8(resp.body.expect_bytes())
            .unwrap()
            .to_string();
        assert_eq!(extract_record_data(&body), vec!["1.0.0.1".to_string()]);

        // EDNS0 IP `200.x.y.z` infers SG (continent AS) → no country/continent
        // match, falls back to the default record.
        let req2 = req_for_dns(&zid, "x.example.com", "A", Some("200.0.0.1"));
        let resp2 = svc.test_dns_answer(&req2).unwrap();
        let body2 = std::str::from_utf8(resp2.body.expect_bytes())
            .unwrap()
            .to_string();
        assert_eq!(extract_record_data(&body2), vec!["9.0.0.9".to_string()]);
    }

    #[test]
    fn test_dns_answer_alias_target_synthesizes_record() {
        let mut r = rrset("ignored");
        r.resource_records = None;
        r.alias_target = Some(crate::model::AliasTarget {
            hosted_zone_id: "Z2FDTNDATAQYW2".to_string(),
            dns_name: "example-lb-1234.us-east-1.elb.amazonaws.com.".to_string(),
            evaluate_target_health: false,
        });
        let (svc, zid) = svc_with_zone(vec![r]);
        let req = req_for_dns(&zid, "x.example.com", "A", None);
        let resp = svc.test_dns_answer(&req).unwrap();
        let body = std::str::from_utf8(resp.body.expect_bytes())
            .unwrap()
            .to_string();
        let data = extract_record_data(&body);
        assert_eq!(data.len(), 1);
        // Synthesised IPv4 in 198.51.0.0/16.
        assert!(
            data[0].starts_with("198.51."),
            "expected synthesised A in 198.51.0.0/16, got {}",
            data[0]
        );
    }
}
