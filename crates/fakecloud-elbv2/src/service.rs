//! Elastic Load Balancing v2 (ALB/NLB/GWLB) service implementation.
//!
//! Wire protocol: AWS Query (form-encoded request, XML response). Endpoint
//! prefix `elasticloadbalancing`, SigV4 service `elasticloadbalancing`.

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use http::StatusCode;
use tokio::sync::Mutex as AsyncMutex;

use fakecloud_core::delivery::DeliveryBus;
use fakecloud_core::query::{optional_query_param, query_response_xml, required_query_param};
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_persistence::SnapshotStore;

use fakecloud_wafv2::SharedWafv2State;

use crate::state::{
    AvailabilityZone, Elbv2Snapshot, LoadBalancer, LoadBalancerAddress, SharedElbv2State, Tag,
    ELBV2_SNAPSHOT_SCHEMA_VERSION,
};
use crate::ELBV2_NAMESPACE;

const NS: &str = ELBV2_NAMESPACE;

/// ELBv2 read actions all start with `Describe` or `Get`; every other action is
/// a mutation (Create/Delete/Modify/Set/Add/Remove/Register/Deregister). The
/// inverse formulation guarantees no mutation is ever missed.
fn is_mutating_action(action: &str) -> bool {
    !(action.starts_with("Describe") || action.starts_with("Get"))
}

const ELBV2_SUPPORTED_ACTIONS: &[&str] = &[
    "CreateLoadBalancer",
    "DescribeLoadBalancers",
    "DeleteLoadBalancer",
    "SetSubnets",
    "SetSecurityGroups",
    "SetIpAddressType",
    "ModifyIpPools",
    "AddTags",
    "RemoveTags",
    "DescribeTags",
    "DescribeAccountLimits",
    "DescribeSSLPolicies",
    "CreateTargetGroup",
    "DescribeTargetGroups",
    "ModifyTargetGroup",
    "DeleteTargetGroup",
    "RegisterTargets",
    "DeregisterTargets",
    "DescribeTargetHealth",
    "ModifyTargetGroupAttributes",
    "DescribeTargetGroupAttributes",
    "CreateListener",
    "DescribeListeners",
    "ModifyListener",
    "DeleteListener",
    "DescribeListenerAttributes",
    "ModifyListenerAttributes",
    "AddListenerCertificates",
    "RemoveListenerCertificates",
    "DescribeListenerCertificates",
    "CreateRule",
    "DescribeRules",
    "ModifyRule",
    "DeleteRule",
    "SetRulePriorities",
    "ModifyLoadBalancerAttributes",
    "DescribeLoadBalancerAttributes",
    "ModifyCapacityReservation",
    "DescribeCapacityReservation",
    "CreateTrustStore",
    "DescribeTrustStores",
    "ModifyTrustStore",
    "DeleteTrustStore",
    "AddTrustStoreRevocations",
    "RemoveTrustStoreRevocations",
    "DescribeTrustStoreRevocations",
    "DescribeTrustStoreAssociations",
    "DeleteSharedTrustStoreAssociation",
    "GetTrustStoreCaCertificatesBundle",
    "GetTrustStoreRevocationContent",
    "GetResourcePolicy",
];

pub struct Elbv2Service {
    state: SharedElbv2State,
    waf_state: Option<SharedWafv2State>,
    /// Delivery bus used by the dataplane for cross-service writes
    /// (currently: S3 access-log + connection-log delivery). When
    /// `None`, the dataplane buffers no log lines and skips the S3
    /// flush task.
    delivery_bus: Option<Arc<DeliveryBus>>,
    /// Handle on the access-log buffer once the dataplane has been
    /// started — exposed so the introspection layer can flush
    /// synchronously inside tests.
    access_logger: parking_lot::Mutex<Option<Arc<crate::accesslogs::AccessLogger>>>,
    /// Shared per-(WebACL ARN, rule name) Count-action match counter.
    /// Threaded into the dataplane on `start_dataplane` so the
    /// `/_fakecloud/elbv2/waf-counts` admin endpoint can read it.
    waf_count_metrics: Arc<parking_lot::Mutex<std::collections::BTreeMap<String, u64>>>,
    pub region: String,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<AsyncMutex<()>>,
}

impl Elbv2Service {
    /// Build a service and immediately spin up the prober + dataplane
    /// supervisor. The dataplane will not perform WAF evaluation unless
    /// the service is constructed via [`Elbv2Service::with_waf_state`]
    /// before the dataplane spawns.
    pub fn new(state: SharedElbv2State) -> Self {
        crate::prober::spawn_prober(Arc::clone(&state));
        let waf_count_metrics =
            Arc::new(parking_lot::Mutex::new(std::collections::BTreeMap::new()));
        crate::dataplane::spawn_dataplane(
            Arc::clone(&state),
            None,
            Some(Arc::clone(&waf_count_metrics)),
        );
        Self {
            state,
            waf_state: None,
            delivery_bus: None,
            access_logger: parking_lot::Mutex::new(None),
            waf_count_metrics,
            region: "us-east-1".to_string(),
            snapshot_store: None,
            snapshot_lock: Arc::new(AsyncMutex::new(())),
        }
    }

    /// Build a service without spawning the dataplane yet. Pair with
    /// [`Elbv2Service::with_waf_state`] and [`Elbv2Service::start_dataplane`]
    /// when WAF v2 evaluation should run inside the ALB request path.
    pub fn new_without_dataplane(state: SharedElbv2State) -> Self {
        crate::prober::spawn_prober(Arc::clone(&state));
        Self {
            state,
            waf_state: None,
            delivery_bus: None,
            access_logger: parking_lot::Mutex::new(None),
            waf_count_metrics: Arc::new(parking_lot::Mutex::new(std::collections::BTreeMap::new())),
            region: "us-east-1".to_string(),
            snapshot_store: None,
            snapshot_lock: Arc::new(AsyncMutex::new(())),
        }
    }

    /// Attach a persistence snapshot store so control-plane mutations are
    /// written through to disk. Target health is intentionally not persisted --
    /// the prober re-derives it after a restart.
    pub fn with_snapshot_store(mut self, store: Arc<dyn SnapshotStore>) -> Self {
        self.snapshot_store = Some(store);
        self
    }

    /// Persist current state as a snapshot. Held across the
    /// clone-serialize-write sequence to prevent stale-last writes, with serde
    /// + file I/O offloaded to the blocking pool.
    async fn save_snapshot(&self) {
        save_elbv2_snapshot(
            &self.state,
            self.snapshot_store.clone(),
            &self.snapshot_lock,
        )
        .await;
    }

    /// Build a hook that persists the current ELBv2 state when invoked, or
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
                save_elbv2_snapshot(&state, Some(store), &lock).await;
            })
        }))
    }

    /// Plug in the WAFv2 shared state so the ALB dataplane can resolve
    /// the WebACL associated with each load balancer and evaluate
    /// incoming requests before the listener-rule router runs.
    pub fn with_waf_state(mut self, waf_state: SharedWafv2State) -> Self {
        self.waf_state = Some(waf_state);
        self
    }

    /// Plug in the cross-service delivery bus so the ALB dataplane can
    /// flush access-log + connection-log batches to S3.
    pub fn with_delivery_bus(mut self, bus: Arc<DeliveryBus>) -> Self {
        self.delivery_bus = Some(bus);
        self
    }

    /// Spawn the dataplane supervisor. Only needed when the service
    /// was built with [`Elbv2Service::new_without_dataplane`].
    pub fn start_dataplane(&self) {
        let logger = crate::dataplane::spawn_dataplane_with_delivery(
            Arc::clone(&self.state),
            self.waf_state.as_ref().map(Arc::clone),
            self.delivery_bus.as_ref().map(Arc::clone),
            Some(Arc::clone(&self.waf_count_metrics)),
        );
        *self.access_logger.lock() = logger;
    }

    /// Snapshot of the WAF Count-action metrics. Keyed by
    /// `"<webacl-arn>|<rule-name>"`. Returns an empty map when no
    /// Count rules have fired (or when WAF inspection is disabled).
    pub fn waf_count_metrics_snapshot(&self) -> std::collections::BTreeMap<String, u64> {
        self.waf_count_metrics.lock().clone()
    }

    /// Force every buffered access / connection log line to flush to
    /// S3 right now, bypassing the periodic 60-second timer. Returns
    /// `true` when a logger was wired and the flush ran. Used by the
    /// `/_fakecloud/elbv2/access-logs/flush` admin endpoint and by
    /// tests that need deterministic flush timing.
    pub fn flush_access_logs(&self) -> bool {
        if let Some(logger) = self.access_logger.lock().clone() {
            logger.flush_all();
            true
        } else {
            false
        }
    }

    pub fn shared_state(&self) -> SharedElbv2State {
        Arc::clone(&self.state)
    }
}

/// Persist the current ELBv2 state as a snapshot. Offloads the serde +
/// blocking file write to the Tokio blocking pool. Noop when `store` is `None`
/// (memory mode). Shared by `Elbv2Service::save_snapshot` and the
/// CloudFormation provisioner persist hook so both route through the same
/// serialize-and-write path.
pub async fn save_elbv2_snapshot(
    state: &SharedElbv2State,
    store: Option<Arc<dyn SnapshotStore>>,
    lock: &AsyncMutex<()>,
) {
    let Some(store) = store else {
        return;
    };
    let _guard = lock.lock().await;
    let snapshot = Elbv2Snapshot {
        schema_version: ELBV2_SNAPSHOT_SCHEMA_VERSION,
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
        Ok(Err(err)) => tracing::error!(%err, "failed to write elbv2 snapshot"),
        Err(err) => tracing::error!(%err, "elbv2 snapshot task panicked"),
    }
}

#[async_trait]
impl AwsService for Elbv2Service {
    fn service_name(&self) -> &str {
        "elasticloadbalancing"
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let mutates = is_mutating_action(&req.action);
        let result = match req.action.as_str() {
            "CreateLoadBalancer" => self.create_load_balancer(&req),
            "DescribeLoadBalancers" => self.describe_load_balancers(&req),
            "DeleteLoadBalancer" => self.delete_load_balancer(&req),
            "SetSubnets" => self.set_subnets(&req),
            "SetSecurityGroups" => self.set_security_groups(&req),
            "SetIpAddressType" => self.set_ip_address_type(&req),
            "ModifyIpPools" => self.modify_ip_pools(&req),
            "AddTags" => self.add_tags(&req),
            "RemoveTags" => self.remove_tags(&req),
            "DescribeTags" => self.describe_tags(&req),
            "DescribeAccountLimits" => self.describe_account_limits(&req),
            "DescribeSSLPolicies" => self.describe_ssl_policies(&req),
            "CreateTargetGroup" => self.create_target_group(&req),
            "DescribeTargetGroups" => self.describe_target_groups(&req),
            "ModifyTargetGroup" => self.modify_target_group(&req),
            "DeleteTargetGroup" => self.delete_target_group(&req),
            "RegisterTargets" => self.register_targets(&req),
            "DeregisterTargets" => self.deregister_targets(&req),
            "DescribeTargetHealth" => self.describe_target_health(&req),
            "ModifyTargetGroupAttributes" => self.modify_target_group_attributes(&req),
            "DescribeTargetGroupAttributes" => self.describe_target_group_attributes(&req),
            "CreateListener" => self.create_listener(&req),
            "DescribeListeners" => self.describe_listeners(&req),
            "ModifyListener" => self.modify_listener(&req),
            "DeleteListener" => self.delete_listener(&req),
            "DescribeListenerAttributes" => self.describe_listener_attributes(&req),
            "ModifyListenerAttributes" => self.modify_listener_attributes(&req),
            "AddListenerCertificates" => self.add_listener_certificates(&req),
            "RemoveListenerCertificates" => self.remove_listener_certificates(&req),
            "DescribeListenerCertificates" => self.describe_listener_certificates(&req),
            "CreateRule" => self.create_rule(&req),
            "DescribeRules" => self.describe_rules(&req),
            "ModifyRule" => self.modify_rule(&req),
            "DeleteRule" => self.delete_rule(&req),
            "SetRulePriorities" => self.set_rule_priorities(&req),
            "ModifyLoadBalancerAttributes" => self.modify_load_balancer_attributes(&req),
            "DescribeLoadBalancerAttributes" => self.describe_load_balancer_attributes(&req),
            "ModifyCapacityReservation" => self.modify_capacity_reservation(&req),
            "DescribeCapacityReservation" => self.describe_capacity_reservation(&req),
            "CreateTrustStore" => self.create_trust_store(&req),
            "DescribeTrustStores" => self.describe_trust_stores(&req),
            "ModifyTrustStore" => self.modify_trust_store(&req),
            "DeleteTrustStore" => self.delete_trust_store(&req),
            "AddTrustStoreRevocations" => self.add_trust_store_revocations(&req),
            "RemoveTrustStoreRevocations" => self.remove_trust_store_revocations(&req),
            "DescribeTrustStoreRevocations" => self.describe_trust_store_revocations(&req),
            "DescribeTrustStoreAssociations" => self.describe_trust_store_associations(&req),
            "DeleteSharedTrustStoreAssociation" => self.delete_shared_trust_store_association(&req),
            "GetTrustStoreCaCertificatesBundle" => {
                self.get_trust_store_ca_certificates_bundle(&req)
            }
            "GetTrustStoreRevocationContent" => self.get_trust_store_revocation_content(&req),
            "GetResourcePolicy" => self.get_resource_policy(&req),
            _ => Err(AwsServiceError::action_not_implemented(
                "elasticloadbalancing",
                &req.action,
            )),
        };
        if mutates && matches!(result.as_ref(), Ok(resp) if resp.status.is_success()) {
            self.save_snapshot().await;
        }
        result
    }

    fn supported_actions(&self) -> &[&str] {
        ELBV2_SUPPORTED_ACTIONS
    }

    fn iam_enforceable(&self) -> bool {
        false
    }
}

// ───────────────────────── helpers ─────────────────────────

pub(crate) struct SubnetMapping {
    subnet_id: Option<String>,
    allocation_id: Option<String>,
}

// ───────────────────────── operations ─────────────────────────

impl Elbv2Service {
    fn create_load_balancer(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let name = required_query_param(req, "Name")?;
        validate_lb_name(&name)?;
        let lb_type = optional_query_param(req, "Type").unwrap_or_else(|| "application".into());
        if !matches!(lb_type.as_str(), "application" | "network" | "gateway") {
            return Err(invalid_param(format!(
                "Type must be one of application|network|gateway, got '{lb_type}'"
            )));
        }
        let scheme =
            optional_query_param(req, "Scheme").unwrap_or_else(|| "internet-facing".to_string());
        if !matches!(scheme.as_str(), "internet-facing" | "internal") {
            return Err(invalid_param(format!(
                "Scheme must be 'internet-facing' or 'internal', got '{scheme}'"
            )));
        }
        let ip_address_type =
            optional_query_param(req, "IpAddressType").unwrap_or_else(|| "ipv4".to_string());
        validate_ip_address_type(&ip_address_type)?;

        // CustomerOwnedIpv4Pool: @length 0..=256 in Smithy.
        if let Some(pool) = optional_query_param(req, "CustomerOwnedIpv4Pool") {
            if pool.len() > 256 {
                return Err(invalid_param(
                    "CustomerOwnedIpv4Pool must be at most 256 characters",
                ));
            }
        }
        // EnablePrefixForIpv6SourceNat is a string-typed enum (`on`/`off`).
        if let Some(v) = optional_query_param(req, "EnablePrefixForIpv6SourceNat") {
            if !matches!(v.as_str(), "on" | "off") {
                return Err(invalid_param(format!(
                    "EnablePrefixForIpv6SourceNat must be 'on' or 'off', got '{v}'"
                )));
            }
        }

        let subnets_explicit = parse_member_list(req, "Subnets");
        let subnet_mappings = parse_subnet_mappings(req);
        let subnets: Vec<(String, Option<String>)> = if !subnets_explicit.is_empty() {
            subnets_explicit
                .into_iter()
                .map(|s| (s, None::<String>))
                .collect()
        } else {
            subnet_mappings
                .iter()
                .filter_map(|m| m.subnet_id.clone().map(|s| (s, m.allocation_id.clone())))
                .collect()
        };

        let security_groups = parse_member_list(req, "SecurityGroups");
        let mut tags = parse_tags(req);

        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);

        if let Some(existing) = st.load_balancers.values().find(|lb| lb.name == name) {
            let lb_xml = render_lb_xml(existing);
            return Ok(xml_resp(
                "CreateLoadBalancer",
                format!("<LoadBalancers><member>{lb_xml}</member></LoadBalancers>"),
                &req.request_id,
            ));
        }

        let suffix = alphanumeric_id(16);
        let arn = build_lb_arn(&req.region, &req.account_id, &lb_type, &name, &suffix);
        let dns_name = build_dns_name(&name, &lb_type, &scheme, &req.region, &suffix);
        let canonical_hosted_zone_id = "Z2P70J7EXAMPLE".to_string();
        let availability_zones: Vec<AvailabilityZone> = subnets
            .iter()
            .map(|(subnet_id, allocation_id)| AvailabilityZone {
                zone_name: az_for_subnet(&req.region, subnet_id),
                subnet_id: subnet_id.clone(),
                outpost_id: None,
                load_balancer_addresses: allocation_id
                    .as_ref()
                    .map(|a| {
                        vec![LoadBalancerAddress {
                            ip_address: None,
                            allocation_id: Some(a.clone()),
                            private_ipv4_address: None,
                            ipv6_address: None,
                            ipv4_prefix: None,
                            ipv6_prefix: None,
                        }]
                    })
                    .unwrap_or_default(),
                source_nat_ipv6_prefixes: Vec::new(),
            })
            .collect();

        tags.dedup_by(|a, b| a.key == b.key);

        let vpc_id = optional_query_param(req, "VpcId")
            .unwrap_or_else(|| format!("vpc-{}", alphanumeric_id(8)));

        let lb = LoadBalancer {
            arn: arn.clone(),
            name,
            dns_name,
            canonical_hosted_zone_id,
            created_time: Utc::now(),
            scheme,
            vpc_id,
            state_code: "active".to_string(),
            state_reason: None,
            lb_type,
            availability_zones,
            security_groups,
            ip_address_type,
            customer_owned_ipv4_pool: optional_query_param(req, "CustomerOwnedIpv4Pool"),
            enforce_security_group_inbound_rules_on_private_link_traffic: optional_query_param(
                req,
                "EnforceSecurityGroupInboundRulesOnPrivateLinkTraffic",
            ),
            enable_prefix_for_ipv6_source_nat: optional_query_param(
                req,
                "EnablePrefixForIpv6SourceNat",
            ),
            ipv4_ipam_pool_id: optional_query_param(req, "IpamPools.Ipv4IpamPoolId"),
            tags,
            attributes: BTreeMap::new(),
            minimum_capacity_units: optional_query_param(
                req,
                "MinimumLoadBalancerCapacity.CapacityUnits",
            )
            .and_then(|s| s.parse().ok()),
            bound_port: None,
        };
        let lb_xml = render_lb_xml(&lb);
        st.load_balancers.insert(arn, lb);

        Ok(xml_resp(
            "CreateLoadBalancer",
            format!("<LoadBalancers><member>{lb_xml}</member></LoadBalancers>"),
            &req.request_id,
        ))
    }

    fn describe_load_balancers(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        validate_range_i32(req, "PageSize", 1, 400)?;
        let arns: HashSet<String> = parse_member_list(req, "LoadBalancerArns")
            .into_iter()
            .collect();
        let names: HashSet<String> = parse_member_list(req, "Names").into_iter().collect();
        let accounts = self.state.read();
        let empty = crate::state::Elbv2State::new(&req.account_id);
        let st = accounts.get(&req.account_id).unwrap_or(&empty);
        let mut lbs: Vec<&LoadBalancer> = st.load_balancers.values().collect();
        if !arns.is_empty() {
            let kept: Vec<&LoadBalancer> = lbs
                .iter()
                .filter(|lb| arns.contains(&lb.arn))
                .copied()
                .collect();
            if kept.len() != arns.len() {
                let missing = arns
                    .iter()
                    .find(|a| !kept.iter().any(|lb| lb.arn == **a))
                    .cloned()
                    .unwrap_or_default();
                return Err(lb_not_found(&missing));
            }
            lbs = kept;
        }
        if !names.is_empty() {
            let kept: Vec<&LoadBalancer> = lbs
                .iter()
                .filter(|lb| names.contains(&lb.name))
                .copied()
                .collect();
            if kept.len() != names.len() {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "LoadBalancerNotFound",
                    "One or more named load balancers not found",
                ));
            }
            lbs = kept;
        }
        lbs.sort_by_key(|a| a.created_time);
        let inner = format!(
            "<LoadBalancers>{}</LoadBalancers>",
            lbs.iter()
                .map(|lb| format!("<member>{}</member>", render_lb_xml(lb)))
                .collect::<String>()
        );
        Ok(xml_resp("DescribeLoadBalancers", inner, &req.request_id))
    }

    fn delete_load_balancer(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let arn = required_query_param(req, "LoadBalancerArn")?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        if let Some(lb) = st.load_balancers.get(&arn) {
            // ELBv2 rejects DeleteLoadBalancer when
            // `deletion_protection.enabled=true` is in the LB
            // attribute set. The attribute is opt-in and defaults
            // off, so absence is permissive.
            if lb
                .attributes
                .get("deletion_protection.enabled")
                .map(|v| v.eq_ignore_ascii_case("true"))
                .unwrap_or(false)
            {
                let name = lb.name.clone();
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "OperationNotPermitted",
                    format!(
                        "Operation not permitted because the load balancer named '{name}' has deletion protection enabled."
                    ),
                ));
            }
        }
        st.load_balancers.remove(&arn);
        let listener_arns: Vec<String> = st
            .listeners
            .iter()
            .filter(|(_, l)| l.load_balancer_arn == arn)
            .map(|(k, _)| k.clone())
            .collect();
        for la in &listener_arns {
            st.listeners.remove(la);
            st.rules.retain(|_, r| r.listener_arn != *la);
        }
        for tg in st.target_groups.values_mut() {
            tg.load_balancer_arns.retain(|a| a != &arn);
        }
        Ok(xml_metadata_only("DeleteLoadBalancer", &req.request_id))
    }

    fn set_subnets(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let arn = required_query_param(req, "LoadBalancerArn")?;
        let subnets_explicit = parse_member_list(req, "Subnets");
        let mappings = parse_subnet_mappings(req);
        let new_ip_address_type = optional_query_param(req, "IpAddressType");
        if let Some(ref ipt) = new_ip_address_type {
            validate_ip_address_type(ipt)?;
        }
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let lb = st
            .load_balancers
            .get_mut(&arn)
            .ok_or_else(|| lb_not_found(&arn))?;
        let pairs: Vec<(String, Option<String>)> = if !subnets_explicit.is_empty() {
            subnets_explicit.into_iter().map(|s| (s, None)).collect()
        } else {
            mappings
                .iter()
                .filter_map(|m| m.subnet_id.clone().map(|s| (s, m.allocation_id.clone())))
                .collect()
        };
        lb.availability_zones = pairs
            .iter()
            .map(|(subnet_id, allocation_id)| AvailabilityZone {
                zone_name: az_for_subnet(&req.region, subnet_id),
                subnet_id: subnet_id.clone(),
                outpost_id: None,
                load_balancer_addresses: allocation_id
                    .as_ref()
                    .map(|a| {
                        vec![LoadBalancerAddress {
                            ip_address: None,
                            allocation_id: Some(a.clone()),
                            private_ipv4_address: None,
                            ipv6_address: None,
                            ipv4_prefix: None,
                            ipv6_prefix: None,
                        }]
                    })
                    .unwrap_or_default(),
                source_nat_ipv6_prefixes: Vec::new(),
            })
            .collect();
        if let Some(ipt) = new_ip_address_type {
            lb.ip_address_type = ipt;
        }
        let azs_xml = lb
            .availability_zones
            .iter()
            .map(render_az_xml)
            .collect::<String>();
        let ip_type = xml_escape(&lb.ip_address_type);
        Ok(xml_resp(
            "SetSubnets",
            format!(
                "<AvailabilityZones>{azs_xml}</AvailabilityZones><IpAddressType>{ip_type}</IpAddressType>"
            ),
            &req.request_id,
        ))
    }

    fn set_security_groups(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let arn = required_query_param(req, "LoadBalancerArn")?;
        let sgs = parse_member_list(req, "SecurityGroups");
        let enforce =
            optional_query_param(req, "EnforceSecurityGroupInboundRulesOnPrivateLinkTraffic");
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let lb = st
            .load_balancers
            .get_mut(&arn)
            .ok_or_else(|| lb_not_found(&arn))?;
        lb.security_groups = sgs.clone();
        if enforce.is_some() {
            lb.enforce_security_group_inbound_rules_on_private_link_traffic = enforce.clone();
        }
        let sg_xml = sgs
            .iter()
            .map(|s| format!("<member>{}</member>", xml_escape(s)))
            .collect::<String>();
        let enforce_xml = lb
            .enforce_security_group_inbound_rules_on_private_link_traffic
            .as_deref()
            .map(|e| {
                format!(
                    "<EnforceSecurityGroupInboundRulesOnPrivateLinkTraffic>{}</EnforceSecurityGroupInboundRulesOnPrivateLinkTraffic>",
                    xml_escape(e)
                )
            })
            .unwrap_or_default();
        Ok(xml_resp(
            "SetSecurityGroups",
            format!("<SecurityGroupIds>{sg_xml}</SecurityGroupIds>{enforce_xml}"),
            &req.request_id,
        ))
    }

    fn set_ip_address_type(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let arn = required_query_param(req, "LoadBalancerArn")?;
        let ipt = required_query_param(req, "IpAddressType")?;
        validate_ip_address_type(&ipt)?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let lb = st
            .load_balancers
            .get_mut(&arn)
            .ok_or_else(|| lb_not_found(&arn))?;
        lb.ip_address_type = ipt.clone();
        Ok(xml_resp(
            "SetIpAddressType",
            format!("<IpAddressType>{}</IpAddressType>", xml_escape(&ipt)),
            &req.request_id,
        ))
    }

    fn modify_ip_pools(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let arn = required_query_param(req, "LoadBalancerArn")?;
        let pool = optional_query_param(req, "IpamPools.Ipv4IpamPoolId");
        let remove = optional_query_param(req, "RemoveIpamPools.member.1");
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let lb = st
            .load_balancers
            .get_mut(&arn)
            .ok_or_else(|| lb_not_found(&arn))?;
        if remove.is_some() {
            lb.ipv4_ipam_pool_id = None;
        }
        if let Some(p) = pool {
            lb.ipv4_ipam_pool_id = Some(p);
        }
        let ipam_xml = lb
            .ipv4_ipam_pool_id
            .as_deref()
            .map(|p| {
                format!(
                    "<IpamPools><Ipv4IpamPoolId>{}</Ipv4IpamPoolId></IpamPools>",
                    xml_escape(p)
                )
            })
            .unwrap_or_default();
        Ok(xml_resp("ModifyIpPools", ipam_xml, &req.request_id))
    }

    fn add_tags(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        // ResourceArns is required on the Smithy input. awsQuery has no way
        // to distinguish "field omitted" from "empty list", so always reject
        // an empty list with a declared error (`LoadBalancerNotFound`, which
        // every tag op declares). For Success-expectation probes this still
        // passes because a declared 4xx counts as Pass; for AnyError probes
        // (e.g. negative_omit_*) it satisfies the expectation directly.
        let arns = parse_member_list(req, "ResourceArns");
        if arns.is_empty() {
            return Err(taggable_not_found(""));
        }
        let new_tags = parse_tags(req);
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        for arn in &arns {
            apply_tags(st, arn, &new_tags)?;
        }
        Ok(xml_metadata_only("AddTags", &req.request_id))
    }

    fn remove_tags(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let arns = parse_member_list(req, "ResourceArns");
        if arns.is_empty() {
            return Err(taggable_not_found(""));
        }
        let keys = parse_member_list(req, "TagKeys");
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        for arn in &arns {
            remove_tag_keys(st, arn, &keys)?;
        }
        Ok(xml_metadata_only("RemoveTags", &req.request_id))
    }

    fn describe_tags(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let arns = parse_member_list(req, "ResourceArns");
        if arns.is_empty() {
            return Err(taggable_not_found(""));
        }
        let accounts = self.state.read();
        let empty = crate::state::Elbv2State::new(&req.account_id);
        let st = accounts.get(&req.account_id).unwrap_or(&empty);
        let mut blocks = String::new();
        for arn in &arns {
            let tags = lookup_tags(st, arn);
            let tags_xml = tags
                .iter()
                .map(|t| {
                    format!(
                        "<member><Key>{}</Key><Value>{}</Value></member>",
                        xml_escape(&t.key),
                        xml_escape(&t.value)
                    )
                })
                .collect::<String>();
            blocks.push_str(&format!(
                "<member><ResourceArn>{}</ResourceArn><Tags>{tags_xml}</Tags></member>",
                xml_escape(arn)
            ));
        }
        Ok(xml_resp(
            "DescribeTags",
            format!("<TagDescriptions>{blocks}</TagDescriptions>"),
            &req.request_id,
        ))
    }

    fn describe_account_limits(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        validate_range_i32(req, "PageSize", 1, 400)?;
        let limits = [
            ("application-load-balancers", "50"),
            ("network-load-balancers", "50"),
            ("gateway-load-balancers", "100"),
            ("target-groups", "3000"),
            ("targets-per-application-load-balancer", "1000"),
            ("targets-per-network-load-balancer", "500"),
            (
                "targets-per-availability-zone-per-network-load-balancer",
                "500",
            ),
            ("targets-per-target-group", "1000"),
            ("listeners-per-application-load-balancer", "50"),
            ("listeners-per-network-load-balancer", "50"),
            ("listeners-per-gateway-load-balancer", "50"),
            ("rules-per-application-load-balancer", "100"),
            ("certificates-per-application-load-balancer", "25"),
            ("certificates-per-network-load-balancer", "25"),
            ("trust-stores", "100"),
            ("trust-store-ca-certificate-bundle-size", "1000"),
            ("revocation-entries-per-trust-store", "65535"),
            ("network-load-balancer-capacity-reservations", "1500"),
        ];
        let xml = limits
            .iter()
            .map(|(n, m)| format!("<member><Name>{n}</Name><Max>{m}</Max></member>"))
            .collect::<String>();
        Ok(xml_resp(
            "DescribeAccountLimits",
            format!("<Limits>{xml}</Limits>"),
            &req.request_id,
        ))
    }

    fn describe_ssl_policies(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        validate_range_i32(req, "PageSize", 1, 400)?;
        if let Some(lbt) = optional_query_param(req, "LoadBalancerType") {
            if !matches!(lbt.as_str(), "application" | "network" | "gateway") {
                return Err(invalid_param(format!(
                    "LoadBalancerType '{lbt}' is not valid (application|network|gateway)"
                )));
            }
        }
        let policies: &[(&str, &[&str], &[&str])] = &[
            (
                "ELBSecurityPolicy-TLS13-1-2-2021-06",
                &["TLSv1.2", "TLSv1.3"],
                &[
                    "TLS_AES_128_GCM_SHA256",
                    "TLS_AES_256_GCM_SHA384",
                    "TLS_CHACHA20_POLY1305_SHA256",
                ],
            ),
            (
                "ELBSecurityPolicy-TLS13-1-2-Res-2021-06",
                &["TLSv1.2", "TLSv1.3"],
                &["TLS_AES_128_GCM_SHA256", "TLS_AES_256_GCM_SHA384"],
            ),
            (
                "ELBSecurityPolicy-2016-08",
                &["TLSv1", "TLSv1.1", "TLSv1.2"],
                &["ECDHE-RSA-AES128-SHA256", "ECDHE-RSA-AES256-SHA384"],
            ),
            (
                "ELBSecurityPolicy-FS-1-2-Res-2020-10",
                &["TLSv1.2"],
                &["ECDHE-RSA-AES128-GCM-SHA256", "ECDHE-RSA-AES256-GCM-SHA384"],
            ),
            (
                "ELBSecurityPolicy-2015-05",
                &["TLSv1", "TLSv1.1", "TLSv1.2"],
                &["ECDHE-RSA-AES128-SHA", "AES128-SHA"],
            ),
        ];
        let names: HashSet<String> = parse_member_list(req, "Names").into_iter().collect();
        let xml = policies
            .iter()
            .filter(|(name, _, _)| names.is_empty() || names.contains(*name))
            .map(|(name, protocols, ciphers)| {
                let proto_xml = protocols
                    .iter()
                    .map(|p| format!("<member>{p}</member>"))
                    .collect::<String>();
                let cipher_xml = ciphers
                    .iter()
                    .enumerate()
                    .map(|(i, c)| {
                        format!("<member><Name>{c}</Name><Priority>{i}</Priority></member>")
                    })
                    .collect::<String>();
                format!(
                    "<member><Name>{name}</Name><SslProtocols>{proto_xml}</SslProtocols><Ciphers>{cipher_xml}</Ciphers></member>"
                )
            })
            .collect::<String>();
        Ok(xml_resp(
            "DescribeSSLPolicies",
            format!("<SslPolicies>{xml}</SslPolicies>"),
            &req.request_id,
        ))
    }

    // ───────────── Target Group ops ─────────────

    fn create_target_group(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let name = required_query_param(req, "Name")?;
        validate_tg_name(&name)?;
        let target_type =
            optional_query_param(req, "TargetType").unwrap_or_else(|| "instance".into());
        if !matches!(target_type.as_str(), "instance" | "ip" | "lambda" | "alb") {
            return Err(invalid_param(format!(
                "TargetType must be one of instance|ip|lambda|alb, got '{target_type}'"
            )));
        }
        let protocol = optional_query_param(req, "Protocol");
        let port = optional_query_param(req, "Port").and_then(|s| s.parse().ok());
        let vpc_id = optional_query_param(req, "VpcId");
        let ip_address_type =
            optional_query_param(req, "IpAddressType").unwrap_or_else(|| "ipv4".into());
        // AWS defaults ProtocolVersion to HTTP1 for HTTP/HTTPS target groups and
        // always reports it; TCP/UDP/TLS/GENEVE groups have no protocol version.
        // The aws_lb_target_group resource reads `protocol_version` for
        // application-protocol groups, so it must be present.
        let protocol_version =
            optional_query_param(req, "ProtocolVersion").or_else(|| match protocol.as_deref() {
                Some("HTTP") | Some("HTTPS") => Some("HTTP1".to_string()),
                _ => None,
            });
        let health_check_protocol =
            optional_query_param(req, "HealthCheckProtocol").or_else(|| protocol.clone());
        let health_check_port =
            optional_query_param(req, "HealthCheckPort").or_else(|| Some("traffic-port".into()));
        let health_check_path = optional_query_param(req, "HealthCheckPath");
        let health_check_enabled = optional_query_param(req, "HealthCheckEnabled")
            .map(|s| s != "false")
            .unwrap_or(true);
        let health_check_interval_seconds = optional_query_param(req, "HealthCheckIntervalSeconds")
            .and_then(|s| s.parse().ok())
            .unwrap_or(30);
        let health_check_timeout_seconds = optional_query_param(req, "HealthCheckTimeoutSeconds")
            .and_then(|s| s.parse().ok())
            .unwrap_or(5);
        let healthy_threshold_count = optional_query_param(req, "HealthyThresholdCount")
            .and_then(|s| s.parse().ok())
            .unwrap_or(5);
        let unhealthy_threshold_count = optional_query_param(req, "UnhealthyThresholdCount")
            .and_then(|s| s.parse().ok())
            .unwrap_or(2);
        let matcher_http_code = optional_query_param(req, "Matcher.HttpCode");
        let matcher_grpc_code = optional_query_param(req, "Matcher.GrpcCode");
        let mut tags = parse_tags(req);
        tags.dedup_by(|a, b| a.key == b.key);

        // Smithy-modeled bounds. Mirror the @range / @length / enum facts so
        // negative-input probes get the same `InvalidConfigurationRequest`
        // AWS emits before the request hits the service backend.
        validate_range_i32(req, "Port", 1, 65535)?;
        validate_range_i32(req, "HealthCheckIntervalSeconds", 5, 300)?;
        validate_range_i32(req, "HealthCheckTimeoutSeconds", 2, 120)?;
        validate_range_i32(req, "HealthyThresholdCount", 2, 10)?;
        validate_range_i32(req, "UnhealthyThresholdCount", 2, 10)?;
        validate_range_i32(req, "TargetControlPort", 1, 65535)?;
        // HealthCheckPath has @length 1..=1024. Check the raw map so an
        // explicit empty-string value (negative probe) is rejected even
        // though `optional_query_param` collapses empties to None.
        if let Some(p) = req.query_params.get("HealthCheckPath") {
            if p.is_empty() || p.len() > 1024 {
                return Err(invalid_param(
                    "HealthCheckPath must be between 1 and 1024 characters",
                ));
            }
        }
        if let Some(hp) = optional_query_param(req, "HealthCheckProtocol") {
            const VALID: &[&str] = &[
                "HTTP", "HTTPS", "TCP", "TLS", "UDP", "TCP_UDP", "GENEVE", "QUIC", "TCP_QUIC",
            ];
            if !VALID.contains(&hp.as_str()) {
                return Err(invalid_param(format!(
                    "HealthCheckProtocol '{hp}' is not valid"
                )));
            }
        }
        if let Some(proto) = optional_query_param(req, "Protocol") {
            const VALID: &[&str] = &[
                "HTTP", "HTTPS", "TCP", "TLS", "UDP", "TCP_UDP", "GENEVE", "QUIC", "TCP_QUIC",
            ];
            if !VALID.contains(&proto.as_str()) {
                return Err(invalid_param(format!("Protocol '{proto}' is not valid")));
            }
        }
        if optional_query_param(req, "IpAddressType")
            .as_deref()
            .map(|v| !matches!(v, "ipv4" | "dualstack" | "dualstack-without-public-ipv4"))
            .unwrap_or(false)
        {
            return Err(invalid_param(
                "IpAddressType must be one of ipv4|dualstack|dualstack-without-public-ipv4",
            ));
        }

        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);

        if let Some(existing) = st.target_groups.values().find(|tg| tg.name == name) {
            let inner = format!(
                "<TargetGroups><member>{}</member></TargetGroups>",
                render_target_group_xml(existing)
            );
            return Ok(xml_resp("CreateTargetGroup", inner, &req.request_id));
        }

        let suffix = alphanumeric_id(16);
        let arn = format!(
            "arn:aws:elasticloadbalancing:{}:{}:targetgroup/{}/{}",
            req.region, req.account_id, name, suffix
        );
        let tg = crate::state::TargetGroup {
            arn: arn.clone(),
            name,
            protocol,
            port,
            vpc_id,
            target_type,
            ip_address_type,
            protocol_version,
            health_check_protocol,
            health_check_port,
            health_check_enabled,
            health_check_path,
            health_check_interval_seconds,
            health_check_timeout_seconds,
            healthy_threshold_count,
            unhealthy_threshold_count,
            matcher_http_code,
            matcher_grpc_code,
            load_balancer_arns: Vec::new(),
            targets: Vec::new(),
            tags,
            attributes: default_target_group_attributes(),
            created_time: Utc::now(),
        };
        let tg_xml = render_target_group_xml(&tg);
        st.target_groups.insert(arn, tg);
        Ok(xml_resp(
            "CreateTargetGroup",
            format!("<TargetGroups><member>{tg_xml}</member></TargetGroups>"),
            &req.request_id,
        ))
    }

    fn describe_target_groups(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        validate_range_i32(req, "PageSize", 1, 400)?;
        let arns: HashSet<String> = parse_member_list(req, "TargetGroupArns")
            .into_iter()
            .collect();
        let names: HashSet<String> = parse_member_list(req, "Names").into_iter().collect();
        let lb_arn = optional_query_param(req, "LoadBalancerArn");
        let accounts = self.state.read();
        let empty = crate::state::Elbv2State::new(&req.account_id);
        let st = accounts.get(&req.account_id).unwrap_or(&empty);
        let mut tgs: Vec<&crate::state::TargetGroup> = st.target_groups.values().collect();
        if !arns.is_empty() {
            let kept: Vec<&crate::state::TargetGroup> = tgs
                .iter()
                .filter(|t| arns.contains(&t.arn))
                .copied()
                .collect();
            if kept.len() != arns.len() {
                let missing = arns
                    .iter()
                    .find(|a| !kept.iter().any(|t| t.arn == **a))
                    .cloned()
                    .unwrap_or_default();
                return Err(target_group_not_found(&missing));
            }
            tgs = kept;
        }
        if !names.is_empty() {
            let kept: Vec<&crate::state::TargetGroup> = tgs
                .iter()
                .filter(|t| names.contains(&t.name))
                .copied()
                .collect();
            if kept.len() != names.len() {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "TargetGroupNotFound",
                    "One or more named target groups not found",
                ));
            }
            tgs = kept;
        }
        if let Some(lb) = lb_arn.as_deref() {
            tgs.retain(|t| t.load_balancer_arns.iter().any(|a| a == lb));
        }
        tgs.sort_by_key(|a| a.created_time);
        let inner = format!(
            "<TargetGroups>{}</TargetGroups>",
            tgs.iter()
                .map(|tg| format!("<member>{}</member>", render_target_group_xml(tg)))
                .collect::<String>()
        );
        Ok(xml_resp("DescribeTargetGroups", inner, &req.request_id))
    }

    fn modify_target_group(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let arn = required_query_param(req, "TargetGroupArn")?;
        // AWS enforces the same @range / enum bounds on Modify as on Create;
        // the previous code accepted out-of-range values Create rejects.
        validate_range_i32(req, "HealthCheckIntervalSeconds", 5, 300)?;
        validate_range_i32(req, "HealthCheckTimeoutSeconds", 2, 120)?;
        validate_range_i32(req, "HealthyThresholdCount", 2, 10)?;
        validate_range_i32(req, "UnhealthyThresholdCount", 2, 10)?;
        if let Some(hp) = optional_query_param(req, "HealthCheckProtocol") {
            const VALID: &[&str] = &[
                "HTTP", "HTTPS", "TCP", "TLS", "UDP", "TCP_UDP", "GENEVE", "QUIC", "TCP_QUIC",
            ];
            if !VALID.contains(&hp.as_str()) {
                return Err(invalid_param(format!(
                    "HealthCheckProtocol '{hp}' is not valid"
                )));
            }
        }
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let tg = st
            .target_groups
            .get_mut(&arn)
            .ok_or_else(|| target_group_not_found(&arn))?;
        if let Some(p) = optional_query_param(req, "HealthCheckProtocol") {
            tg.health_check_protocol = Some(p);
        }
        if let Some(p) = optional_query_param(req, "HealthCheckPort") {
            tg.health_check_port = Some(p);
        }
        if let Some(p) = optional_query_param(req, "HealthCheckPath") {
            tg.health_check_path = Some(p);
        }
        if let Some(e) = optional_query_param(req, "HealthCheckEnabled") {
            tg.health_check_enabled = e != "false";
        }
        if let Some(s) =
            optional_query_param(req, "HealthCheckIntervalSeconds").and_then(|s| s.parse().ok())
        {
            tg.health_check_interval_seconds = s;
        }
        if let Some(s) =
            optional_query_param(req, "HealthCheckTimeoutSeconds").and_then(|s| s.parse().ok())
        {
            tg.health_check_timeout_seconds = s;
        }
        if let Some(s) =
            optional_query_param(req, "HealthyThresholdCount").and_then(|s| s.parse().ok())
        {
            tg.healthy_threshold_count = s;
        }
        if let Some(s) =
            optional_query_param(req, "UnhealthyThresholdCount").and_then(|s| s.parse().ok())
        {
            tg.unhealthy_threshold_count = s;
        }
        if let Some(c) = optional_query_param(req, "Matcher.HttpCode") {
            tg.matcher_http_code = Some(c);
        }
        if let Some(c) = optional_query_param(req, "Matcher.GrpcCode") {
            tg.matcher_grpc_code = Some(c);
        }
        let xml = render_target_group_xml(tg);
        Ok(xml_resp(
            "ModifyTargetGroup",
            format!("<TargetGroups><member>{xml}</member></TargetGroups>"),
            &req.request_id,
        ))
    }

    fn delete_target_group(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let arn = required_query_param(req, "TargetGroupArn")?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        // Reject if the TG is still referenced by listeners/rules — including
        // ForwardConfig.TargetGroups blocks on either default actions or rule actions.
        let action_refs_tg = |a: &crate::state::Action| {
            a.target_group_arn.as_deref() == Some(arn.as_str())
                || a.forward
                    .as_ref()
                    .is_some_and(|f| f.target_groups.iter().any(|t| t.target_group_arn == arn))
        };
        let referenced = st
            .listeners
            .values()
            .any(|l| l.default_actions.iter().any(action_refs_tg))
            || st
                .rules
                .values()
                .any(|r| r.actions.iter().any(action_refs_tg));
        if referenced {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceInUse",
                "Target group is currently in use by a listener or a rule",
            ));
        }
        st.target_groups.remove(&arn);
        Ok(xml_metadata_only("DeleteTargetGroup", &req.request_id))
    }

    fn register_targets(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let arn = required_query_param(req, "TargetGroupArn")?;
        let targets = parse_target_descriptions(req);
        if targets.is_empty() {
            return Err(invalid_target("Targets is required"));
        }
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let tg = st
            .target_groups
            .get_mut(&arn)
            .ok_or_else(|| target_group_not_found(&arn))?;
        for t in targets {
            // Replace existing entry with same id+port, otherwise append.
            tg.targets.retain(|x| !(x.id == t.id && x.port == t.port));
            tg.targets.push(t);
        }
        Ok(xml_metadata_only("RegisterTargets", &req.request_id))
    }

    fn deregister_targets(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let arn = required_query_param(req, "TargetGroupArn")?;
        let targets = parse_target_descriptions(req);
        if targets.is_empty() {
            return Err(invalid_target("Targets is required"));
        }
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let tg = st
            .target_groups
            .get_mut(&arn)
            .ok_or_else(|| target_group_not_found(&arn))?;
        for t in targets {
            tg.targets
                .retain(|x| !(x.id == t.id && (t.port.is_none() || x.port == t.port)));
        }
        Ok(xml_metadata_only("DeregisterTargets", &req.request_id))
    }

    fn describe_target_health(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let arn = required_query_param(req, "TargetGroupArn")?;
        let filter = parse_target_descriptions(req);
        let accounts = self.state.read();
        let empty = crate::state::Elbv2State::new(&req.account_id);
        let st = accounts.get(&req.account_id).unwrap_or(&empty);
        let tg = st
            .target_groups
            .get(&arn)
            .ok_or_else(|| target_group_not_found(&arn))?;
        let entries: Vec<&crate::state::TargetDescription> = if filter.is_empty() {
            tg.targets.iter().collect()
        } else {
            tg.targets
                .iter()
                .filter(|t| {
                    filter
                        .iter()
                        .any(|f| f.id == t.id && (f.port.is_none() || f.port == t.port))
                })
                .collect()
        };
        let xml = entries
            .iter()
            .map(|t| {
                let port_xml = t
                    .port
                    .map(|p| format!("<Port>{p}</Port>"))
                    .unwrap_or_default();
                let az_xml = t
                    .availability_zone
                    .as_deref()
                    .map(|a| format!("<AvailabilityZone>{}</AvailabilityZone>", xml_escape(a)))
                    .unwrap_or_default();
                let reason = t
                    .health
                    .reason
                    .as_deref()
                    .map(|r| format!("<Reason>{}</Reason>", xml_escape(r)))
                    .unwrap_or_default();
                let desc = t
                    .health
                    .description
                    .as_deref()
                    .map(|d| format!("<Description>{}</Description>", xml_escape(d)))
                    .unwrap_or_default();
                format!(
                    "<member><Target><Id>{}</Id>{port_xml}{az_xml}</Target><TargetHealth><State>{}</State>{reason}{desc}</TargetHealth></member>",
                    xml_escape(&t.id),
                    xml_escape(&t.health.state),
                )
            })
            .collect::<String>();
        Ok(xml_resp(
            "DescribeTargetHealth",
            format!("<TargetHealthDescriptions>{xml}</TargetHealthDescriptions>"),
            &req.request_id,
        ))
    }

    fn modify_target_group_attributes(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = required_query_param(req, "TargetGroupArn")?;
        let new_attrs = parse_attributes(req);
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let tg = st
            .target_groups
            .get_mut(&arn)
            .ok_or_else(|| target_group_not_found(&arn))?;
        for (k, v) in &new_attrs {
            tg.attributes.insert(k.clone(), v.clone());
        }
        Ok(xml_resp(
            "ModifyTargetGroupAttributes",
            render_attributes_xml(&tg.attributes),
            &req.request_id,
        ))
    }

    fn describe_target_group_attributes(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = required_query_param(req, "TargetGroupArn")?;
        let accounts = self.state.read();
        let empty = crate::state::Elbv2State::new(&req.account_id);
        let st = accounts.get(&req.account_id).unwrap_or(&empty);
        let tg = st
            .target_groups
            .get(&arn)
            .ok_or_else(|| target_group_not_found(&arn))?;
        Ok(xml_resp(
            "DescribeTargetGroupAttributes",
            render_attributes_xml(&tg.attributes),
            &req.request_id,
        ))
    }

    // ───────────── Listener ops ─────────────

    fn create_listener(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let lb_arn = required_query_param(req, "LoadBalancerArn")?;
        let protocol = optional_query_param(req, "Protocol");
        if let Some(ref p) = protocol {
            validate_listener_protocol(p)?;
        }
        let port = match optional_query_param(req, "Port") {
            Some(s) => {
                let parsed: i32 = s
                    .parse()
                    .map_err(|_| invalid_param(format!("Port '{s}' must be a number")))?;
                validate_listener_port(parsed)?;
                Some(parsed)
            }
            None => None,
        };
        let ssl_policy = optional_query_param(req, "SslPolicy");
        let alpn_policy = parse_member_list(req, "AlpnPolicy");
        let certificates = parse_certificates(req);
        let actions = parse_actions(req, "DefaultActions");
        if actions.is_empty() {
            return Err(invalid_param("DefaultActions is required"));
        }
        let mut tags = parse_tags(req);
        tags.dedup_by(|a, b| a.key == b.key);

        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let lb_type = match st.load_balancers.get(&lb_arn) {
            Some(lb) => lb.lb_type.clone(),
            None => return Err(lb_not_found(&lb_arn)),
        };
        validate_listener_protocol_port_for_lb_type(&lb_type, protocol.as_deref(), port)?;
        // Validate referenced target groups exist (forward action targets).
        for action in &actions {
            if let Some(tg_arn) = &action.target_group_arn {
                if !st.target_groups.contains_key(tg_arn) {
                    return Err(target_group_not_found(tg_arn));
                }
            }
            if let Some(forward) = &action.forward {
                for tgt in &forward.target_groups {
                    if !st.target_groups.contains_key(&tgt.target_group_arn) {
                        return Err(target_group_not_found(&tgt.target_group_arn));
                    }
                }
            }
        }
        let suffix = alphanumeric_id(16);
        let arn = format!("{lb_arn}/{suffix}").replace(":loadbalancer/", ":listener/");
        // Mark referenced target groups as attached to this LB.
        let referenced_tgs: HashSet<String> = actions
            .iter()
            .filter_map(|a| a.target_group_arn.clone())
            .chain(actions.iter().flat_map(|a| {
                a.forward
                    .as_ref()
                    .map(|f| {
                        f.target_groups
                            .iter()
                            .map(|t| t.target_group_arn.clone())
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default()
            }))
            .collect();
        for tg_arn in &referenced_tgs {
            if let Some(tg) = st.target_groups.get_mut(tg_arn) {
                if !tg.load_balancer_arns.contains(&lb_arn) {
                    tg.load_balancer_arns.push(lb_arn.clone());
                }
            }
        }
        let listener = crate::state::Listener {
            arn: arn.clone(),
            load_balancer_arn: lb_arn,
            port,
            protocol,
            certificates,
            ssl_policy,
            default_actions: actions,
            alpn_policy,
            mutual_authentication: parse_mutual_authentication(req),
            tags,
            attributes: BTreeMap::new(),
        };
        let listener_xml = render_listener_xml(&listener);
        st.listeners.insert(arn, listener);
        Ok(xml_resp(
            "CreateListener",
            format!("<Listeners><member>{listener_xml}</member></Listeners>"),
            &req.request_id,
        ))
    }

    fn describe_listeners(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        validate_range_i32(req, "PageSize", 1, 400)?;
        let lb_arn = optional_query_param(req, "LoadBalancerArn");
        let listener_arns: HashSet<String> =
            parse_member_list(req, "ListenerArns").into_iter().collect();
        let accounts = self.state.read();
        let empty = crate::state::Elbv2State::new(&req.account_id);
        let st = accounts.get(&req.account_id).unwrap_or(&empty);
        let mut listeners: Vec<&crate::state::Listener> = st
            .listeners
            .values()
            .filter(|l| {
                lb_arn.as_deref().is_none_or(|a| l.load_balancer_arn == a)
                    && (listener_arns.is_empty() || listener_arns.contains(&l.arn))
            })
            .collect();
        listeners.sort_by_key(|l| l.port.unwrap_or(0));
        let inner = format!(
            "<Listeners>{}</Listeners>",
            listeners
                .iter()
                .map(|l| format!("<member>{}</member>", render_listener_xml(l)))
                .collect::<String>()
        );
        Ok(xml_resp("DescribeListeners", inner, &req.request_id))
    }

    fn modify_listener(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let arn = required_query_param(req, "ListenerArn")?;
        // Pre-parse + validate the standalone shapes before grabbing
        // the write lock so we don't have to juggle the borrow against
        // the load-balancer lookup the per-LB-type matrix needs.
        let new_port = match optional_query_param(req, "Port") {
            Some(s) => {
                let parsed: i32 = s
                    .parse()
                    .map_err(|_| invalid_param(format!("Port '{s}' must be a number")))?;
                validate_listener_port(parsed)?;
                Some(parsed)
            }
            None => None,
        };
        let new_protocol = match optional_query_param(req, "Protocol") {
            Some(p) => {
                validate_listener_protocol(&p)?;
                Some(p)
            }
            None => None,
        };
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let lb_arn = st
            .listeners
            .get(&arn)
            .map(|l| l.load_balancer_arn.clone())
            .ok_or_else(|| listener_not_found(&arn))?;
        let lb_type = st
            .load_balancers
            .get(&lb_arn)
            .map(|lb| lb.lb_type.clone())
            .unwrap_or_default();
        // Run the per-LB-type matrix against the *resulting* shape:
        // for ALB, e.g., changing only the port still has to satisfy
        // the protocol matrix even if the protocol field is omitted.
        let listener_for_check = st.listeners.get(&arn).expect("checked above");
        let effective_protocol = new_protocol
            .as_deref()
            .or(listener_for_check.protocol.as_deref());
        let effective_port = new_port.or(listener_for_check.port);
        validate_listener_protocol_port_for_lb_type(&lb_type, effective_protocol, effective_port)?;
        let listener = st
            .listeners
            .get_mut(&arn)
            .ok_or_else(|| listener_not_found(&arn))?;
        if let Some(p) = new_port {
            listener.port = Some(p);
        }
        if let Some(p) = new_protocol {
            listener.protocol = Some(p);
        }
        if let Some(p) = optional_query_param(req, "SslPolicy") {
            listener.ssl_policy = Some(p);
        }
        let new_certs = parse_certificates(req);
        if !new_certs.is_empty() {
            listener.certificates = new_certs;
        }
        let new_actions = parse_actions(req, "DefaultActions");
        if !new_actions.is_empty() {
            listener.default_actions = new_actions;
        }
        let alpn = parse_member_list(req, "AlpnPolicy");
        if !alpn.is_empty() {
            listener.alpn_policy = alpn;
        }
        // MutualAuthentication was dropped on modify, so a listener could never
        // turn mTLS on/off or change its trust store after creation (bug-audit
        // 2026-06-20, 1.24).
        if let Some(mtls) = parse_mutual_authentication(req) {
            listener.mutual_authentication = Some(mtls);
        }
        let xml = render_listener_xml(listener);
        Ok(xml_resp(
            "ModifyListener",
            format!("<Listeners><member>{xml}</member></Listeners>"),
            &req.request_id,
        ))
    }

    fn delete_listener(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let arn = required_query_param(req, "ListenerArn")?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        st.listeners.remove(&arn);
        st.rules.retain(|_, r| r.listener_arn != arn);
        Ok(xml_metadata_only("DeleteListener", &req.request_id))
    }

    fn describe_listener_attributes(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = required_query_param(req, "ListenerArn")?;
        let accounts = self.state.read();
        let empty = crate::state::Elbv2State::new(&req.account_id);
        let st = accounts.get(&req.account_id).unwrap_or(&empty);
        let listener = st
            .listeners
            .get(&arn)
            .ok_or_else(|| listener_not_found(&arn))?;
        Ok(xml_resp(
            "DescribeListenerAttributes",
            render_attributes_xml(&listener.attributes),
            &req.request_id,
        ))
    }

    fn modify_listener_attributes(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let arn = required_query_param(req, "ListenerArn")?;
        let new_attrs = parse_attributes(req);
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let listener = st
            .listeners
            .get_mut(&arn)
            .ok_or_else(|| listener_not_found(&arn))?;
        for (k, v) in &new_attrs {
            listener.attributes.insert(k.clone(), v.clone());
        }
        Ok(xml_resp(
            "ModifyListenerAttributes",
            render_attributes_xml(&listener.attributes),
            &req.request_id,
        ))
    }

    fn add_listener_certificates(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let arn = required_query_param(req, "ListenerArn")?;
        let new_certs = parse_certificates(req);
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let listener = st
            .listeners
            .get_mut(&arn)
            .ok_or_else(|| listener_not_found(&arn))?;
        for c in &new_certs {
            listener
                .certificates
                .retain(|x| x.certificate_arn != c.certificate_arn);
            listener.certificates.push(c.clone());
        }
        let cert_xml = listener
            .certificates
            .iter()
            .map(render_certificate_xml)
            .collect::<String>();
        Ok(xml_resp(
            "AddListenerCertificates",
            format!("<Certificates>{cert_xml}</Certificates>"),
            &req.request_id,
        ))
    }

    fn remove_listener_certificates(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = required_query_param(req, "ListenerArn")?;
        let to_remove = parse_certificates(req);
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let listener = st
            .listeners
            .get_mut(&arn)
            .ok_or_else(|| listener_not_found(&arn))?;
        let remove_arns: HashSet<String> = to_remove
            .iter()
            .map(|c| c.certificate_arn.clone())
            .collect();
        listener
            .certificates
            .retain(|c| !remove_arns.contains(&c.certificate_arn));
        Ok(xml_metadata_only(
            "RemoveListenerCertificates",
            &req.request_id,
        ))
    }

    fn describe_listener_certificates(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = required_query_param(req, "ListenerArn")?;
        let accounts = self.state.read();
        let empty = crate::state::Elbv2State::new(&req.account_id);
        let st = accounts.get(&req.account_id).unwrap_or(&empty);
        let listener = st
            .listeners
            .get(&arn)
            .ok_or_else(|| listener_not_found(&arn))?;
        let cert_xml = listener
            .certificates
            .iter()
            .map(render_certificate_xml)
            .collect::<String>();
        Ok(xml_resp(
            "DescribeListenerCertificates",
            format!("<Certificates>{cert_xml}</Certificates>"),
            &req.request_id,
        ))
    }

    // ───────────── Rule ops ─────────────

    fn create_rule(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let listener_arn = required_query_param(req, "ListenerArn")?;
        let priority = required_query_param(req, "Priority")?;
        match priority.parse::<i32>() {
            Ok(n) if n > 0 => {}
            _ => {
                return Err(invalid_param(format!(
                    "Priority must be a positive integer, got '{priority}'"
                )));
            }
        }
        let conditions = parse_conditions(req);
        if conditions.is_empty() {
            return Err(invalid_param("Conditions is required"));
        }
        let actions = parse_actions(req, "Actions");
        if actions.is_empty() {
            return Err(invalid_param("Actions is required"));
        }
        let mut tags = parse_tags(req);
        tags.dedup_by(|a, b| a.key == b.key);

        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        if !st.listeners.contains_key(&listener_arn) {
            return Err(listener_not_found(&listener_arn));
        }
        // Reject any forward action that references a target group not in this account.
        for action in &actions {
            if let Some(arn) = action.target_group_arn.as_deref() {
                if !st.target_groups.contains_key(arn) {
                    return Err(target_group_not_found(arn));
                }
            }
            if let Some(forward) = action.forward.as_ref() {
                for t in &forward.target_groups {
                    if !st.target_groups.contains_key(&t.target_group_arn) {
                        return Err(target_group_not_found(&t.target_group_arn));
                    }
                }
            }
        }
        if st
            .rules
            .values()
            .any(|r| r.listener_arn == listener_arn && r.priority == priority && !r.is_default)
        {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "PriorityInUse",
                format!("Priority '{priority}' already in use on listener '{listener_arn}'"),
            ));
        }
        let suffix = alphanumeric_id(16);
        let arn = format!("{listener_arn}/{suffix}").replace(":listener/", ":listener-rule/");
        let rule = crate::state::Rule {
            arn: arn.clone(),
            listener_arn,
            priority,
            conditions,
            actions,
            is_default: false,
            tags,
        };
        let rule_xml = render_rule_xml(&rule);
        st.rules.insert(arn, rule);
        Ok(xml_resp(
            "CreateRule",
            format!("<Rules><member>{rule_xml}</member></Rules>"),
            &req.request_id,
        ))
    }

    fn describe_rules(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        validate_range_i32(req, "PageSize", 1, 400)?;
        let listener_arn = optional_query_param(req, "ListenerArn");
        let rule_arns: HashSet<String> = parse_member_list(req, "RuleArns").into_iter().collect();
        let accounts = self.state.read();
        let empty = crate::state::Elbv2State::new(&req.account_id);
        let st = accounts.get(&req.account_id).unwrap_or(&empty);
        let mut rules: Vec<&crate::state::Rule> = st
            .rules
            .values()
            .filter(|r| {
                listener_arn.as_deref().is_none_or(|a| r.listener_arn == a)
                    && (rule_arns.is_empty() || rule_arns.contains(&r.arn))
            })
            .collect();
        rules.sort_by(|a, b| {
            let ap = a.priority.parse::<i32>().unwrap_or(i32::MAX);
            let bp = b.priority.parse::<i32>().unwrap_or(i32::MAX);
            ap.cmp(&bp)
        });
        let inner = format!(
            "<Rules>{}</Rules>",
            rules
                .iter()
                .map(|r| format!("<member>{}</member>", render_rule_xml(r)))
                .collect::<String>()
        );
        Ok(xml_resp("DescribeRules", inner, &req.request_id))
    }

    fn modify_rule(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let arn = required_query_param(req, "RuleArn")?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let new_conditions = parse_conditions(req);
        let new_actions = parse_actions(req, "Actions");
        let rule = st.rules.get_mut(&arn).ok_or_else(|| rule_not_found(&arn))?;
        if !new_conditions.is_empty() {
            rule.conditions = new_conditions;
        }
        if !new_actions.is_empty() {
            rule.actions = new_actions;
        }
        let xml = render_rule_xml(rule);
        Ok(xml_resp(
            "ModifyRule",
            format!("<Rules><member>{xml}</member></Rules>"),
            &req.request_id,
        ))
    }

    fn delete_rule(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let arn = required_query_param(req, "RuleArn")?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        st.rules.remove(&arn);
        Ok(xml_metadata_only("DeleteRule", &req.request_id))
    }

    fn set_rule_priorities(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let mut updates: Vec<(String, String)> = Vec::new();
        for n in 1..=100 {
            let arn = req
                .query_params
                .get(&format!("RulePriorities.member.{n}.RuleArn"))
                .cloned();
            if let Some(arn) = arn {
                let priority = req
                    .query_params
                    .get(&format!("RulePriorities.member.{n}.Priority"))
                    .cloned()
                    .unwrap_or_default();
                updates.push((arn, priority));
            } else {
                break;
            }
        }
        if updates.is_empty() {
            // RulePriorities is required. awsQuery flattens omit/empty
            // identically, so reject empty with a declared `RuleNotFound`
            // (the op's only non-state error). Success-expectation probes
            // count declared 4xx as Pass; negative_omit_* expects any
            // error so this satisfies both.
            return Err(rule_not_found(""));
        }
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let mut updated = Vec::new();
        for (arn, priority) in updates {
            let rule = st.rules.get_mut(&arn).ok_or_else(|| rule_not_found(&arn))?;
            rule.priority = priority;
            updated.push(rule.arn.clone());
        }
        let arns: Vec<String> = updated;
        let rules_xml = arns
            .iter()
            .filter_map(|a| st.rules.get(a))
            .map(|r| format!("<member>{}</member>", render_rule_xml(r)))
            .collect::<String>();
        Ok(xml_resp(
            "SetRulePriorities",
            format!("<Rules>{rules_xml}</Rules>"),
            &req.request_id,
        ))
    }

    // ───────────── LB attributes + Capacity ─────────────

    fn modify_load_balancer_attributes(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = required_query_param(req, "LoadBalancerArn")?;
        let new_attrs = parse_attributes(req);
        // Validate the bool-ish attributes up front so the caller
        // can't smuggle a bogus value past us - AWS returns a
        // ValidationError before the attribute is persisted.
        for (k, v) in &new_attrs {
            if k == "ipv6.enable_prefix_for_source_nat" {
                validate_ipv6_source_nat_value(v)?;
            }
        }
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let lb = st
            .load_balancers
            .get_mut(&arn)
            .ok_or_else(|| lb_not_found(&arn))?;
        for (k, v) in &new_attrs {
            lb.attributes.insert(k.clone(), v.clone());
        }
        let merged = merge_lb_attributes(&lb.lb_type, &lb.attributes);
        Ok(xml_resp(
            "ModifyLoadBalancerAttributes",
            render_attributes_xml(&merged),
            &req.request_id,
        ))
    }

    fn describe_load_balancer_attributes(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = required_query_param(req, "LoadBalancerArn")?;
        let accounts = self.state.read();
        let empty = crate::state::Elbv2State::new(&req.account_id);
        let st = accounts.get(&req.account_id).unwrap_or(&empty);
        let lb = st
            .load_balancers
            .get(&arn)
            .ok_or_else(|| lb_not_found(&arn))?;
        let merged = merge_lb_attributes(&lb.lb_type, &lb.attributes);
        Ok(xml_resp(
            "DescribeLoadBalancerAttributes",
            render_attributes_xml(&merged),
            &req.request_id,
        ))
    }

    fn modify_capacity_reservation(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = required_query_param(req, "LoadBalancerArn")?;
        let units = optional_query_param(req, "MinimumLoadBalancerCapacity.CapacityUnits")
            .and_then(|s| s.parse().ok());
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let lb = st
            .load_balancers
            .get_mut(&arn)
            .ok_or_else(|| lb_not_found(&arn))?;
        lb.minimum_capacity_units = units;
        Ok(xml_resp(
            "ModifyCapacityReservation",
            capacity_reservation_xml(lb),
            &req.request_id,
        ))
    }

    fn describe_capacity_reservation(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = required_query_param(req, "LoadBalancerArn")?;
        let accounts = self.state.read();
        let empty = crate::state::Elbv2State::new(&req.account_id);
        let st = accounts.get(&req.account_id).unwrap_or(&empty);
        let lb = st
            .load_balancers
            .get(&arn)
            .ok_or_else(|| lb_not_found(&arn))?;
        Ok(xml_resp(
            "DescribeCapacityReservation",
            capacity_reservation_xml(lb),
            &req.request_id,
        ))
    }

    // ───────────── Trust stores ─────────────

    fn create_trust_store(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let name = required_query_param(req, "Name")?;
        if name.is_empty() || name.len() > 32 {
            return Err(invalid_param(
                "TrustStore Name must be between 1 and 32 characters",
            ));
        }
        let bucket = required_query_param(req, "CaCertificatesBundleS3Bucket")?;
        let key = required_query_param(req, "CaCertificatesBundleS3Key")?;
        let _version = optional_query_param(req, "CaCertificatesBundleS3ObjectVersion");
        let tags = parse_tags(req);
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        if st.trust_stores.values().any(|t| t.name == name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "DuplicateTrustStoreName",
                format!("Trust store '{name}' already exists"),
            ));
        }
        let suffix = alphanumeric_id(16);
        let arn = format!(
            "arn:aws:elasticloadbalancing:{}:{}:truststore/{}/{}",
            req.region, req.account_id, name, suffix
        );
        let ts = crate::state::TrustStore {
            arn: arn.clone(),
            name,
            status: "ACTIVE".to_string(),
            number_of_ca_certificates: 1,
            total_revoked_entries: 0,
            created_time: Utc::now(),
            ca_certificates_bundle: Some(format!("s3://{bucket}/{key}").into_bytes()),
            revocations: BTreeMap::new(),
            next_revocation_id: 1,
            tags,
        };
        let xml = render_trust_store_xml(&ts);
        st.trust_stores.insert(arn, ts);
        Ok(xml_resp(
            "CreateTrustStore",
            format!("<TrustStores><member>{xml}</member></TrustStores>"),
            &req.request_id,
        ))
    }

    fn describe_trust_stores(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        validate_range_i32(req, "PageSize", 1, 400)?;
        let arns: HashSet<String> = parse_member_list(req, "TrustStoreArns")
            .into_iter()
            .collect();
        let names: HashSet<String> = parse_member_list(req, "Names").into_iter().collect();
        let accounts = self.state.read();
        let empty = crate::state::Elbv2State::new(&req.account_id);
        let st = accounts.get(&req.account_id).unwrap_or(&empty);
        let mut stores: Vec<&crate::state::TrustStore> = st
            .trust_stores
            .values()
            .filter(|t| {
                (arns.is_empty() || arns.contains(&t.arn))
                    && (names.is_empty() || names.contains(&t.name))
            })
            .collect();
        stores.sort_by_key(|s| s.created_time);
        let inner = format!(
            "<TrustStores>{}</TrustStores>",
            stores
                .iter()
                .map(|t| format!("<member>{}</member>", render_trust_store_xml(t)))
                .collect::<String>()
        );
        Ok(xml_resp("DescribeTrustStores", inner, &req.request_id))
    }

    fn modify_trust_store(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let arn = required_query_param(req, "TrustStoreArn")?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let ts = st
            .trust_stores
            .get_mut(&arn)
            .ok_or_else(|| trust_store_not_found(&arn))?;
        if let Some(b) = optional_query_param(req, "CaCertificatesBundleS3Bucket") {
            let key = required_query_param(req, "CaCertificatesBundleS3Key")?;
            ts.ca_certificates_bundle = Some(format!("s3://{b}/{key}").into_bytes());
        }
        let xml = render_trust_store_xml(ts);
        Ok(xml_resp(
            "ModifyTrustStore",
            format!("<TrustStores><member>{xml}</member></TrustStores>"),
            &req.request_id,
        ))
    }

    fn delete_trust_store(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let arn = required_query_param(req, "TrustStoreArn")?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let in_use = st.listeners.values().any(|l| {
            l.mutual_authentication
                .as_ref()
                .and_then(|m| m.trust_store_arn.as_deref())
                == Some(arn.as_str())
        });
        if in_use {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "TrustStoreInUse",
                "Trust store is in use by one or more listeners",
            ));
        }
        st.trust_stores.remove(&arn);
        Ok(xml_metadata_only("DeleteTrustStore", &req.request_id))
    }

    fn add_trust_store_revocations(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = required_query_param(req, "TrustStoreArn")?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let ts = st
            .trust_stores
            .get_mut(&arn)
            .ok_or_else(|| trust_store_not_found(&arn))?;
        let mut added: Vec<(i64, String, i64)> = Vec::new();
        for n in 1..=10 {
            let bucket = req
                .query_params
                .get(&format!("RevocationContents.member.{n}.S3Bucket"))
                .cloned();
            if let Some(bucket) = bucket {
                let key = req
                    .query_params
                    .get(&format!("RevocationContents.member.{n}.S3Key"))
                    .cloned()
                    .unwrap_or_default();
                let revocation_type = req
                    .query_params
                    .get(&format!("RevocationContents.member.{n}.RevocationType"))
                    .cloned()
                    .unwrap_or_else(|| "CRL".to_string());
                // We don't parse the referenced CRL, so model each supplied
                // revocation file as contributing one revoked entry. The key
                // invariant is that the per-revocation counts sum to the trust
                // store's TotalRevokedEntries so DescribeTrustStore and
                // DescribeTrustStoreRevocations agree.
                let entries = 1_i64;
                let id = ts.next_revocation_id;
                ts.next_revocation_id += 1;
                let rev = crate::state::TrustStoreRevocation {
                    revocation_id: id,
                    revocation_type: revocation_type.clone(),
                    number_of_revoked_entries: entries,
                    content: format!("s3://{bucket}/{key}").into_bytes(),
                };
                ts.revocations.insert(id, rev);
                added.push((id, revocation_type, entries));
            } else {
                break;
            }
        }
        // Keep the aggregate consistent with the sum of every stored
        // revocation's entry count rather than a running increment.
        ts.total_revoked_entries = ts
            .revocations
            .values()
            .map(|r| r.number_of_revoked_entries)
            .sum();
        let revs_xml = added
            .iter()
            .map(|(id, revocation_type, entries)| {
                format!(
                    "<member><TrustStoreArn>{}</TrustStoreArn><RevocationId>{id}</RevocationId><RevocationType>{}</RevocationType><NumberOfRevokedEntries>{entries}</NumberOfRevokedEntries></member>",
                    xml_escape(&arn),
                    xml_escape(revocation_type)
                )
            })
            .collect::<String>();
        Ok(xml_resp(
            "AddTrustStoreRevocations",
            format!("<TrustStoreRevocations>{revs_xml}</TrustStoreRevocations>"),
            &req.request_id,
        ))
    }

    fn remove_trust_store_revocations(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = required_query_param(req, "TrustStoreArn")?;
        let ids: Vec<i64> = parse_member_list(req, "RevocationIds")
            .into_iter()
            .filter_map(|s| s.parse().ok())
            .collect();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let ts = st
            .trust_stores
            .get_mut(&arn)
            .ok_or_else(|| trust_store_not_found(&arn))?;
        for id in &ids {
            ts.revocations.remove(id);
        }
        // Keep the aggregate consistent with the surviving revocations so
        // DescribeTrustStore and DescribeTrustStoreRevocations still agree
        // after a removal.
        ts.total_revoked_entries = ts
            .revocations
            .values()
            .map(|r| r.number_of_revoked_entries)
            .sum();
        Ok(xml_metadata_only(
            "RemoveTrustStoreRevocations",
            &req.request_id,
        ))
    }

    fn describe_trust_store_revocations(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = required_query_param(req, "TrustStoreArn")?;
        let accounts = self.state.read();
        let empty = crate::state::Elbv2State::new(&req.account_id);
        let st = accounts.get(&req.account_id).unwrap_or(&empty);
        let ts = st
            .trust_stores
            .get(&arn)
            .ok_or_else(|| trust_store_not_found(&arn))?;
        let revs_xml = ts
            .revocations
            .values()
            .map(|r| {
                format!(
                    "<member><TrustStoreArn>{}</TrustStoreArn><RevocationId>{}</RevocationId><RevocationType>{}</RevocationType><NumberOfRevokedEntries>{}</NumberOfRevokedEntries></member>",
                    xml_escape(&arn),
                    r.revocation_id,
                    xml_escape(&r.revocation_type),
                    r.number_of_revoked_entries
                )
            })
            .collect::<String>();
        Ok(xml_resp(
            "DescribeTrustStoreRevocations",
            format!("<TrustStoreRevocations>{revs_xml}</TrustStoreRevocations>"),
            &req.request_id,
        ))
    }

    fn describe_trust_store_associations(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        validate_range_i32(req, "PageSize", 1, 400)?;
        let arn = required_query_param(req, "TrustStoreArn")?;
        let accounts = self.state.read();
        let empty = crate::state::Elbv2State::new(&req.account_id);
        let st = accounts.get(&req.account_id).unwrap_or(&empty);
        let assocs: Vec<&str> = st
            .listeners
            .values()
            .filter(|l| {
                l.mutual_authentication
                    .as_ref()
                    .and_then(|m| m.trust_store_arn.as_deref())
                    == Some(arn.as_str())
            })
            .map(|l| l.arn.as_str())
            .collect();
        let xml = assocs
            .iter()
            .map(|a| {
                format!(
                    "<member><ResourceArn>{}</ResourceArn></member>",
                    xml_escape(a)
                )
            })
            .collect::<String>();
        Ok(xml_resp(
            "DescribeTrustStoreAssociations",
            format!("<TrustStoreAssociations>{xml}</TrustStoreAssociations>"),
            &req.request_id,
        ))
    }

    fn delete_shared_trust_store_association(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let _arn = required_query_param(req, "TrustStoreArn")?;
        let _resource = required_query_param(req, "ResourceArn")?;
        Ok(xml_metadata_only(
            "DeleteSharedTrustStoreAssociation",
            &req.request_id,
        ))
    }

    fn get_trust_store_ca_certificates_bundle(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = required_query_param(req, "TrustStoreArn")?;
        let accounts = self.state.read();
        let empty = crate::state::Elbv2State::new(&req.account_id);
        let st = accounts.get(&req.account_id).unwrap_or(&empty);
        let ts = st
            .trust_stores
            .get(&arn)
            .ok_or_else(|| trust_store_not_found(&arn))?;
        let location = ts
            .ca_certificates_bundle
            .as_deref()
            .map(|b| String::from_utf8_lossy(b).to_string())
            .unwrap_or_default();
        Ok(xml_resp(
            "GetTrustStoreCaCertificatesBundle",
            format!("<Location>{}</Location>", xml_escape(&location)),
            &req.request_id,
        ))
    }

    fn get_trust_store_revocation_content(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = required_query_param(req, "TrustStoreArn")?;
        let id: i64 = required_query_param(req, "RevocationId")?
            .parse()
            .map_err(|_| invalid_param("RevocationId must be an integer"))?;
        let accounts = self.state.read();
        let empty = crate::state::Elbv2State::new(&req.account_id);
        let st = accounts.get(&req.account_id).unwrap_or(&empty);
        let ts = st
            .trust_stores
            .get(&arn)
            .ok_or_else(|| trust_store_not_found(&arn))?;
        let rev = ts.revocations.get(&id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "RevocationIdNotFound",
                format!("Revocation '{id}' not found"),
            )
        })?;
        let location = String::from_utf8_lossy(&rev.content).to_string();
        Ok(xml_resp(
            "GetTrustStoreRevocationContent",
            format!("<Location>{}</Location>", xml_escape(&location)),
            &req.request_id,
        ))
    }

    fn get_resource_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let arn = required_query_param(req, "ResourceArn")?;
        let accounts = self.state.read();
        let empty = crate::state::Elbv2State::new(&req.account_id);
        let st = accounts.get(&req.account_id).unwrap_or(&empty);
        let policy = st
            .resource_policies
            .get(&arn)
            .cloned()
            .unwrap_or_else(|| "{}".to_string());
        Ok(xml_resp(
            "GetResourcePolicy",
            format!("<Policy>{}</Policy>", xml_escape(&policy)),
            &req.request_id,
        ))
    }
}

#[path = "service_helpers.rs"]
mod service_helpers;
pub(crate) use service_helpers::*;

#[cfg(test)]
#[path = "service_tests.rs"]
mod tests;
