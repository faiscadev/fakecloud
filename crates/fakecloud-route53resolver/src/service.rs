//! Amazon Route 53 Resolver (`route53resolver`) awsJson1_1 service.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use parking_lot::RwLock;
use serde_json::{json, Value};
use tokio::sync::Mutex as AsyncMutex;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_persistence::SnapshotStore;

use crate::persistence::save_route53resolver_snapshot;
use crate::state::{
    EndpointRecord, FirewallConfig, FirewallDomainList, FirewallRule, FirewallRuleGroup,
    FirewallRuleGroupAssociation, IpAddressResponse, OutpostResolver, ResolverConfig,
    ResolverDnssecConfig, ResolverEndpoint, ResolverQueryLogConfig,
    ResolverQueryLogConfigAssociation, ResolverRule, ResolverRuleAssociation,
    Route53ResolverAccounts, SharedRoute53ResolverState,
};
use crate::validate::{
    arn, conflict, fnv1a, hex17, invalid_next_token, invalid_parameter, invalid_request, not_found,
    now_rfc3339, parse_tags, parse_target_ips, required_str, resource_in_use, synth_vpc,
    unknown_resource, validation,
};

/// How long a resource takes to settle from its transient creation state to its
/// terminal state (endpoint `CREATING`->`OPERATIONAL`, association
/// `CREATING`->`COMPLETE`, etc.). Fast, pure control-plane — no container.
const SETTLE_DELAY: Duration = Duration::from_millis(300);

const SUPPORTED_ACTIONS: &[&str] = &[
    "AssociateFirewallRuleGroup",
    "AssociateResolverEndpointIpAddress",
    "AssociateResolverQueryLogConfig",
    "AssociateResolverRule",
    "BatchCreateFirewallRule",
    "BatchDeleteFirewallRule",
    "BatchUpdateFirewallRule",
    "CreateFirewallDomainList",
    "CreateFirewallRule",
    "CreateFirewallRuleGroup",
    "CreateOutpostResolver",
    "CreateResolverEndpoint",
    "CreateResolverQueryLogConfig",
    "CreateResolverRule",
    "DeleteFirewallDomainList",
    "DeleteFirewallRule",
    "DeleteFirewallRuleGroup",
    "DeleteOutpostResolver",
    "DeleteResolverEndpoint",
    "DeleteResolverQueryLogConfig",
    "DeleteResolverRule",
    "DisassociateFirewallRuleGroup",
    "DisassociateResolverEndpointIpAddress",
    "DisassociateResolverQueryLogConfig",
    "DisassociateResolverRule",
    "GetFirewallConfig",
    "GetFirewallDomainList",
    "GetFirewallRuleGroup",
    "GetFirewallRuleGroupAssociation",
    "GetFirewallRuleGroupPolicy",
    "GetOutpostResolver",
    "GetResolverConfig",
    "GetResolverDnssecConfig",
    "GetResolverEndpoint",
    "GetResolverQueryLogConfig",
    "GetResolverQueryLogConfigAssociation",
    "GetResolverQueryLogConfigPolicy",
    "GetResolverRule",
    "GetResolverRuleAssociation",
    "GetResolverRulePolicy",
    "ImportFirewallDomains",
    "ListFirewallConfigs",
    "ListFirewallDomainLists",
    "ListFirewallDomains",
    "ListFirewallRuleGroupAssociations",
    "ListFirewallRuleGroups",
    "ListFirewallRuleTypes",
    "ListFirewallRules",
    "ListOutpostResolvers",
    "ListResolverConfigs",
    "ListResolverDnssecConfigs",
    "ListResolverEndpointIpAddresses",
    "ListResolverEndpoints",
    "ListResolverQueryLogConfigAssociations",
    "ListResolverQueryLogConfigs",
    "ListResolverRuleAssociations",
    "ListResolverRules",
    "ListTagsForResource",
    "PutFirewallRuleGroupPolicy",
    "PutResolverQueryLogConfigPolicy",
    "PutResolverRulePolicy",
    "TagResource",
    "UntagResource",
    "UpdateFirewallConfig",
    "UpdateFirewallDomains",
    "UpdateFirewallRule",
    "UpdateFirewallRuleGroupAssociation",
    "UpdateOutpostResolver",
    "UpdateResolverConfig",
    "UpdateResolverDnssecConfig",
    "UpdateResolverEndpoint",
    "UpdateResolverRule",
];

/// Actions that mutate persisted state and therefore trigger a snapshot write.
const NON_MUTATING_PREFIXES: &[&str] = &["Get", "List"];

/// What terminal state a background-settled resource lands in.
enum Settle {
    Endpoint,
    RuleAssociation,
    QueryLogAssociation,
    FirewallAssociation,
    Dnssec(String),
    ResolverConfig(String),
    Outpost,
}

pub struct Route53ResolverService {
    state: SharedRoute53ResolverState,
    ec2_state: Option<fakecloud_ec2::SharedEc2State>,
    s3_state: Option<fakecloud_s3::SharedS3State>,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<AsyncMutex<()>>,
}

impl Route53ResolverService {
    pub fn new(state: SharedRoute53ResolverState) -> Self {
        Self {
            state,
            ec2_state: None,
            s3_state: None,
            snapshot_store: None,
            snapshot_lock: Arc::new(AsyncMutex::new(())),
        }
    }

    pub fn with_ec2_state(mut self, ec2_state: fakecloud_ec2::SharedEc2State) -> Self {
        self.ec2_state = Some(ec2_state);
        self
    }

    /// Wire S3 state so `ImportFirewallDomains` can read the domains file the
    /// caller stored in S3 (the operation's only domain source).
    pub fn with_s3_state(mut self, s3_state: fakecloud_s3::SharedS3State) -> Self {
        self.s3_state = Some(s3_state);
        self
    }

    pub fn with_snapshot_store(mut self, store: Arc<dyn SnapshotStore>) -> Self {
        self.snapshot_store = Some(store);
        self
    }

    pub fn shared_state(&self) -> SharedRoute53ResolverState {
        Arc::clone(&self.state)
    }

    async fn save_snapshot(&self) {
        save_route53resolver_snapshot(
            &self.state,
            self.snapshot_store.clone(),
            &self.snapshot_lock,
        )
        .await;
    }

    /// Build a hook that persists current state when invoked, or `None` in
    /// memory mode. The CloudFormation provisioner mutates `state` directly and
    /// uses this to write CFN-provisioned resources through to disk.
    pub fn snapshot_hook(&self) -> Option<fakecloud_persistence::SnapshotHook> {
        let store = self.snapshot_store.clone()?;
        let state = self.state.clone();
        let lock = self.snapshot_lock.clone();
        Some(Arc::new(move || {
            let state = state.clone();
            let store = store.clone();
            let lock = lock.clone();
            Box::pin(async move {
                save_route53resolver_snapshot(&state, Some(store), &lock).await;
            })
        }))
    }

    /// After loading a snapshot, re-arm the background settle for any resource
    /// left in a transient state by a process that exited mid-transition.
    pub fn rearm_pending(&self) {
        let mut pending: Vec<(String, Settle, String)> = Vec::new();
        {
            let st = self.state.read();
            for (account, acc) in st.accounts.iter() {
                for (id, rec) in acc.endpoints.iter() {
                    if rec.endpoint.status == "CREATING" || rec.endpoint.status == "UPDATING" {
                        pending.push((account.clone(), Settle::Endpoint, id.clone()));
                    }
                }
                for (id, a) in acc.rule_associations.iter() {
                    if a.status == "CREATING" {
                        pending.push((account.clone(), Settle::RuleAssociation, id.clone()));
                    }
                }
                for (id, a) in acc.query_log_associations.iter() {
                    if a.status == "CREATING" {
                        pending.push((account.clone(), Settle::QueryLogAssociation, id.clone()));
                    }
                }
                for (id, a) in acc.firewall_rule_group_associations.iter() {
                    if a.status == "UPDATING" {
                        pending.push((account.clone(), Settle::FirewallAssociation, id.clone()));
                    }
                }
                for (id, o) in acc.outpost_resolvers.iter() {
                    if o.status == "CREATING" {
                        pending.push((account.clone(), Settle::Outpost, id.clone()));
                    }
                }
                // DNSSEC + resolver configs snapshotted mid-transition
                // (ENABLING/DISABLING/UPDATING_...) must resume settling to their
                // terminal status; otherwise a restart within the settle window
                // wedges them in the transient state forever.
                for (id, c) in acc.dnssec_configs.iter() {
                    if let Some(target) = transient_terminal(&c.validation_status) {
                        pending.push((account.clone(), Settle::Dnssec(target), id.clone()));
                    }
                }
                for (id, c) in acc.resolver_configs.iter() {
                    if let Some(target) = transient_terminal(&c.autodefined_reverse) {
                        pending.push((account.clone(), Settle::ResolverConfig(target), id.clone()));
                    }
                }
            }
        }
        for (account, kind, id) in pending {
            self.spawn_settle(account, kind, id);
        }
    }

    /// Sleep briefly, then flip a resource from its transient creation/update
    /// state to its terminal state and persist. Mirrors AWS's short async
    /// settle for Resolver control-plane resources.
    fn spawn_settle(&self, account: String, kind: Settle, id: String) {
        let state = Arc::clone(&self.state);
        let store = self.snapshot_store.clone();
        let lock = self.snapshot_lock.clone();
        tokio::spawn(async move {
            tokio::time::sleep(SETTLE_DELAY).await;
            {
                let mut st = state.write();
                let Some(acc) = st.accounts.get_mut(&account) else {
                    return;
                };
                match kind {
                    Settle::Endpoint => {
                        if let Some(rec) = acc.endpoints.get_mut(&id) {
                            rec.endpoint.status = "OPERATIONAL".to_string();
                            rec.endpoint.status_message =
                                "This Resolver Endpoint is operational.".to_string();
                            rec.endpoint.modification_time = now_rfc3339();
                            for ip in rec.ip_addresses.iter_mut() {
                                ip.status = "ATTACHED".to_string();
                                ip.status_message = "This IP address is operational.".to_string();
                            }
                        }
                    }
                    Settle::RuleAssociation => {
                        if let Some(a) = acc.rule_associations.get_mut(&id) {
                            a.status = "COMPLETE".to_string();
                        }
                    }
                    Settle::QueryLogAssociation => {
                        if let Some(a) = acc.query_log_associations.get_mut(&id) {
                            a.status = "ACTIVE".to_string();
                        }
                    }
                    Settle::FirewallAssociation => {
                        if let Some(a) = acc.firewall_rule_group_associations.get_mut(&id) {
                            a.status = "COMPLETE".to_string();
                            a.modification_time = now_rfc3339();
                        }
                    }
                    Settle::Dnssec(target) => {
                        if let Some(c) = acc.dnssec_configs.get_mut(&id) {
                            c.validation_status = target;
                        }
                    }
                    Settle::ResolverConfig(target) => {
                        if let Some(c) = acc.resolver_configs.get_mut(&id) {
                            c.autodefined_reverse = target;
                        }
                    }
                    Settle::Outpost => {
                        if let Some(o) = acc.outpost_resolvers.get_mut(&id) {
                            o.status = "OPERATIONAL".to_string();
                            o.modification_time = now_rfc3339();
                        }
                    }
                }
            }
            save_route53resolver_snapshot(&state, store, &lock).await;
        });
    }

    // ─── Cross-service EC2 validation ─────────────────────────────────────

    /// Resolve a subnet's VPC id from EC2 state. Returns `Ok(Some(vpc))` when the
    /// subnet is real, `Ok(None)` when EC2 state is not wired (accept and let the
    /// caller synthesize), or `Err(..)` when EC2 is wired but the subnet is
    /// absent — matching AWS's rejection of an unknown subnet.
    fn resolve_subnet_vpc(
        &self,
        account: &str,
        subnet_id: &str,
    ) -> Result<Option<String>, AwsServiceError> {
        let Some(ec2) = self.ec2_state.as_ref() else {
            return Ok(None);
        };
        let guard = ec2.read();
        match guard.get(account).and_then(|s| s.subnets.get(subnet_id)) {
            Some(subnet) => Ok(Some(subnet.vpc_id.clone())),
            None => Err(invalid_parameter(format!(
                "The subnet ID '{subnet_id}' does not exist"
            ))),
        }
    }

    /// Validate that every security group exists (when EC2 state is wired).
    fn validate_security_groups(
        &self,
        account: &str,
        sgs: &[String],
    ) -> Result<(), AwsServiceError> {
        let Some(ec2) = self.ec2_state.as_ref() else {
            return Ok(());
        };
        let guard = ec2.read();
        let acc = guard.get(account);
        for sg in sgs {
            let ok = acc
                .map(|s| s.security_groups.contains_key(sg))
                .unwrap_or(false);
            if !ok {
                return Err(invalid_parameter(format!(
                    "The security group '{sg}' does not exist"
                )));
            }
        }
        Ok(())
    }

    /// `true` when EC2 state is wired and the VPC does not exist. Returns
    /// `false` when EC2 is not wired (accept) or the VPC is present. Callers pick
    /// the error shape their operation declares (resolver ops:
    /// `InvalidParameterException`; firewall ops: `ValidationException`).
    fn vpc_missing(&self, account: &str, vpc_id: &str) -> bool {
        let Some(ec2) = self.ec2_state.as_ref() else {
            return false;
        };
        let guard = ec2.read();
        !guard
            .get(account)
            .map(|s| s.vpcs.contains_key(vpc_id))
            .unwrap_or(false)
    }
}

impl Default for Route53ResolverService {
    fn default() -> Self {
        Self::new(Arc::new(RwLock::new(Route53ResolverAccounts::new())))
    }
}

#[async_trait]
impl AwsService for Route53ResolverService {
    fn service_name(&self) -> &str {
        "route53resolver"
    }

    fn supported_actions(&self) -> &[&str] {
        SUPPORTED_ACTIONS
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let action = req.action.clone();
        validate_constraints(&action, &req.json_body())?;
        let mutates = !NON_MUTATING_PREFIXES.iter().any(|p| action.starts_with(p));
        let result = match action.as_str() {
            "CreateResolverEndpoint" => self.create_resolver_endpoint(&req),
            "GetResolverEndpoint" => self.get_resolver_endpoint(&req),
            "UpdateResolverEndpoint" => self.update_resolver_endpoint(&req),
            "DeleteResolverEndpoint" => self.delete_resolver_endpoint(&req),
            "ListResolverEndpoints" => self.list_resolver_endpoints(&req),
            "ListResolverEndpointIpAddresses" => self.list_resolver_endpoint_ip_addresses(&req),
            "AssociateResolverEndpointIpAddress" => self.associate_endpoint_ip(&req),
            "DisassociateResolverEndpointIpAddress" => self.disassociate_endpoint_ip(&req),

            "CreateResolverRule" => self.create_resolver_rule(&req),
            "GetResolverRule" => self.get_resolver_rule(&req),
            "UpdateResolverRule" => self.update_resolver_rule(&req),
            "DeleteResolverRule" => self.delete_resolver_rule(&req),
            "ListResolverRules" => self.list_resolver_rules(&req),
            "AssociateResolverRule" => self.associate_resolver_rule(&req),
            "DisassociateResolverRule" => self.disassociate_resolver_rule(&req),
            "GetResolverRuleAssociation" => self.get_resolver_rule_association(&req),
            "ListResolverRuleAssociations" => self.list_resolver_rule_associations(&req),

            "CreateResolverQueryLogConfig" => self.create_query_log_config(&req),
            "GetResolverQueryLogConfig" => self.get_query_log_config(&req),
            "DeleteResolverQueryLogConfig" => self.delete_query_log_config(&req),
            "ListResolverQueryLogConfigs" => self.list_query_log_configs(&req),
            "AssociateResolverQueryLogConfig" => self.associate_query_log_config(&req),
            "DisassociateResolverQueryLogConfig" => self.disassociate_query_log_config(&req),
            "GetResolverQueryLogConfigAssociation" => self.get_query_log_association(&req),
            "ListResolverQueryLogConfigAssociations" => self.list_query_log_associations(&req),

            "GetResolverConfig" => self.get_resolver_config(&req),
            "UpdateResolverConfig" => self.update_resolver_config(&req),
            "ListResolverConfigs" => self.list_resolver_configs(&req),

            "GetResolverDnssecConfig" => self.get_dnssec_config(&req),
            "UpdateResolverDnssecConfig" => self.update_dnssec_config(&req),
            "ListResolverDnssecConfigs" => self.list_dnssec_configs(&req),

            "CreateFirewallRuleGroup" => self.create_firewall_rule_group(&req),
            "GetFirewallRuleGroup" => self.get_firewall_rule_group(&req),
            "DeleteFirewallRuleGroup" => self.delete_firewall_rule_group(&req),
            "ListFirewallRuleGroups" => self.list_firewall_rule_groups(&req),

            "CreateFirewallDomainList" => self.create_firewall_domain_list(&req),
            "GetFirewallDomainList" => self.get_firewall_domain_list(&req),
            "DeleteFirewallDomainList" => self.delete_firewall_domain_list(&req),
            "ListFirewallDomainLists" => self.list_firewall_domain_lists(&req),
            "ImportFirewallDomains" => self.import_firewall_domains(&req),
            "UpdateFirewallDomains" => self.update_firewall_domains(&req),
            "ListFirewallDomains" => self.list_firewall_domains(&req),

            "CreateFirewallRule" => self.create_firewall_rule(&req),
            "UpdateFirewallRule" => self.update_firewall_rule(&req),
            "DeleteFirewallRule" => self.delete_firewall_rule(&req),
            "ListFirewallRules" => self.list_firewall_rules(&req),
            "ListFirewallRuleTypes" => self.list_firewall_rule_types(&req),
            "BatchCreateFirewallRule" => self.batch_create_firewall_rule(&req),
            "BatchUpdateFirewallRule" => self.batch_update_firewall_rule(&req),
            "BatchDeleteFirewallRule" => self.batch_delete_firewall_rule(&req),

            "AssociateFirewallRuleGroup" => self.associate_firewall_rule_group(&req),
            "DisassociateFirewallRuleGroup" => self.disassociate_firewall_rule_group(&req),
            "GetFirewallRuleGroupAssociation" => self.get_firewall_rule_group_association(&req),
            "UpdateFirewallRuleGroupAssociation" => {
                self.update_firewall_rule_group_association(&req)
            }
            "ListFirewallRuleGroupAssociations" => self.list_firewall_rule_group_associations(&req),

            "GetFirewallConfig" => self.get_firewall_config(&req),
            "UpdateFirewallConfig" => self.update_firewall_config(&req),
            "ListFirewallConfigs" => self.list_firewall_configs(&req),

            "CreateOutpostResolver" => self.create_outpost_resolver(&req),
            "GetOutpostResolver" => self.get_outpost_resolver(&req),
            "UpdateOutpostResolver" => self.update_outpost_resolver(&req),
            "DeleteOutpostResolver" => self.delete_outpost_resolver(&req),
            "ListOutpostResolvers" => self.list_outpost_resolvers(&req),

            "PutFirewallRuleGroupPolicy" => self.put_firewall_rule_group_policy(&req),
            "GetFirewallRuleGroupPolicy" => self.get_firewall_rule_group_policy(&req),
            "PutResolverQueryLogConfigPolicy" => self.put_query_log_config_policy(&req),
            "GetResolverQueryLogConfigPolicy" => self.get_query_log_config_policy(&req),
            "PutResolverRulePolicy" => self.put_resolver_rule_policy(&req),
            "GetResolverRulePolicy" => self.get_resolver_rule_policy(&req),

            "TagResource" => self.tag_resource(&req),
            "UntagResource" => self.untag_resource(&req),
            "ListTagsForResource" => self.list_tags_for_resource(&req),

            other => Err(AwsServiceError::action_not_implemented(
                "route53resolver",
                other,
            )),
        };
        if mutates && matches!(result.as_ref(), Ok(resp) if resp.status.is_success()) {
            self.save_snapshot().await;
        }
        result
    }
}

// ─── Resolver endpoints ──────────────────────────────────────────────────

impl Route53ResolverService {
    fn create_resolver_endpoint(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let account = account_id(req);
        let region = region(req);
        let creator_request_id = required_str(&body, "CreatorRequestId")?;
        let direction = required_str(&body, "Direction")?;
        if direction != "INBOUND" && direction != "OUTBOUND" && direction != "INBOUND_DELEGATION" {
            return Err(invalid_parameter(format!("Invalid Direction: {direction}")));
        }
        let security_group_ids: Vec<String> = body
            .get("SecurityGroupIds")
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default();
        if security_group_ids.is_empty() {
            return Err(invalid_parameter("SecurityGroupIds is required"));
        }
        self.validate_security_groups(&account, &security_group_ids)?;

        // A Resolver endpoint requires at least two IP addresses (the DNS
        // service is highly available across two subnets); the model bounds
        // `IpAddresses` to [2, 20].
        let ip_reqs = body
            .get("IpAddresses")
            .and_then(Value::as_array)
            .ok_or_else(|| invalid_parameter("IpAddresses is required"))?;
        if ip_reqs.len() < 2 {
            return Err(invalid_request(
                "Resolver endpoints must have at least two IP addresses in two different subnets",
            ));
        }

        // When EC2 state is wired, every subnet must exist and share one VPC.
        // In standalone mode (no EC2 state) there are no real subnets, so a
        // valid multi-subnet endpoint (AWS requires two) must still succeed: all
        // its subnets map to a single synthesized VPC derived from the first
        // subnet rather than a distinct VPC per subnet.
        let ec2_wired = self.ec2_state.is_some();
        let now = now_rfc3339();
        let mut ip_addresses = Vec::new();
        let mut host_vpc: Option<String> = None;
        for ipr in ip_reqs {
            let subnet_id = ipr
                .get("SubnetId")
                .and_then(Value::as_str)
                .ok_or_else(|| invalid_parameter("Each IpAddress requires a SubnetId"))?
                .to_string();
            if ec2_wired {
                let vpc = self
                    .resolve_subnet_vpc(&account, &subnet_id)?
                    .unwrap_or_else(|| synth_vpc(&subnet_id));
                match &host_vpc {
                    Some(existing) if existing != &vpc => {
                        return Err(invalid_parameter(
                            "All subnets for a Resolver endpoint must be in the same VPC",
                        ));
                    }
                    _ => host_vpc = Some(vpc),
                }
            } else if host_vpc.is_none() {
                host_vpc = Some(synth_vpc(&subnet_id));
            }
            let ip = ipr.get("Ip").and_then(Value::as_str).map(str::to_string);
            let ipv6 = ipr.get("Ipv6").and_then(Value::as_str).map(str::to_string);
            ip_addresses.push(IpAddressResponse {
                ip_id: format!("rni-{}", hex17()),
                subnet_id,
                ip: Some(ip.unwrap_or_else(|| synth_ip(&ip_addresses))),
                ipv6,
                status: "CREATING".to_string(),
                status_message: "Creating the IP address".to_string(),
                creation_time: now.clone(),
                modification_time: now.clone(),
            });
        }
        let host_vpc_id = host_vpc.unwrap_or_default();
        let prefix = if direction == "OUTBOUND" {
            "rslvr-out-"
        } else {
            "rslvr-in-"
        };
        let id = format!("{prefix}{}", hex17());
        let endpoint = ResolverEndpoint {
            id: id.clone(),
            creator_request_id,
            arn: arn(&region, &account, "resolver-endpoint", &id),
            name: body.get("Name").and_then(Value::as_str).map(str::to_string),
            security_group_ids,
            direction,
            ip_address_count: ip_addresses.len() as i64,
            host_vpc_id,
            status: "CREATING".to_string(),
            status_message: "[Trace id: 1] Creating the Resolver Endpoint".to_string(),
            creation_time: now.clone(),
            modification_time: now,
            resolver_endpoint_type: body
                .get("ResolverEndpointType")
                .and_then(Value::as_str)
                .unwrap_or("IPV4")
                .to_string(),
            protocols: string_list(body.get("Protocols")),
            outpost_arn: body
                .get("OutpostArn")
                .and_then(Value::as_str)
                .map(str::to_string),
            preferred_instance_type: body
                .get("PreferredInstanceType")
                .and_then(Value::as_str)
                .map(str::to_string),
            dns64_enabled: body.get("Dns64Enabled").and_then(Value::as_bool),
            ipv6_internet_access_enabled: body
                .get("Ipv6InternetAccessEnabled")
                .and_then(Value::as_bool),
            rni_enhanced_metrics_enabled: body
                .get("RniEnhancedMetricsEnabled")
                .and_then(Value::as_bool),
            target_name_server_metrics_enabled: body
                .get("TargetNameServerMetricsEnabled")
                .and_then(Value::as_bool),
        };
        let arn_str = endpoint.arn.clone();
        let tags = parse_tags(body.get("Tags"))?;
        {
            let mut st = self.state.write();
            let acc = st.account_mut(&account);
            acc.endpoints.insert(
                id.clone(),
                EndpointRecord {
                    endpoint: endpoint.clone(),
                    ip_addresses,
                },
            );
            if !tags.is_empty() {
                acc.tags.insert(arn_str, tags);
            }
        }
        self.spawn_settle(account, Settle::Endpoint, id);
        Ok(AwsResponse::ok_json(
            json!({ "ResolverEndpoint": to_val(&endpoint) }),
        ))
    }

    fn get_resolver_endpoint(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = required_str(&body, "ResolverEndpointId")?;
        let st = self.state.read();
        let rec = st
            .accounts
            .get(&account_id(req))
            .and_then(|a| a.endpoints.get(&id))
            .ok_or_else(|| not_found(format!("Resolver endpoint '{id}' not found")))?;
        Ok(AwsResponse::ok_json(
            json!({ "ResolverEndpoint": to_val(&rec.endpoint) }),
        ))
    }

    fn update_resolver_endpoint(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = required_str(&body, "ResolverEndpointId")?;
        let account = account_id(req);
        let endpoint;
        {
            let mut st = self.state.write();
            let rec = st
                .accounts
                .get_mut(&account)
                .and_then(|a| a.endpoints.get_mut(&id))
                .ok_or_else(|| not_found(format!("Resolver endpoint '{id}' not found")))?;
            if let Some(name) = body.get("Name").and_then(Value::as_str) {
                rec.endpoint.name = Some(name.to_string());
            }
            if let Some(t) = body.get("ResolverEndpointType").and_then(Value::as_str) {
                rec.endpoint.resolver_endpoint_type = t.to_string();
            }
            let protocols = string_list(body.get("Protocols"));
            if !protocols.is_empty() {
                rec.endpoint.protocols = protocols;
            }
            if let Some(v) = body.get("Dns64Enabled").and_then(Value::as_bool) {
                rec.endpoint.dns64_enabled = Some(v);
            }
            if let Some(v) = body
                .get("Ipv6InternetAccessEnabled")
                .and_then(Value::as_bool)
            {
                rec.endpoint.ipv6_internet_access_enabled = Some(v);
            }
            if let Some(v) = body
                .get("RniEnhancedMetricsEnabled")
                .and_then(Value::as_bool)
            {
                rec.endpoint.rni_enhanced_metrics_enabled = Some(v);
            }
            if let Some(v) = body
                .get("TargetNameServerMetricsEnabled")
                .and_then(Value::as_bool)
            {
                rec.endpoint.target_name_server_metrics_enabled = Some(v);
            }
            rec.endpoint.status = "UPDATING".to_string();
            rec.endpoint.modification_time = now_rfc3339();
            endpoint = rec.endpoint.clone();
        }
        self.spawn_settle(account, Settle::Endpoint, id);
        Ok(AwsResponse::ok_json(
            json!({ "ResolverEndpoint": to_val(&endpoint) }),
        ))
    }

    fn delete_resolver_endpoint(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = required_str(&body, "ResolverEndpointId")?;
        let account = account_id(req);
        let mut st = self.state.write();
        let acc = st
            .accounts
            .get_mut(&account)
            .ok_or_else(|| not_found(format!("Resolver endpoint '{id}' not found")))?;
        let mut rec = acc
            .endpoints
            .remove(&id)
            .ok_or_else(|| not_found(format!("Resolver endpoint '{id}' not found")))?;
        acc.tags.remove(&rec.endpoint.arn);
        rec.endpoint.status = "DELETING".to_string();
        rec.endpoint.modification_time = now_rfc3339();
        Ok(AwsResponse::ok_json(
            json!({ "ResolverEndpoint": to_val(&rec.endpoint) }),
        ))
    }

    fn list_resolver_endpoints(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let all: Vec<Value> = self
            .state
            .read()
            .accounts
            .get(&account_id(req))
            .map(|a| a.endpoints.values().map(|r| to_val(&r.endpoint)).collect())
            .unwrap_or_default();
        let all = apply_filters(all, &body, |name| match name {
            "CreatorRequestId" => Some("CreatorRequestId"),
            "Direction" => Some("Direction"),
            "HostVPCId" => Some("HostVPCId"),
            "IpAddressCount" => Some("IpAddressCount"),
            "Name" => Some("Name"),
            "SecurityGroupIds" => Some("SecurityGroupIds"),
            "Status" => Some("Status"),
            _ => None,
        })?;
        let (page, next) = paginate(&body, all);
        Ok(list_response(
            json!({ "MaxResults": echoed_max(&body, page.len()), "ResolverEndpoints": page }),
            next,
        ))
    }

    fn list_resolver_endpoint_ip_addresses(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = required_str(&body, "ResolverEndpointId")?;
        let st = self.state.read();
        let rec = st
            .accounts
            .get(&account_id(req))
            .and_then(|a| a.endpoints.get(&id))
            .ok_or_else(|| not_found(format!("Resolver endpoint '{id}' not found")))?;
        let ips: Vec<Value> = rec.ip_addresses.iter().map(to_val).collect();
        let (page, next) = paginate(&body, ips);
        Ok(list_response(
            json!({ "MaxResults": echoed_max(&body, page.len()), "IpAddresses": page }),
            next,
        ))
    }

    fn associate_endpoint_ip(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = required_str(&body, "ResolverEndpointId")?;
        let account = account_id(req);
        let update = body
            .get("IpAddress")
            .filter(|v| v.is_object())
            .ok_or_else(|| invalid_parameter("IpAddress is required"))?;
        let subnet_id = update
            .get("SubnetId")
            .and_then(Value::as_str)
            .ok_or_else(|| invalid_parameter("IpAddress.SubnetId is required"))?
            .to_string();
        self.resolve_subnet_vpc(&account, &subnet_id)?;
        let ip = update.get("Ip").and_then(Value::as_str).map(str::to_string);
        let ipv6 = update
            .get("Ipv6")
            .and_then(Value::as_str)
            .map(str::to_string);
        let now = now_rfc3339();
        let endpoint;
        {
            let mut st = self.state.write();
            let rec = st
                .accounts
                .get_mut(&account)
                .and_then(|a| a.endpoints.get_mut(&id))
                .ok_or_else(|| not_found(format!("Resolver endpoint '{id}' not found")))?;
            rec.ip_addresses.push(IpAddressResponse {
                ip_id: format!("rni-{}", hex17()),
                subnet_id,
                ip: Some(ip.unwrap_or_else(|| synth_ip(&rec.ip_addresses))),
                ipv6,
                status: "CREATING".to_string(),
                status_message: "Creating the IP address".to_string(),
                creation_time: now.clone(),
                modification_time: now.clone(),
            });
            rec.endpoint.ip_address_count = rec.ip_addresses.len() as i64;
            rec.endpoint.status = "UPDATING".to_string();
            rec.endpoint.modification_time = now;
            endpoint = rec.endpoint.clone();
        }
        self.spawn_settle(account, Settle::Endpoint, id);
        Ok(AwsResponse::ok_json(
            json!({ "ResolverEndpoint": to_val(&endpoint) }),
        ))
    }

    fn disassociate_endpoint_ip(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = required_str(&body, "ResolverEndpointId")?;
        let account = account_id(req);
        let update = body
            .get("IpAddress")
            .filter(|v| v.is_object())
            .ok_or_else(|| invalid_parameter("IpAddress is required"))?;
        let ip_id = update
            .get("IpId")
            .and_then(Value::as_str)
            .map(str::to_string);
        let subnet_id = update
            .get("SubnetId")
            .and_then(Value::as_str)
            .map(str::to_string);
        let ip = update.get("Ip").and_then(Value::as_str).map(str::to_string);
        let endpoint;
        {
            let mut st = self.state.write();
            let rec = st
                .accounts
                .get_mut(&account)
                .and_then(|a| a.endpoints.get_mut(&id))
                .ok_or_else(|| not_found(format!("Resolver endpoint '{id}' not found")))?;
            // A Resolver endpoint must retain at least two IP addresses.
            if rec.ip_addresses.len() <= 2 {
                return Err(invalid_request(
                    "A Resolver endpoint must have at least two IP addresses",
                ));
            }
            let pos = rec.ip_addresses.iter().position(|a| {
                ip_id.as_deref().map(|x| x == a.ip_id).unwrap_or(false)
                    || (subnet_id.as_deref() == Some(a.subnet_id.as_str())
                        && ip.as_deref() == a.ip.as_deref())
            });
            let Some(pos) = pos else {
                return Err(not_found("The specified IP address was not found"));
            };
            rec.ip_addresses.remove(pos);
            rec.endpoint.ip_address_count = rec.ip_addresses.len() as i64;
            rec.endpoint.status = "UPDATING".to_string();
            rec.endpoint.modification_time = now_rfc3339();
            endpoint = rec.endpoint.clone();
        }
        self.spawn_settle(account, Settle::Endpoint, id);
        Ok(AwsResponse::ok_json(
            json!({ "ResolverEndpoint": to_val(&endpoint) }),
        ))
    }
}

// ─── Resolver rules + associations ───────────────────────────────────────

impl Route53ResolverService {
    fn create_resolver_rule(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let account = account_id(req);
        let region = region(req);
        let creator_request_id = required_str(&body, "CreatorRequestId")?;
        let rule_type = required_str(&body, "RuleType")?;
        if !["FORWARD", "SYSTEM", "RECURSIVE", "DELEGATE"].contains(&rule_type.as_str()) {
            return Err(invalid_parameter(format!("Invalid RuleType: {rule_type}")));
        }
        let domain_name = body
            .get("DomainName")
            .and_then(Value::as_str)
            .map(str::to_string);
        let resolver_endpoint_id = body
            .get("ResolverEndpointId")
            .and_then(Value::as_str)
            .map(str::to_string);
        let target_ips = parse_target_ips(body.get("TargetIps"))?;
        // A FORWARD rule forwards queries to specific target IPs via an OUTBOUND
        // resolver endpoint, so both TargetIps and the endpoint are required.
        if rule_type == "FORWARD" {
            if target_ips.is_empty() {
                return Err(invalid_request(
                    "TargetIps is required for a rule of type FORWARD",
                ));
            }
            let ep_id = resolver_endpoint_id
                .clone()
                .ok_or_else(|| invalid_parameter("A FORWARD rule requires a ResolverEndpointId"))?;
            let st = self.state.read();
            let ep = st
                .accounts
                .get(&account)
                .and_then(|a| a.endpoints.get(&ep_id))
                .ok_or_else(|| not_found(format!("Resolver endpoint '{ep_id}' not found")))?;
            if ep.endpoint.direction != "OUTBOUND" {
                return Err(invalid_parameter(
                    "A FORWARD rule must reference an OUTBOUND Resolver endpoint",
                ));
            }
        }
        let id = format!("rslvr-rr-{}", hex17());
        let rule = ResolverRule {
            id: id.clone(),
            creator_request_id,
            arn: arn(&region, &account, "resolver-rule", &id),
            domain_name,
            status: "COMPLETE".to_string(),
            status_message: "[Trace id: 1] Successfully created Resolver Rule".to_string(),
            rule_type,
            name: body.get("Name").and_then(Value::as_str).map(str::to_string),
            target_ips,
            resolver_endpoint_id,
            owner_id: account.clone(),
            share_status: "NOT_SHARED".to_string(),
            creation_time: now_rfc3339(),
            modification_time: now_rfc3339(),
            delegation_record: body
                .get("DelegationRecord")
                .and_then(Value::as_str)
                .map(str::to_string),
        };
        let arn_str = rule.arn.clone();
        let tags = parse_tags(body.get("Tags"))?;
        let mut st = self.state.write();
        let acc = st.account_mut(&account);
        acc.rules.insert(id, rule.clone());
        if !tags.is_empty() {
            acc.tags.insert(arn_str, tags);
        }
        Ok(AwsResponse::ok_json(
            json!({ "ResolverRule": to_val(&rule) }),
        ))
    }

    fn get_resolver_rule(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = required_str(&body, "ResolverRuleId")?;
        let st = self.state.read();
        let rule = st
            .accounts
            .get(&account_id(req))
            .and_then(|a| a.rules.get(&id))
            .ok_or_else(|| not_found(format!("Resolver rule '{id}' not found")))?;
        Ok(AwsResponse::ok_json(
            json!({ "ResolverRule": to_val(rule) }),
        ))
    }

    fn update_resolver_rule(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = required_str(&body, "ResolverRuleId")?;
        let config = body
            .get("Config")
            .filter(|v| v.is_object())
            .ok_or_else(|| invalid_parameter("Config is required"))?;
        let target_ips = parse_target_ips(config.get("TargetIps"))?;
        let mut st = self.state.write();
        let rule = st
            .accounts
            .get_mut(&account_id(req))
            .and_then(|a| a.rules.get_mut(&id))
            .ok_or_else(|| not_found(format!("Resolver rule '{id}' not found")))?;
        if let Some(name) = config.get("Name").and_then(Value::as_str) {
            rule.name = Some(name.to_string());
        }
        if let Some(ep) = config.get("ResolverEndpointId").and_then(Value::as_str) {
            rule.resolver_endpoint_id = Some(ep.to_string());
        }
        if !target_ips.is_empty() {
            rule.target_ips = target_ips;
        }
        rule.modification_time = now_rfc3339();
        Ok(AwsResponse::ok_json(
            json!({ "ResolverRule": to_val(rule) }),
        ))
    }

    fn delete_resolver_rule(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = required_str(&body, "ResolverRuleId")?;
        let mut st = self.state.write();
        let acc = st
            .accounts
            .get_mut(&account_id(req))
            .ok_or_else(|| not_found(format!("Resolver rule '{id}' not found")))?;
        if acc
            .rule_associations
            .values()
            .any(|a| a.resolver_rule_id == id)
        {
            return Err(resource_in_use(
                "Cannot delete a Resolver rule that has associations. Disassociate it first",
            ));
        }
        let mut rule = acc
            .rules
            .remove(&id)
            .ok_or_else(|| not_found(format!("Resolver rule '{id}' not found")))?;
        acc.tags.remove(&rule.arn);
        rule.status = "DELETING".to_string();
        Ok(AwsResponse::ok_json(
            json!({ "ResolverRule": to_val(&rule) }),
        ))
    }

    fn list_resolver_rules(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let all: Vec<Value> = self
            .state
            .read()
            .accounts
            .get(&account_id(req))
            .map(|a| a.rules.values().map(to_val).collect())
            .unwrap_or_default();
        let all = apply_filters(all, &body, |name| match name {
            "CreatorRequestId" => Some("CreatorRequestId"),
            "DomainName" => Some("DomainName"),
            "Name" => Some("Name"),
            "ResolverEndpointId" => Some("ResolverEndpointId"),
            "Status" => Some("Status"),
            "Type" => Some("RuleType"),
            _ => None,
        })?;
        let (page, next) = paginate(&body, all);
        Ok(list_response(
            json!({ "MaxResults": echoed_max(&body, page.len()), "ResolverRules": page }),
            next,
        ))
    }

    fn associate_resolver_rule(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let account = account_id(req);
        let rule_id = required_str(&body, "ResolverRuleId")?;
        let vpc_id = required_str(&body, "VPCId")?;
        if self.vpc_missing(&account, &vpc_id) {
            return Err(invalid_parameter(format!(
                "The vpc ID '{vpc_id}' does not exist"
            )));
        }
        {
            let st = self.state.read();
            let acc = st.accounts.get(&account);
            if acc.map(|a| !a.rules.contains_key(&rule_id)).unwrap_or(true) {
                return Err(not_found(format!("Resolver rule '{rule_id}' not found")));
            }
            if acc
                .map(|a| {
                    a.rule_associations
                        .values()
                        .any(|x| x.resolver_rule_id == rule_id && x.vpc_id == vpc_id)
                })
                .unwrap_or(false)
            {
                return Err(crate::validate::resource_exists(
                    "The Resolver rule is already associated with this VPC",
                ));
            }
        }
        let id = format!("rslvr-rrassoc-{}", hex17());
        let assoc = ResolverRuleAssociation {
            id: id.clone(),
            resolver_rule_id: rule_id,
            name: body.get("Name").and_then(Value::as_str).map(str::to_string),
            vpc_id,
            status: "CREATING".to_string(),
            status_message: "[Trace id: 1] Creating the association".to_string(),
        };
        {
            let mut st = self.state.write();
            st.account_mut(&account)
                .rule_associations
                .insert(id.clone(), assoc.clone());
        }
        self.spawn_settle(account, Settle::RuleAssociation, id);
        Ok(AwsResponse::ok_json(
            json!({ "ResolverRuleAssociation": to_val(&assoc) }),
        ))
    }

    fn disassociate_resolver_rule(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let account = account_id(req);
        let rule_id = required_str(&body, "ResolverRuleId")?;
        let vpc_id = required_str(&body, "VPCId")?;
        let mut st = self.state.write();
        let acc = st
            .accounts
            .get_mut(&account)
            .ok_or_else(|| not_found("Association not found"))?;
        let key = acc
            .rule_associations
            .iter()
            .find(|(_, a)| a.resolver_rule_id == rule_id && a.vpc_id == vpc_id)
            .map(|(k, _)| k.clone())
            .ok_or_else(|| not_found("The specified rule/VPC association was not found"))?;
        let mut assoc = acc.rule_associations.remove(&key).unwrap();
        assoc.status = "DELETING".to_string();
        Ok(AwsResponse::ok_json(
            json!({ "ResolverRuleAssociation": to_val(&assoc) }),
        ))
    }

    fn get_resolver_rule_association(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = required_str(&body, "ResolverRuleAssociationId")?;
        let st = self.state.read();
        let assoc = st
            .accounts
            .get(&account_id(req))
            .and_then(|a| a.rule_associations.get(&id))
            .ok_or_else(|| not_found(format!("Association '{id}' not found")))?;
        Ok(AwsResponse::ok_json(
            json!({ "ResolverRuleAssociation": to_val(assoc) }),
        ))
    }

    fn list_resolver_rule_associations(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let all: Vec<Value> = self
            .state
            .read()
            .accounts
            .get(&account_id(req))
            .map(|a| a.rule_associations.values().map(to_val).collect())
            .unwrap_or_default();
        let all = apply_filters(all, &body, |name| match name {
            "Name" => Some("Name"),
            "ResolverRuleId" => Some("ResolverRuleId"),
            "Status" => Some("Status"),
            "VPCId" => Some("VPCId"),
            _ => None,
        })?;
        let (page, next) = paginate(&body, all);
        Ok(list_response(
            json!({ "MaxResults": echoed_max(&body, page.len()), "ResolverRuleAssociations": page }),
            next,
        ))
    }
}

// ─── Query-log configs + associations ────────────────────────────────────

impl Route53ResolverService {
    fn create_query_log_config(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let account = account_id(req);
        let region = region(req);
        let name = required_str(&body, "Name")?;
        let destination_arn = required_str(&body, "DestinationArn")?;
        let creator_request_id = required_str(&body, "CreatorRequestId")?;
        let id = format!("rslvr-qlc-{}", hex17());
        let cfg = ResolverQueryLogConfig {
            id: id.clone(),
            owner_id: account.clone(),
            status: "CREATED".to_string(),
            share_status: "NOT_SHARED".to_string(),
            association_count: 0,
            arn: arn(&region, &account, "resolver-query-log-config", &id),
            name,
            destination_arn,
            creator_request_id,
            creation_time: now_rfc3339(),
        };
        let arn_str = cfg.arn.clone();
        let tags = parse_tags(body.get("Tags"))?;
        let mut st = self.state.write();
        let acc = st.account_mut(&account);
        acc.query_log_configs.insert(id, cfg.clone());
        if !tags.is_empty() {
            acc.tags.insert(arn_str, tags);
        }
        Ok(AwsResponse::ok_json(
            json!({ "ResolverQueryLogConfig": to_val(&cfg) }),
        ))
    }

    fn get_query_log_config(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = required_str(&body, "ResolverQueryLogConfigId")?;
        let st = self.state.read();
        let cfg = st
            .accounts
            .get(&account_id(req))
            .and_then(|a| a.query_log_configs.get(&id))
            .ok_or_else(|| not_found(format!("Query log config '{id}' not found")))?;
        Ok(AwsResponse::ok_json(
            json!({ "ResolverQueryLogConfig": to_val(cfg) }),
        ))
    }

    fn delete_query_log_config(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = required_str(&body, "ResolverQueryLogConfigId")?;
        let mut st = self.state.write();
        let acc = st
            .accounts
            .get_mut(&account_id(req))
            .ok_or_else(|| not_found(format!("Query log config '{id}' not found")))?;
        if acc
            .query_log_associations
            .values()
            .any(|a| a.resolver_query_log_config_id == id)
        {
            return Err(invalid_request(
                "Cannot delete a query logging configuration that still has associations",
            ));
        }
        let mut cfg = acc
            .query_log_configs
            .remove(&id)
            .ok_or_else(|| not_found(format!("Query log config '{id}' not found")))?;
        acc.tags.remove(&cfg.arn);
        cfg.status = "DELETING".to_string();
        Ok(AwsResponse::ok_json(
            json!({ "ResolverQueryLogConfig": to_val(&cfg) }),
        ))
    }

    fn list_query_log_configs(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let all: Vec<Value> = self
            .state
            .read()
            .accounts
            .get(&account_id(req))
            .map(|a| a.query_log_configs.values().map(to_val).collect())
            .unwrap_or_default();
        let total = all.len();
        let all = apply_filters(all, &body, |name| match name {
            "Arn" => Some("Arn"),
            "AssociationCount" => Some("AssociationCount"),
            "CreationTime" => Some("CreationTime"),
            "CreatorRequestId" => Some("CreatorRequestId"),
            "Destination" | "DestinationArn" => Some("DestinationArn"),
            "Id" => Some("Id"),
            "Name" => Some("Name"),
            "OwnerId" => Some("OwnerId"),
            "Status" => Some("Status"),
            _ => None,
        })?;
        let filtered = all.len();
        let (list, next) = paginate(&body, all);
        Ok(list_response(
            json!({
                "TotalCount": total,
                "TotalFilteredCount": filtered,
                "ResolverQueryLogConfigs": list,
            }),
            next,
        ))
    }

    fn associate_query_log_config(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let account = account_id(req);
        let cfg_id = required_str(&body, "ResolverQueryLogConfigId")?;
        let resource_id = required_str(&body, "ResourceId")?;
        if self.vpc_missing(&account, &resource_id) {
            return Err(invalid_parameter(format!(
                "The vpc ID '{resource_id}' does not exist"
            )));
        }
        {
            let st = self.state.read();
            let acc = st.accounts.get(&account);
            if acc
                .map(|a| !a.query_log_configs.contains_key(&cfg_id))
                .unwrap_or(true)
            {
                return Err(not_found(format!("Query log config '{cfg_id}' not found")));
            }
            // A query-log config can be associated with a given resource only
            // once; a duplicate is rejected (matching AWS + the rule-association
            // path) so the AssociationCount is not double-counted.
            if acc
                .map(|a| {
                    a.query_log_associations.values().any(|x| {
                        x.resolver_query_log_config_id == cfg_id && x.resource_id == resource_id
                    })
                })
                .unwrap_or(false)
            {
                return Err(crate::validate::resource_exists(
                    "The query logging configuration is already associated with this resource",
                ));
            }
        }
        let id = format!("rslvr-qlcassoc-{}", hex17());
        let assoc = ResolverQueryLogConfigAssociation {
            id: id.clone(),
            resolver_query_log_config_id: cfg_id.clone(),
            resource_id,
            status: "CREATING".to_string(),
            error: None,
            error_message: None,
            creation_time: now_rfc3339(),
        };
        {
            let mut st = self.state.write();
            let acc = st.account_mut(&account);
            acc.query_log_associations.insert(id.clone(), assoc.clone());
            if let Some(cfg) = acc.query_log_configs.get_mut(&cfg_id) {
                cfg.association_count += 1;
            }
        }
        self.spawn_settle(account, Settle::QueryLogAssociation, id);
        Ok(AwsResponse::ok_json(
            json!({ "ResolverQueryLogConfigAssociation": to_val(&assoc) }),
        ))
    }

    fn disassociate_query_log_config(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let account = account_id(req);
        let cfg_id = required_str(&body, "ResolverQueryLogConfigId")?;
        let resource_id = required_str(&body, "ResourceId")?;
        let mut st = self.state.write();
        let acc = st
            .accounts
            .get_mut(&account)
            .ok_or_else(|| not_found("Association not found"))?;
        let key = acc
            .query_log_associations
            .iter()
            .find(|(_, a)| a.resolver_query_log_config_id == cfg_id && a.resource_id == resource_id)
            .map(|(k, _)| k.clone())
            .ok_or_else(|| not_found("The specified query log association was not found"))?;
        let mut assoc = acc.query_log_associations.remove(&key).unwrap();
        assoc.status = "DELETING".to_string();
        if let Some(cfg) = acc.query_log_configs.get_mut(&cfg_id) {
            cfg.association_count = (cfg.association_count - 1).max(0);
        }
        Ok(AwsResponse::ok_json(
            json!({ "ResolverQueryLogConfigAssociation": to_val(&assoc) }),
        ))
    }

    fn get_query_log_association(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = required_str(&body, "ResolverQueryLogConfigAssociationId")?;
        let st = self.state.read();
        let assoc = st
            .accounts
            .get(&account_id(req))
            .and_then(|a| a.query_log_associations.get(&id))
            .ok_or_else(|| not_found(format!("Association '{id}' not found")))?;
        Ok(AwsResponse::ok_json(
            json!({ "ResolverQueryLogConfigAssociation": to_val(assoc) }),
        ))
    }

    fn list_query_log_associations(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let all: Vec<Value> = self
            .state
            .read()
            .accounts
            .get(&account_id(req))
            .map(|a| a.query_log_associations.values().map(to_val).collect())
            .unwrap_or_default();
        let total = all.len();
        let all = apply_filters(all, &body, |name| match name {
            "CreationTime" => Some("CreationTime"),
            "Error" => Some("Error"),
            "Id" => Some("Id"),
            "ResolverQueryLogConfigId" => Some("ResolverQueryLogConfigId"),
            "ResourceId" => Some("ResourceId"),
            "Status" => Some("Status"),
            _ => None,
        })?;
        let filtered = all.len();
        let (list, next) = paginate(&body, all);
        Ok(list_response(
            json!({
                "TotalCount": total,
                "TotalFilteredCount": filtered,
                "ResolverQueryLogConfigAssociations": list,
            }),
            next,
        ))
    }
}

// ─── Resolver config + DNSSEC config ─────────────────────────────────────

impl Route53ResolverService {
    fn get_resolver_config(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let account = account_id(req);
        let resource_id = required_str(&body, "ResourceId")?;
        let cfg = self.resolver_config_or_default(&account, &resource_id);
        Ok(AwsResponse::ok_json(
            json!({ "ResolverConfig": to_val(&cfg) }),
        ))
    }

    fn update_resolver_config(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let account = account_id(req);
        let resource_id = required_str(&body, "ResourceId")?;
        let flag = required_str(&body, "AutodefinedReverseFlag")?;
        if flag != "ENABLE" && flag != "DISABLE" && flag != "USE_LOCAL_RESOURCE_SETTING" {
            return Err(invalid_parameter(format!(
                "Invalid AutodefinedReverseFlag: {flag}"
            )));
        }
        if self.vpc_missing(&account, &resource_id) {
            return Err(invalid_parameter(format!(
                "The vpc ID '{resource_id}' does not exist"
            )));
        }
        let (transient, terminal) = match flag.as_str() {
            "ENABLE" => ("ENABLING", "ENABLED"),
            "DISABLE" => ("DISABLING", "DISABLED"),
            _ => (
                "UPDATING_TO_USE_LOCAL_RESOURCE_SETTING",
                "USE_LOCAL_RESOURCE_SETTING",
            ),
        };
        let mut cfg = self.resolver_config_or_default(&account, &resource_id);
        cfg.autodefined_reverse = transient.to_string();
        {
            let mut st = self.state.write();
            st.account_mut(&account)
                .resolver_configs
                .insert(resource_id.clone(), cfg.clone());
        }
        self.spawn_settle(
            account,
            Settle::ResolverConfig(terminal.to_string()),
            resource_id,
        );
        Ok(AwsResponse::ok_json(
            json!({ "ResolverConfig": to_val(&cfg) }),
        ))
    }

    fn list_resolver_configs(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let all: Vec<Value> = self
            .state
            .read()
            .accounts
            .get(&account_id(req))
            .map(|a| a.resolver_configs.values().map(to_val).collect())
            .unwrap_or_default();
        let (page, next) = paginate(&body, all);
        Ok(list_response(json!({ "ResolverConfigs": page }), next))
    }

    fn resolver_config_or_default(&self, account: &str, resource_id: &str) -> ResolverConfig {
        if let Some(cfg) = self
            .state
            .read()
            .accounts
            .get(account)
            .and_then(|a| a.resolver_configs.get(resource_id))
        {
            return cfg.clone();
        }
        ResolverConfig {
            id: format!("rslvr-rc-{}", deterministic_suffix(resource_id)),
            resource_id: resource_id.to_string(),
            owner_id: account.to_string(),
            autodefined_reverse: "ENABLED".to_string(),
        }
    }

    fn get_dnssec_config(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let account = account_id(req);
        let resource_id = required_str(&body, "ResourceId")?;
        let cfg = self.dnssec_config_or_default(&account, &resource_id);
        Ok(AwsResponse::ok_json(
            json!({ "ResolverDNSSECConfig": to_val(&cfg) }),
        ))
    }

    fn update_dnssec_config(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let account = account_id(req);
        let resource_id = required_str(&body, "ResourceId")?;
        let validation = required_str(&body, "Validation")?;
        if validation != "ENABLE"
            && validation != "DISABLE"
            && validation != "USE_LOCAL_RESOURCE_SETTING"
        {
            return Err(invalid_parameter(format!(
                "Invalid Validation: {validation}"
            )));
        }
        if self.vpc_missing(&account, &resource_id) {
            return Err(invalid_parameter(format!(
                "The vpc ID '{resource_id}' does not exist"
            )));
        }
        let (transient, terminal) = match validation.as_str() {
            "ENABLE" => ("ENABLING", "ENABLED"),
            "DISABLE" => ("DISABLING", "DISABLED"),
            _ => (
                "UPDATING_TO_USE_LOCAL_RESOURCE_SETTING",
                "USE_LOCAL_RESOURCE_SETTING",
            ),
        };
        let mut cfg = self.dnssec_config_or_default(&account, &resource_id);
        cfg.validation_status = transient.to_string();
        {
            let mut st = self.state.write();
            st.account_mut(&account)
                .dnssec_configs
                .insert(cfg.id.clone(), cfg.clone());
        }
        self.spawn_settle(
            account,
            Settle::Dnssec(terminal.to_string()),
            cfg.id.clone(),
        );
        Ok(AwsResponse::ok_json(
            json!({ "ResolverDNSSECConfig": to_val(&cfg) }),
        ))
    }

    fn list_dnssec_configs(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let all: Vec<Value> = self
            .state
            .read()
            .accounts
            .get(&account_id(req))
            .map(|a| a.dnssec_configs.values().map(to_val).collect())
            .unwrap_or_default();
        let (page, next) = paginate(&body, all);
        Ok(list_response(
            json!({ "ResolverDnssecConfigs": page }),
            next,
        ))
    }

    fn dnssec_config_or_default(&self, account: &str, resource_id: &str) -> ResolverDnssecConfig {
        if let Some(cfg) = self.state.read().accounts.get(account).and_then(|a| {
            a.dnssec_configs
                .values()
                .find(|c| c.resource_id == resource_id)
        }) {
            return cfg.clone();
        }
        ResolverDnssecConfig {
            id: format!("rslvr-ds-{}", deterministic_suffix(resource_id)),
            owner_id: account.to_string(),
            resource_id: resource_id.to_string(),
            validation_status: "DISABLED".to_string(),
        }
    }
}

// ─── DNS Firewall: rule groups ───────────────────────────────────────────

impl Route53ResolverService {
    fn create_firewall_rule_group(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let account = account_id(req);
        let region = region(req);
        let creator_request_id = required_str_v(&body, "CreatorRequestId")?;
        let name = required_str_v(&body, "Name")?;
        let id = format!("rslvr-frg-{}", hex17());
        let group = FirewallRuleGroup {
            id: id.clone(),
            arn: arn(&region, &account, "firewall-rule-group", &id),
            name,
            rule_count: 0,
            status: "COMPLETE".to_string(),
            status_message: "Created Firewall Rule Group".to_string(),
            owner_id: account.clone(),
            creator_request_id,
            share_status: "NOT_SHARED".to_string(),
            creation_time: now_rfc3339(),
            modification_time: now_rfc3339(),
        };
        let arn_str = group.arn.clone();
        let tags = parse_tags(body.get("Tags"))?;
        let mut st = self.state.write();
        let acc = st.account_mut(&account);
        acc.firewall_rule_groups.insert(id.clone(), group.clone());
        acc.firewall_rules.insert(id, Vec::new());
        if !tags.is_empty() {
            acc.tags.insert(arn_str, tags);
        }
        Ok(AwsResponse::ok_json(
            json!({ "FirewallRuleGroup": to_val(&group) }),
        ))
    }

    fn get_firewall_rule_group(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = required_str(&body, "FirewallRuleGroupId")?;
        let st = self.state.read();
        let group = st
            .accounts
            .get(&account_id(req))
            .and_then(|a| a.firewall_rule_groups.get(&id))
            .ok_or_else(|| not_found(format!("Firewall rule group '{id}' not found")))?;
        Ok(AwsResponse::ok_json(
            json!({ "FirewallRuleGroup": to_val(group) }),
        ))
    }

    fn delete_firewall_rule_group(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = required_str(&body, "FirewallRuleGroupId")?;
        let mut st = self.state.write();
        let acc = st
            .accounts
            .get_mut(&account_id(req))
            .ok_or_else(|| not_found(format!("Firewall rule group '{id}' not found")))?;
        if acc
            .firewall_rules
            .get(&id)
            .map(|r| !r.is_empty())
            .unwrap_or(false)
        {
            return Err(conflict(
                "Cannot delete a firewall rule group that still contains rules",
            ));
        }
        if acc
            .firewall_rule_group_associations
            .values()
            .any(|a| a.firewall_rule_group_id == id)
        {
            return Err(conflict(
                "Cannot delete a firewall rule group that has associations",
            ));
        }
        let mut group = acc
            .firewall_rule_groups
            .remove(&id)
            .ok_or_else(|| not_found(format!("Firewall rule group '{id}' not found")))?;
        acc.firewall_rules.remove(&id);
        acc.tags.remove(&group.arn);
        group.status = "DELETING".to_string();
        Ok(AwsResponse::ok_json(
            json!({ "FirewallRuleGroup": to_val(&group) }),
        ))
    }

    fn list_firewall_rule_groups(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let all: Vec<Value> = self
            .state
            .read()
            .accounts
            .get(&account_id(req))
            .map(|a| {
                a.firewall_rule_groups
                    .values()
                    .map(|g| {
                        json!({
                            "Id": g.id,
                            "Arn": g.arn,
                            "Name": g.name,
                            "OwnerId": g.owner_id,
                            "CreatorRequestId": g.creator_request_id,
                            "ShareStatus": g.share_status,
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();
        let (page, next) = paginate(&body, all);
        Ok(list_response(json!({ "FirewallRuleGroups": page }), next))
    }
}

// ─── DNS Firewall: domain lists ──────────────────────────────────────────

impl Route53ResolverService {
    fn create_firewall_domain_list(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let account = account_id(req);
        let region = region(req);
        let creator_request_id = required_str_v(&body, "CreatorRequestId")?;
        let name = required_str_v(&body, "Name")?;
        let id = format!("rslvr-fdl-{}", hex17());
        let list = FirewallDomainList {
            id: id.clone(),
            arn: arn(&region, &account, "firewall-domain-list", &id),
            name,
            domain_count: 0,
            status: "COMPLETE".to_string(),
            status_message: "Created Firewall Domain List".to_string(),
            creator_request_id,
            creation_time: now_rfc3339(),
            modification_time: now_rfc3339(),
        };
        let arn_str = list.arn.clone();
        let tags = parse_tags(body.get("Tags"))?;
        let mut st = self.state.write();
        let acc = st.account_mut(&account);
        acc.firewall_domain_lists.insert(id.clone(), list.clone());
        acc.firewall_domains.insert(id, Vec::new());
        if !tags.is_empty() {
            acc.tags.insert(arn_str, tags);
        }
        Ok(AwsResponse::ok_json(
            json!({ "FirewallDomainList": to_val(&list) }),
        ))
    }

    fn get_firewall_domain_list(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = required_str(&body, "FirewallDomainListId")?;
        let st = self.state.read();
        let list = st
            .accounts
            .get(&account_id(req))
            .and_then(|a| a.firewall_domain_lists.get(&id))
            .ok_or_else(|| not_found(format!("Firewall domain list '{id}' not found")))?;
        Ok(AwsResponse::ok_json(
            json!({ "FirewallDomainList": to_val(list) }),
        ))
    }

    fn delete_firewall_domain_list(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = required_str(&body, "FirewallDomainListId")?;
        let mut st = self.state.write();
        let acc = st
            .accounts
            .get_mut(&account_id(req))
            .ok_or_else(|| not_found(format!("Firewall domain list '{id}' not found")))?;
        // A domain list referenced by any firewall rule cannot be deleted.
        let referenced = acc
            .firewall_rules
            .values()
            .flatten()
            .any(|r| r.firewall_domain_list_id.as_deref() == Some(id.as_str()));
        if referenced {
            return Err(conflict(
                "Cannot delete a firewall domain list that is referenced by a rule",
            ));
        }
        let mut list = acc
            .firewall_domain_lists
            .remove(&id)
            .ok_or_else(|| not_found(format!("Firewall domain list '{id}' not found")))?;
        acc.firewall_domains.remove(&id);
        acc.tags.remove(&list.arn);
        list.status = "DELETING".to_string();
        Ok(AwsResponse::ok_json(
            json!({ "FirewallDomainList": to_val(&list) }),
        ))
    }

    fn list_firewall_domain_lists(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let all: Vec<Value> = self
            .state
            .read()
            .accounts
            .get(&account_id(req))
            .map(|a| {
                a.firewall_domain_lists
                    .values()
                    .map(|l| {
                        json!({
                            "Id": l.id,
                            "Arn": l.arn,
                            "Name": l.name,
                            "CreatorRequestId": l.creator_request_id,
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();
        let (page, next) = paginate(&body, all);
        Ok(list_response(json!({ "FirewallDomainLists": page }), next))
    }

    fn import_firewall_domains(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        // Route 53 Resolver's ImportFirewallDomains reads a plain-text file (one
        // domain per line) that the caller stored in S3, then REPLACEs the
        // domain list with its contents. When S3 state is wired and the object
        // is present we perform the real import; otherwise the list is left
        // unchanged and the response says so (rather than fabricating a
        // successful import of zero domains).
        let body = req.json_body();
        let account = account_id(req);
        let id = required_str(&body, "FirewallDomainListId")?;
        let _op = required_str(&body, "Operation")?;
        let url = required_str(&body, "DomainFileUrl")?;
        let imported = self.fetch_domain_file(&account, &url);
        let mut st = self.state.write();
        let acc = st
            .accounts
            .get_mut(&account)
            .ok_or_else(|| not_found(format!("Firewall domain list '{id}' not found")))?;
        if !acc.firewall_domain_lists.contains_key(&id) {
            return Err(not_found(format!("Firewall domain list '{id}' not found")));
        }
        let (count, status_message) = match imported {
            Some(domains) => {
                let n = domains.len() as i64;
                acc.firewall_domains.insert(id.clone(), domains);
                (n, "Successfully imported domains from the file".to_string())
            }
            None => {
                let n = acc
                    .firewall_domains
                    .get(&id)
                    .map(|d| d.len() as i64)
                    .unwrap_or(0);
                (
                    n,
                    "The domains file could not be retrieved; the domain list is unchanged"
                        .to_string(),
                )
            }
        };
        let list = acc.firewall_domain_lists.get_mut(&id).unwrap();
        list.domain_count = count;
        list.status = "COMPLETE".to_string();
        list.modification_time = now_rfc3339();
        Ok(AwsResponse::ok_json(json!({
            "Id": list.id,
            "Name": list.name,
            "Status": list.status,
            "StatusMessage": status_message,
        })))
    }

    /// Read the domains file referenced by an S3 URL and return one trimmed,
    /// non-empty domain per line. Returns `None` when S3 state is not wired, the
    /// URL is unparseable, or the object is absent/unreadable.
    fn fetch_domain_file(&self, account: &str, url: &str) -> Option<Vec<String>> {
        let s3 = self.s3_state.as_ref()?;
        let (bucket, key) = parse_s3_url(url)?;
        let bytes = {
            let guard = s3.read();
            let s3_state = guard.get(account)?;
            let obj = s3_state.buckets.get(&bucket)?.objects.get(&key)?;
            s3_state.read_body(&obj.body).ok()?
        };
        let text = String::from_utf8_lossy(&bytes);
        Some(
            text.lines()
                .map(str::trim)
                .filter(|l| !l.is_empty())
                .map(str::to_string)
                .collect(),
        )
    }

    fn update_firewall_domains(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let account = account_id(req);
        let id = required_str(&body, "FirewallDomainListId")?;
        let operation = required_str(&body, "Operation")?;
        if !["ADD", "REMOVE", "REPLACE"].contains(&operation.as_str()) {
            return Err(validation(format!("Invalid Operation: {operation}")));
        }
        let domains: Vec<String> = body
            .get("Domains")
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default();
        let mut st = self.state.write();
        let acc = st
            .accounts
            .get_mut(&account)
            .ok_or_else(|| not_found(format!("Firewall domain list '{id}' not found")))?;
        if !acc.firewall_domain_lists.contains_key(&id) {
            return Err(not_found(format!("Firewall domain list '{id}' not found")));
        }
        let existing = acc.firewall_domains.entry(id.clone()).or_default();
        match operation.as_str() {
            "ADD" => {
                for d in domains {
                    if !existing.contains(&d) {
                        existing.push(d);
                    }
                }
            }
            "REMOVE" => existing.retain(|d| !domains.contains(d)),
            "REPLACE" => *existing = domains,
            _ => unreachable!(),
        }
        let count = existing.len() as i64;
        let list = acc.firewall_domain_lists.get_mut(&id).unwrap();
        list.domain_count = count;
        list.status = "COMPLETE".to_string();
        list.modification_time = now_rfc3339();
        Ok(AwsResponse::ok_json(json!({
            "Id": list.id,
            "Name": list.name,
            "Status": list.status,
            "StatusMessage": "Domain list updated",
        })))
    }

    fn list_firewall_domains(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = required_str(&body, "FirewallDomainListId")?;
        let st = self.state.read();
        let acc = st
            .accounts
            .get(&account_id(req))
            .ok_or_else(|| not_found(format!("Firewall domain list '{id}' not found")))?;
        if !acc.firewall_domain_lists.contains_key(&id) {
            return Err(not_found(format!("Firewall domain list '{id}' not found")));
        }
        let domains: Vec<Value> = acc
            .firewall_domains
            .get(&id)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .map(Value::String)
            .collect();
        let (page, next) = paginate(&body, domains);
        Ok(list_response(json!({ "Domains": page }), next))
    }
}

// ─── DNS Firewall: rules ─────────────────────────────────────────────────

impl Route53ResolverService {
    fn build_firewall_rule(
        &self,
        account: &str,
        entry: &Value,
    ) -> Result<FirewallRule, AwsServiceError> {
        let group_id = required_str_v(entry, "FirewallRuleGroupId")?;
        let name = required_str_v(entry, "Name")?;
        let action = required_str_v(entry, "Action")?;
        if !["ALLOW", "BLOCK", "ALERT"].contains(&action.as_str()) {
            return Err(validation(format!("Invalid Action: {action}")));
        }
        let priority = entry
            .get("Priority")
            .and_then(Value::as_i64)
            .ok_or_else(|| validation("Priority is required"))?;
        let domain_list_id = entry
            .get("FirewallDomainListId")
            .and_then(Value::as_str)
            .map(str::to_string);
        // Validate references exist.
        {
            let st = self.state.read();
            let acc = st.accounts.get(account);
            if acc
                .map(|a| !a.firewall_rule_groups.contains_key(&group_id))
                .unwrap_or(true)
            {
                return Err(validation(format!(
                    "Firewall rule group '{group_id}' not found"
                )));
            }
            if let Some(dl) = &domain_list_id {
                if acc
                    .map(|a| !a.firewall_domain_lists.contains_key(dl))
                    .unwrap_or(true)
                {
                    return Err(validation(format!("Firewall domain list '{dl}' not found")));
                }
            }
        }
        // BLOCK with OVERRIDE requires override fields.
        let block_response = entry
            .get("BlockResponse")
            .and_then(Value::as_str)
            .map(str::to_string);
        Ok(FirewallRule {
            firewall_rule_group_id: group_id,
            firewall_domain_list_id: domain_list_id,
            name,
            priority,
            action,
            block_response,
            block_override_domain: entry
                .get("BlockOverrideDomain")
                .and_then(Value::as_str)
                .map(str::to_string),
            block_override_dns_type: entry
                .get("BlockOverrideDnsType")
                .and_then(Value::as_str)
                .map(str::to_string),
            block_override_ttl: entry.get("BlockOverrideTtl").and_then(Value::as_i64),
            creator_request_id: entry
                .get("CreatorRequestId")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string(),
            creation_time: now_rfc3339(),
            modification_time: now_rfc3339(),
            firewall_domain_redirection_action: entry
                .get("FirewallDomainRedirectionAction")
                .and_then(Value::as_str)
                .map(str::to_string),
            qtype: entry
                .get("Qtype")
                .and_then(Value::as_str)
                .map(str::to_string),
            dns_threat_protection: entry
                .get("DnsThreatProtection")
                .and_then(Value::as_str)
                .map(str::to_string),
            confidence_threshold: entry
                .get("ConfidenceThreshold")
                .and_then(Value::as_str)
                .map(str::to_string),
            firewall_rule_type: entry
                .get("FirewallRuleType")
                .filter(|v| !v.is_null())
                .cloned(),
            // AWS mints a threat-protection id for rules that carry a
            // DnsThreatProtection detector or a structured FirewallRuleType.
            firewall_threat_protection_id: if entry
                .get("DnsThreatProtection")
                .is_some_and(|v| !v.is_null())
                || entry.get("FirewallRuleType").is_some_and(|v| !v.is_null())
            {
                Some(format!("rslvr-ftp-{}", hex17()))
            } else {
                None
            },
        })
    }

    /// The identity of a firewall rule within its group: domain-list id + Qtype.
    fn rule_matches(
        rule: &FirewallRule,
        domain_list_id: &Option<String>,
        qtype: &Option<String>,
    ) -> bool {
        rule.firewall_domain_list_id == *domain_list_id && rule.qtype == *qtype
    }

    fn insert_firewall_rule(
        &self,
        account: &str,
        rule: FirewallRule,
    ) -> Result<FirewallRule, AwsServiceError> {
        let mut st = self.state.write();
        let acc = st.account_mut(account);
        let group_id = rule.firewall_rule_group_id.clone();
        let bucket = acc.firewall_rules.entry(group_id.clone()).or_default();
        if bucket
            .iter()
            .any(|r| Self::rule_matches(r, &rule.firewall_domain_list_id, &rule.qtype))
        {
            return Err(validation(
                "A firewall rule for this domain list and Qtype already exists in the group",
            ));
        }
        bucket.push(rule.clone());
        if let Some(g) = acc.firewall_rule_groups.get_mut(&group_id) {
            g.rule_count = bucket.len() as i64;
            g.modification_time = now_rfc3339();
        }
        Ok(rule)
    }

    fn create_firewall_rule(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let account = account_id(req);
        let rule = self.build_firewall_rule(&account, &body)?;
        let rule = self.insert_firewall_rule(&account, rule)?;
        Ok(AwsResponse::ok_json(
            json!({ "FirewallRule": to_val(&rule) }),
        ))
    }

    fn update_firewall_rule(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let account = account_id(req);
        let group_id = required_str(&body, "FirewallRuleGroupId")?;
        let domain_list_id = body
            .get("FirewallDomainListId")
            .and_then(Value::as_str)
            .map(str::to_string);
        let qtype = body
            .get("Qtype")
            .and_then(Value::as_str)
            .map(str::to_string);
        let mut st = self.state.write();
        let acc = st
            .accounts
            .get_mut(&account)
            .ok_or_else(|| not_found("Firewall rule not found"))?;
        let bucket = acc
            .firewall_rules
            .get_mut(&group_id)
            .ok_or_else(|| not_found(format!("Firewall rule group '{group_id}' not found")))?;
        let rule = bucket
            .iter_mut()
            .find(|r| Self::rule_matches(r, &domain_list_id, &qtype))
            .ok_or_else(|| not_found("The specified firewall rule was not found"))?;
        if let Some(p) = body.get("Priority").and_then(Value::as_i64) {
            rule.priority = p;
        }
        if let Some(a) = body.get("Action").and_then(Value::as_str) {
            rule.action = a.to_string();
        }
        if let Some(n) = body.get("Name").and_then(Value::as_str) {
            rule.name = n.to_string();
        }
        if let Some(b) = body.get("BlockResponse").and_then(Value::as_str) {
            rule.block_response = Some(b.to_string());
        }
        if let Some(d) = body.get("BlockOverrideDomain").and_then(Value::as_str) {
            rule.block_override_domain = Some(d.to_string());
        }
        if let Some(t) = body.get("BlockOverrideDnsType").and_then(Value::as_str) {
            rule.block_override_dns_type = Some(t.to_string());
        }
        if let Some(ttl) = body.get("BlockOverrideTtl").and_then(Value::as_i64) {
            rule.block_override_ttl = Some(ttl);
        }
        if let Some(a) = body
            .get("FirewallDomainRedirectionAction")
            .and_then(Value::as_str)
        {
            rule.firewall_domain_redirection_action = Some(a.to_string());
        }
        if let Some(d) = body.get("DnsThreatProtection").and_then(Value::as_str) {
            rule.dns_threat_protection = Some(d.to_string());
        }
        if let Some(c) = body.get("ConfidenceThreshold").and_then(Value::as_str) {
            rule.confidence_threshold = Some(c.to_string());
        }
        if let Some(frt) = body.get("FirewallRuleType").filter(|v| !v.is_null()) {
            rule.firewall_rule_type = Some(frt.clone());
        }
        rule.modification_time = now_rfc3339();
        let out = rule.clone();
        Ok(AwsResponse::ok_json(
            json!({ "FirewallRule": to_val(&out) }),
        ))
    }

    fn delete_firewall_rule(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let account = account_id(req);
        let group_id = required_str(&body, "FirewallRuleGroupId")?;
        let domain_list_id = body
            .get("FirewallDomainListId")
            .and_then(Value::as_str)
            .map(str::to_string);
        let qtype = body
            .get("Qtype")
            .and_then(Value::as_str)
            .map(str::to_string);
        let mut st = self.state.write();
        let acc = st
            .accounts
            .get_mut(&account)
            .ok_or_else(|| not_found("Firewall rule not found"))?;
        let bucket = acc
            .firewall_rules
            .get_mut(&group_id)
            .ok_or_else(|| not_found(format!("Firewall rule group '{group_id}' not found")))?;
        let pos = bucket
            .iter()
            .position(|r| Self::rule_matches(r, &domain_list_id, &qtype))
            .ok_or_else(|| not_found("The specified firewall rule was not found"))?;
        let rule = bucket.remove(pos);
        let count = bucket.len() as i64;
        if let Some(g) = acc.firewall_rule_groups.get_mut(&group_id) {
            g.rule_count = count;
            g.modification_time = now_rfc3339();
        }
        Ok(AwsResponse::ok_json(
            json!({ "FirewallRule": to_val(&rule) }),
        ))
    }

    fn list_firewall_rules(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let group_id = required_str(&body, "FirewallRuleGroupId")?;
        let st = self.state.read();
        let acc = st
            .accounts
            .get(&account_id(req))
            .ok_or_else(|| not_found(format!("Firewall rule group '{group_id}' not found")))?;
        if !acc.firewall_rule_groups.contains_key(&group_id) {
            return Err(not_found(format!(
                "Firewall rule group '{group_id}' not found"
            )));
        }
        let rules: Vec<Value> = acc
            .firewall_rules
            .get(&group_id)
            .map(|r| r.iter().map(to_val).collect())
            .unwrap_or_default();
        let (page, next) = paginate(&body, rules);
        Ok(list_response(json!({ "FirewallRules": page }), next))
    }

    fn list_firewall_rule_types(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        // The static catalog of DNS Firewall rule types.
        let types = vec![
            json!({
                "RuleType": "STANDARD",
                "Value": "STANDARD",
                "DisplayName": "Standard",
                "Description": "A standard DNS Firewall rule that matches queries against a domain list.",
            }),
            json!({
                "RuleType": "DNS_THREAT_PROTECTION",
                "Value": "DNS_THREAT_PROTECTION",
                "DisplayName": "Advanced DNS threat protection",
                "Description": "A rule that detects DNS tunneling and domain generation algorithm threats.",
            }),
        ];
        let (page, next) = paginate(&body, types);
        Ok(list_response(json!({ "FirewallRuleTypes": page }), next))
    }

    fn batch_create_firewall_rule(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let account = account_id(req);
        let entries = body
            .get("CreateFirewallRuleEntries")
            .and_then(Value::as_array)
            .ok_or_else(|| validation("CreateFirewallRuleEntries is required"))?;
        let mut created = Vec::new();
        for e in entries {
            let rule = self.build_firewall_rule(&account, e)?;
            let rule = self.insert_firewall_rule(&account, rule)?;
            created.push(to_val(&rule));
        }
        Ok(AwsResponse::ok_json(
            json!({ "CreatedFirewallRules": created }),
        ))
    }

    fn batch_update_firewall_rule(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let account = account_id(req);
        let entries = body
            .get("UpdateFirewallRuleEntries")
            .and_then(Value::as_array)
            .ok_or_else(|| validation("UpdateFirewallRuleEntries is required"))?
            .clone();
        let mut updated = Vec::new();
        for e in &entries {
            let group_id = required_str_v(e, "FirewallRuleGroupId")?;
            let domain_list_id = e
                .get("FirewallDomainListId")
                .and_then(Value::as_str)
                .map(str::to_string);
            let qtype = e.get("Qtype").and_then(Value::as_str).map(str::to_string);
            let mut st = self.state.write();
            let acc = st
                .accounts
                .get_mut(&account)
                .ok_or_else(|| validation("Firewall rule not found"))?;
            let bucket = acc
                .firewall_rules
                .get_mut(&group_id)
                .ok_or_else(|| validation(format!("Firewall rule group '{group_id}' not found")))?;
            let rule = bucket
                .iter_mut()
                .find(|r| Self::rule_matches(r, &domain_list_id, &qtype))
                .ok_or_else(|| validation("The specified firewall rule was not found"))?;
            if let Some(p) = e.get("Priority").and_then(Value::as_i64) {
                rule.priority = p;
            }
            if let Some(a) = e.get("Action").and_then(Value::as_str) {
                rule.action = a.to_string();
            }
            if let Some(n) = e.get("Name").and_then(Value::as_str) {
                rule.name = n.to_string();
            }
            if let Some(b) = e.get("BlockResponse").and_then(Value::as_str) {
                rule.block_response = Some(b.to_string());
            }
            if let Some(d) = e.get("BlockOverrideDomain").and_then(Value::as_str) {
                rule.block_override_domain = Some(d.to_string());
            }
            if let Some(t) = e.get("BlockOverrideDnsType").and_then(Value::as_str) {
                rule.block_override_dns_type = Some(t.to_string());
            }
            if let Some(ttl) = e.get("BlockOverrideTtl").and_then(Value::as_i64) {
                rule.block_override_ttl = Some(ttl);
            }
            if let Some(a) = e
                .get("FirewallDomainRedirectionAction")
                .and_then(Value::as_str)
            {
                rule.firewall_domain_redirection_action = Some(a.to_string());
            }
            if let Some(d) = e.get("DnsThreatProtection").and_then(Value::as_str) {
                rule.dns_threat_protection = Some(d.to_string());
            }
            if let Some(c) = e.get("ConfidenceThreshold").and_then(Value::as_str) {
                rule.confidence_threshold = Some(c.to_string());
            }
            if let Some(frt) = e.get("FirewallRuleType").filter(|v| !v.is_null()) {
                rule.firewall_rule_type = Some(frt.clone());
            }
            rule.modification_time = now_rfc3339();
            updated.push(to_val(&rule.clone()));
        }
        Ok(AwsResponse::ok_json(
            json!({ "UpdatedFirewallRules": updated }),
        ))
    }

    fn batch_delete_firewall_rule(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let account = account_id(req);
        let entries = body
            .get("DeleteFirewallRuleEntries")
            .and_then(Value::as_array)
            .ok_or_else(|| validation("DeleteFirewallRuleEntries is required"))?
            .clone();
        let mut deleted = Vec::new();
        for e in &entries {
            let group_id = required_str_v(e, "FirewallRuleGroupId")?;
            let domain_list_id = e
                .get("FirewallDomainListId")
                .and_then(Value::as_str)
                .map(str::to_string);
            let qtype = e.get("Qtype").and_then(Value::as_str).map(str::to_string);
            let mut st = self.state.write();
            let acc = st
                .accounts
                .get_mut(&account)
                .ok_or_else(|| validation("Firewall rule not found"))?;
            let bucket = acc
                .firewall_rules
                .get_mut(&group_id)
                .ok_or_else(|| validation(format!("Firewall rule group '{group_id}' not found")))?;
            if let Some(pos) = bucket
                .iter()
                .position(|r| Self::rule_matches(r, &domain_list_id, &qtype))
            {
                let rule = bucket.remove(pos);
                let count = bucket.len() as i64;
                if let Some(g) = acc.firewall_rule_groups.get_mut(&group_id) {
                    g.rule_count = count;
                }
                deleted.push(to_val(&rule));
            }
        }
        Ok(AwsResponse::ok_json(
            json!({ "DeletedFirewallRules": deleted }),
        ))
    }
}

// ─── DNS Firewall: rule-group associations + config ──────────────────────

impl Route53ResolverService {
    fn associate_firewall_rule_group(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let account = account_id(req);
        let region = region(req);
        let creator_request_id = required_str_v(&body, "CreatorRequestId")?;
        let group_id = required_str_v(&body, "FirewallRuleGroupId")?;
        let vpc_id = required_str_v(&body, "VpcId")?;
        let priority = body
            .get("Priority")
            .and_then(Value::as_i64)
            .ok_or_else(|| validation("Priority is required"))?;
        let name = required_str_v(&body, "Name")?;
        if self.vpc_missing(&account, &vpc_id) {
            return Err(validation(format!("The vpc ID '{vpc_id}' does not exist")));
        }
        {
            let st = self.state.read();
            if st
                .accounts
                .get(&account)
                .map(|a| !a.firewall_rule_groups.contains_key(&group_id))
                .unwrap_or(true)
            {
                return Err(not_found(format!(
                    "Firewall rule group '{group_id}' not found"
                )));
            }
        }
        let id = format!("rslvr-frgassoc-{}", hex17());
        let assoc = FirewallRuleGroupAssociation {
            id: id.clone(),
            arn: arn(&region, &account, "firewall-rule-group-association", &id),
            firewall_rule_group_id: group_id,
            vpc_id,
            name,
            priority,
            mutation_protection: body
                .get("MutationProtection")
                .and_then(Value::as_str)
                .unwrap_or("DISABLED")
                .to_string(),
            status: "UPDATING".to_string(),
            status_message: "Creating Firewall Rule Group Association".to_string(),
            creator_request_id,
            creation_time: now_rfc3339(),
            modification_time: now_rfc3339(),
        };
        let arn_str = assoc.arn.clone();
        let tags = parse_tags(body.get("Tags"))?;
        {
            let mut st = self.state.write();
            let acc = st.account_mut(&account);
            acc.firewall_rule_group_associations
                .insert(id.clone(), assoc.clone());
            if !tags.is_empty() {
                acc.tags.insert(arn_str, tags);
            }
        }
        self.spawn_settle(account, Settle::FirewallAssociation, id);
        Ok(AwsResponse::ok_json(
            json!({ "FirewallRuleGroupAssociation": to_val(&assoc) }),
        ))
    }

    fn disassociate_firewall_rule_group(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = required_str(&body, "FirewallRuleGroupAssociationId")?;
        let mut st = self.state.write();
        let acc = st
            .accounts
            .get_mut(&account_id(req))
            .ok_or_else(|| not_found(format!("Association '{id}' not found")))?;
        let mut assoc = acc
            .firewall_rule_group_associations
            .remove(&id)
            .ok_or_else(|| not_found(format!("Association '{id}' not found")))?;
        acc.tags.remove(&assoc.arn);
        assoc.status = "DELETING".to_string();
        Ok(AwsResponse::ok_json(
            json!({ "FirewallRuleGroupAssociation": to_val(&assoc) }),
        ))
    }

    fn get_firewall_rule_group_association(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = required_str(&body, "FirewallRuleGroupAssociationId")?;
        let st = self.state.read();
        let assoc = st
            .accounts
            .get(&account_id(req))
            .and_then(|a| a.firewall_rule_group_associations.get(&id))
            .ok_or_else(|| not_found(format!("Association '{id}' not found")))?;
        Ok(AwsResponse::ok_json(
            json!({ "FirewallRuleGroupAssociation": to_val(assoc) }),
        ))
    }

    fn update_firewall_rule_group_association(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = required_str(&body, "FirewallRuleGroupAssociationId")?;
        let mut st = self.state.write();
        let assoc = st
            .accounts
            .get_mut(&account_id(req))
            .and_then(|a| a.firewall_rule_group_associations.get_mut(&id))
            .ok_or_else(|| not_found(format!("Association '{id}' not found")))?;
        if let Some(p) = body.get("Priority").and_then(Value::as_i64) {
            assoc.priority = p;
        }
        if let Some(m) = body.get("MutationProtection").and_then(Value::as_str) {
            assoc.mutation_protection = m.to_string();
        }
        if let Some(n) = body.get("Name").and_then(Value::as_str) {
            assoc.name = n.to_string();
        }
        assoc.modification_time = now_rfc3339();
        let out = assoc.clone();
        Ok(AwsResponse::ok_json(
            json!({ "FirewallRuleGroupAssociation": to_val(&out) }),
        ))
    }

    fn list_firewall_rule_group_associations(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        // Unlike the resolver-* lists, ListFirewallRuleGroupAssociations narrows
        // by dedicated top-level request parameters (not a `Filters` list): an
        // association is kept only when it matches every supplied parameter.
        let f_group = body.get("FirewallRuleGroupId").and_then(Value::as_str);
        let f_vpc = body.get("VpcId").and_then(Value::as_str);
        let f_priority = body.get("Priority").and_then(Value::as_i64);
        let f_status = body.get("Status").and_then(Value::as_str);
        let all: Vec<Value> = self
            .state
            .read()
            .accounts
            .get(&account_id(req))
            .map(|a| {
                a.firewall_rule_group_associations
                    .values()
                    .filter(|assoc| {
                        f_group.is_none_or(|g| g == assoc.firewall_rule_group_id)
                            && f_vpc.is_none_or(|v| v == assoc.vpc_id)
                            && f_priority.is_none_or(|p| p == assoc.priority)
                            && f_status.is_none_or(|s| s == assoc.status)
                    })
                    .map(to_val)
                    .collect()
            })
            .unwrap_or_default();
        let (page, next) = paginate(&body, all);
        Ok(list_response(
            json!({ "FirewallRuleGroupAssociations": page }),
            next,
        ))
    }

    fn get_firewall_config(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let account = account_id(req);
        let resource_id = required_str(&body, "ResourceId")?;
        let cfg = self.firewall_config_or_default(&account, &resource_id);
        Ok(AwsResponse::ok_json(
            json!({ "FirewallConfig": to_val(&cfg) }),
        ))
    }

    fn update_firewall_config(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let account = account_id(req);
        let resource_id = required_str_v(&body, "ResourceId")?;
        let fail_open = required_str_v(&body, "FirewallFailOpen")?;
        if !["ENABLED", "DISABLED", "USE_LOCAL_RESOURCE_SETTING"].contains(&fail_open.as_str()) {
            return Err(validation(format!("Invalid FirewallFailOpen: {fail_open}")));
        }
        if self.vpc_missing(&account, &resource_id) {
            return Err(validation(format!(
                "The vpc ID '{resource_id}' does not exist"
            )));
        }
        let mut cfg = self.firewall_config_or_default(&account, &resource_id);
        cfg.firewall_fail_open = fail_open;
        {
            let mut st = self.state.write();
            st.account_mut(&account)
                .firewall_configs
                .insert(resource_id, cfg.clone());
        }
        Ok(AwsResponse::ok_json(
            json!({ "FirewallConfig": to_val(&cfg) }),
        ))
    }

    fn list_firewall_configs(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let all: Vec<Value> = self
            .state
            .read()
            .accounts
            .get(&account_id(req))
            .map(|a| a.firewall_configs.values().map(to_val).collect())
            .unwrap_or_default();
        let (page, next) = paginate(&body, all);
        Ok(list_response(json!({ "FirewallConfigs": page }), next))
    }

    fn firewall_config_or_default(&self, account: &str, resource_id: &str) -> FirewallConfig {
        if let Some(cfg) = self
            .state
            .read()
            .accounts
            .get(account)
            .and_then(|a| a.firewall_configs.get(resource_id))
        {
            return cfg.clone();
        }
        FirewallConfig {
            id: format!("rslvr-fc-{}", deterministic_suffix(resource_id)),
            resource_id: resource_id.to_string(),
            owner_id: account.to_string(),
            firewall_fail_open: "DISABLED".to_string(),
        }
    }
}

// ─── Outpost resolvers ───────────────────────────────────────────────────

impl Route53ResolverService {
    fn create_outpost_resolver(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let account = account_id(req);
        let region = region(req);
        let creator_request_id = required_str(&body, "CreatorRequestId")?;
        let name = required_str(&body, "Name")?;
        let preferred_instance_type = required_str(&body, "PreferredInstanceType")?;
        let outpost_arn = required_str(&body, "OutpostArn")?;
        let id = format!("rslvr-op-{}", hex17());
        let resolver = OutpostResolver {
            arn: arn(&region, &account, "outpost-resolver", &id),
            creation_time: now_rfc3339(),
            modification_time: now_rfc3339(),
            creator_request_id,
            id: id.clone(),
            instance_count: body
                .get("InstanceCount")
                .and_then(Value::as_i64)
                .unwrap_or(4),
            preferred_instance_type,
            name,
            status: "CREATING".to_string(),
            status_message: "Creating the Outpost Resolver".to_string(),
            outpost_arn,
        };
        let arn_str = resolver.arn.clone();
        let tags = parse_tags(body.get("Tags"))?;
        {
            let mut st = self.state.write();
            let acc = st.account_mut(&account);
            acc.outpost_resolvers.insert(id.clone(), resolver.clone());
            if !tags.is_empty() {
                acc.tags.insert(arn_str, tags);
            }
        }
        self.spawn_settle(account, Settle::Outpost, id);
        Ok(AwsResponse::ok_json(
            json!({ "OutpostResolver": to_val(&resolver) }),
        ))
    }

    fn get_outpost_resolver(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = required_str(&body, "Id")?;
        let st = self.state.read();
        let r = st
            .accounts
            .get(&account_id(req))
            .and_then(|a| a.outpost_resolvers.get(&id))
            .ok_or_else(|| not_found(format!("Outpost resolver '{id}' not found")))?;
        Ok(AwsResponse::ok_json(
            json!({ "OutpostResolver": to_val(r) }),
        ))
    }

    fn update_outpost_resolver(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = required_str(&body, "Id")?;
        let mut st = self.state.write();
        let r = st
            .accounts
            .get_mut(&account_id(req))
            .and_then(|a| a.outpost_resolvers.get_mut(&id))
            .ok_or_else(|| not_found(format!("Outpost resolver '{id}' not found")))?;
        if let Some(n) = body.get("Name").and_then(Value::as_str) {
            r.name = n.to_string();
        }
        if let Some(c) = body.get("InstanceCount").and_then(Value::as_i64) {
            r.instance_count = c;
        }
        if let Some(t) = body.get("PreferredInstanceType").and_then(Value::as_str) {
            r.preferred_instance_type = t.to_string();
        }
        r.modification_time = now_rfc3339();
        let out = r.clone();
        Ok(AwsResponse::ok_json(
            json!({ "OutpostResolver": to_val(&out) }),
        ))
    }

    fn delete_outpost_resolver(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = required_str(&body, "Id")?;
        let mut st = self.state.write();
        let acc = st
            .accounts
            .get_mut(&account_id(req))
            .ok_or_else(|| not_found(format!("Outpost resolver '{id}' not found")))?;
        let mut r = acc
            .outpost_resolvers
            .remove(&id)
            .ok_or_else(|| not_found(format!("Outpost resolver '{id}' not found")))?;
        acc.tags.remove(&r.arn);
        r.status = "DELETING".to_string();
        Ok(AwsResponse::ok_json(
            json!({ "OutpostResolver": to_val(&r) }),
        ))
    }

    fn list_outpost_resolvers(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let all: Vec<Value> = self
            .state
            .read()
            .accounts
            .get(&account_id(req))
            .map(|a| a.outpost_resolvers.values().map(to_val).collect())
            .unwrap_or_default();
        let (page, next) = paginate(&body, all);
        Ok(list_response(json!({ "OutpostResolvers": page }), next))
    }
}

// ─── Resource policies + tags ────────────────────────────────────────────

impl Route53ResolverService {
    fn put_firewall_rule_group_policy(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let arn = required_str_v(&body, "Arn")?;
        let policy = required_str_v(&body, "FirewallRuleGroupPolicy")?;
        self.state
            .write()
            .account_mut(&account_id(req))
            .firewall_rule_group_policies
            .insert(arn, policy);
        Ok(AwsResponse::ok_json(json!({ "ReturnValue": true })))
    }

    fn get_firewall_rule_group_policy(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let arn = required_str(&body, "Arn")?;
        let st = self.state.read();
        let policy = st
            .accounts
            .get(&account_id(req))
            .and_then(|a| a.firewall_rule_group_policies.get(&arn))
            .cloned()
            .ok_or_else(|| not_found("No policy found for the specified resource"))?;
        Ok(AwsResponse::ok_json(
            json!({ "FirewallRuleGroupPolicy": policy }),
        ))
    }

    fn put_query_log_config_policy(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let arn = required_str(&body, "Arn")?;
        let policy = required_str(&body, "ResolverQueryLogConfigPolicy")?;
        self.state
            .write()
            .account_mut(&account_id(req))
            .query_log_config_policies
            .insert(arn, policy);
        Ok(AwsResponse::ok_json(json!({ "ReturnValue": true })))
    }

    fn get_query_log_config_policy(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let arn = required_str(&body, "Arn")?;
        let st = self.state.read();
        let policy = st
            .accounts
            .get(&account_id(req))
            .and_then(|a| a.query_log_config_policies.get(&arn))
            .cloned()
            .ok_or_else(|| unknown_resource("No policy found for the specified resource"))?;
        Ok(AwsResponse::ok_json(
            json!({ "ResolverQueryLogConfigPolicy": policy }),
        ))
    }

    fn put_resolver_rule_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let arn = required_str(&body, "Arn")?;
        let policy = required_str(&body, "ResolverRulePolicy")?;
        self.state
            .write()
            .account_mut(&account_id(req))
            .resolver_rule_policies
            .insert(arn, policy);
        Ok(AwsResponse::ok_json(json!({ "ReturnValue": true })))
    }

    fn get_resolver_rule_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let arn = required_str(&body, "Arn")?;
        let st = self.state.read();
        let policy = st
            .accounts
            .get(&account_id(req))
            .and_then(|a| a.resolver_rule_policies.get(&arn))
            .cloned()
            .ok_or_else(|| unknown_resource("No policy found for the specified resource"))?;
        Ok(AwsResponse::ok_json(
            json!({ "ResolverRulePolicy": policy }),
        ))
    }

    fn tag_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let arn = required_str(&body, "ResourceArn")?;
        let tags = parse_tags(body.get("Tags"))?;
        let mut st = self.state.write();
        let acc = st.account_mut(&account_id(req));
        let entry = acc.tags.entry(arn).or_default();
        for t in tags {
            entry.retain(|e| e.key != t.key);
            entry.push(t);
        }
        Ok(AwsResponse::ok_json(json!({})))
    }

    fn untag_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let arn = required_str(&body, "ResourceArn")?;
        let keys: Vec<String> = body
            .get("TagKeys")
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default();
        let mut st = self.state.write();
        if let Some(entry) = st.account_mut(&account_id(req)).tags.get_mut(&arn) {
            entry.retain(|e| !keys.contains(&e.key));
        }
        Ok(AwsResponse::ok_json(json!({})))
    }

    fn list_tags_for_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let arn = required_str(&body, "ResourceArn")?;
        let st = self.state.read();
        let tags: Vec<Value> = st
            .accounts
            .get(&account_id(req))
            .and_then(|a| a.tags.get(&arn))
            .map(|t| t.iter().map(to_val).collect())
            .unwrap_or_default();
        drop(st);
        let (page, next) = paginate(&body, tags);
        Ok(list_response(json!({ "Tags": page }), next))
    }
}

// ─── Free helpers ────────────────────────────────────────────────────────

fn account_id(req: &AwsRequest) -> String {
    if req.account_id.is_empty() {
        "000000000000".to_string()
    } else {
        req.account_id.clone()
    }
}

fn region(req: &AwsRequest) -> String {
    if req.region.is_empty() {
        "us-east-1".to_string()
    } else {
        req.region.clone()
    }
}

/// Enforce the Smithy `@length` / `@range` / enum constraints on an operation's
/// input members, returning the input-error shape the operation declares
/// (`InvalidParameterException` for the resolver-endpoint/rule/query-log ops,
/// `ValidationException` for the DNS Firewall + Outpost ops). Only present
/// members are checked; absent required members are handled by the handlers.
fn validate_constraints(action: &str, body: &Value) -> Result<(), AwsServiceError> {
    // `ip` selects the declared input-error shape for this op family.
    let ip = !matches!(
        action,
        "CreateFirewallDomainList"
            | "CreateFirewallRuleGroup"
            | "CreateOutpostResolver"
            | "GetFirewallConfig"
            | "ListFirewallConfigs"
            | "ListFirewallDomainLists"
            | "ListFirewallRuleGroups"
            | "ListFirewallRuleGroupAssociations"
            | "ListFirewallRuleTypes"
            | "ListOutpostResolvers"
            | "PutFirewallRuleGroupPolicy"
    );
    let err = |msg: String| -> AwsServiceError {
        if ip {
            invalid_parameter(msg)
        } else {
            validation(msg)
        }
    };
    let slen = |field: &str, min: usize, max: usize| -> Result<(), AwsServiceError> {
        if let Some(v) = body.get(field).and_then(Value::as_str) {
            let n = v.chars().count();
            if n < min || n > max {
                return Err(err(format!(
                    "{field} length must be between {min} and {max}"
                )));
            }
        }
        Ok(())
    };
    let mres = |min: i64, max: i64| -> Result<(), AwsServiceError> {
        if let Some(v) = body.get("MaxResults").and_then(Value::as_i64) {
            if v < min || v > max {
                return Err(err(format!("MaxResults must be between {min} and {max}")));
            }
        }
        Ok(())
    };
    let senum = |field: &str, allowed: &[&str]| -> Result<(), AwsServiceError> {
        if let Some(v) = body.get(field).and_then(Value::as_str) {
            if !allowed.contains(&v) {
                return Err(err(format!("Invalid value for {field}: {v}")));
            }
        }
        Ok(())
    };

    // A `NextToken` is an opaque page-offset this service minted; a present,
    // non-empty token that does not parse as an offset was never issued by us
    // and is rejected (rather than silently restarting at page 1, which would
    // loop the caller forever). Use the error each operation actually declares:
    // most resolver list ops declare `InvalidNextTokenException`,
    // `ListResolverQueryLogConfigAssociations` declares `InvalidParameterException`,
    // and the DNS Firewall + Outpost list ops declare `ValidationException`.
    if let Some(tok) = body.get("NextToken").and_then(Value::as_str) {
        if !tok.is_empty() && tok.parse::<usize>().is_err() {
            const NEXT_TOKEN_OPS: &[&str] = &[
                "ListResolverConfigs",
                "ListResolverDnssecConfigs",
                "ListResolverEndpointIpAddresses",
                "ListResolverEndpoints",
                "ListResolverQueryLogConfigs",
                "ListResolverRuleAssociations",
                "ListResolverRules",
                "ListTagsForResource",
            ];
            let msg = "Invalid value for parameter NextToken";
            return Err(if NEXT_TOKEN_OPS.contains(&action) {
                invalid_next_token(msg)
            } else if action == "ListResolverQueryLogConfigAssociations" {
                invalid_parameter(msg)
            } else {
                validation(msg)
            });
        }
    }

    match action {
        "CreateResolverEndpoint" => {
            slen("CreatorRequestId", 1, 255)?;
            slen("Name", 0, 64)?;
        }
        "CreateResolverRule" => {
            slen("CreatorRequestId", 1, 255)?;
            slen("Name", 0, 64)?;
            slen("DomainName", 1, 256)?;
            slen("DelegationRecord", 1, 256)?;
            slen("ResolverEndpointId", 1, 64)?;
        }
        "CreateResolverQueryLogConfig" => {
            slen("CreatorRequestId", 1, 255)?;
            slen("Name", 1, 64)?;
            slen("DestinationArn", 1, 600)?;
        }
        "CreateFirewallDomainList" | "CreateFirewallRuleGroup" => {
            slen("CreatorRequestId", 1, 255)?;
            slen("Name", 0, 64)?;
        }
        "CreateOutpostResolver" => {
            slen("CreatorRequestId", 1, 255)?;
            slen("Name", 1, 255)?;
            slen("OutpostArn", 1, 255)?;
            slen("PreferredInstanceType", 1, 255)?;
        }
        "GetFirewallConfig" | "GetResolverConfig" | "GetResolverDnssecConfig" => {
            slen("ResourceId", 1, 64)?;
        }
        "ListFirewallConfigs" => mres(5, 10)?,
        "ListResolverConfigs" => mres(5, 100)?,
        "ListFirewallDomainLists" | "ListFirewallRuleGroups" => mres(1, 100)?,
        "ListFirewallRuleGroupAssociations" => {
            mres(1, 100)?;
            slen("FirewallRuleGroupId", 1, 64)?;
            slen("VpcId", 1, 64)?;
            senum("Status", &["COMPLETE", "DELETING", "UPDATING"])?;
        }
        "ListFirewallRuleTypes" => {
            mres(1, 100)?;
            slen("RuleType", 0, 128)?;
        }
        "ListOutpostResolvers" => {
            mres(1, 100)?;
            slen("OutpostArn", 1, 255)?;
        }
        "ListResolverDnssecConfigs"
        | "ListResolverEndpoints"
        | "ListResolverRuleAssociations"
        | "ListResolverRules" => mres(1, 100)?,
        "ListResolverQueryLogConfigs" | "ListResolverQueryLogConfigAssociations" => {
            mres(1, 100)?;
            slen("SortBy", 1, 64)?;
            senum("SortOrder", &["ASCENDING", "DESCENDING"])?;
        }
        "ListTagsForResource" => {
            mres(1, 100)?;
            slen("ResourceArn", 1, 255)?;
        }
        "TagResource" | "UntagResource" => slen("ResourceArn", 1, 255)?,
        "PutFirewallRuleGroupPolicy" => {
            slen("Arn", 1, 255)?;
            slen("FirewallRuleGroupPolicy", 0, 30000)?;
        }
        "PutResolverQueryLogConfigPolicy" => {
            slen("Arn", 1, 255)?;
            slen("ResolverQueryLogConfigPolicy", 0, 30000)?;
        }
        "PutResolverRulePolicy" => {
            slen("Arn", 1, 255)?;
            slen("ResolverRulePolicy", 0, 30000)?;
        }
        _ => {}
    }
    Ok(())
}

/// Like [`required_str`] but reports `ValidationException`, which is the input
/// error the DNS Firewall operations declare (they do not declare
/// `InvalidParameterException`).
fn required_str_v(body: &Value, field: &str) -> Result<String, AwsServiceError> {
    body.get(field)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .ok_or_else(|| validation(format!("{field} is required")))
}

/// Apply the request's `MaxResults` page size + `NextToken` offset to a full
/// result set. The token is a plain start offset (opaque to callers) that
/// round-trips: the returned `Some(token)` fed back as `NextToken` yields the
/// next page, and `None` marks the last page. Returns `(page, next_token)`.
///
/// A malformed `NextToken` (one this service never minted) is rejected up front
/// by [`validate_constraints`], so by the time it reaches here the token either
/// parses as an offset or is treated as the start.
fn paginate(body: &Value, items: Vec<Value>) -> (Vec<Value>, Option<String>) {
    let total = items.len();
    let start = body
        .get("NextToken")
        .and_then(Value::as_str)
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(0)
        .min(total);
    let end = match body.get("MaxResults").and_then(Value::as_i64) {
        Some(m) if m > 0 => (start + m as usize).min(total),
        _ => total,
    };
    let page: Vec<Value> = items.into_iter().skip(start).take(end - start).collect();
    let next = if end < total {
        Some(end.to_string())
    } else {
        None
    };
    (page, next)
}

/// Narrow a serialized-resource list by the request's `Filters`. Each filter is
/// `{Name, Values}`; an item is kept only when it satisfies **every** filter,
/// and a filter is satisfied when the item's mapped field equals **one** of the
/// filter's values (AWS Route 53 Resolver filter semantics: AND across filters,
/// OR within a filter's values).
///
/// `field_of` maps a filter `Name` to the JSON key on the serialized item —
/// Route 53 Resolver filter names largely match the response field names, with
/// a few aliases (e.g. resolver-rule `Type` -> `RuleType`). An unrecognized
/// filter name is rejected with `InvalidParameterException`, matching AWS.
///
/// Number fields (e.g. `IpAddressCount`) are compared by their decimal string,
/// and list fields (e.g. `SecurityGroupIds`) match when any element equals a
/// value.
fn apply_filters(
    items: Vec<Value>,
    body: &Value,
    field_of: impl Fn(&str) -> Option<&'static str>,
) -> Result<Vec<Value>, AwsServiceError> {
    let filters = match body.get("Filters").and_then(Value::as_array) {
        Some(f) if !f.is_empty() => f,
        _ => return Ok(items),
    };
    // Resolve every filter's field up front so an unknown name fails once,
    // before scanning any items.
    let mut resolved: Vec<(&'static str, Vec<String>)> = Vec::with_capacity(filters.len());
    for f in filters {
        let name = f
            .get("Name")
            .and_then(Value::as_str)
            .ok_or_else(|| invalid_parameter("Each filter requires a Name"))?;
        let key = field_of(name)
            .ok_or_else(|| invalid_parameter(format!("The filter '{name}' is invalid")))?;
        let values: Vec<String> = f
            .get("Values")
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default();
        resolved.push((key, values));
    }
    let kept = items
        .into_iter()
        .filter(|item| {
            resolved.iter().all(|(key, values)| match item.get(*key) {
                Some(Value::String(s)) => values.iter().any(|v| v == s),
                Some(Value::Number(n)) => values.iter().any(|v| *v == n.to_string()),
                Some(Value::Array(arr)) => arr
                    .iter()
                    .filter_map(Value::as_str)
                    .any(|s| values.iter().any(|v| v == s)),
                _ => false,
            })
        })
        .collect();
    Ok(kept)
}

/// The `MaxResults` value a paginated list echoes back: the request's page size
/// when supplied, otherwise the number of items returned in this page.
fn echoed_max(body: &Value, page_len: usize) -> i64 {
    body.get("MaxResults")
        .and_then(Value::as_i64)
        .unwrap_or(page_len as i64)
}

/// Attach a `NextToken` to a list response object when there is a next page.
fn list_response(mut obj: Value, next: Option<String>) -> AwsResponse {
    if let Some(t) = next {
        obj["NextToken"] = Value::String(t);
    }
    AwsResponse::ok_json(obj)
}

/// Serialize a typed wire struct (PascalCase, `None` skipped) to a JSON value.
fn to_val<T: serde::Serialize>(v: &T) -> Value {
    serde_json::to_value(v).unwrap_or(Value::Null)
}

fn string_list(v: Option<&Value>) -> Vec<String> {
    v.and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(|x| x.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}

/// A stable 17-hex-character id suffix derived from a seed (e.g. a VPC id), so
/// the config singletons (`rslvr-rc-*`/`rslvr-ds-*`/`rslvr-fc-*`) synthesized
/// for an unconfigured resource return the same id on every `Get`, matching AWS.
fn deterministic_suffix(seed: &str) -> String {
    format!("{:017x}", fnv1a(seed) & 0x000f_ffff_ffff_ffff)
}

/// Map a config's transient status to the terminal status it settles into, or
/// `None` when the status is already terminal.
fn transient_terminal(status: &str) -> Option<String> {
    match status {
        "ENABLING" => Some("ENABLED"),
        "DISABLING" => Some("DISABLED"),
        "UPDATING_TO_USE_LOCAL_RESOURCE_SETTING" => Some("USE_LOCAL_RESOURCE_SETTING"),
        _ => None,
    }
    .map(str::to_string)
}

/// Synthesize a private IP for an endpoint IP address when the caller omits one.
fn synth_ip(existing: &[IpAddressResponse]) -> String {
    format!("10.0.0.{}", 10 + existing.len())
}

/// Parse an S3 object URL into `(bucket, key)`, accepting the forms AWS and the
/// SDKs emit: `s3://bucket/key`, path-style `https://host/bucket/key`, and
/// virtual-hosted `https://bucket.s3.<...>/key`. Any query string or fragment
/// is stripped. Returns `None` when the URL does not name both a bucket and a
/// key.
fn parse_s3_url(url: &str) -> Option<(String, String)> {
    if let Some(rest) = url.strip_prefix("s3://") {
        let (bucket, key) = rest.split_once('/')?;
        return non_empty_pair(bucket, key);
    }
    let after_scheme = url.split_once("://").map(|(_, r)| r)?;
    let (host, rest) = after_scheme.split_once('/')?;
    let path = rest.split(['?', '#']).next().unwrap_or(rest);
    // Virtual-hosted style: the bucket is the label before `.s3.`/`.s3-`.
    if let Some(idx) = host.find(".s3.").or_else(|| host.find(".s3-")) {
        return non_empty_pair(&host[..idx], path);
    }
    // Path-style: the first path segment is the bucket.
    let (bucket, key) = path.split_once('/')?;
    non_empty_pair(bucket, key)
}

/// `Some((bucket, key))` only when neither part is empty.
fn non_empty_pair(bucket: &str, key: &str) -> Option<(String, String)> {
    if bucket.is_empty() || key.is_empty() {
        None
    } else {
        Some((bucket.to_string(), key.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use http::{HeaderMap, Method};
    use std::collections::HashMap;

    fn req(action: &str, body: Value) -> AwsRequest {
        AwsRequest {
            service: "route53resolver".into(),
            action: action.into(),
            region: "us-east-1".into(),
            account_id: "123456789012".into(),
            request_id: "r".into(),
            headers: HeaderMap::new(),
            query_params: HashMap::new(),
            body: Bytes::from(body.to_string()),
            body_stream: parking_lot::Mutex::new(None),
            path_segments: vec![],
            raw_path: "/".into(),
            raw_query: String::new(),
            method: Method::POST,
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        }
    }

    async fn call(svc: &Route53ResolverService, action: &str, body: Value) -> (u16, Value) {
        let resp = match svc.handle(req(action, body)).await {
            Ok(r) => r,
            Err(e) => panic!("handle {action} failed: {}", e.code()),
        };
        let status = resp.status.as_u16();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        (status, v)
    }

    // Finding 7: a valid multi-subnet endpoint (AWS minimum is two) must succeed
    // in standalone mode where no EC2 state is wired.
    #[tokio::test]
    async fn multi_subnet_endpoint_succeeds_without_ec2() {
        let svc = Route53ResolverService::default();
        let (status, body) = call(
            &svc,
            "CreateResolverEndpoint",
            json!({
                "CreatorRequestId": "c1",
                "Direction": "OUTBOUND",
                "SecurityGroupIds": ["sg-1"],
                "IpAddresses": [
                    { "SubnetId": "subnet-a" },
                    { "SubnetId": "subnet-b" },
                ],
            }),
        )
        .await;
        assert_eq!(status, 200, "{body}");
        assert!(body["ResolverEndpoint"]["Id"]
            .as_str()
            .unwrap()
            .starts_with("rslvr-out-"));
        // Both subnets map to one synthesized VPC.
        assert!(!body["ResolverEndpoint"]["HostVPCId"]
            .as_str()
            .unwrap()
            .is_empty());
        assert_eq!(body["ResolverEndpoint"]["IpAddressCount"], 2);
    }

    // Finding 2: associating the same query-log config with the same resource
    // twice is rejected instead of double-counting.
    #[tokio::test]
    async fn duplicate_query_log_association_rejected() {
        let svc = Route53ResolverService::default();
        let (_, cfg) = call(
            &svc,
            "CreateResolverQueryLogConfig",
            json!({ "Name": "n", "DestinationArn": "arn:aws:logs:us-east-1:123456789012:log-group:/g", "CreatorRequestId": "c" }),
        )
        .await;
        let id = cfg["ResolverQueryLogConfig"]["Id"]
            .as_str()
            .unwrap()
            .to_string();
        let (s1, _) = call(
            &svc,
            "AssociateResolverQueryLogConfig",
            json!({ "ResolverQueryLogConfigId": id, "ResourceId": "vpc-1" }),
        )
        .await;
        assert_eq!(s1, 200);
        let err = match svc
            .handle(req(
                "AssociateResolverQueryLogConfig",
                json!({ "ResolverQueryLogConfigId": id, "ResourceId": "vpc-1" }),
            ))
            .await
        {
            Ok(_) => panic!("duplicate association should have been rejected"),
            Err(e) => e,
        };
        assert_eq!(err.code(), "ResourceExistsException");
    }

    // Finding 6: an unconfigured resolver config returns a stable Id on repeated
    // Gets (deterministic, derived from the resource id).
    #[tokio::test]
    async fn stable_config_id_across_gets() {
        let svc = Route53ResolverService::default();
        let (_, a) = call(
            &svc,
            "GetResolverConfig",
            json!({ "ResourceId": "vpc-xyz" }),
        )
        .await;
        let (_, b) = call(
            &svc,
            "GetResolverConfig",
            json!({ "ResourceId": "vpc-xyz" }),
        )
        .await;
        assert_eq!(a["ResolverConfig"]["Id"], b["ResolverConfig"]["Id"]);
        // Different resources get different ids.
        let (_, c) = call(
            &svc,
            "GetResolverConfig",
            json!({ "ResourceId": "vpc-other" }),
        )
        .await;
        assert_ne!(a["ResolverConfig"]["Id"], c["ResolverConfig"]["Id"]);
    }

    // Finding 5: list operations apply the request MaxResults page size and emit
    // a NextToken that round-trips to the next page.
    #[tokio::test]
    async fn pagination_round_trips() {
        let svc = Route53ResolverService::default();
        for i in 0..3 {
            call(
                &svc,
                "CreateResolverEndpoint",
                json!({
                    "CreatorRequestId": format!("c{i}"),
                    "Direction": "INBOUND",
                    "SecurityGroupIds": ["sg-1"],
                    "IpAddresses": [ { "SubnetId": "subnet-a" }, { "SubnetId": "subnet-b" } ],
                }),
            )
            .await;
        }
        let (_, p1) = call(&svc, "ListResolverEndpoints", json!({ "MaxResults": 2 })).await;
        assert_eq!(p1["ResolverEndpoints"].as_array().unwrap().len(), 2);
        let token = p1["NextToken"]
            .as_str()
            .expect("first page has a NextToken")
            .to_string();
        let (_, p2) = call(
            &svc,
            "ListResolverEndpoints",
            json!({ "MaxResults": 2, "NextToken": token }),
        )
        .await;
        assert_eq!(p2["ResolverEndpoints"].as_array().unwrap().len(), 1);
        assert!(p2.get("NextToken").is_none(), "last page has no NextToken");
    }

    // Finding 1: a DNSSEC config snapshotted mid-transition settles to its
    // terminal status after a restart (rearm_pending).
    #[tokio::test]
    async fn dnssec_config_settles_after_restart() {
        let svc = Route53ResolverService::default();
        let state = svc.shared_state();
        {
            let mut st = state.write();
            let acc = st.account_mut("123456789012");
            acc.dnssec_configs.insert(
                "rslvr-ds-abc".to_string(),
                crate::state::ResolverDnssecConfig {
                    id: "rslvr-ds-abc".to_string(),
                    owner_id: "123456789012".to_string(),
                    resource_id: "vpc-1".to_string(),
                    validation_status: "ENABLING".to_string(),
                },
            );
        }
        svc.rearm_pending();
        tokio::time::sleep(SETTLE_DELAY + Duration::from_millis(300)).await;
        let st = state.read();
        let c = st.accounts["123456789012"].dnssec_configs["rslvr-ds-abc"].clone();
        assert_eq!(c.validation_status, "ENABLED");
    }

    async fn call_err(svc: &Route53ResolverService, action: &str, body: Value) -> AwsServiceError {
        match svc.handle(req(action, body)).await {
            Ok(_) => panic!("{action} should have failed"),
            Err(e) => e,
        }
    }

    async fn make_endpoint(svc: &Route53ResolverService, req_id: &str, direction: &str) {
        let (status, body) = call(
            svc,
            "CreateResolverEndpoint",
            json!({
                "CreatorRequestId": req_id,
                "Direction": direction,
                "SecurityGroupIds": ["sg-1"],
                "IpAddresses": [ { "SubnetId": "subnet-a" }, { "SubnetId": "subnet-b" } ],
            }),
        )
        .await;
        assert_eq!(status, 200, "{body}");
    }

    // Finding 1: a List with Filters actually narrows the result set (AND across
    // filters, OR within a filter's values), instead of returning everything.
    #[tokio::test]
    async fn filters_narrow_resolver_endpoints() {
        let svc = Route53ResolverService::default();
        make_endpoint(&svc, "in", "INBOUND").await;
        make_endpoint(&svc, "out", "OUTBOUND").await;

        let (_, all) = call(&svc, "ListResolverEndpoints", json!({})).await;
        assert_eq!(all["ResolverEndpoints"].as_array().unwrap().len(), 2);

        let (_, only_out) = call(
            &svc,
            "ListResolverEndpoints",
            json!({ "Filters": [ { "Name": "Direction", "Values": ["OUTBOUND"] } ] }),
        )
        .await;
        let eps = only_out["ResolverEndpoints"].as_array().unwrap();
        assert_eq!(eps.len(), 1);
        assert_eq!(eps[0]["Direction"], "OUTBOUND");
    }

    // Finding 1: an unrecognized filter name is rejected, matching AWS.
    #[tokio::test]
    async fn invalid_filter_name_rejected() {
        let svc = Route53ResolverService::default();
        make_endpoint(&svc, "in", "INBOUND").await;
        let err = call_err(
            &svc,
            "ListResolverEndpoints",
            json!({ "Filters": [ { "Name": "Nope", "Values": ["x"] } ] }),
        )
        .await;
        assert_eq!(err.code(), "InvalidParameterException");
    }

    // Finding 2: UpdateFirewallRule persists the BlockOverride* and
    // FirewallDomainRedirectionAction fields, which round-trip on List.
    #[tokio::test]
    async fn firewall_rule_override_fields_round_trip() {
        let svc = Route53ResolverService::default();
        let (_, g) = call(
            &svc,
            "CreateFirewallRuleGroup",
            json!({ "CreatorRequestId": "c", "Name": "g" }),
        )
        .await;
        let group_id = g["FirewallRuleGroup"]["Id"].as_str().unwrap().to_string();
        let (_, dl) = call(
            &svc,
            "CreateFirewallDomainList",
            json!({ "CreatorRequestId": "c", "Name": "dl" }),
        )
        .await;
        let dl_id = dl["FirewallDomainList"]["Id"].as_str().unwrap().to_string();
        let (s, _) = call(
            &svc,
            "CreateFirewallRule",
            json!({
                "FirewallRuleGroupId": group_id,
                "FirewallDomainListId": dl_id,
                "Name": "r",
                "Priority": 10,
                "Action": "BLOCK",
                "BlockResponse": "OVERRIDE",
            }),
        )
        .await;
        assert_eq!(s, 200);
        let (s, upd) = call(
            &svc,
            "UpdateFirewallRule",
            json!({
                "FirewallRuleGroupId": group_id,
                "FirewallDomainListId": dl_id,
                "BlockResponse": "OVERRIDE",
                "BlockOverrideDomain": "safe.example.com",
                "BlockOverrideDnsType": "CNAME",
                "BlockOverrideTtl": 42,
                "FirewallDomainRedirectionAction": "TRUST_REDIRECTION_DOMAIN",
            }),
        )
        .await;
        assert_eq!(s, 200);
        assert_eq!(
            upd["FirewallRule"]["BlockOverrideDomain"],
            "safe.example.com"
        );

        let (_, listed) = call(
            &svc,
            "ListFirewallRules",
            json!({ "FirewallRuleGroupId": group_id }),
        )
        .await;
        let rule = &listed["FirewallRules"][0];
        assert_eq!(rule["BlockOverrideDomain"], "safe.example.com");
        assert_eq!(rule["BlockOverrideDnsType"], "CNAME");
        assert_eq!(rule["BlockOverrideTtl"], 42);
        assert_eq!(
            rule["FirewallDomainRedirectionAction"],
            "TRUST_REDIRECTION_DOMAIN"
        );
    }

    // bug-hunt 2026-07-19: CreateResolverEndpoint persists OutpostArn,
    // PreferredInstanceType and the Dns64/Ipv6/metrics booleans; Get/List echo
    // them, and UpdateResolverEndpoint round-trips the mutable booleans.
    #[tokio::test]
    async fn resolver_endpoint_optional_fields_round_trip() {
        let svc = Route53ResolverService::default();
        let (status, body) = call(
            &svc,
            "CreateResolverEndpoint",
            json!({
                "CreatorRequestId": "c1",
                "Direction": "INBOUND",
                "SecurityGroupIds": ["sg-1"],
                "IpAddresses": [ { "SubnetId": "subnet-a" }, { "SubnetId": "subnet-b" } ],
                "OutpostArn": "arn:aws:outposts:us-east-1:123456789012:outpost/op-abc",
                "PreferredInstanceType": "m5.large",
                "Dns64Enabled": true,
                "Ipv6InternetAccessEnabled": false,
                "RniEnhancedMetricsEnabled": true,
                "TargetNameServerMetricsEnabled": true,
            }),
        )
        .await;
        assert_eq!(status, 200, "{body}");
        let ep = &body["ResolverEndpoint"];
        assert_eq!(
            ep["OutpostArn"],
            "arn:aws:outposts:us-east-1:123456789012:outpost/op-abc"
        );
        assert_eq!(ep["PreferredInstanceType"], "m5.large");
        assert_eq!(ep["Dns64Enabled"], true);
        assert_eq!(ep["Ipv6InternetAccessEnabled"], false);
        assert_eq!(ep["RniEnhancedMetricsEnabled"], true);
        assert_eq!(ep["TargetNameServerMetricsEnabled"], true);
        let id = ep["Id"].as_str().unwrap().to_string();

        let (_, got) = call(
            &svc,
            "GetResolverEndpoint",
            json!({ "ResolverEndpointId": id }),
        )
        .await;
        assert_eq!(got["ResolverEndpoint"]["OutpostArn"], ep["OutpostArn"]);
        assert_eq!(got["ResolverEndpoint"]["Dns64Enabled"], true);

        // Update flips Dns64Enabled.
        let (s, upd) = call(
            &svc,
            "UpdateResolverEndpoint",
            json!({ "ResolverEndpointId": id, "Dns64Enabled": false }),
        )
        .await;
        assert_eq!(s, 200, "{upd}");
        assert_eq!(upd["ResolverEndpoint"]["Dns64Enabled"], false);
    }

    // bug-hunt 2026-07-19: CreateResolverRule persists DelegationRecord and it
    // round-trips on Get/List.
    #[tokio::test]
    async fn resolver_rule_delegation_record_round_trips() {
        let svc = Route53ResolverService::default();
        let (s, body) = call(
            &svc,
            "CreateResolverRule",
            json!({
                "CreatorRequestId": "c",
                "RuleType": "RECURSIVE",
                "DomainName": "example.com",
                "DelegationRecord": "ns-1.example.com",
            }),
        )
        .await;
        assert_eq!(s, 200, "{body}");
        assert_eq!(body["ResolverRule"]["DelegationRecord"], "ns-1.example.com");
        let id = body["ResolverRule"]["Id"].as_str().unwrap().to_string();
        let (_, got) = call(&svc, "GetResolverRule", json!({ "ResolverRuleId": id })).await;
        assert_eq!(got["ResolverRule"]["DelegationRecord"], "ns-1.example.com");
    }

    // bug-hunt 2026-07-19: CreateFirewallRule persists DnsThreatProtection,
    // ConfidenceThreshold and the structured FirewallRuleType; they round-trip
    // on List and mint a FirewallThreatProtectionId.
    #[tokio::test]
    async fn firewall_rule_threat_protection_fields_round_trip() {
        let svc = Route53ResolverService::default();
        let (_, g) = call(
            &svc,
            "CreateFirewallRuleGroup",
            json!({ "CreatorRequestId": "c", "Name": "g" }),
        )
        .await;
        let group_id = g["FirewallRuleGroup"]["Id"].as_str().unwrap().to_string();
        let (s, created) = call(
            &svc,
            "CreateFirewallRule",
            json!({
                "FirewallRuleGroupId": group_id,
                "Name": "threat-rule",
                "Priority": 5,
                "Action": "BLOCK",
                "BlockResponse": "NODATA",
                "DnsThreatProtection": "DGA",
                "ConfidenceThreshold": "HIGH",
                "FirewallRuleType": { "DnsThreatProtection": { "Detector": "DGA" } },
            }),
        )
        .await;
        assert_eq!(s, 200, "{created}");
        let rule = &created["FirewallRule"];
        assert_eq!(rule["DnsThreatProtection"], "DGA");
        assert_eq!(rule["ConfidenceThreshold"], "HIGH");
        assert_eq!(
            rule["FirewallRuleType"]["DnsThreatProtection"]["Detector"],
            "DGA"
        );
        assert!(rule["FirewallThreatProtectionId"]
            .as_str()
            .unwrap()
            .starts_with("rslvr-ftp-"));

        let (_, listed) = call(
            &svc,
            "ListFirewallRules",
            json!({ "FirewallRuleGroupId": group_id }),
        )
        .await;
        let listed_rule = &listed["FirewallRules"][0];
        assert_eq!(listed_rule["DnsThreatProtection"], "DGA");
        assert_eq!(listed_rule["ConfidenceThreshold"], "HIGH");
    }

    // BatchUpdateFirewallRule must apply DnsThreatProtection / ConfidenceThreshold
    // / FirewallRuleType exactly like the single-rule update, so the batch path
    // persists them and they round-trip on List.
    #[tokio::test]
    async fn batch_update_firewall_rule_persists_threat_protection_fields() {
        let svc = Route53ResolverService::default();
        let (_, g) = call(
            &svc,
            "CreateFirewallRuleGroup",
            json!({ "CreatorRequestId": "c", "Name": "g" }),
        )
        .await;
        let group_id = g["FirewallRuleGroup"]["Id"].as_str().unwrap().to_string();
        let (s, _) = call(
            &svc,
            "CreateFirewallRule",
            json!({
                "FirewallRuleGroupId": group_id,
                "Name": "r",
                "Priority": 5,
                "Action": "BLOCK",
                "BlockResponse": "NODATA",
                "DnsThreatProtection": "DGA",
                "ConfidenceThreshold": "LOW",
            }),
        )
        .await;
        assert_eq!(s, 200);

        let (s, upd) = call(
            &svc,
            "BatchUpdateFirewallRule",
            json!({
                "UpdateFirewallRuleEntries": [{
                    "FirewallRuleGroupId": group_id,
                    "DnsThreatProtection": "DNS_TUNNELING",
                    "ConfidenceThreshold": "HIGH",
                    "FirewallRuleType": { "DnsThreatProtection": { "Detector": "DNS_TUNNELING" } },
                }],
            }),
        )
        .await;
        assert_eq!(s, 200, "{upd}");
        let updated = &upd["UpdatedFirewallRules"][0];
        assert_eq!(updated["DnsThreatProtection"], "DNS_TUNNELING");
        assert_eq!(updated["ConfidenceThreshold"], "HIGH");

        // The change persists to state, visible on List.
        let (_, listed) = call(
            &svc,
            "ListFirewallRules",
            json!({ "FirewallRuleGroupId": group_id }),
        )
        .await;
        let listed_rule = &listed["FirewallRules"][0];
        assert_eq!(listed_rule["DnsThreatProtection"], "DNS_TUNNELING");
        assert_eq!(listed_rule["ConfidenceThreshold"], "HIGH");
        assert_eq!(
            listed_rule["FirewallRuleType"]["DnsThreatProtection"]["Detector"],
            "DNS_TUNNELING"
        );
    }

    // Finding 5: a NextToken that this service never minted is rejected with
    // InvalidNextTokenException instead of silently restarting at page 1.
    #[tokio::test]
    async fn bad_next_token_rejected() {
        let svc = Route53ResolverService::default();
        make_endpoint(&svc, "in", "INBOUND").await;
        let err = call_err(
            &svc,
            "ListResolverEndpoints",
            json!({ "NextToken": "not-a-number" }),
        )
        .await;
        assert_eq!(err.code(), "InvalidNextTokenException");
    }

    // Finding 7: a Resolver endpoint requires at least two IP addresses.
    #[tokio::test]
    async fn endpoint_requires_two_ips() {
        let svc = Route53ResolverService::default();
        let err = call_err(
            &svc,
            "CreateResolverEndpoint",
            json!({
                "CreatorRequestId": "c",
                "Direction": "INBOUND",
                "SecurityGroupIds": ["sg-1"],
                "IpAddresses": [ { "SubnetId": "subnet-a" } ],
            }),
        )
        .await;
        assert_eq!(err.code(), "InvalidRequestException");
    }

    // Finding 7: a FORWARD rule requires TargetIps.
    #[tokio::test]
    async fn forward_rule_requires_targets() {
        let svc = Route53ResolverService::default();
        make_endpoint(&svc, "out", "OUTBOUND").await;
        let (_, eps) = call(&svc, "ListResolverEndpoints", json!({})).await;
        let ep_id = eps["ResolverEndpoints"][0]["Id"]
            .as_str()
            .unwrap()
            .to_string();
        let err = call_err(
            &svc,
            "CreateResolverRule",
            json!({
                "CreatorRequestId": "c",
                "RuleType": "FORWARD",
                "DomainName": "example.com",
                "ResolverEndpointId": ep_id,
            }),
        )
        .await;
        assert_eq!(err.code(), "InvalidRequestException");
    }

    // Finding 6: ImportFirewallDomains reads the domain file from wired S3 state
    // and REPLACEs the list (one trimmed, non-empty domain per line).
    #[tokio::test]
    async fn import_firewall_domains_from_s3() {
        use fakecloud_core::multi_account::MultiAccountState;
        use fakecloud_s3::{memory_body, S3Bucket, S3Object};

        let s3: fakecloud_s3::SharedS3State = Arc::new(RwLock::new(MultiAccountState::new(
            "123456789012",
            "us-east-1",
            "http://localhost",
        )));
        {
            let mut g = s3.write();
            let st = g.get_or_create("123456789012");
            let mut bucket = S3Bucket::new("my-bucket", "us-east-1", "123456789012");
            bucket.objects.insert(
                "domains.txt".to_string(),
                S3Object {
                    key: "domains.txt".to_string(),
                    body: memory_body(Bytes::from("example.com\n  bad.example.org \n\nfoo.test\n")),
                    ..Default::default()
                },
            );
            st.buckets.insert("my-bucket".to_string(), bucket);
        }
        let svc = Route53ResolverService::default().with_s3_state(s3);
        let (_, dl) = call(
            &svc,
            "CreateFirewallDomainList",
            json!({ "CreatorRequestId": "c", "Name": "list" }),
        )
        .await;
        let id = dl["FirewallDomainList"]["Id"].as_str().unwrap().to_string();
        let (s, r) = call(
            &svc,
            "ImportFirewallDomains",
            json!({
                "FirewallDomainListId": id,
                "Operation": "REPLACE",
                "DomainFileUrl": "s3://my-bucket/domains.txt",
            }),
        )
        .await;
        assert_eq!(s, 200, "{r}");
        assert_eq!(r["Status"], "COMPLETE");

        let (_, ld) = call(
            &svc,
            "ListFirewallDomains",
            json!({ "FirewallDomainListId": id }),
        )
        .await;
        let domains: Vec<String> = ld["Domains"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap().to_string())
            .collect();
        assert_eq!(domains, ["example.com", "bad.example.org", "foo.test"]);
    }

    // Finding 6: without S3 wired the import leaves the list unchanged and says
    // so, rather than fabricating a successful import.
    #[tokio::test]
    async fn import_firewall_domains_without_s3_is_honest() {
        let svc = Route53ResolverService::default();
        let (_, dl) = call(
            &svc,
            "CreateFirewallDomainList",
            json!({ "CreatorRequestId": "c", "Name": "list" }),
        )
        .await;
        let id = dl["FirewallDomainList"]["Id"].as_str().unwrap().to_string();
        let (s, r) = call(
            &svc,
            "ImportFirewallDomains",
            json!({
                "FirewallDomainListId": id,
                "Operation": "REPLACE",
                "DomainFileUrl": "s3://my-bucket/domains.txt",
            }),
        )
        .await;
        assert_eq!(s, 200, "{r}");
        assert!(r["StatusMessage"].as_str().unwrap().contains("unchanged"));
    }

    #[test]
    fn parse_s3_url_forms() {
        assert_eq!(
            parse_s3_url("s3://bucket/path/to/key.txt"),
            Some(("bucket".to_string(), "path/to/key.txt".to_string()))
        );
        assert_eq!(
            parse_s3_url("https://s3.us-east-1.amazonaws.com/bucket/key.txt"),
            Some(("bucket".to_string(), "key.txt".to_string()))
        );
        assert_eq!(
            parse_s3_url("https://bucket.s3.us-east-1.amazonaws.com/key.txt?X-Amz-Signature=abc"),
            Some(("bucket".to_string(), "key.txt".to_string()))
        );
        assert_eq!(parse_s3_url("s3://bucket-only"), None);
        assert_eq!(parse_s3_url("not a url"), None);
    }
}
