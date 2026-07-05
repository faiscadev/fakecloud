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
    arn, conflict, hex17, invalid_parameter, invalid_request, not_found, now_rfc3339, parse_tags,
    parse_target_ips, required_str, resource_in_use, unknown_resource, validation,
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
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<AsyncMutex<()>>,
}

impl Route53ResolverService {
    pub fn new(state: SharedRoute53ResolverState) -> Self {
        Self {
            state,
            ec2_state: None,
            snapshot_store: None,
            snapshot_lock: Arc::new(AsyncMutex::new(())),
        }
    }

    pub fn with_ec2_state(mut self, ec2_state: fakecloud_ec2::SharedEc2State) -> Self {
        self.ec2_state = Some(ec2_state);
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

        let ip_reqs = body
            .get("IpAddresses")
            .and_then(Value::as_array)
            .filter(|a| !a.is_empty())
            .ok_or_else(|| invalid_parameter("At least one IpAddress is required"))?;

        let now = now_rfc3339();
        let mut ip_addresses = Vec::new();
        let mut host_vpc: Option<String> = None;
        for ipr in ip_reqs {
            let subnet_id = ipr
                .get("SubnetId")
                .and_then(Value::as_str)
                .ok_or_else(|| invalid_parameter("Each IpAddress requires a SubnetId"))?
                .to_string();
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
        let st = self.state.read();
        let list: Vec<Value> = st
            .accounts
            .get(&account_id(req))
            .map(|a| a.endpoints.values().map(|r| to_val(&r.endpoint)).collect())
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(json!({
            "MaxResults": list.len(),
            "ResolverEndpoints": list,
        })))
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
        Ok(AwsResponse::ok_json(json!({
            "MaxResults": ips.len(),
            "IpAddresses": ips,
        })))
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
        // A FORWARD rule must target an OUTBOUND resolver endpoint.
        if rule_type == "FORWARD" {
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
        let st = self.state.read();
        let list: Vec<Value> = st
            .accounts
            .get(&account_id(req))
            .map(|a| a.rules.values().map(to_val).collect())
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(json!({
            "MaxResults": list.len(),
            "ResolverRules": list,
        })))
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
        let st = self.state.read();
        let list: Vec<Value> = st
            .accounts
            .get(&account_id(req))
            .map(|a| a.rule_associations.values().map(to_val).collect())
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(json!({
            "MaxResults": list.len(),
            "ResolverRuleAssociations": list,
        })))
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
        let st = self.state.read();
        let list: Vec<Value> = st
            .accounts
            .get(&account_id(req))
            .map(|a| a.query_log_configs.values().map(to_val).collect())
            .unwrap_or_default();
        let n = list.len();
        Ok(AwsResponse::ok_json(json!({
            "TotalCount": n,
            "TotalFilteredCount": n,
            "ResolverQueryLogConfigs": list,
        })))
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
            if st
                .accounts
                .get(&account)
                .map(|a| !a.query_log_configs.contains_key(&cfg_id))
                .unwrap_or(true)
            {
                return Err(not_found(format!("Query log config '{cfg_id}' not found")));
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
        let st = self.state.read();
        let list: Vec<Value> = st
            .accounts
            .get(&account_id(req))
            .map(|a| a.query_log_associations.values().map(to_val).collect())
            .unwrap_or_default();
        let n = list.len();
        Ok(AwsResponse::ok_json(json!({
            "TotalCount": n,
            "TotalFilteredCount": n,
            "ResolverQueryLogConfigAssociations": list,
        })))
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
        let st = self.state.read();
        let list: Vec<Value> = st
            .accounts
            .get(&account_id(req))
            .map(|a| a.resolver_configs.values().map(to_val).collect())
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(json!({ "ResolverConfigs": list })))
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
            id: format!("rslvr-rc-{}", hex17()),
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
        let st = self.state.read();
        let list: Vec<Value> = st
            .accounts
            .get(&account_id(req))
            .map(|a| a.dnssec_configs.values().map(to_val).collect())
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(
            json!({ "ResolverDnssecConfigs": list }),
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
            id: format!("rslvr-ds-{}", hex17()),
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
        let st = self.state.read();
        let list: Vec<Value> = st
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
        Ok(AwsResponse::ok_json(json!({ "FirewallRuleGroups": list })))
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
        let st = self.state.read();
        let list: Vec<Value> = st
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
        Ok(AwsResponse::ok_json(json!({ "FirewallDomainLists": list })))
    }

    fn import_firewall_domains(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        // Route 53 Resolver imports domains from an S3 object URL. We do not
        // fetch the object; instead we accept the request and mark the list
        // COMPLETE (the domains land empty). Real import content is a data-plane
        // concern out of scope for the control plane.
        let body = req.json_body();
        let account = account_id(req);
        let id = required_str(&body, "FirewallDomainListId")?;
        let _op = required_str(&body, "Operation")?;
        let _url = required_str(&body, "DomainFileUrl")?;
        let mut st = self.state.write();
        let acc = st
            .accounts
            .get_mut(&account)
            .ok_or_else(|| not_found(format!("Firewall domain list '{id}' not found")))?;
        let list = acc
            .firewall_domain_lists
            .get_mut(&id)
            .ok_or_else(|| not_found(format!("Firewall domain list '{id}' not found")))?;
        list.status = "COMPLETE".to_string();
        list.modification_time = now_rfc3339();
        Ok(AwsResponse::ok_json(json!({
            "Id": list.id,
            "Name": list.name,
            "Status": list.status,
            "StatusMessage": "Import complete",
        })))
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
        let domains = acc.firewall_domains.get(&id).cloned().unwrap_or_default();
        Ok(AwsResponse::ok_json(json!({ "Domains": domains })))
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
        Ok(AwsResponse::ok_json(json!({ "FirewallRules": rules })))
    }

    fn list_firewall_rule_types(&self, _req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        // The static catalog of DNS Firewall rule types.
        let types = json!([
            {
                "RuleType": "STANDARD",
                "Value": "STANDARD",
                "DisplayName": "Standard",
                "Description": "A standard DNS Firewall rule that matches queries against a domain list.",
            },
            {
                "RuleType": "DNS_THREAT_PROTECTION",
                "Value": "DNS_THREAT_PROTECTION",
                "DisplayName": "Advanced DNS threat protection",
                "Description": "A rule that detects DNS tunneling and domain generation algorithm threats.",
            }
        ]);
        Ok(AwsResponse::ok_json(json!({ "FirewallRuleTypes": types })))
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
        let st = self.state.read();
        let list: Vec<Value> = st
            .accounts
            .get(&account_id(req))
            .map(|a| {
                a.firewall_rule_group_associations
                    .values()
                    .map(to_val)
                    .collect()
            })
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(
            json!({ "FirewallRuleGroupAssociations": list }),
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
        let st = self.state.read();
        let list: Vec<Value> = st
            .accounts
            .get(&account_id(req))
            .map(|a| a.firewall_configs.values().map(to_val).collect())
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(json!({ "FirewallConfigs": list })))
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
            id: format!("rslvr-fc-{}", hex17()),
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
        let st = self.state.read();
        let list: Vec<Value> = st
            .accounts
            .get(&account_id(req))
            .map(|a| a.outpost_resolvers.values().map(to_val).collect())
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(json!({ "OutpostResolvers": list })))
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
        Ok(AwsResponse::ok_json(json!({ "Tags": tags })))
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

/// Deterministically synthesize a VPC id for a subnet when EC2 state is not
/// wired, so `HostVPCId` is stable across calls for the same subnet.
fn synth_vpc(subnet_id: &str) -> String {
    let mut h: u64 = 1469598103934665603;
    for b in subnet_id.bytes() {
        h ^= b as u64;
        h = h.wrapping_mul(1099511628211);
    }
    format!("vpc-{:017x}", h & 0x000f_ffff_ffff_ffff)
}

/// Synthesize a private IP for an endpoint IP address when the caller omits one.
fn synth_ip(existing: &[IpAddressResponse]) -> String {
    format!("10.0.0.{}", 10 + existing.len())
}
