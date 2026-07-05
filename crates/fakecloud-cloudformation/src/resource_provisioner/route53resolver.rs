//! CloudFormation provisioner arms for `AWS::Route53Resolver::*`.
//!
//! Without a provisioner arm a service with a registered snapshot hook yields
//! phantom `CREATE_COMPLETE` resources that vanish on restart (#1766). These
//! arms mutate the snapshot-backed `route53resolver` state directly, and the
//! server's `route53resolver` snapshot hook writes them through to disk.
//!
//! CloudFormation provisioning is synchronous (not probe-timed), so resources
//! are created in their terminal state (`OPERATIONAL` / `COMPLETE` / `ENABLED`)
//! rather than going through the API handler's background settle.

use super::*;

use fakecloud_route53resolver::state::{
    EndpointRecord, FirewallConfig, FirewallDomainList, FirewallRule, FirewallRuleGroup,
    FirewallRuleGroupAssociation, IpAddressResponse, ResolverConfig, ResolverDnssecConfig,
    ResolverEndpoint, ResolverQueryLogConfig, ResolverQueryLogConfigAssociation, ResolverRule,
    ResolverRuleAssociation, Tag, TargetAddress,
};
use fakecloud_route53resolver::validate::{arn as r53r_arn, hex17, now_rfc3339};

impl ResourceProvisioner {
    fn r53r_tags(&self, props: &serde_json::Value) -> Vec<Tag> {
        props
            .get("Tags")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|t| {
                        let key = t.get("Key").and_then(|v| v.as_str())?.to_string();
                        let value = t
                            .get("Value")
                            .and_then(|v| v.as_str())
                            .unwrap_or_default()
                            .to_string();
                        Some(Tag { key, value })
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    // --- AWS::Route53Resolver::ResolverEndpoint ---

    pub(super) fn create_r53r_resolver_endpoint(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let direction = props
            .get("Direction")
            .and_then(|v| v.as_str())
            .unwrap_or("INBOUND")
            .to_string();
        let security_group_ids: Vec<String> = props
            .get("SecurityGroupIds")
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        let now = now_rfc3339();
        let mut ip_addresses = Vec::new();
        let mut host_vpc = String::new();
        for (i, ipr) in props
            .get("IpAddresses")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default()
            .iter()
            .enumerate()
        {
            let subnet_id = ipr
                .get("SubnetId")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string();
            if host_vpc.is_empty() {
                host_vpc = self
                    .route53resolver_state
                    .read()
                    .accounts
                    .get(&self.account_id)
                    .map(|_| String::new())
                    .unwrap_or_default();
            }
            let ip = ipr
                .get("Ip")
                .and_then(|v| v.as_str())
                .map(String::from)
                .unwrap_or_else(|| format!("10.0.0.{}", 10 + i));
            ip_addresses.push(IpAddressResponse {
                ip_id: format!("rni-{}", hex17()),
                subnet_id,
                ip: Some(ip),
                ipv6: ipr.get("Ipv6").and_then(|v| v.as_str()).map(String::from),
                status: "ATTACHED".to_string(),
                status_message: "This IP address is operational.".to_string(),
                creation_time: now.clone(),
                modification_time: now.clone(),
            });
        }
        let prefix = if direction == "OUTBOUND" {
            "rslvr-out-"
        } else {
            "rslvr-in-"
        };
        let id = format!("{prefix}{}", hex17());
        let arn = r53r_arn(&self.region, &self.account_id, "resolver-endpoint", &id);
        let endpoint = ResolverEndpoint {
            id: id.clone(),
            creator_request_id: id.clone(),
            arn: arn.clone(),
            name: props.get("Name").and_then(|v| v.as_str()).map(String::from),
            security_group_ids,
            direction,
            ip_address_count: ip_addresses.len() as i64,
            host_vpc_id: host_vpc,
            status: "OPERATIONAL".to_string(),
            status_message: "This Resolver Endpoint is operational.".to_string(),
            creation_time: now.clone(),
            modification_time: now,
            resolver_endpoint_type: props
                .get("ResolverEndpointType")
                .and_then(|v| v.as_str())
                .unwrap_or("IPV4")
                .to_string(),
            protocols: props
                .get("Protocols")
                .and_then(|v| v.as_array())
                .map(|a| {
                    a.iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default(),
        };
        let ip_count = endpoint.ip_address_count;
        let tags = self.r53r_tags(props);
        {
            let mut st = self.route53resolver_state.write();
            let acc = st.account_mut(&self.account_id);
            acc.endpoints.insert(
                id.clone(),
                EndpointRecord {
                    endpoint,
                    ip_addresses,
                },
            );
            if !tags.is_empty() {
                acc.tags.insert(arn.clone(), tags);
            }
        }
        Ok(ProvisionResult::new(id.clone())
            .with("Arn", arn)
            .with("ResolverEndpointId", id)
            .with("IpAddressCount", ip_count.to_string()))
    }

    pub(super) fn delete_r53r_resolver_endpoint(&self, physical_id: &str) -> Result<(), String> {
        let mut st = self.route53resolver_state.write();
        if let Some(acc) = st.accounts.get_mut(&self.account_id) {
            acc.endpoints.remove(physical_id);
        }
        Ok(())
    }

    // --- AWS::Route53Resolver::ResolverRule ---

    pub(super) fn create_r53r_resolver_rule(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let rule_type = props
            .get("RuleType")
            .and_then(|v| v.as_str())
            .unwrap_or("FORWARD")
            .to_string();
        let target_ips: Vec<TargetAddress> = props
            .get("TargetIps")
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .map(|t| TargetAddress {
                        ip: t.get("Ip").and_then(|v| v.as_str()).map(String::from),
                        port: Some(t.get("Port").and_then(|v| v.as_i64()).unwrap_or(53)),
                        ipv6: t.get("Ipv6").and_then(|v| v.as_str()).map(String::from),
                        protocol: t.get("Protocol").and_then(|v| v.as_str()).map(String::from),
                        server_name_indication: t
                            .get("ServerNameIndication")
                            .and_then(|v| v.as_str())
                            .map(String::from),
                    })
                    .collect()
            })
            .unwrap_or_default();
        let id = format!("rslvr-rr-{}", hex17());
        let arn = r53r_arn(&self.region, &self.account_id, "resolver-rule", &id);
        let domain_name = props
            .get("DomainName")
            .and_then(|v| v.as_str())
            .map(String::from);
        let rule = ResolverRule {
            id: id.clone(),
            creator_request_id: id.clone(),
            arn: arn.clone(),
            domain_name: domain_name.clone(),
            status: "COMPLETE".to_string(),
            status_message: "[Trace id: 1] Successfully created Resolver Rule".to_string(),
            rule_type,
            name: props.get("Name").and_then(|v| v.as_str()).map(String::from),
            target_ips,
            resolver_endpoint_id: props
                .get("ResolverEndpointId")
                .and_then(|v| v.as_str())
                .map(String::from),
            owner_id: self.account_id.clone(),
            share_status: "NOT_SHARED".to_string(),
            creation_time: now_rfc3339(),
            modification_time: now_rfc3339(),
        };
        let tags = self.r53r_tags(props);
        {
            let mut st = self.route53resolver_state.write();
            let acc = st.account_mut(&self.account_id);
            acc.rules.insert(id.clone(), rule);
            if !tags.is_empty() {
                acc.tags.insert(arn.clone(), tags);
            }
        }
        let mut out = ProvisionResult::new(id.clone())
            .with("Arn", arn)
            .with("ResolverRuleId", id);
        if let Some(dn) = domain_name {
            out = out.with("DomainName", dn);
        }
        Ok(out)
    }

    pub(super) fn delete_r53r_resolver_rule(&self, physical_id: &str) -> Result<(), String> {
        let mut st = self.route53resolver_state.write();
        if let Some(acc) = st.accounts.get_mut(&self.account_id) {
            acc.rules.remove(physical_id);
        }
        Ok(())
    }

    // --- AWS::Route53Resolver::ResolverRuleAssociation ---

    pub(super) fn create_r53r_rule_association(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let resolver_rule_id = props
            .get("ResolverRuleId")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "ResolverRuleId is required".to_string())?
            .to_string();
        let vpc_id = props
            .get("VPCId")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "VPCId is required".to_string())?
            .to_string();
        let id = format!("rslvr-rrassoc-{}", hex17());
        let assoc = ResolverRuleAssociation {
            id: id.clone(),
            resolver_rule_id,
            name: props.get("Name").and_then(|v| v.as_str()).map(String::from),
            vpc_id,
            status: "COMPLETE".to_string(),
            status_message: String::new(),
        };
        self.route53resolver_state
            .write()
            .account_mut(&self.account_id)
            .rule_associations
            .insert(id.clone(), assoc);
        Ok(ProvisionResult::new(id.clone()).with("ResolverRuleAssociationId", id))
    }

    pub(super) fn delete_r53r_rule_association(&self, physical_id: &str) -> Result<(), String> {
        let mut st = self.route53resolver_state.write();
        if let Some(acc) = st.accounts.get_mut(&self.account_id) {
            acc.rule_associations.remove(physical_id);
        }
        Ok(())
    }

    // --- AWS::Route53Resolver::ResolverQueryLoggingConfig ---

    pub(super) fn create_r53r_query_log_config(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("Name")
            .and_then(|v| v.as_str())
            .unwrap_or("query-log-config")
            .to_string();
        let destination_arn = props
            .get("DestinationArn")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let id = format!("rslvr-qlc-{}", hex17());
        let arn = r53r_arn(
            &self.region,
            &self.account_id,
            "resolver-query-log-config",
            &id,
        );
        let cfg = ResolverQueryLogConfig {
            id: id.clone(),
            owner_id: self.account_id.clone(),
            status: "CREATED".to_string(),
            share_status: "NOT_SHARED".to_string(),
            association_count: 0,
            arn: arn.clone(),
            name,
            destination_arn,
            creator_request_id: id.clone(),
            creation_time: now_rfc3339(),
        };
        let tags = self.r53r_tags(props);
        {
            let mut st = self.route53resolver_state.write();
            let acc = st.account_mut(&self.account_id);
            acc.query_log_configs.insert(id.clone(), cfg);
            if !tags.is_empty() {
                acc.tags.insert(arn.clone(), tags);
            }
        }
        Ok(ProvisionResult::new(id.clone())
            .with("Arn", arn)
            .with("Id", id))
    }

    pub(super) fn delete_r53r_query_log_config(&self, physical_id: &str) -> Result<(), String> {
        let mut st = self.route53resolver_state.write();
        if let Some(acc) = st.accounts.get_mut(&self.account_id) {
            acc.query_log_configs.remove(physical_id);
        }
        Ok(())
    }

    // --- AWS::Route53Resolver::ResolverQueryLoggingConfigAssociation ---

    pub(super) fn create_r53r_query_log_association(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let cfg_id = props
            .get("ResolverQueryLogConfigId")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "ResolverQueryLogConfigId is required".to_string())?
            .to_string();
        let resource_id = props
            .get("ResourceId")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "ResourceId is required".to_string())?
            .to_string();
        let id = format!("rslvr-qlcassoc-{}", hex17());
        let assoc = ResolverQueryLogConfigAssociation {
            id: id.clone(),
            resolver_query_log_config_id: cfg_id.clone(),
            resource_id,
            status: "ACTIVE".to_string(),
            error: None,
            error_message: None,
            creation_time: now_rfc3339(),
        };
        {
            let mut st = self.route53resolver_state.write();
            let acc = st.account_mut(&self.account_id);
            acc.query_log_associations.insert(id.clone(), assoc);
            if let Some(cfg) = acc.query_log_configs.get_mut(&cfg_id) {
                cfg.association_count += 1;
            }
        }
        Ok(ProvisionResult::new(id.clone()).with("Id", id))
    }

    pub(super) fn delete_r53r_query_log_association(
        &self,
        physical_id: &str,
    ) -> Result<(), String> {
        let mut st = self.route53resolver_state.write();
        if let Some(acc) = st.accounts.get_mut(&self.account_id) {
            acc.query_log_associations.remove(physical_id);
        }
        Ok(())
    }

    // --- AWS::Route53Resolver::FirewallDomainList ---

    pub(super) fn create_r53r_firewall_domain_list(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("Name")
            .and_then(|v| v.as_str())
            .unwrap_or("domain-list")
            .to_string();
        let domains: Vec<String> = props
            .get("Domains")
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        let id = format!("rslvr-fdl-{}", hex17());
        let arn = r53r_arn(&self.region, &self.account_id, "firewall-domain-list", &id);
        let list = FirewallDomainList {
            id: id.clone(),
            arn: arn.clone(),
            name,
            domain_count: domains.len() as i64,
            status: "COMPLETE".to_string(),
            status_message: "Created Firewall Domain List".to_string(),
            creator_request_id: id.clone(),
            creation_time: now_rfc3339(),
            modification_time: now_rfc3339(),
        };
        let tags = self.r53r_tags(props);
        {
            let mut st = self.route53resolver_state.write();
            let acc = st.account_mut(&self.account_id);
            acc.firewall_domain_lists.insert(id.clone(), list);
            acc.firewall_domains.insert(id.clone(), domains);
            if !tags.is_empty() {
                acc.tags.insert(arn.clone(), tags);
            }
        }
        Ok(ProvisionResult::new(id.clone())
            .with("Arn", arn)
            .with("Id", id))
    }

    pub(super) fn delete_r53r_firewall_domain_list(&self, physical_id: &str) -> Result<(), String> {
        let mut st = self.route53resolver_state.write();
        if let Some(acc) = st.accounts.get_mut(&self.account_id) {
            acc.firewall_domain_lists.remove(physical_id);
            acc.firewall_domains.remove(physical_id);
        }
        Ok(())
    }

    // --- AWS::Route53Resolver::FirewallRuleGroup ---

    pub(super) fn create_r53r_firewall_rule_group(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("Name")
            .and_then(|v| v.as_str())
            .unwrap_or("rule-group")
            .to_string();
        let id = format!("rslvr-frg-{}", hex17());
        let arn = r53r_arn(&self.region, &self.account_id, "firewall-rule-group", &id);
        // Inline FirewallRules on the rule group resource.
        let mut rules = Vec::new();
        for r in props
            .get("FirewallRules")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default()
        {
            rules.push(FirewallRule {
                firewall_rule_group_id: id.clone(),
                firewall_domain_list_id: r
                    .get("FirewallDomainListId")
                    .and_then(|v| v.as_str())
                    .map(String::from),
                name: r
                    .get("Name")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string(),
                priority: r.get("Priority").and_then(|v| v.as_i64()).unwrap_or(100),
                action: r
                    .get("Action")
                    .and_then(|v| v.as_str())
                    .unwrap_or("ALLOW")
                    .to_string(),
                block_response: r
                    .get("BlockResponse")
                    .and_then(|v| v.as_str())
                    .map(String::from),
                block_override_domain: r
                    .get("BlockOverrideDomain")
                    .and_then(|v| v.as_str())
                    .map(String::from),
                block_override_dns_type: r
                    .get("BlockOverrideDnsType")
                    .and_then(|v| v.as_str())
                    .map(String::from),
                block_override_ttl: r.get("BlockOverrideTtl").and_then(|v| v.as_i64()),
                creator_request_id: id.clone(),
                creation_time: now_rfc3339(),
                modification_time: now_rfc3339(),
                firewall_domain_redirection_action: None,
                qtype: r.get("Qtype").and_then(|v| v.as_str()).map(String::from),
            });
        }
        let rule_count = rules.len() as i64;
        let group = FirewallRuleGroup {
            id: id.clone(),
            arn: arn.clone(),
            name,
            rule_count,
            status: "COMPLETE".to_string(),
            status_message: "Created Firewall Rule Group".to_string(),
            owner_id: self.account_id.clone(),
            creator_request_id: id.clone(),
            share_status: "NOT_SHARED".to_string(),
            creation_time: now_rfc3339(),
            modification_time: now_rfc3339(),
        };
        let tags = self.r53r_tags(props);
        {
            let mut st = self.route53resolver_state.write();
            let acc = st.account_mut(&self.account_id);
            acc.firewall_rule_groups.insert(id.clone(), group);
            acc.firewall_rules.insert(id.clone(), rules);
            if !tags.is_empty() {
                acc.tags.insert(arn.clone(), tags);
            }
        }
        Ok(ProvisionResult::new(id.clone())
            .with("Arn", arn)
            .with("Id", id)
            .with("RuleCount", rule_count.to_string()))
    }

    pub(super) fn delete_r53r_firewall_rule_group(&self, physical_id: &str) -> Result<(), String> {
        let mut st = self.route53resolver_state.write();
        if let Some(acc) = st.accounts.get_mut(&self.account_id) {
            acc.firewall_rule_groups.remove(physical_id);
            acc.firewall_rules.remove(physical_id);
        }
        Ok(())
    }

    // --- AWS::Route53Resolver::FirewallRuleGroupAssociation ---

    pub(super) fn create_r53r_firewall_rule_group_association(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let group_id = props
            .get("FirewallRuleGroupId")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "FirewallRuleGroupId is required".to_string())?
            .to_string();
        let vpc_id = props
            .get("VpcId")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "VpcId is required".to_string())?
            .to_string();
        let id = format!("rslvr-frgassoc-{}", hex17());
        let arn = r53r_arn(
            &self.region,
            &self.account_id,
            "firewall-rule-group-association",
            &id,
        );
        let assoc = FirewallRuleGroupAssociation {
            id: id.clone(),
            arn: arn.clone(),
            firewall_rule_group_id: group_id,
            vpc_id,
            name: props
                .get("Name")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string(),
            priority: props
                .get("Priority")
                .and_then(|v| v.as_i64())
                .unwrap_or(100),
            mutation_protection: props
                .get("MutationProtection")
                .and_then(|v| v.as_str())
                .unwrap_or("DISABLED")
                .to_string(),
            status: "COMPLETE".to_string(),
            status_message: String::new(),
            creator_request_id: id.clone(),
            creation_time: now_rfc3339(),
            modification_time: now_rfc3339(),
        };
        let tags = self.r53r_tags(props);
        {
            let mut st = self.route53resolver_state.write();
            let acc = st.account_mut(&self.account_id);
            acc.firewall_rule_group_associations
                .insert(id.clone(), assoc);
            if !tags.is_empty() {
                acc.tags.insert(arn.clone(), tags);
            }
        }
        Ok(ProvisionResult::new(id.clone())
            .with("Arn", arn)
            .with("Id", id))
    }

    pub(super) fn delete_r53r_firewall_rule_group_association(
        &self,
        physical_id: &str,
    ) -> Result<(), String> {
        let mut st = self.route53resolver_state.write();
        if let Some(acc) = st.accounts.get_mut(&self.account_id) {
            acc.firewall_rule_group_associations.remove(physical_id);
        }
        Ok(())
    }

    // --- AWS::Route53Resolver::FirewallConfig ---

    pub(super) fn create_r53r_firewall_config(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let resource_id = props
            .get("ResourceId")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "ResourceId is required".to_string())?
            .to_string();
        let id = format!("rslvr-fc-{}", hex17());
        let cfg = FirewallConfig {
            id: id.clone(),
            resource_id: resource_id.clone(),
            owner_id: self.account_id.clone(),
            firewall_fail_open: props
                .get("FirewallFailOpen")
                .and_then(|v| v.as_str())
                .unwrap_or("DISABLED")
                .to_string(),
        };
        self.route53resolver_state
            .write()
            .account_mut(&self.account_id)
            .firewall_configs
            .insert(resource_id, cfg);
        Ok(ProvisionResult::new(id.clone()).with("Id", id))
    }

    pub(super) fn delete_r53r_firewall_config(&self, _physical_id: &str) -> Result<(), String> {
        Ok(())
    }

    // --- AWS::Route53Resolver::ResolverConfig ---

    pub(super) fn create_r53r_resolver_config(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let resource_id = props
            .get("ResourceId")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "ResourceId is required".to_string())?
            .to_string();
        let flag = props
            .get("AutodefinedReverseFlag")
            .and_then(|v| v.as_str())
            .unwrap_or("ENABLE");
        let id = format!("rslvr-rc-{}", hex17());
        let cfg = ResolverConfig {
            id: id.clone(),
            resource_id: resource_id.clone(),
            owner_id: self.account_id.clone(),
            autodefined_reverse: if flag == "DISABLE" {
                "DISABLED"
            } else {
                "ENABLED"
            }
            .to_string(),
        };
        self.route53resolver_state
            .write()
            .account_mut(&self.account_id)
            .resolver_configs
            .insert(resource_id, cfg);
        Ok(ProvisionResult::new(id.clone()).with("Id", id))
    }

    pub(super) fn delete_r53r_resolver_config(&self, _physical_id: &str) -> Result<(), String> {
        Ok(())
    }

    // --- AWS::Route53Resolver::ResolverDNSSECConfig ---

    pub(super) fn create_r53r_dnssec_config(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let resource_id = props
            .get("ResourceId")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "ResourceId is required".to_string())?
            .to_string();
        let id = format!("rslvr-ds-{}", hex17());
        let cfg = ResolverDnssecConfig {
            id: id.clone(),
            owner_id: self.account_id.clone(),
            resource_id,
            validation_status: "ENABLED".to_string(),
        };
        self.route53resolver_state
            .write()
            .account_mut(&self.account_id)
            .dnssec_configs
            .insert(id.clone(), cfg);
        Ok(ProvisionResult::new(id.clone()).with("Id", id))
    }

    pub(super) fn delete_r53r_dnssec_config(&self, physical_id: &str) -> Result<(), String> {
        let mut st = self.route53resolver_state.write();
        if let Some(acc) = st.accounts.get_mut(&self.account_id) {
            acc.dnssec_configs.remove(physical_id);
        }
        Ok(())
    }
}
