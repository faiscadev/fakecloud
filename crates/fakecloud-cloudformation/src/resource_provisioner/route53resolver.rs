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

use fakecloud_route53resolver::validate::{arn as r53r_arn, hex17, now_rfc3339, synth_vpc};
use fakecloud_route53resolver::{
    EndpointRecord, FirewallConfig, FirewallDomainList, FirewallRule, FirewallRuleGroup,
    FirewallRuleGroupAssociation, IpAddressResponse, ResolverConfig, ResolverDnssecConfig,
    ResolverEndpoint, ResolverQueryLogConfig, ResolverQueryLogConfigAssociation, ResolverRule,
    ResolverRuleAssociation, Tag, TargetAddress,
};

impl ResourceProvisioner {
    /// Live-state `Fn::GetAtt` fallback for `AWS::Route53Resolver::*`. Captured
    /// create-time attributes are the primary source (see `get_att`); this
    /// resolves attributes that change after creation (e.g. an endpoint's
    /// `IpAddressCount` after an IP is associated in a stack update) by reading
    /// the current route53resolver state by physical id.
    pub(super) fn get_att_route53resolver(
        &self,
        resource: &StackResource,
        attribute: &str,
    ) -> Option<String> {
        let st = self.route53resolver_state.read();
        let acc = st.accounts.get(&self.account_id)?;
        let pid = resource.physical_id.as_str();
        match resource.resource_type.as_str() {
            "AWS::Route53Resolver::ResolverEndpoint" => {
                let e = &acc.endpoints.get(pid)?.endpoint;
                match attribute {
                    "Arn" => Some(e.arn.clone()),
                    "ResolverEndpointId" => Some(e.id.clone()),
                    "IpAddressCount" => Some(e.ip_address_count.to_string()),
                    "Direction" => Some(e.direction.clone()),
                    "HostVPCId" => Some(e.host_vpc_id.clone()),
                    "Name" => e.name.clone(),
                    _ => None,
                }
            }
            "AWS::Route53Resolver::ResolverRule" => {
                let r = acc.rules.get(pid)?;
                match attribute {
                    "Arn" => Some(r.arn.clone()),
                    "ResolverRuleId" => Some(r.id.clone()),
                    "Name" => r.name.clone(),
                    "DomainName" => r.domain_name.clone(),
                    "ResolverEndpointId" => r.resolver_endpoint_id.clone(),
                    "TargetIps" => serde_json::to_string(&r.target_ips).ok(),
                    _ => None,
                }
            }
            "AWS::Route53Resolver::ResolverRuleAssociation" => {
                let a = acc.rule_associations.get(pid)?;
                match attribute {
                    "ResolverRuleAssociationId" => Some(a.id.clone()),
                    "Name" => a.name.clone(),
                    _ => None,
                }
            }
            "AWS::Route53Resolver::ResolverQueryLoggingConfig" => {
                let c = acc.query_log_configs.get(pid)?;
                match attribute {
                    "Arn" => Some(c.arn.clone()),
                    "Id" => Some(c.id.clone()),
                    _ => None,
                }
            }
            "AWS::Route53Resolver::ResolverQueryLoggingConfigAssociation" => {
                let a = acc.query_log_associations.get(pid)?;
                match attribute {
                    "Id" => Some(a.id.clone()),
                    _ => None,
                }
            }
            "AWS::Route53Resolver::FirewallDomainList" => {
                let l = acc.firewall_domain_lists.get(pid)?;
                match attribute {
                    "Arn" => Some(l.arn.clone()),
                    "Id" => Some(l.id.clone()),
                    _ => None,
                }
            }
            "AWS::Route53Resolver::FirewallRuleGroup" => {
                let g = acc.firewall_rule_groups.get(pid)?;
                match attribute {
                    "Arn" => Some(g.arn.clone()),
                    "Id" => Some(g.id.clone()),
                    "RuleCount" => Some(g.rule_count.to_string()),
                    _ => None,
                }
            }
            "AWS::Route53Resolver::FirewallRuleGroupAssociation" => {
                let a = acc.firewall_rule_group_associations.get(pid)?;
                match attribute {
                    "Arn" => Some(a.arn.clone()),
                    "Id" => Some(a.id.clone()),
                    _ => None,
                }
            }
            "AWS::Route53Resolver::FirewallConfig" => {
                let c = acc.firewall_configs.values().find(|c| c.id == pid)?;
                match attribute {
                    "Id" => Some(c.id.clone()),
                    "OwnerId" => Some(c.owner_id.clone()),
                    "ResourceId" => Some(c.resource_id.clone()),
                    _ => None,
                }
            }
            "AWS::Route53Resolver::ResolverConfig" => {
                let c = acc.resolver_configs.values().find(|c| c.id == pid)?;
                match attribute {
                    "Id" => Some(c.id.clone()),
                    "OwnerId" => Some(c.owner_id.clone()),
                    "ResourceId" => Some(c.resource_id.clone()),
                    _ => None,
                }
            }
            "AWS::Route53Resolver::ResolverDNSSECConfig" => {
                let c = acc.dnssec_configs.get(pid)?;
                match attribute {
                    "Id" => Some(c.id.clone()),
                    "OwnerId" => Some(c.owner_id.clone()),
                    "ResourceId" => Some(c.resource_id.clone()),
                    _ => None,
                }
            }
            _ => None,
        }
    }

    /// Resolve a subnet's VPC id from EC2 state (same lookup the service's
    /// `resolve_subnet_vpc` uses), or `None` when the subnet is not present.
    fn r53r_subnet_vpc(&self, subnet_id: &str) -> Option<String> {
        let guard = self.ec2_state.read();
        Some(
            guard
                .get(&self.account_id)?
                .subnets
                .get(subnet_id)?
                .vpc_id
                .clone(),
        )
    }

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
            // Resolve the endpoint's HostVPCId from the subnet's real VPC in EC2
            // state (the same source the direct-API path uses), so a
            // CFN-provisioned endpoint carries the real VPC id — not an empty
            // string — and `Fn::GetAtt HostVPCId` resolves correctly. When the
            // subnet is not present in EC2 state, synthesize a stable VPC id
            // from the subnet just like the direct-API path, rather than leaving
            // HostVPCId empty.
            if host_vpc.is_empty() && !subnet_id.is_empty() {
                host_vpc = self
                    .r53r_subnet_vpc(&subnet_id)
                    .unwrap_or_else(|| synth_vpc(&subnet_id));
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
            outpost_arn: props
                .get("OutpostArn")
                .and_then(|v| v.as_str())
                .map(String::from),
            preferred_instance_type: props
                .get("PreferredInstanceType")
                .and_then(|v| v.as_str())
                .map(String::from),
            dns64_enabled: props.get("Dns64Enabled").and_then(|v| v.as_bool()),
            ipv6_internet_access_enabled: props
                .get("Ipv6InternetAccessEnabled")
                .and_then(|v| v.as_bool()),
            rni_enhanced_metrics_enabled: props
                .get("RniEnhancedMetricsEnabled")
                .and_then(|v| v.as_bool()),
            target_name_server_metrics_enabled: props
                .get("TargetNameServerMetricsEnabled")
                .and_then(|v| v.as_bool()),
        };
        let ip_count = endpoint.ip_address_count;
        let direction_att = endpoint.direction.clone();
        let name_att = endpoint.name.clone().unwrap_or_default();
        let host_vpc_att = endpoint.host_vpc_id.clone();
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
            .with("IpAddressCount", ip_count.to_string())
            .with("Direction", direction_att)
            .with("HostVPCId", host_vpc_att)
            .with("Name", name_att))
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
        let name = props.get("Name").and_then(|v| v.as_str()).map(String::from);
        let resolver_endpoint_id = props
            .get("ResolverEndpointId")
            .and_then(|v| v.as_str())
            .map(String::from);
        let target_ips_att =
            serde_json::to_string(&target_ips).unwrap_or_else(|_| "[]".to_string());
        let rule = ResolverRule {
            id: id.clone(),
            creator_request_id: id.clone(),
            arn: arn.clone(),
            domain_name: domain_name.clone(),
            status: "COMPLETE".to_string(),
            status_message: "[Trace id: 1] Successfully created Resolver Rule".to_string(),
            rule_type,
            name: name.clone(),
            target_ips,
            resolver_endpoint_id: resolver_endpoint_id.clone(),
            owner_id: self.account_id.clone(),
            share_status: "NOT_SHARED".to_string(),
            creation_time: now_rfc3339(),
            modification_time: now_rfc3339(),
            delegation_record: props
                .get("DelegationRecord")
                .and_then(|v| v.as_str())
                .map(String::from),
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
            .with("ResolverRuleId", id)
            .with("Name", name.unwrap_or_default())
            .with("DomainName", domain_name.unwrap_or_default())
            .with("TargetIps", target_ips_att);
        if let Some(ep) = resolver_endpoint_id {
            out = out.with("ResolverEndpointId", ep);
        }
        Ok(out)
    }

    /// In-place `UpdateResolverRule`: reprovision would churn the random
    /// rslvr-rr- id, dangling every `AWS::Route53Resolver::ResolverRuleAssociation`
    /// that stored it. Name/TargetIps/ResolverEndpointId are the mutable
    /// properties; RuleType/DomainName are immutable. Preserve id/arn/creation.
    pub(super) fn update_r53r_resolver_rule(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let id = existing.physical_id.clone();
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
        let name = props.get("Name").and_then(|v| v.as_str()).map(String::from);
        let resolver_endpoint_id = props
            .get("ResolverEndpointId")
            .and_then(|v| v.as_str())
            .map(String::from);
        let target_ips_att =
            serde_json::to_string(&target_ips).unwrap_or_else(|_| "[]".to_string());

        let tags = self.r53r_tags(props);
        let (arn, domain_name) = {
            let mut st = self.route53resolver_state.write();
            let acc = st.account_mut(&self.account_id);
            let rule = acc
                .rules
                .get_mut(&id)
                .ok_or_else(|| format!("Resolver rule {id} not yet provisioned"))?;
            rule.name = name.clone();
            rule.target_ips = target_ips;
            rule.resolver_endpoint_id = resolver_endpoint_id.clone();
            rule.modification_time = now_rfc3339();
            let arn = rule.arn.clone();
            let domain_name = rule.domain_name.clone();
            if tags.is_empty() {
                acc.tags.remove(&arn);
            } else {
                acc.tags.insert(arn.clone(), tags);
            }
            (arn, domain_name)
        };

        let mut out = ProvisionResult::new(id.clone())
            .with("Arn", arn)
            .with("ResolverRuleId", id)
            .with("Name", name.unwrap_or_default())
            .with("DomainName", domain_name.unwrap_or_default())
            .with("TargetIps", target_ips_att);
        if let Some(ep) = resolver_endpoint_id {
            out = out.with("ResolverEndpointId", ep);
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
        let name = props.get("Name").and_then(|v| v.as_str()).map(String::from);
        let assoc = ResolverRuleAssociation {
            id: id.clone(),
            resolver_rule_id,
            name: name.clone(),
            vpc_id,
            status: "COMPLETE".to_string(),
            status_message: String::new(),
        };
        self.route53resolver_state
            .write()
            .account_mut(&self.account_id)
            .rule_associations
            .insert(id.clone(), assoc);
        Ok(ProvisionResult::new(id.clone())
            .with("ResolverRuleAssociationId", id)
            .with("Name", name.unwrap_or_default()))
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

    /// In-place update: `ResolverQueryLoggingConfig` has no mutable property
    /// other than tags, but reprovision would churn the random rslvr-qlc- id
    /// (dangling its associations) and reset `association_count`. Preserve the id.
    pub(super) fn update_r53r_query_log_config(
        &self,
        existing: &StackResource,
        _resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let id = existing.physical_id.clone();
        let arn = {
            let st = self.route53resolver_state.read();
            st.accounts
                .get(&self.account_id)
                .and_then(|a| a.query_log_configs.get(&id))
                .map(|c| c.arn.clone())
                .ok_or_else(|| format!("Query log config {id} not yet provisioned"))?
        };
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

    /// In-place update: reprovision would churn the random rslvr-fdl- id,
    /// dangling every FirewallRule that references this domain list. Name/Domains
    /// are the mutable properties (UpdateFirewallDomains); preserve id/arn.
    pub(super) fn update_r53r_firewall_domain_list(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let id = existing.physical_id.clone();
        let name = props.get("Name").and_then(|v| v.as_str());
        let domains: Vec<String> = props
            .get("Domains")
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        let tags = self.r53r_tags(props);

        let mut st = self.route53resolver_state.write();
        let acc = st.account_mut(&self.account_id);
        let arn = {
            let list = acc
                .firewall_domain_lists
                .get_mut(&id)
                .ok_or_else(|| format!("Firewall domain list {id} not yet provisioned"))?;
            if let Some(n) = name {
                list.name = n.to_string();
            }
            list.domain_count = domains.len() as i64;
            list.modification_time = now_rfc3339();
            list.arn.clone()
        };
        acc.firewall_domains.insert(id.clone(), domains);
        if tags.is_empty() {
            acc.tags.remove(&arn);
        } else {
            acc.tags.insert(arn.clone(), tags);
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
                dns_threat_protection: r
                    .get("DnsThreatProtection")
                    .and_then(|v| v.as_str())
                    .map(String::from),
                confidence_threshold: r
                    .get("ConfidenceThreshold")
                    .and_then(|v| v.as_str())
                    .map(String::from),
                firewall_rule_type: r.get("FirewallRuleType").filter(|v| !v.is_null()).cloned(),
                firewall_threat_protection_id: None,
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

    /// In-place update: reprovision would churn the random rslvr-frg- id
    /// (dangling every FirewallRuleGroupAssociation) and drop the group's inline
    /// rules. Rebuild the rules from the template + update Name in place, and
    /// preserve the id/arn.
    pub(super) fn update_r53r_firewall_rule_group(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let id = existing.physical_id.clone();
        let name = props.get("Name").and_then(|v| v.as_str());
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
                dns_threat_protection: r
                    .get("DnsThreatProtection")
                    .and_then(|v| v.as_str())
                    .map(String::from),
                confidence_threshold: r
                    .get("ConfidenceThreshold")
                    .and_then(|v| v.as_str())
                    .map(String::from),
                firewall_rule_type: r.get("FirewallRuleType").filter(|v| !v.is_null()).cloned(),
                firewall_threat_protection_id: None,
            });
        }
        let rule_count = rules.len() as i64;
        let tags = self.r53r_tags(props);

        let mut st = self.route53resolver_state.write();
        let acc = st.account_mut(&self.account_id);
        let arn = {
            let group = acc
                .firewall_rule_groups
                .get_mut(&id)
                .ok_or_else(|| format!("Firewall rule group {id} not yet provisioned"))?;
            if let Some(n) = name {
                group.name = n.to_string();
            }
            group.rule_count = rule_count;
            group.modification_time = now_rfc3339();
            group.arn.clone()
        };
        acc.firewall_rules.insert(id.clone(), rules);
        if tags.is_empty() {
            acc.tags.remove(&arn);
        } else {
            acc.tags.insert(arn.clone(), tags);
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
            .insert(resource_id.clone(), cfg);
        Ok(ProvisionResult::new(id.clone())
            .with("Id", id)
            .with("OwnerId", self.account_id.clone())
            .with("ResourceId", resource_id))
    }

    pub(super) fn delete_r53r_firewall_config(&self, physical_id: &str) -> Result<(), String> {
        // The firewall-config map is keyed by VPC (resource) id, while the CFN
        // physical id is the config's own `rslvr-fc-*` id; drop the singleton
        // whose id matches so a stack teardown does not leave it behind.
        let mut st = self.route53resolver_state.write();
        if let Some(acc) = st.accounts.get_mut(&self.account_id) {
            let key = acc
                .firewall_configs
                .iter()
                .find(|(_, c)| c.id == physical_id)
                .map(|(k, _)| k.clone());
            if let Some(key) = key {
                acc.firewall_configs.remove(&key);
            }
        }
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
            .insert(resource_id.clone(), cfg);
        Ok(ProvisionResult::new(id.clone())
            .with("Id", id)
            .with("OwnerId", self.account_id.clone())
            .with("ResourceId", resource_id))
    }

    pub(super) fn delete_r53r_resolver_config(&self, physical_id: &str) -> Result<(), String> {
        // Keyed by VPC (resource) id; the physical id is the `rslvr-rc-*` config
        // id. Remove the singleton whose id matches on stack teardown.
        let mut st = self.route53resolver_state.write();
        if let Some(acc) = st.accounts.get_mut(&self.account_id) {
            let key = acc
                .resolver_configs
                .iter()
                .find(|(_, c)| c.id == physical_id)
                .map(|(k, _)| k.clone());
            if let Some(key) = key {
                acc.resolver_configs.remove(&key);
            }
        }
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
            resource_id: resource_id.clone(),
            validation_status: "ENABLED".to_string(),
        };
        self.route53resolver_state
            .write()
            .account_mut(&self.account_id)
            .dnssec_configs
            .insert(id.clone(), cfg);
        Ok(ProvisionResult::new(id.clone())
            .with("Id", id)
            .with("OwnerId", self.account_id.clone())
            .with("ResourceId", resource_id))
    }

    pub(super) fn delete_r53r_dnssec_config(&self, physical_id: &str) -> Result<(), String> {
        let mut st = self.route53resolver_state.write();
        if let Some(acc) = st.accounts.get_mut(&self.account_id) {
            acc.dnssec_configs.remove(physical_id);
        }
        Ok(())
    }
}
