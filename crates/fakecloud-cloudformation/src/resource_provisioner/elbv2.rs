//! `AWS::ELBV2::*` CloudFormation provisioning (extracted from the provisioner's core module).

#![allow(clippy::too_many_lines)]

use super::*;

impl ResourceProvisioner {
    pub(crate) fn create_elbv2_load_balancer(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("Name")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id)
            .to_string();
        let scheme = props
            .get("Scheme")
            .and_then(|v| v.as_str())
            .unwrap_or("internet-facing")
            .to_string();
        let lb_type = props
            .get("Type")
            .and_then(|v| v.as_str())
            .unwrap_or("application")
            .to_string();
        let ip_address_type = props
            .get("IpAddressType")
            .and_then(|v| v.as_str())
            .unwrap_or("ipv4")
            .to_string();
        let security_groups: Vec<String> = props
            .get("SecurityGroups")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|s| s.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();
        let tags = parse_elb_tags(props.get("Tags"));

        let mut accounts = self.elbv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let lb_id = Uuid::new_v4().simple().to_string();
        let arn = format!(
            "arn:aws:elasticloadbalancing:{}:{}:loadbalancer/{}/{}/{}",
            self.region,
            self.account_id,
            if lb_type == "network" { "net" } else { "app" },
            name,
            &lb_id[..16]
        );
        let dns_name = format!(
            "{}-{}.{}.elb.{}.amazonaws.com",
            name,
            &lb_id[..16],
            self.region,
            self.region
        );

        let mut availability_zones: Vec<fakecloud_elbv2::AvailabilityZone> = Vec::new();
        if let Some(arr) = props.get("Subnets").and_then(|v| v.as_array()) {
            for s in arr {
                if let Some(subnet_id) = s.as_str() {
                    availability_zones.push(fakecloud_elbv2::AvailabilityZone {
                        zone_name: format!("{}a", self.region),
                        subnet_id: subnet_id.to_string(),
                        outpost_id: None,
                        load_balancer_addresses: Vec::new(),
                        source_nat_ipv6_prefixes: Vec::new(),
                    });
                }
            }
        }

        state.load_balancers.insert(
            arn.clone(),
            LoadBalancer {
                arn: arn.clone(),
                name: name.clone(),
                dns_name: dns_name.clone(),
                canonical_hosted_zone_id: "Z2P70J7EXAMPLE".to_string(),
                created_time: Utc::now(),
                scheme,
                vpc_id: String::new(),
                state_code: "active".to_string(),
                state_reason: None,
                lb_type,
                availability_zones,
                security_groups,
                ip_address_type,
                customer_owned_ipv4_pool: None,
                enforce_security_group_inbound_rules_on_private_link_traffic: None,
                enable_prefix_for_ipv6_source_nat: None,
                ipv4_ipam_pool_id: None,
                tags,
                attributes: BTreeMap::new(),
                minimum_capacity_units: None,
                bound_port: None,
            },
        );

        Ok(ProvisionResult::new(arn.clone())
            .with("LoadBalancerArn", arn)
            .with(
                "LoadBalancerFullName",
                format!("app/{name}/{}", &lb_id[..16]),
            )
            .with("LoadBalancerName", name)
            .with("DNSName", dns_name)
            .with("CanonicalHostedZoneID", "Z2P70J7EXAMPLE"))
    }

    pub(crate) fn delete_elbv2_load_balancer(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.elbv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.load_balancers.remove(physical_id);
        // Cascade-delete listeners and rules attached to this LB.
        let listeners: Vec<String> = state
            .listeners
            .iter()
            .filter(|(_, l)| l.load_balancer_arn == physical_id)
            .map(|(arn, _)| arn.clone())
            .collect();
        for arn in &listeners {
            state.listeners.remove(arn);
            let rules: Vec<String> = state
                .rules
                .iter()
                .filter(|(_, r)| r.listener_arn == *arn)
                .map(|(a, _)| a.clone())
                .collect();
            for r in rules {
                state.rules.remove(&r);
            }
        }
        for tg in state.target_groups.values_mut() {
            tg.load_balancer_arns.retain(|a| a != physical_id);
        }
        Ok(())
    }

    pub(crate) fn create_elbv2_target_group(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("Name")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id)
            .to_string();
        let protocol = props
            .get("Protocol")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let port = props.get("Port").and_then(|v| v.as_i64()).map(|n| n as i32);
        let vpc_id = props
            .get("VpcId")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let target_type = props
            .get("TargetType")
            .and_then(|v| v.as_str())
            .unwrap_or("instance")
            .to_string();
        let ip_address_type = props
            .get("IpAddressType")
            .and_then(|v| v.as_str())
            .unwrap_or("ipv4")
            .to_string();
        let protocol_version = props
            .get("ProtocolVersion")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let tags = parse_elb_tags(props.get("Tags"));

        let mut accounts = self.elbv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let id = Uuid::new_v4().simple().to_string();
        let arn = format!(
            "arn:aws:elasticloadbalancing:{}:{}:targetgroup/{}/{}",
            self.region,
            self.account_id,
            name,
            &id[..16]
        );

        state.target_groups.insert(
            arn.clone(),
            TargetGroup {
                arn: arn.clone(),
                name: name.clone(),
                protocol,
                port,
                vpc_id,
                target_type,
                ip_address_type,
                protocol_version,
                health_check_protocol: props
                    .get("HealthCheckProtocol")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string()),
                health_check_port: props
                    .get("HealthCheckPort")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string()),
                health_check_enabled: props
                    .get("HealthCheckEnabled")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(true),
                health_check_path: props
                    .get("HealthCheckPath")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string()),
                health_check_interval_seconds: props
                    .get("HealthCheckIntervalSeconds")
                    .and_then(|v| v.as_i64())
                    .unwrap_or(30) as i32,
                health_check_timeout_seconds: props
                    .get("HealthCheckTimeoutSeconds")
                    .and_then(|v| v.as_i64())
                    .unwrap_or(5) as i32,
                healthy_threshold_count: props
                    .get("HealthyThresholdCount")
                    .and_then(|v| v.as_i64())
                    .unwrap_or(5) as i32,
                unhealthy_threshold_count: props
                    .get("UnhealthyThresholdCount")
                    .and_then(|v| v.as_i64())
                    .unwrap_or(2) as i32,
                matcher_http_code: props
                    .get("Matcher")
                    .and_then(|v| v.get("HttpCode"))
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string()),
                matcher_grpc_code: props
                    .get("Matcher")
                    .and_then(|v| v.get("GrpcCode"))
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string()),
                load_balancer_arns: Vec::new(),
                targets: Vec::new(),
                tags,
                attributes: BTreeMap::new(),
                created_time: Utc::now(),
            },
        );

        Ok(ProvisionResult::new(arn.clone())
            .with("TargetGroupArn", arn)
            .with("TargetGroupName", name)
            .with("TargetGroupFullName", format!("targetgroup/{}", &id[..16])))
    }

    pub(crate) fn delete_elbv2_target_group(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.elbv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.target_groups.remove(physical_id);
        Ok(())
    }

    pub(crate) fn create_elbv2_listener(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let load_balancer_arn = props
            .get("LoadBalancerArn")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "LoadBalancerArn is required".to_string())?
            .to_string();
        let port = props.get("Port").and_then(|v| v.as_i64()).map(|n| n as i32);
        let protocol = props
            .get("Protocol")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let default_actions = parse_elb_actions(props.get("DefaultActions"));

        let mut accounts = self.elbv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if !state.load_balancers.contains_key(&load_balancer_arn) {
            return Err(format!(
                "LoadBalancer {load_balancer_arn} not yet provisioned"
            ));
        }

        let lb_full = load_balancer_arn
            .rsplit("loadbalancer/")
            .next()
            .unwrap_or("")
            .to_string();
        let listener_id = Uuid::new_v4().simple().to_string();
        let arn = format!(
            "arn:aws:elasticloadbalancing:{}:{}:listener/{}/{}",
            self.region,
            self.account_id,
            lb_full,
            &listener_id[..16]
        );

        // Wire forward target groups -> LB association so dataplane probing
        // and DescribeTargetGroups round-trip the relationship.
        for action in &default_actions {
            if let Some(tg_arn) = &action.target_group_arn {
                if let Some(tg) = state.target_groups.get_mut(tg_arn) {
                    if !tg.load_balancer_arns.contains(&load_balancer_arn) {
                        tg.load_balancer_arns.push(load_balancer_arn.clone());
                    }
                }
            }
            if let Some(forward) = &action.forward {
                for tgt in &forward.target_groups {
                    if let Some(tg) = state.target_groups.get_mut(&tgt.target_group_arn) {
                        if !tg.load_balancer_arns.contains(&load_balancer_arn) {
                            tg.load_balancer_arns.push(load_balancer_arn.clone());
                        }
                    }
                }
            }
        }

        state.listeners.insert(
            arn.clone(),
            Listener {
                arn: arn.clone(),
                load_balancer_arn,
                port,
                protocol,
                certificates: Vec::new(),
                ssl_policy: props
                    .get("SslPolicy")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string()),
                default_actions,
                alpn_policy: Vec::new(),
                mutual_authentication: None,
                tags: parse_elb_tags(props.get("Tags")),
                attributes: BTreeMap::new(),
            },
        );

        Ok(ProvisionResult::new(arn.clone()).with("ListenerArn", arn))
    }

    pub(crate) fn delete_elbv2_listener(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.elbv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.listeners.remove(physical_id);
        let rules: Vec<String> = state
            .rules
            .iter()
            .filter(|(_, r)| r.listener_arn == physical_id)
            .map(|(arn, _)| arn.clone())
            .collect();
        for r in rules {
            state.rules.remove(&r);
        }
        Ok(())
    }

    pub(crate) fn create_elbv2_listener_rule(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let listener_arn = props
            .get("ListenerArn")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "ListenerArn is required".to_string())?
            .to_string();
        let priority = props
            .get("Priority")
            .map(|v| {
                if let Some(s) = v.as_str() {
                    s.to_string()
                } else if let Some(n) = v.as_i64() {
                    n.to_string()
                } else {
                    "1".to_string()
                }
            })
            .unwrap_or_else(|| "1".to_string());
        let actions = parse_elb_actions(props.get("Actions"));
        let conditions = parse_elb_rule_conditions(props.get("Conditions"));

        let mut accounts = self.elbv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if !state.listeners.contains_key(&listener_arn) {
            return Err(format!("Listener {listener_arn} not yet provisioned"));
        }
        let listener_full = listener_arn
            .rsplit("listener/")
            .next()
            .unwrap_or("")
            .to_string();
        let rule_id = Uuid::new_v4().simple().to_string();
        let arn = format!(
            "arn:aws:elasticloadbalancing:{}:{}:listener-rule/{}/{}",
            self.region,
            self.account_id,
            listener_full,
            &rule_id[..16]
        );

        state.rules.insert(
            arn.clone(),
            ElbRule {
                arn: arn.clone(),
                listener_arn,
                priority,
                conditions,
                actions,
                is_default: false,
                tags: parse_elb_tags(props.get("Tags")),
            },
        );

        Ok(ProvisionResult::new(arn.clone()).with("RuleArn", arn))
    }

    pub(crate) fn delete_elbv2_listener_rule(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.elbv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.rules.remove(physical_id);
        Ok(())
    }

    /// Provision an `AWS::ElasticLoadBalancingV2::ListenerCertificate`.
    /// Appends each non-default certificate from `Certificates` to the
    /// target listener (the default listener cert is set on Listener
    /// creation, so this resource only manages SNI extras).
    /// Provision an `AWS::ElasticLoadBalancingV2::ListenerCertificate`.
    /// Appends each non-default certificate from `Certificates` to the
    /// target listener (the default listener cert is set on Listener
    /// creation, so this resource only manages SNI extras).
    pub(crate) fn create_elbv2_listener_certificate(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let listener_arn = props
            .get("ListenerArn")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "ListenerArn is required".to_string())?
            .to_string();
        let certs: Vec<String> = props
            .get("Certificates")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|c| c.get("CertificateArn").and_then(|v| v.as_str()))
                    .map(|s| s.to_string())
                    .collect()
            })
            .unwrap_or_default();
        if certs.is_empty() {
            return Err("Certificates must contain at least one CertificateArn".to_string());
        }
        let mut accounts = self.elbv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let listener = state
            .listeners
            .get_mut(&listener_arn)
            .ok_or_else(|| format!("Listener {listener_arn} does not exist"))?;
        for arn in &certs {
            listener.certificates.retain(|c| &c.certificate_arn != arn);
            listener.certificates.push(fakecloud_elbv2::Certificate {
                certificate_arn: arn.clone(),
                is_default: false,
            });
        }
        Ok(ProvisionResult::new(format!(
            "{}#{}",
            listener_arn,
            certs.join(",")
        )))
    }

    pub(crate) fn delete_elbv2_listener_certificate(
        &self,
        physical_id: &str,
    ) -> Result<(), String> {
        let (listener_arn, cert_list) = match physical_id.split_once('#') {
            Some(parts) => parts,
            None => return Ok(()),
        };
        let cert_arns: Vec<&str> = cert_list.split(',').collect();
        let mut accounts = self.elbv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if let Some(listener) = state.listeners.get_mut(listener_arn) {
            listener
                .certificates
                .retain(|c| !cert_arns.iter().any(|a| *a == c.certificate_arn));
        }
        Ok(())
    }

    /// Provision an `AWS::ElasticLoadBalancingV2::TrustStore`.
    /// Provision an `AWS::ElasticLoadBalancingV2::TrustStore`.
    pub(crate) fn create_elbv2_trust_store(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("Name")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id)
            .to_string();
        let bucket = props
            .get("CaCertificatesBundleS3Bucket")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "CaCertificatesBundleS3Bucket is required".to_string())?;
        let key = props
            .get("CaCertificatesBundleS3Key")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "CaCertificatesBundleS3Key is required".to_string())?;
        let tags: Vec<fakecloud_elbv2::Tag> = props
            .get("Tags")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|t| {
                        let k = t.get("Key").and_then(|v| v.as_str())?;
                        let val = t.get("Value").and_then(|v| v.as_str()).unwrap_or("");
                        Some(fakecloud_elbv2::Tag {
                            key: k.to_string(),
                            value: val.to_string(),
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();

        let mut accounts = self.elbv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if state.trust_stores.values().any(|t| t.name == name) {
            return Err(format!("Trust store {name} already exists"));
        }
        let suffix: String = Uuid::new_v4()
            .simple()
            .to_string()
            .chars()
            .take(16)
            .collect();
        let arn = format!(
            "arn:aws:elasticloadbalancing:{}:{}:truststore/{}/{}",
            self.region, self.account_id, name, suffix
        );
        let ts = fakecloud_elbv2::TrustStore {
            arn: arn.clone(),
            name: name.clone(),
            status: "ACTIVE".to_string(),
            number_of_ca_certificates: 1,
            total_revoked_entries: 0,
            created_time: Utc::now(),
            ca_certificates_bundle: Some(format!("s3://{bucket}/{key}").into_bytes()),
            revocations: BTreeMap::new(),
            next_revocation_id: 1,
            tags,
        };
        state.trust_stores.insert(arn.clone(), ts);
        Ok(ProvisionResult::new(arn.clone())
            .with("TrustStoreArn", arn)
            .with("Name", name)
            .with("Status", "ACTIVE".to_string()))
    }

    pub(crate) fn delete_elbv2_trust_store(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.elbv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.trust_stores.remove(physical_id);
        Ok(())
    }

    /// In-place update for AWS::ElasticLoadBalancingV2::LoadBalancer. Name,
    /// scheme, type and subnet topology are immutable in real AWS — CFN
    /// would replace the resource. We only mutate fields the SetSubnets /
    /// SetSecurityGroups / SetIpAddressType APIs would touch.
    /// In-place update for AWS::ElasticLoadBalancingV2::LoadBalancer. Name,
    /// scheme, type and subnet topology are immutable in real AWS — CFN
    /// would replace the resource. We only mutate fields the SetSubnets /
    /// SetSecurityGroups / SetIpAddressType APIs would touch.
    pub(crate) fn update_elbv2_load_balancer(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let arn = existing.physical_id.clone();
        let mut accounts = self.elbv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let lb = state
            .load_balancers
            .get_mut(&arn)
            .ok_or_else(|| format!("LoadBalancer {arn} no longer exists"))?;
        if let Some(arr) = props.get("SecurityGroups").and_then(|v| v.as_array()) {
            lb.security_groups = arr
                .iter()
                .filter_map(|s| s.as_str().map(|s| s.to_string()))
                .collect();
        }
        if let Some(s) = props.get("IpAddressType").and_then(|v| v.as_str()) {
            lb.ip_address_type = s.to_string();
        }
        if let Some(arr) = props.get("Subnets").and_then(|v| v.as_array()) {
            let mut zones: Vec<fakecloud_elbv2::AvailabilityZone> = Vec::new();
            for s in arr {
                if let Some(subnet_id) = s.as_str() {
                    zones.push(fakecloud_elbv2::AvailabilityZone {
                        zone_name: format!("{}a", self.region),
                        subnet_id: subnet_id.to_string(),
                        outpost_id: None,
                        load_balancer_addresses: Vec::new(),
                        source_nat_ipv6_prefixes: Vec::new(),
                    });
                }
            }
            lb.availability_zones = zones;
        }
        if props.get("Tags").is_some() {
            lb.tags = parse_elb_tags(props.get("Tags"));
        }
        let name = lb.name.clone();
        let dns_name = lb.dns_name.clone();
        let canonical = lb.canonical_hosted_zone_id.clone();
        let lb_full = arn.rsplit("loadbalancer/").next().unwrap_or("").to_string();
        Ok(ProvisionResult::new(arn.clone())
            .with("LoadBalancerArn", arn)
            .with("LoadBalancerFullName", lb_full)
            .with("LoadBalancerName", name)
            .with("DNSName", dns_name)
            .with("CanonicalHostedZoneID", canonical))
    }

    /// In-place update for AWS::ElasticLoadBalancingV2::TargetGroup. Mirrors
    /// ModifyTargetGroup: only health-check fields and matcher are mutable.
    /// In-place update for AWS::ElasticLoadBalancingV2::TargetGroup. Mirrors
    /// ModifyTargetGroup: only health-check fields and matcher are mutable.
    pub(crate) fn update_elbv2_target_group(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let arn = existing.physical_id.clone();
        let mut accounts = self.elbv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let tg = state
            .target_groups
            .get_mut(&arn)
            .ok_or_else(|| format!("TargetGroup {arn} no longer exists"))?;
        if let Some(s) = props.get("HealthCheckProtocol").and_then(|v| v.as_str()) {
            tg.health_check_protocol = Some(s.to_string());
        }
        if let Some(s) = props.get("HealthCheckPort").and_then(|v| v.as_str()) {
            tg.health_check_port = Some(s.to_string());
        }
        if let Some(b) = props.get("HealthCheckEnabled").and_then(|v| v.as_bool()) {
            tg.health_check_enabled = b;
        }
        if let Some(s) = props.get("HealthCheckPath").and_then(|v| v.as_str()) {
            tg.health_check_path = Some(s.to_string());
        }
        if let Some(n) = props.get("HealthCheckIntervalSeconds").and_then(cfn_as_i64) {
            tg.health_check_interval_seconds = n as i32;
        }
        if let Some(n) = props.get("HealthCheckTimeoutSeconds").and_then(cfn_as_i64) {
            tg.health_check_timeout_seconds = n as i32;
        }
        if let Some(n) = props.get("HealthyThresholdCount").and_then(cfn_as_i64) {
            tg.healthy_threshold_count = n as i32;
        }
        if let Some(n) = props.get("UnhealthyThresholdCount").and_then(cfn_as_i64) {
            tg.unhealthy_threshold_count = n as i32;
        }
        if let Some(matcher) = props.get("Matcher") {
            tg.matcher_http_code = matcher
                .get("HttpCode")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            tg.matcher_grpc_code = matcher
                .get("GrpcCode")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
        }
        if props.get("Tags").is_some() {
            tg.tags = parse_elb_tags(props.get("Tags"));
        }
        let name = tg.name.clone();
        let tg_full = arn
            .rsplit("targetgroup/")
            .next()
            .map(|s| format!("targetgroup/{s}"))
            .unwrap_or_default();
        Ok(ProvisionResult::new(arn.clone())
            .with("TargetGroupArn", arn)
            .with("TargetGroupName", name)
            .with("TargetGroupFullName", tg_full))
    }

    /// In-place update for AWS::ElasticLoadBalancingV2::Listener. Mirrors
    /// ModifyListener: port, protocol, default actions, certs, ssl policy.
    /// In-place update for AWS::ElasticLoadBalancingV2::Listener. Mirrors
    /// ModifyListener: port, protocol, default actions, certs, ssl policy.
    pub(crate) fn update_elbv2_listener(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let arn = existing.physical_id.clone();
        let new_default_actions = props
            .get("DefaultActions")
            .map(|v| parse_elb_actions(Some(v)));
        let mut accounts = self.elbv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let listener = state
            .listeners
            .get_mut(&arn)
            .ok_or_else(|| format!("Listener {arn} no longer exists"))?;
        if let Some(n) = props.get("Port").and_then(cfn_as_i64) {
            listener.port = Some(n as i32);
        }
        if let Some(s) = props.get("Protocol").and_then(|v| v.as_str()) {
            listener.protocol = Some(s.to_string());
        }
        if let Some(s) = props.get("SslPolicy").and_then(|v| v.as_str()) {
            listener.ssl_policy = Some(s.to_string());
        }
        if let Some(actions) = new_default_actions {
            listener.default_actions = actions;
        }
        if props.get("Tags").is_some() {
            listener.tags = parse_elb_tags(props.get("Tags"));
        }
        Ok(ProvisionResult::new(arn.clone()).with("ListenerArn", arn))
    }

    /// In-place update for AWS::ElasticLoadBalancingV2::ListenerRule. Mirrors
    /// ModifyRule + SetRulePriorities.
    /// In-place update for AWS::ElasticLoadBalancingV2::ListenerRule. Mirrors
    /// ModifyRule + SetRulePriorities.
    pub(crate) fn update_elbv2_listener_rule(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let arn = existing.physical_id.clone();
        let new_actions = props.get("Actions").map(|v| parse_elb_actions(Some(v)));
        let new_conditions = props
            .get("Conditions")
            .map(|v| parse_elb_rule_conditions(Some(v)));
        let mut accounts = self.elbv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let rule = state
            .rules
            .get_mut(&arn)
            .ok_or_else(|| format!("ListenerRule {arn} no longer exists"))?;
        if let Some(v) = props.get("Priority") {
            rule.priority = if let Some(s) = v.as_str() {
                s.to_string()
            } else if let Some(n) = v.as_i64() {
                n.to_string()
            } else {
                rule.priority.clone()
            };
        }
        if let Some(actions) = new_actions {
            rule.actions = actions;
        }
        if let Some(conditions) = new_conditions {
            rule.conditions = conditions;
        }
        if props.get("Tags").is_some() {
            rule.tags = parse_elb_tags(props.get("Tags"));
        }
        Ok(ProvisionResult::new(arn.clone()).with("RuleArn", arn))
    }

    /// In-place update for AWS::ElasticLoadBalancingV2::ListenerCertificate.
    /// CFN treats this as replace-on-cert-list-change in real AWS, but we
    /// can rebuild the SNI cert set against the same physical id without
    /// disrupting the listener.
    /// In-place update for AWS::ElasticLoadBalancingV2::ListenerCertificate.
    /// CFN treats this as replace-on-cert-list-change in real AWS, but we
    /// can rebuild the SNI cert set against the same physical id without
    /// disrupting the listener.
    pub(crate) fn update_elbv2_listener_certificate(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let physical_id = existing.physical_id.clone();
        let listener_arn = props
            .get("ListenerArn")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .or_else(|| physical_id.split_once('#').map(|(l, _)| l.to_string()))
            .ok_or_else(|| "ListenerArn is required".to_string())?;
        let new_certs: Vec<String> = props
            .get("Certificates")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|c| c.get("CertificateArn").and_then(|v| v.as_str()))
                    .map(|s| s.to_string())
                    .collect()
            })
            .unwrap_or_default();
        if new_certs.is_empty() {
            return Err("Certificates must contain at least one CertificateArn".to_string());
        }

        // Strip the previously-managed certs, then attach the new set.
        let prev_certs: Vec<String> = physical_id
            .split_once('#')
            .map(|(_, list)| list.split(',').map(|s| s.to_string()).collect())
            .unwrap_or_default();

        let mut accounts = self.elbv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let listener = state
            .listeners
            .get_mut(&listener_arn)
            .ok_or_else(|| format!("Listener {listener_arn} does not exist"))?;
        listener
            .certificates
            .retain(|c| !prev_certs.iter().any(|p| p == &c.certificate_arn));
        for arn in &new_certs {
            listener.certificates.retain(|c| &c.certificate_arn != arn);
            listener.certificates.push(fakecloud_elbv2::Certificate {
                certificate_arn: arn.clone(),
                is_default: false,
            });
        }
        Ok(ProvisionResult::new(format!(
            "{}#{}",
            listener_arn,
            new_certs.join(",")
        )))
    }

    /// In-place update for AWS::ElasticLoadBalancingV2::TrustStore. Only the
    /// CA bundle and tags are mutable; name is immutable in real AWS.
    /// In-place update for AWS::ElasticLoadBalancingV2::TrustStore. Only the
    /// CA bundle and tags are mutable; name is immutable in real AWS.
    pub(crate) fn update_elbv2_trust_store(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let arn = existing.physical_id.clone();
        let mut accounts = self.elbv2_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let ts = state
            .trust_stores
            .get_mut(&arn)
            .ok_or_else(|| format!("TrustStore {arn} no longer exists"))?;
        let new_bucket = props
            .get("CaCertificatesBundleS3Bucket")
            .and_then(|v| v.as_str());
        let new_key = props
            .get("CaCertificatesBundleS3Key")
            .and_then(|v| v.as_str());
        if let (Some(b), Some(k)) = (new_bucket, new_key) {
            ts.ca_certificates_bundle = Some(format!("s3://{b}/{k}").into_bytes());
        }
        if let Some(arr) = props.get("Tags").and_then(|v| v.as_array()) {
            ts.tags = arr
                .iter()
                .filter_map(|t| {
                    let k = t.get("Key").and_then(|v| v.as_str())?;
                    let v = t.get("Value").and_then(|v| v.as_str()).unwrap_or("");
                    Some(fakecloud_elbv2::Tag {
                        key: k.to_string(),
                        value: v.to_string(),
                    })
                })
                .collect();
        }
        let name = ts.name.clone();
        let status = ts.status.clone();
        Ok(ProvisionResult::new(arn.clone())
            .with("TrustStoreArn", arn)
            .with("Name", name)
            .with("Status", status))
    }
}
