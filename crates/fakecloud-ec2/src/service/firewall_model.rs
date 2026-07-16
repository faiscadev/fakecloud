//! Builds the per-subnet firewall model (#1745 phase 3) from EC2 control-plane
//! state and drives the runtime's nftables reconcile.
//!
//! The host nft ruleset is global, so the model is built across *every* account
//! partition and applied as one atomic swap — reconciling for a single account
//! would flush the others' rules. Reconcile is a no-op when enforcement is
//! disabled (the common case), and the control plane skips even building the
//! model then.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use crate::runtime::firewall::{
    group_by_subnet, FirewallRule, InstanceFirewall, InstanceRules, NaclRule, SubnetFirewall,
};
use crate::runtime::{subnet_network_name, Ec2Runtime};
use crate::state::{Ec2State, NetworkAcl, SecurityGroupRule, SharedEc2State};

/// True for an instance that should be in the firewall model: running and
/// placed in a subnet (so it has a backing subnet network to filter on).
fn enforced(inst: &crate::state::Instance) -> Option<&str> {
    if inst.state_name == "running" {
        inst.subnet_id.as_deref()
    } else {
        None
    }
}

/// Flatten one stored security-group rule into zero or more firewall rules.
/// IPv4 CIDR rules pass through; a referenced-group rule expands to the `/32`
/// of every running member of that group (so the default SG's "allow from
/// self" actually works); IPv6 / prefix-list sources are not modeled (the nft
/// table is `ip`-only) and are skipped.
fn flatten_rule(
    rule: &SecurityGroupRule,
    sg_members: &HashMap<String, Vec<String>>,
) -> Vec<FirewallRule> {
    let mk = |cidr: Option<String>| FirewallRule {
        protocol: rule.ip_protocol.clone(),
        from_port: rule.from_port,
        to_port: rule.to_port,
        cidr,
    };
    if let Some(group) = &rule.referenced_group_id {
        sg_members
            .get(group)
            .map(|ips| ips.iter().map(|ip| mk(Some(format!("{ip}/32")))).collect())
            .unwrap_or_default()
    } else if let Some(cidr) = &rule.cidr_ipv4 {
        vec![mk(Some(cidr.clone()))]
    } else {
        Vec::new()
    }
}

/// NACL deny/allow entries that apply to a subnet, as the renderer's model.
/// Both allows and denies are carried with their `rule_number` so the renderer
/// can apply AWS first-match-by-rule-number precedence (allow shadows a
/// higher-numbered deny).
fn nacl_rules(acl: &NetworkAcl) -> Vec<NaclRule> {
    acl.entries
        .iter()
        // The catch-all `*` deny (rule 32767) is the implicit policy, not a
        // user rule — skip it so it doesn't blackhole everything.
        .filter(|e| e.rule_number != 32767)
        .map(|e| NaclRule {
            rule_number: e.rule_number,
            egress: e.egress,
            allow: e.rule_action == "allow",
            protocol: e.protocol.clone(),
            from_port: e.port_range.map(|(f, _)| f).unwrap_or(-1),
            to_port: e.port_range.map(|(_, t)| t).unwrap_or(-1),
            cidr: e.cidr_block.clone(),
        })
        .collect()
}

/// Find the NACL governing `subnet_id`: an explicit association wins, else the
/// default NACL of the subnet's VPC.
fn subnet_nacl<'a>(state: &'a Ec2State, subnet_id: &str) -> Option<&'a NetworkAcl> {
    if let Some(acl) = state
        .network_acls
        .values()
        .find(|a| a.associations.iter().any(|x| x.subnet_id == subnet_id))
    {
        return Some(acl);
    }
    let vpc = state.subnets.get(subnet_id).map(|s| &s.vpc_id)?;
    state
        .network_acls
        .values()
        .find(|a| a.is_default && &a.vpc_id == vpc)
}

/// Flatten every enforced (running, subnet-placed) instance's security groups
/// into per-instance ingress/egress rules, expanding referenced groups to
/// member `/32`s.
pub(crate) fn instance_rules(state: &Ec2State) -> Vec<InstanceRules> {
    // sg-id -> running member IPs, for referenced-group expansion.
    let mut sg_members: HashMap<String, Vec<String>> = HashMap::new();
    for inst in state.instances.values() {
        if enforced(inst).is_some() {
            for sg in &inst.security_group_ids {
                sg_members
                    .entry(sg.clone())
                    .or_default()
                    .push(inst.private_ip.clone());
            }
        }
    }

    let mut out = Vec::new();
    for inst in state.instances.values() {
        let Some(subnet_id) = enforced(inst) else {
            continue;
        };
        let mut ingress = Vec::new();
        let mut egress = Vec::new();
        for sg_id in &inst.security_group_ids {
            if let Some(sg) = state.security_groups.get(sg_id) {
                for rule in &sg.rules {
                    let flat = flatten_rule(rule, &sg_members);
                    if rule.is_egress {
                        egress.extend(flat);
                    } else {
                        ingress.extend(flat);
                    }
                }
            }
        }
        out.push(InstanceRules {
            instance_id: inst.instance_id.clone(),
            subnet_id: subnet_id.to_string(),
            private_ip: inst.private_ip.clone(),
            ingress,
            egress,
        });
    }
    out
}

/// Build the per-subnet nftables model for one account partition.
pub(crate) fn build_for_state(state: &Ec2State) -> Vec<SubnetFirewall> {
    let mut instances: Vec<(String, InstanceFirewall)> = Vec::new();
    let mut subnets_in_play: Vec<String> = Vec::new();
    for r in instance_rules(state) {
        instances.push((
            subnet_network_name(&r.subnet_id),
            InstanceFirewall {
                private_ip: r.private_ip,
                ingress: r.ingress,
                egress: r.egress,
            },
        ));
        if !subnets_in_play.contains(&r.subnet_id) {
            subnets_in_play.push(r.subnet_id);
        }
    }

    let mut nacls: BTreeMap<String, Vec<NaclRule>> = BTreeMap::new();
    for subnet_id in &subnets_in_play {
        if let Some(acl) = subnet_nacl(state, subnet_id) {
            nacls.insert(subnet_network_name(subnet_id), nacl_rules(acl));
        }
    }

    group_by_subnet(instances, nacls)
}

/// Re-derive the firewall model across every account partition and apply it via
/// the runtime — nftables for the Docker backend, NetworkPolicies for k8s. The
/// runtime dispatches on its backend; this only assembles the global model.
pub(crate) async fn reconcile(state: &SharedEc2State, runtime: &Arc<Ec2Runtime>) {
    if runtime.is_k8s() {
        // k8s: one NetworkPolicy per instance, built from the shared flatten.
        let rules: Vec<InstanceRules> = {
            let accounts = state.read();
            accounts
                .iter()
                .flat_map(|(_, s)| instance_rules(s))
                .collect()
        };
        runtime.reconcile_network_policies(rules).await;
        return;
    }
    let model: Vec<SubnetFirewall> = {
        let accounts = state.read();
        accounts
            .iter()
            .flat_map(|(_, s)| build_for_state(s))
            .collect()
    };
    runtime.reconcile_firewall(model).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{Instance, SecurityGroup, SecurityGroupRule};

    fn running_instance(id: &str, ip: &str, subnet: &str, sgs: &[&str]) -> Instance {
        Instance {
            instance_id: id.into(),
            image_id: "ami-1".into(),
            instance_type: "t3.micro".into(),
            state_code: 16,
            state_name: "running".into(),
            private_ip: ip.into(),
            public_ip: None,
            subnet_id: Some(subnet.into()),
            vpc_id: Some("vpc-x".into()),
            key_name: None,
            security_group_ids: sgs.iter().map(|s| s.to_string()).collect(),
            reservation_id: "r-1".into(),
            ami_launch_index: 0,
            monitoring: false,
            az: "us-east-1a".into(),
            launch_time: "2024-01-01T00:00:00.000Z".into(),
            container_id: None,
            disable_api_termination: false,
            disable_api_stop: false,
            source_dest_check: true,
            ebs_optimized: false,
            instance_initiated_shutdown_behavior: "stop".into(),
            user_data: None,
            metadata_options: Default::default(),
            cpu_options: None,
            bandwidth_weighting: None,
            maintenance_options: Default::default(),
            placement_tenancy: None,
            placement_affinity: None,
            placement_group_name: None,
            private_dns_hostname_type: None,
            enable_resource_name_dns_a_record: false,
            enable_resource_name_dns_aaaa_record: false,
        }
    }

    fn sg(id: &str, rules: Vec<SecurityGroupRule>) -> SecurityGroup {
        SecurityGroup {
            group_id: id.into(),
            group_name: "g".into(),
            description: String::new(),
            vpc_id: "vpc-x".into(),
            rules,
        }
    }

    fn ingress_tcp(
        group: &str,
        port: i64,
        cidr: Option<&str>,
        refg: Option<&str>,
    ) -> SecurityGroupRule {
        SecurityGroupRule {
            rule_id: "sgr-1".into(),
            group_id: group.into(),
            is_egress: false,
            ip_protocol: "tcp".into(),
            from_port: port,
            to_port: port,
            cidr_ipv4: cidr.map(str::to_string),
            cidr_ipv6: None,
            prefix_list_id: None,
            referenced_group_id: refg.map(str::to_string),
            referenced_group_name: None,
            referenced_user_id: None,
            description: String::new(),
        }
    }

    #[test]
    fn builds_ingress_from_cidr_rule() {
        let mut state = Ec2State::new("123456789012", "us-east-1");
        state.security_groups.insert(
            "sg-1".into(),
            sg(
                "sg-1",
                vec![ingress_tcp("sg-1", 22, Some("10.0.0.0/8"), None)],
            ),
        );
        state.instances.insert(
            "i-1".into(),
            running_instance("i-1", "172.30.0.2", "subnet-1", &["sg-1"]),
        );

        let model = build_for_state(&state);
        assert_eq!(model.len(), 1);
        assert_eq!(model[0].network_name, subnet_network_name("subnet-1"));
        let inst = &model[0].instances[0];
        assert_eq!(inst.ingress.len(), 1);
        assert_eq!(inst.ingress[0].cidr.as_deref(), Some("10.0.0.0/8"));
    }

    #[test]
    fn referenced_group_expands_to_member_ips() {
        let mut state = Ec2State::new("123456789012", "us-east-1");
        // sg-1 allows all from itself (the default-SG shape).
        state.security_groups.insert(
            "sg-1".into(),
            sg("sg-1", vec![ingress_tcp("sg-1", 80, None, Some("sg-1"))]),
        );
        state.instances.insert(
            "i-1".into(),
            running_instance("i-1", "172.30.0.2", "subnet-1", &["sg-1"]),
        );
        state.instances.insert(
            "i-2".into(),
            running_instance("i-2", "172.30.0.3", "subnet-1", &["sg-1"]),
        );

        let model = build_for_state(&state);
        let inst = model[0]
            .instances
            .iter()
            .find(|i| i.private_ip == "172.30.0.2")
            .unwrap();
        // referenced sg-1 -> both members' /32s
        let cidrs: Vec<_> = inst.ingress.iter().filter_map(|r| r.cidr.clone()).collect();
        assert!(cidrs.contains(&"172.30.0.2/32".to_string()));
        assert!(cidrs.contains(&"172.30.0.3/32".to_string()));
    }

    #[test]
    fn pending_instances_are_excluded() {
        let mut state = Ec2State::new("123456789012", "us-east-1");
        let mut inst = running_instance("i-1", "172.30.0.2", "subnet-1", &[]);
        inst.state_name = "pending".into();
        state.instances.insert("i-1".into(), inst);
        assert!(build_for_state(&state).is_empty());
    }
}
