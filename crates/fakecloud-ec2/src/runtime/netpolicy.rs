//! Kubernetes NetworkPolicy enforcement for EC2 security groups (#1745 ph4).
//!
//! The Docker backend filters traffic with host nftables (phase 3). k8s Pods
//! share a flat L3 network with no bridge to hook, so isolation there is
//! expressed as **NetworkPolicy** objects (L3/L4 pod/IP selectors) and enforced
//! by the cluster CNI — *if* the CNI enforces NetworkPolicy at all. Several
//! common CNIs (notably kind's default `kindnet`) ignore NetworkPolicy, so this
//! module always creates the (correct) policies and **degrades gracefully**:
//! it detects the CNI, warns once when enforcement isn't guaranteed, and never
//! blocks Pod creation on it.
//!
//! ## Pluggable CNI
//!
//! [`CniDriver`] abstracts "does this cluster enforce NetworkPolicy". Calico is
//! the first known-enforcing driver; the enum is the extension point for
//! Cilium/others. Detection is best-effort (presence of the CNI's API group /
//! components); unknown CNIs degrade to "create but don't assume enforcement".
//!
//! The policy *translation* ([`build_policies`]) is pure and unit-tested; the
//! apply path lives in the k8s client.

use std::collections::BTreeMap;

use k8s_openapi::api::networking::v1::{
    IPBlock, NetworkPolicy, NetworkPolicyEgressRule, NetworkPolicyIngressRule, NetworkPolicyPeer,
    NetworkPolicyPort, NetworkPolicySpec,
};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{LabelSelector, ObjectMeta};
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;

use fakecloud_k8s::{labels, names};

use super::firewall::{FirewallRule, InstanceRules};

/// The `fakecloud-ec2` pod label whose value is the (DNS-safe) instance id —
/// the same label the instance Pod carries, so a policy's `podSelector` targets
/// exactly that instance.
const INSTANCE_LABEL: &str = "fakecloud-ec2";

/// Name of the NetworkPolicy backing one instance.
pub fn policy_name(instance_id: &str) -> String {
    format!("fakecloud-ec2-{}", names::label_safe(instance_id))
}

/// Build one NetworkPolicy per instance from the shared flattened rules. Each
/// policy selects its instance Pod and restricts ingress/egress to the CIDRs +
/// ports its security groups allow. `instance_label` is this fakecloud
/// process's ownership label, stamped on every policy so the reaper can prune
/// policies orphaned by a previous process.
pub fn build_policies(
    rules: &[InstanceRules],
    namespace: &str,
    instance_label: &str,
) -> Vec<NetworkPolicy> {
    rules
        .iter()
        .map(|r| build_one(r, namespace, instance_label))
        .collect()
}

fn build_one(r: &InstanceRules, namespace: &str, instance_label: &str) -> NetworkPolicy {
    let slug = names::label_safe(&r.instance_id);

    let mut policy_labels = BTreeMap::new();
    policy_labels.insert(
        labels::MANAGED_BY.to_string(),
        labels::MANAGED_BY_VALUE.to_string(),
    );
    policy_labels.insert(labels::INSTANCE.to_string(), instance_label.to_string());
    policy_labels.insert(labels::SERVICE.to_string(), "ec2".to_string());

    let mut selector_labels = BTreeMap::new();
    selector_labels.insert(INSTANCE_LABEL.to_string(), slug);

    NetworkPolicy {
        metadata: ObjectMeta {
            name: Some(policy_name(&r.instance_id)),
            namespace: Some(namespace.to_string()),
            labels: Some(policy_labels),
            ..ObjectMeta::default()
        },
        spec: Some(NetworkPolicySpec {
            pod_selector: LabelSelector {
                match_labels: Some(selector_labels),
                ..LabelSelector::default()
            },
            policy_types: Some(vec!["Ingress".to_string(), "Egress".to_string()]),
            ingress: Some(r.ingress.iter().map(ingress_rule).collect()),
            egress: Some(r.egress.iter().map(egress_rule).collect()),
        }),
    }
}

fn ingress_rule(r: &FirewallRule) -> NetworkPolicyIngressRule {
    NetworkPolicyIngressRule {
        from: peers(&r.cidr),
        ports: ports(r),
    }
}

fn egress_rule(r: &FirewallRule) -> NetworkPolicyEgressRule {
    NetworkPolicyEgressRule {
        to: peers(&r.cidr),
        ports: ports(r),
    }
}

/// `None` (any source/destination) for `0.0.0.0/0`/absent; otherwise a single
/// `ipBlock` peer. Pod-to-pod allows (referenced security groups) arrive as the
/// members' `/32`s, which match as ipBlocks.
fn peers(cidr: &Option<String>) -> Option<Vec<NetworkPolicyPeer>> {
    let c = cidr.as_deref()?;
    if c == "0.0.0.0/0" || c.is_empty() {
        return None;
    }
    Some(vec![NetworkPolicyPeer {
        ip_block: Some(IPBlock {
            cidr: c.to_string(),
            except: None,
        }),
        ..NetworkPolicyPeer::default()
    }])
}

/// Translate a rule's protocol/ports into NetworkPolicy ports. `None` (all
/// ports) for all-protocols / portless / ICMP rules — standard NetworkPolicy
/// only matches TCP/UDP/SCTP, so ICMP can't be narrowed and rides as "all".
fn ports(r: &FirewallRule) -> Option<Vec<NetworkPolicyPort>> {
    let proto = match r.protocol.as_str() {
        "tcp" | "6" => "TCP",
        "udp" | "17" => "UDP",
        // all-protocols / icmp / unknown: cannot express as a NetworkPolicy
        // port -> leave ports unset (all ports of all protocols).
        _ => return None,
    };
    if r.from_port < 0 || r.to_port < 0 {
        return None;
    }
    let mut port = NetworkPolicyPort {
        protocol: Some(proto.to_string()),
        port: Some(IntOrString::Int(r.from_port as i32)),
        end_port: None,
    };
    if r.to_port > r.from_port {
        port.end_port = Some(r.to_port as i32);
    }
    Some(vec![port])
}

/// Container Network Interface plugins fakecloud knows how to reason about for
/// NetworkPolicy enforcement. The pluggable seam: add a variant + detection to
/// support a new enforcing CNI.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CniDriver {
    /// Calico — enforces NetworkPolicy.
    Calico,
    /// Cilium — enforces NetworkPolicy.
    Cilium,
    /// A CNI we couldn't identify, or one known not to enforce NetworkPolicy
    /// (e.g. kindnet). Policies are still created; enforcement isn't assumed.
    Unknown,
}

impl CniDriver {
    /// Whether this CNI is known to enforce NetworkPolicy.
    pub fn enforces(self) -> bool {
        matches!(self, CniDriver::Calico | CniDriver::Cilium)
    }

    /// Identify the CNI from the set of well-known component/daemonset names
    /// present in the cluster (typically read from `kube-system`). Pure so the
    /// detection logic is unit-testable without a cluster.
    pub fn from_components<I, S>(component_names: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut calico = false;
        let mut cilium = false;
        for name in component_names {
            let n = name.as_ref();
            if n.contains("calico") {
                calico = true;
            }
            if n.contains("cilium") {
                cilium = true;
            }
        }
        if calico {
            CniDriver::Calico
        } else if cilium {
            CniDriver::Cilium
        } else {
            CniDriver::Unknown
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rules(
        instance: &str,
        ingress: Vec<FirewallRule>,
        egress: Vec<FirewallRule>,
    ) -> InstanceRules {
        InstanceRules {
            instance_id: instance.to_string(),
            subnet_id: "subnet-1".to_string(),
            private_ip: "172.30.0.2".to_string(),
            ingress,
            egress,
        }
    }

    fn tcp(port: i64, cidr: Option<&str>) -> FirewallRule {
        FirewallRule {
            protocol: "tcp".into(),
            from_port: port,
            to_port: port,
            cidr: cidr.map(str::to_string),
        }
    }

    #[test]
    fn policy_selects_instance_and_sets_both_directions() {
        let r = rules("i-0ABC", vec![tcp(22, Some("10.0.0.0/8"))], vec![]);
        let p = &build_policies(&[r], "fakecloud", "fakecloud-123")[0];
        assert_eq!(p.metadata.name.as_deref(), Some("fakecloud-ec2-i-0abc"));
        let spec = p.spec.as_ref().unwrap();
        // selects the instance pod by its label-safe id
        assert_eq!(
            spec.pod_selector
                .match_labels
                .as_ref()
                .unwrap()
                .get("fakecloud-ec2")
                .map(String::as_str),
            Some("i-0abc")
        );
        assert_eq!(
            spec.policy_types.as_ref().unwrap(),
            &vec!["Ingress".to_string(), "Egress".to_string()]
        );
        // one ingress rule: from 10.0.0.0/8 tcp/22
        let ing = &spec.ingress.as_ref().unwrap()[0];
        let peer = &ing.from.as_ref().unwrap()[0];
        assert_eq!(peer.ip_block.as_ref().unwrap().cidr, "10.0.0.0/8");
        let port = &ing.ports.as_ref().unwrap()[0];
        assert_eq!(port.protocol.as_deref(), Some("TCP"));
        assert_eq!(port.port, Some(IntOrString::Int(22)));
    }

    #[test]
    fn anywhere_all_protocols_rule_has_no_peer_or_ports() {
        let all = FirewallRule {
            protocol: "-1".into(),
            from_port: -1,
            to_port: -1,
            cidr: Some("0.0.0.0/0".into()),
        };
        let r = rules("i-1", vec![all], vec![]);
        let p = &build_policies(&[r], "fakecloud", "x")[0];
        let ing = &p.spec.as_ref().unwrap().ingress.as_ref().unwrap()[0];
        assert!(ing.from.is_none(), "0.0.0.0/0 -> any source");
        assert!(ing.ports.is_none(), "all protocols -> any port");
    }

    #[test]
    fn port_range_uses_end_port() {
        let range = FirewallRule {
            protocol: "tcp".into(),
            from_port: 8000,
            to_port: 8100,
            cidr: None,
        };
        let r = rules("i-1", vec![range], vec![]);
        let p = &build_policies(&[r], "fakecloud", "x")[0];
        let port = &p.spec.as_ref().unwrap().ingress.as_ref().unwrap()[0]
            .ports
            .as_ref()
            .unwrap()[0];
        assert_eq!(port.port, Some(IntOrString::Int(8000)));
        assert_eq!(port.end_port, Some(8100));
    }

    #[test]
    fn referenced_member_ip_becomes_ipblock() {
        let r = rules("i-1", vec![tcp(80, Some("172.30.0.3/32"))], vec![]);
        let p = &build_policies(&[r], "fakecloud", "x")[0];
        let peer = &p.spec.as_ref().unwrap().ingress.as_ref().unwrap()[0]
            .from
            .as_ref()
            .unwrap()[0];
        assert_eq!(peer.ip_block.as_ref().unwrap().cidr, "172.30.0.3/32");
    }

    #[test]
    fn cni_detection_and_enforcement() {
        assert_eq!(
            CniDriver::from_components(["calico-node", "coredns"]),
            CniDriver::Calico
        );
        assert_eq!(
            CniDriver::from_components(["cilium-abc"]),
            CniDriver::Cilium
        );
        assert_eq!(
            CniDriver::from_components(["kindnet-xyz", "kube-proxy"]),
            CniDriver::Unknown
        );
        assert!(CniDriver::Calico.enforces());
        assert!(CniDriver::Cilium.enforces());
        assert!(!CniDriver::Unknown.enforces());
    }
}
