//! Security-group + network-ACL packet filtering (issue #1745 phase 3).
//!
//! Phase 2 isolates instances at L3 by giving each subnet its own daemon
//! bridge. That stops cross-VPC traffic but does nothing *within* a subnet —
//! security-group and NACL rules still block nothing. This module closes that
//! gap by translating the SG/NACL model into an **nftables** ruleset and
//! applying it on the host, scoped to fakecloud's per-subnet bridges.
//!
//! ## Why nftables, and why opt-in
//!
//! Real packet filtering needs `CAP_NET_ADMIN`, which instance containers
//! deliberately don't have. nftables (over iptables) is chosen for its atomic
//! ruleset swaps — a clean fit for the dynamic Authorize/Revoke churn of
//! security groups. Because applying host firewall rules is privileged and can
//! interfere with a user's own networking, enforcement is **opt-in** via
//! `FAKECLOUD_EC2_SG_ENFORCEMENT` and **degrades gracefully**: when nft or
//! `CAP_NET_ADMIN` is missing (CI, Docker Desktop, rootless podman) the driver
//! logs one warning and falls back to metadata-only — phase-2 isolation still
//! holds, exactly as before (no regression).
//!
//! ## What's tested where
//!
//! The translation from the SG/NACL model to the nft ruleset
//! ([`render_ruleset`]) is pure and exhaustively unit-tested. The apply path
//! shells out to `nft -f -`; it cannot be exercised in CI (no `CAP_NET_ADMIN`),
//! so it is kept thin and the *generated ruleset* is the verified artifact.

use std::collections::BTreeMap;

/// A single allow rule flattened out of a security group: one protocol/port
/// range from one CIDR (referenced-group and prefix-list sources are resolved
/// to CIDRs by the caller, or dropped when they can't be).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FirewallRule {
    /// `tcp` | `udp` | `icmp` | `-1` (all protocols).
    pub protocol: String,
    /// Port range; `-1`/`-1` means "all ports" (omit the port match).
    pub from_port: i64,
    pub to_port: i64,
    /// Source (ingress) / destination (egress) IPv4 CIDR. `None` = anywhere.
    pub cidr: Option<String>,
}

/// One instance's firewall view: its address on the subnet bridge plus the
/// ingress/egress rules flattened from every security group attached to it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstanceFirewall {
    pub private_ip: String,
    pub ingress: Vec<FirewallRule>,
    pub egress: Vec<FirewallRule>,
}

/// One running instance's flattened firewall view, keyed by both its id (for
/// the k8s NetworkPolicy `podSelector`) and its IP (for nft). The shared
/// intermediate the service layer produces from EC2 state; the nft model
/// builder and the k8s NetworkPolicy builder both consume it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstanceRules {
    pub instance_id: String,
    pub subnet_id: String,
    pub private_ip: String,
    pub ingress: Vec<FirewallRule>,
    pub egress: Vec<FirewallRule>,
}

/// A subnet-level NACL entry (allow/deny, ordered by rule number by the
/// caller). NACLs are stateless and apply to the whole subnet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NaclRule {
    pub egress: bool,
    /// True = allow, false = deny.
    pub allow: bool,
    pub protocol: String,
    pub from_port: i64,
    pub to_port: i64,
    pub cidr: Option<String>,
}

/// Everything needed to render the firewall for one subnet bridge.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubnetFirewall {
    /// The daemon network name (`fakecloud-subnet-<id>`); doubles as the nft
    /// chain comment so a human reading `nft list ruleset` can see which subnet
    /// a rule belongs to.
    pub network_name: String,
    pub instances: Vec<InstanceFirewall>,
    pub nacl: Vec<NaclRule>,
}

/// The nftables table fakecloud owns. Kept in its own table so a full
/// `flush table` + re-add is an atomic, side-effect-free swap that never
/// touches docker's own iptables/nftables rules.
const TABLE: &str = "inet fakecloud_ec2";

/// Render the complete nft ruleset for a set of subnets. Deterministic
/// (subnets and rules emitted in the order given; the caller sorts for
/// stability) so the output can be diffed and unit-tested.
///
/// Model: a single `forward` chain, default-accept, that for every instance
/// emits its allow rules followed by a default-deny to that instance's IP.
/// Established/related traffic is accepted up front so security groups behave
/// statefully, like AWS. NACL deny rules are emitted per subnet before the
/// per-instance rules (stateless, subnet-wide).
pub fn render_ruleset(subnets: &[SubnetFirewall]) -> String {
    let mut out = String::new();
    out.push_str(&format!("flush table {TABLE}\n"));
    out.push_str(&format!("table {TABLE} {{\n"));
    out.push_str("  chain forward {\n");
    out.push_str("    type filter hook forward priority -5; policy accept;\n");
    // Stateful: let replies through so SG rules only need to describe the
    // opening direction, matching AWS security-group semantics.
    out.push_str("    ct state established,related accept\n");

    for subnet in subnets {
        out.push_str(&format!("    # subnet {}\n", subnet.network_name));

        // Subnet-wide NACL denies first (stateless, highest precedence).
        for rule in subnet.nacl.iter().filter(|r| !r.allow) {
            if let Some(line) = render_nacl_drop(rule) {
                out.push_str(&format!("    {line}\n"));
            }
        }

        for inst in &subnet.instances {
            // Ingress: allow matching, then default-deny to this instance.
            for rule in &inst.ingress {
                out.push_str(&format!(
                    "    {}\n",
                    render_rule(rule, Direction::Ingress, &inst.private_ip)
                ));
            }
            out.push_str(&format!(
                "    ip daddr {} drop comment \"default-deny ingress\"\n",
                inst.private_ip
            ));

            // Egress: allow matching, then default-deny from this instance.
            for rule in &inst.egress {
                out.push_str(&format!(
                    "    {}\n",
                    render_rule(rule, Direction::Egress, &inst.private_ip)
                ));
            }
            out.push_str(&format!(
                "    ip saddr {} drop comment \"default-deny egress\"\n",
                inst.private_ip
            ));
        }
    }

    out.push_str("  }\n");
    out.push_str("}\n");
    out
}

#[derive(Clone, Copy)]
enum Direction {
    Ingress,
    Egress,
}

/// Render one allow rule. Ingress matches on `ip daddr <instance>` (+ optional
/// `ip saddr <cidr>`); egress mirrors it.
fn render_rule(rule: &FirewallRule, dir: Direction, instance_ip: &str) -> String {
    let mut parts = Vec::new();
    match dir {
        Direction::Ingress => {
            parts.push(format!("ip daddr {instance_ip}"));
            if let Some(cidr) = normalized_cidr(&rule.cidr) {
                parts.push(format!("ip saddr {cidr}"));
            }
        }
        Direction::Egress => {
            parts.push(format!("ip saddr {instance_ip}"));
            if let Some(cidr) = normalized_cidr(&rule.cidr) {
                parts.push(format!("ip daddr {cidr}"));
            }
        }
    }
    push_proto_ports(&mut parts, &rule.protocol, rule.from_port, rule.to_port);
    parts.push("accept".to_string());
    parts.join(" ")
}

/// Render a NACL deny as a drop line scoped to its direction + match. Returns
/// `None` for an allow rule (allows are the default-accept policy; only denies
/// need an explicit line).
fn render_nacl_drop(rule: &NaclRule) -> Option<String> {
    if rule.allow {
        return None;
    }
    let mut parts = Vec::new();
    if let Some(cidr) = normalized_cidr(&rule.cidr) {
        // Deny traffic from (ingress) / to (egress) the CIDR.
        if rule.egress {
            parts.push(format!("ip daddr {cidr}"));
        } else {
            parts.push(format!("ip saddr {cidr}"));
        }
    }
    push_proto_ports(&mut parts, &rule.protocol, rule.from_port, rule.to_port);
    parts.push("drop".to_string());
    parts.push("comment \"nacl-deny\"".to_string());
    Some(parts.join(" "))
}

/// Append protocol + (for tcp/udp) destination-port matching to an nft rule.
/// Protocol `-1` matches everything (no clause); a `-1` port range likewise
/// omits the port match.
fn push_proto_ports(parts: &mut Vec<String>, protocol: &str, from: i64, to: i64) {
    match protocol {
        "-1" | "" => {}
        "icmp" | "1" => parts.push("ip protocol icmp".to_string()),
        proto @ ("tcp" | "udp" | "6" | "17") => {
            let p = match proto {
                "6" => "tcp",
                "17" => "udp",
                other => other,
            };
            parts.push(p.to_string());
            if from >= 0 && to >= 0 {
                if from == to {
                    parts.push(format!("dport {from}"));
                } else {
                    parts.push(format!("dport {from}-{to}"));
                }
            }
        }
        other => parts.push(format!("ip protocol {other}")),
    }
}

/// Drop `0.0.0.0/0` (which nft rejects as a no-op match) to `None`, and strip a
/// redundant `/32` host suffix so single-host rules read cleanly.
fn normalized_cidr(cidr: &Option<String>) -> Option<String> {
    let c = cidr.as_deref()?;
    if c == "0.0.0.0/0" || c.is_empty() {
        return None;
    }
    Some(c.trim_end_matches("/32").to_string())
}

/// How security-group enforcement is backed in this process.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnforcementMode {
    /// nftables on the host (requires `CAP_NET_ADMIN` + `nft`).
    Nftables,
    /// Degraded: rules are tracked but not enforced (metadata-only).
    Disabled,
}

/// Decide the enforcement mode from the environment. Enforcement is opt-in:
/// `FAKECLOUD_EC2_SG_ENFORCEMENT` must be set to `1`/`true`/`nftables`, and
/// `nft` must actually be runnable, or we degrade to `Disabled` with a single
/// warning. `env` and `nft_probe` are injected so the decision is unit-testable
/// without touching the real environment or running `nft`.
pub fn resolve_enforcement_mode(
    env: Option<&str>,
    nft_probe: impl FnOnce() -> bool,
) -> EnforcementMode {
    let opted_in = matches!(
        env.map(|v| v.to_ascii_lowercase()).as_deref(),
        Some("1") | Some("true") | Some("nftables") | Some("on")
    );
    if !opted_in {
        return EnforcementMode::Disabled;
    }
    if nft_probe() {
        EnforcementMode::Nftables
    } else {
        EnforcementMode::Disabled
    }
}

/// True when `nft list ruleset` runs successfully — i.e. nft exists and this
/// process holds enough capability to read the ruleset (a good proxy for being
/// able to write it).
pub fn nft_available() -> bool {
    std::process::Command::new("nft")
        .args(["list", "ruleset"])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

/// Group instances by their subnet network name into the per-subnet model the
/// renderer consumes. Pure helper so the service layer can build the model from
/// its own state without depending on render internals.
pub fn group_by_subnet(
    instances: Vec<(String, InstanceFirewall)>,
    nacls: BTreeMap<String, Vec<NaclRule>>,
) -> Vec<SubnetFirewall> {
    let mut by_net: BTreeMap<String, Vec<InstanceFirewall>> = BTreeMap::new();
    for (network_name, inst) in instances {
        by_net.entry(network_name).or_default().push(inst);
    }
    by_net
        .into_iter()
        .map(|(network_name, mut instances)| {
            instances.sort_by(|a, b| a.private_ip.cmp(&b.private_ip));
            let nacl = nacls.get(&network_name).cloned().unwrap_or_default();
            SubnetFirewall {
                network_name,
                instances,
                nacl,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tcp(port: i64, cidr: Option<&str>) -> FirewallRule {
        FirewallRule {
            protocol: "tcp".into(),
            from_port: port,
            to_port: port,
            cidr: cidr.map(str::to_string),
        }
    }

    #[test]
    fn renders_allow_then_default_deny_for_ingress() {
        let model = vec![SubnetFirewall {
            network_name: "fakecloud-subnet-a".into(),
            instances: vec![InstanceFirewall {
                private_ip: "172.30.0.2".into(),
                ingress: vec![tcp(22, Some("10.0.0.0/8"))],
                egress: vec![],
            }],
            nacl: vec![],
        }];
        let rs = render_ruleset(&model);
        assert!(rs.contains("flush table inet fakecloud_ec2"));
        assert!(rs.contains("ct state established,related accept"));
        assert!(rs.contains("ip daddr 172.30.0.2 ip saddr 10.0.0.0/8 tcp dport 22 accept"));
        assert!(rs.contains("ip daddr 172.30.0.2 drop comment \"default-deny ingress\""));
        // egress had no explicit allows -> still a default-deny line
        assert!(rs.contains("ip saddr 172.30.0.2 drop comment \"default-deny egress\""));
    }

    #[test]
    fn all_protocols_and_anywhere_omit_match_clauses() {
        let rule = FirewallRule {
            protocol: "-1".into(),
            from_port: -1,
            to_port: -1,
            cidr: Some("0.0.0.0/0".into()),
        };
        let line = render_rule(&rule, Direction::Ingress, "172.30.0.5");
        // no saddr (anywhere), no proto, no port:
        assert_eq!(line, "ip daddr 172.30.0.5 accept");
    }

    #[test]
    fn port_range_and_single_port() {
        let range = FirewallRule {
            protocol: "tcp".into(),
            from_port: 8000,
            to_port: 8100,
            cidr: None,
        };
        assert!(render_rule(&range, Direction::Egress, "172.30.0.9")
            .contains("tcp dport 8000-8100 accept"));
        assert!(
            render_rule(&tcp(443, None), Direction::Ingress, "172.30.0.9")
                .contains("tcp dport 443 accept")
        );
    }

    #[test]
    fn icmp_and_numeric_protocols() {
        let icmp = FirewallRule {
            protocol: "icmp".into(),
            from_port: -1,
            to_port: -1,
            cidr: None,
        };
        assert!(render_rule(&icmp, Direction::Ingress, "172.30.0.2").contains("ip protocol icmp"));
        let udp = FirewallRule {
            protocol: "17".into(),
            from_port: 53,
            to_port: 53,
            cidr: None,
        };
        assert!(render_rule(&udp, Direction::Ingress, "172.30.0.2").contains("udp dport 53"));
    }

    #[test]
    fn host_cidr_strips_slash_32() {
        let r = tcp(22, Some("203.0.113.7/32"));
        assert!(render_rule(&r, Direction::Ingress, "172.30.0.2")
            .contains("ip saddr 203.0.113.7 tcp dport 22"));
    }

    #[test]
    fn nacl_deny_emitted_before_instance_rules() {
        let model = vec![SubnetFirewall {
            network_name: "fakecloud-subnet-a".into(),
            instances: vec![InstanceFirewall {
                private_ip: "172.30.0.2".into(),
                ingress: vec![],
                egress: vec![],
            }],
            nacl: vec![NaclRule {
                egress: false,
                allow: false,
                protocol: "tcp".into(),
                from_port: 3389,
                to_port: 3389,
                cidr: Some("198.51.100.0/24".into()),
            }],
        }];
        let rs = render_ruleset(&model);
        let deny = rs
            .find("ip saddr 198.51.100.0/24 tcp dport 3389 drop")
            .unwrap();
        let inst = rs.find("ip daddr 172.30.0.2 drop").unwrap();
        assert!(
            deny < inst,
            "nacl deny must precede the instance default-deny"
        );
        // allow NACL entries produce no explicit line
        assert!(!rs.contains("nacl-allow"));
    }

    #[test]
    fn enforcement_mode_is_opt_in_and_capability_gated() {
        // not opted in -> disabled regardless of nft availability
        assert_eq!(
            resolve_enforcement_mode(None, || true),
            EnforcementMode::Disabled
        );
        assert_eq!(
            resolve_enforcement_mode(Some("0"), || true),
            EnforcementMode::Disabled
        );
        // opted in but nft missing -> degrade
        assert_eq!(
            resolve_enforcement_mode(Some("1"), || false),
            EnforcementMode::Disabled
        );
        // opted in + capable -> nftables
        assert_eq!(
            resolve_enforcement_mode(Some("nftables"), || true),
            EnforcementMode::Nftables
        );
        assert_eq!(
            resolve_enforcement_mode(Some("TRUE"), || true),
            EnforcementMode::Nftables
        );
    }

    #[test]
    fn group_by_subnet_sorts_and_attaches_nacls() {
        let instances = vec![
            (
                "net-a".to_string(),
                InstanceFirewall {
                    private_ip: "172.30.0.9".into(),
                    ingress: vec![],
                    egress: vec![],
                },
            ),
            (
                "net-a".to_string(),
                InstanceFirewall {
                    private_ip: "172.30.0.2".into(),
                    ingress: vec![],
                    egress: vec![],
                },
            ),
        ];
        let mut nacls = BTreeMap::new();
        nacls.insert(
            "net-a".to_string(),
            vec![NaclRule {
                egress: false,
                allow: false,
                protocol: "-1".into(),
                from_port: -1,
                to_port: -1,
                cidr: Some("10.0.0.0/8".into()),
            }],
        );
        let grouped = group_by_subnet(instances, nacls);
        assert_eq!(grouped.len(), 1);
        assert_eq!(grouped[0].instances[0].private_ip, "172.30.0.2");
        assert_eq!(grouped[0].instances[1].private_ip, "172.30.0.9");
        assert_eq!(grouped[0].nacl.len(), 1);
    }
}
