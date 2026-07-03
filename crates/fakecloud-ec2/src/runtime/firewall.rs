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

/// A subnet-level NACL entry. NACLs are stateless and apply to the whole
/// subnet; AWS evaluates them in ascending `rule_number` order, first match
/// wins (so a lower-numbered `allow` shadows a higher-numbered `deny` for the
/// same traffic).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NaclRule {
    /// AWS rule number; lower numbers evaluate first.
    pub rule_number: i64,
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
    // `add table` first so the following `flush` doesn't error on the *first*
    // apply (when the table doesn't exist yet) — which would fail the entire
    // `nft -f -` load and leave enforcement silently off. `add` is idempotent;
    // `add`+`flush`+re-add is the canonical atomic-replace idiom.
    out.push_str(&format!("add table {TABLE}\n"));
    out.push_str(&format!("flush table {TABLE}\n"));
    out.push_str(&format!("table {TABLE} {{\n"));
    out.push_str("  chain forward {\n");
    out.push_str("    type filter hook forward priority -5; policy accept;\n");
    // Stateful: let replies through so SG rules only need to describe the
    // opening direction, matching AWS security-group semantics.
    out.push_str("    ct state established,related accept\n");

    for subnet in subnets {
        out.push_str(&format!("    # subnet {}\n", subnet.network_name));

        // Subnet-wide NACL denies, evaluated in ascending rule-number order so
        // a lower-numbered `allow` shadows a higher-numbered `deny` for the
        // same traffic (AWS first-match semantics). A deny is emitted as a drop
        // only when no earlier-numbered allow covers the identical
        // direction/protocol/ports/CIDR — otherwise the allow wins and the deny
        // never fires (bug-hunt 2026-06-18 finding 1.4). NACL allows ride the
        // default-accept policy (the SG layer below still applies; NACL and SG
        // are independent gates, both must permit).
        let mut ordered = subnet.nacl.clone();
        ordered.sort_by_key(|r| r.rule_number);
        for (i, rule) in ordered.iter().enumerate() {
            if rule.allow {
                continue;
            }
            let shadowed = ordered[..i]
                .iter()
                .any(|earlier| earlier.allow && nacl_same_traffic(earlier, rule));
            if shadowed {
                continue;
            }
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

/// The bridge-family table fakecloud owns for **same-subnet L2 enforcement**.
/// Kept separate from the `inet` table so it can be applied in its own
/// best-effort `nft -f -` — a kernel without `nf_conntrack_bridge` rejects the
/// `ct state` line, and isolating it means that failure never takes the `inet`
/// table (cross-subnet routed enforcement) down with it.
const BRIDGE_TABLE: &str = "bridge fakecloud_ec2_l2";

/// Render the **bridge-family** mirror of [`render_ruleset`].
///
/// Traffic between two containers on the *same* fakecloud subnet bridge is
/// L2-switched between bridge ports and only reaches the `inet` `forward` hook
/// when `bridge-nf-call-iptables=1` — and on some kernels/runners not even
/// then. The `bridge`-family `forward` hook sees those forwarded frames
/// directly, so mirroring the SG/NACL drops here makes same-subnet enforcement
/// hold regardless of the bridge-netfilter sysctl. IPv4 matches are guarded
/// with `ether type ip` (required in the bridge family before an `ip` match);
/// `ct state established,related` keeps replies flowing statefully via
/// `nf_conntrack_bridge`.
pub fn render_bridge_ruleset(subnets: &[SubnetFirewall]) -> String {
    let mut out = String::new();
    out.push_str(&format!("add table {BRIDGE_TABLE}\n"));
    out.push_str(&format!("flush table {BRIDGE_TABLE}\n"));
    out.push_str(&format!("table {BRIDGE_TABLE} {{\n"));
    out.push_str("  chain forward {\n");
    // Lower (earlier) priority than the default bridge filter so our decision
    // lands before anything else in the bridge path.
    out.push_str("    type filter hook forward priority -300; policy accept;\n");
    out.push_str("    ct state established,related accept\n");

    for subnet in subnets {
        out.push_str(&format!("    # subnet {}\n", subnet.network_name));

        let mut ordered = subnet.nacl.clone();
        ordered.sort_by_key(|r| r.rule_number);
        for (i, rule) in ordered.iter().enumerate() {
            if rule.allow {
                continue;
            }
            let shadowed = ordered[..i]
                .iter()
                .any(|earlier| earlier.allow && nacl_same_traffic(earlier, rule));
            if shadowed {
                continue;
            }
            if let Some(line) = render_nacl_drop(rule) {
                out.push_str(&format!("    ether type ip {line}\n"));
            }
        }

        for inst in &subnet.instances {
            for rule in &inst.ingress {
                out.push_str(&format!(
                    "    ether type ip {}\n",
                    render_rule(rule, Direction::Ingress, &inst.private_ip)
                ));
            }
            out.push_str(&format!(
                "    ether type ip ip daddr {} drop comment \"default-deny ingress\"\n",
                inst.private_ip
            ));

            for rule in &inst.egress {
                out.push_str(&format!(
                    "    ether type ip {}\n",
                    render_rule(rule, Direction::Egress, &inst.private_ip)
                ));
            }
            out.push_str(&format!(
                "    ether type ip ip saddr {} drop comment \"default-deny egress\"\n",
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

/// Whether two NACL entries match the *same* traffic (same direction,
/// protocol, port range, CIDR) — used to decide when a lower-numbered allow
/// shadows a higher-numbered deny. Conservative: only exact matches shadow, so
/// partially-overlapping rules still emit their drop (safer to over-deny than
/// to silently allow).
fn nacl_same_traffic(a: &NaclRule, b: &NaclRule) -> bool {
    a.egress == b.egress
        && a.protocol == b.protocol
        && a.from_port == b.from_port
        && a.to_port == b.to_port
        && a.cidr == b.cidr
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
        // An unrecognized protocol is interpolated into the nft script, so
        // restrict it to the protocol-token charset `[a-z0-9-]` to avoid
        // ruleset injection (finding 2.2); anything else emits no proto match.
        other if other.chars().all(|c| c.is_ascii_alphanumeric() || c == '-') => {
            parts.push(format!("ip protocol {other}"))
        }
        _ => {}
    }
}

/// Drop `0.0.0.0/0` (which nft rejects as a no-op match) to `None`, and strip a
/// redundant `/32` host suffix so single-host rules read cleanly.
///
/// Also **sanitizes**: the CIDR comes from an Authorize/RevokeSecurityGroup
/// param and is interpolated raw into the `nft -f -` script, so a value
/// containing nft metacharacters (whitespace, `;`, `{`, newline, …) could
/// inject ruleset syntax. Anything outside the IPv4/IPv6-CIDR character set
/// `[0-9a-fA-F.:/]` is rejected to `None` (the match clause is dropped, never
/// the whole rule), closing that injection surface (bug-hunt 2026-06-18
/// finding 2.2).
fn normalized_cidr(cidr: &Option<String>) -> Option<String> {
    let c = cidr.as_deref()?;
    if c == "0.0.0.0/0" || c.is_empty() {
        return None;
    }
    if !c
        .chars()
        .all(|ch| ch.is_ascii_hexdigit() || matches!(ch, '.' | ':' | '/'))
    {
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
/// `FAKECLOUD_EC2_SG_ENFORCEMENT` must be set to `1`/`true`/`nftables`, `nft`
/// must be runnable, AND the daemon must run on this host's network namespace
/// (`host_local`). `env`, `host_local`, and `nft_probe` are injected so the
/// decision is unit-testable without touching the environment or running `nft`.
///
/// `host_local` guards the false-positive on Docker Desktop / podman-machine
/// (macOS/Windows): there the per-subnet bridges live inside the daemon's Linux
/// VM, so `nft` on the host installs rules against the wrong netfilter and
/// silently filters nothing — yet the probe would pass on a Linux box. We treat
/// only a native-Linux host as able to filter (bug-hunt 2026-06-18 finding 1.5),
/// so `enforced` never claims active enforcement that can't take effect.
pub fn resolve_enforcement_mode(
    env: Option<&str>,
    host_local: bool,
    nft_probe: impl FnOnce() -> bool,
) -> EnforcementMode {
    let opted_in = matches!(
        env.map(|v| v.to_ascii_lowercase()).as_deref(),
        Some("1") | Some("true") | Some("nftables") | Some("on")
    );
    if !opted_in || !host_local {
        return EnforcementMode::Disabled;
    }
    if nft_probe() {
        EnforcementMode::Nftables
    } else {
        EnforcementMode::Disabled
    }
}

/// Whether the container daemon shares this process's network namespace, so
/// host nftables rules actually see the inter-container traffic. True only on a
/// native-Linux host; Docker Desktop / podman-machine on macOS/Windows run the
/// daemon in a separate Linux VM. (Honest default; can be overridden by the
/// caller when fakecloud and the daemon are known to share a netns.)
pub fn host_shares_daemon_netns() -> bool {
    cfg!(target_os = "linux")
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
        // `add table` must precede `flush table` so the first apply (table
        // absent) doesn't error and abort the whole ruleset load.
        let add = rs.find("add table inet fakecloud_ec2").expect("add table");
        let flush = rs
            .find("flush table inet fakecloud_ec2")
            .expect("flush table");
        assert!(add < flush, "add table must come before flush:\n{rs}");
        assert!(rs.contains("ct state established,related accept"));
        assert!(rs.contains("ip daddr 172.30.0.2 ip saddr 10.0.0.0/8 tcp dport 22 accept"));
        assert!(rs.contains("ip daddr 172.30.0.2 drop comment \"default-deny ingress\""));
        // egress had no explicit allows -> still a default-deny line
        assert!(rs.contains("ip saddr 172.30.0.2 drop comment \"default-deny egress\""));
    }

    #[test]
    fn bridge_ruleset_mirrors_inet_with_ether_type_guard() {
        let model = vec![SubnetFirewall {
            network_name: "fakecloud-subnet-a".into(),
            instances: vec![InstanceFirewall {
                private_ip: "172.30.0.2".into(),
                ingress: vec![tcp(22, Some("10.0.0.0/8"))],
                egress: vec![],
            }],
            nacl: vec![],
        }];
        let rs = render_bridge_ruleset(&model);
        // Its own bridge-family table, atomically add-before-flush.
        let add = rs
            .find("add table bridge fakecloud_ec2_l2")
            .expect("add table");
        let flush = rs
            .find("flush table bridge fakecloud_ec2_l2")
            .expect("flush table");
        assert!(add < flush, "add table must come before flush:\n{rs}");
        // Bridge-family forward hook + conntrack for stateful replies.
        assert!(rs.contains("type filter hook forward priority -300; policy accept;"));
        assert!(rs.contains("ct state established,related accept"));
        // Every IPv4 match is guarded with `ether type ip` (required in the
        // bridge family before an `ip` match).
        assert!(rs
            .contains("ether type ip ip daddr 172.30.0.2 ip saddr 10.0.0.0/8 tcp dport 22 accept"));
        assert!(
            rs.contains("ether type ip ip daddr 172.30.0.2 drop comment \"default-deny ingress\"")
        );
        assert!(
            rs.contains("ether type ip ip saddr 172.30.0.2 drop comment \"default-deny egress\"")
        );
        // No bare `ip daddr`/`ip saddr` outside an `ether type ip` guard.
        for line in rs.lines().map(str::trim) {
            if line.starts_with("ip daddr") || line.starts_with("ip saddr") {
                panic!("unguarded ip match in bridge family:\n{line}");
            }
        }
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
    fn cidr_with_nft_metacharacters_is_dropped_not_injected() {
        // A CIDR carrying nft syntax (`;`, spaces, words) must never reach the
        // `nft -f -` script (finding 2.2). The match clause is omitted; the
        // rule still renders safely and terminates in `accept`.
        let r = tcp(22, Some("10.0.0.0/8; drop comment \"x\""));
        let line = render_rule(&r, Direction::Ingress, "172.30.0.2");
        assert!(!line.contains(';'), "no injected semicolon: {line}");
        assert!(!line.contains("comment"), "no injected tokens: {line}");
        assert!(
            !line.contains("ip saddr"),
            "malformed cidr clause omitted: {line}"
        );
        assert!(line.ends_with("accept"), "rule still valid: {line}");
    }

    #[test]
    fn unknown_protocol_with_bad_chars_emits_no_proto_match() {
        let r = FirewallRule {
            protocol: "tcp; drop".into(),
            from_port: -1,
            to_port: -1,
            cidr: None,
        };
        let line = render_rule(&r, Direction::Ingress, "172.30.0.2");
        assert!(
            !line.contains(';') && !line.contains("ip protocol"),
            "{line}"
        );
        assert_eq!(line, "ip daddr 172.30.0.2 accept");
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
                rule_number: 100,
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
    fn nacl_lower_numbered_allow_shadows_higher_numbered_deny() {
        // AWS first-match-by-rule-number: `100 allow tcp/22 10/8` must win over
        // `200 deny tcp/22 10/8`, so the deny is NOT emitted (finding 1.4).
        let nacl_entry = |rule_number, allow| NaclRule {
            rule_number,
            egress: false,
            allow,
            protocol: "tcp".into(),
            from_port: 22,
            to_port: 22,
            cidr: Some("10.0.0.0/8".into()),
        };
        let model = vec![SubnetFirewall {
            network_name: "fakecloud-subnet-a".into(),
            instances: vec![InstanceFirewall {
                private_ip: "172.30.0.2".into(),
                ingress: vec![],
                egress: vec![],
            }],
            // Intentionally out of order to exercise the sort.
            nacl: vec![nacl_entry(200, false), nacl_entry(100, true)],
        }];
        let rs = render_ruleset(&model);
        assert!(
            !rs.contains("ip saddr 10.0.0.0/8 tcp dport 22 drop"),
            "a lower-numbered allow must shadow the deny:\n{rs}"
        );

        // Reverse the precedence: deny 100 before allow 200 -> deny fires.
        let model2 = vec![SubnetFirewall {
            network_name: "fakecloud-subnet-a".into(),
            instances: vec![],
            nacl: vec![nacl_entry(100, false), nacl_entry(200, true)],
        }];
        assert!(render_ruleset(&model2).contains("ip saddr 10.0.0.0/8 tcp dport 22 drop"));
    }

    #[test]
    fn enforcement_mode_is_opt_in_and_capability_gated() {
        // not opted in -> disabled regardless of nft availability / host
        assert_eq!(
            resolve_enforcement_mode(None, true, || true),
            EnforcementMode::Disabled
        );
        assert_eq!(
            resolve_enforcement_mode(Some("0"), true, || true),
            EnforcementMode::Disabled
        );
        // opted in but nft missing -> degrade
        assert_eq!(
            resolve_enforcement_mode(Some("1"), true, || false),
            EnforcementMode::Disabled
        );
        // opted in + capable but daemon not host-local (Docker Desktop/VM) ->
        // degrade rather than falsely claim enforced (finding 1.5)
        assert_eq!(
            resolve_enforcement_mode(Some("1"), false, || true),
            EnforcementMode::Disabled
        );
        // opted in + host-local + capable -> nftables
        assert_eq!(
            resolve_enforcement_mode(Some("nftables"), true, || true),
            EnforcementMode::Nftables
        );
        assert_eq!(
            resolve_enforcement_mode(Some("TRUE"), true, || true),
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
                rule_number: 100,
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
