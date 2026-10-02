//! The `awsvpc` task ENI: its AWS-visible private IP and the data-plane route
//! to it.
//!
//! On ECS an `awsvpc` task gets an ENI in one of its subnets, and DescribeTasks
//! (and an ELBv2 `ip` target group the service registers it with) report that
//! ENI's private IP. Locally the task runs on a per-task bridge whose
//! addresses are not routable from fakecloud on Docker Desktop, podman machine
//! or a containerized fakecloud, so the two concerns are split:
//!
//! - the reported IP is allocated from the subnet's real CIDR (via the EC2
//!   lookup on the delivery bus), skipping the addresses AWS reserves and the
//!   IPs other live tasks in the account already hold;
//! - each container port is published on the host, and the ENI IP + container
//!   port is registered with [`fakecloud_core::dataplane`] so the ELBv2 data
//!   plane and health prober connect to the published port.

use std::collections::HashSet;
use std::net::Ipv4Addr;

/// CIDR used when the task names no subnet, or one the EC2 lookup doesn't
/// know (EC2 not wired, or a subnet ID made up by the caller).
pub(crate) const FALLBACK_CIDR: &str = "10.0.0.0/16";

/// Subnet ID reported when the task names none.
pub(crate) const FALLBACK_SUBNET: &str = "subnet-fakecloud";

/// Parse an IPv4 CIDR (`10.0.1.0/24`) into its network address and prefix.
fn parse_cidr(cidr: &str) -> Option<(u32, u32)> {
    let (addr, len) = cidr.trim().split_once('/')?;
    let addr: Ipv4Addr = addr.parse().ok()?;
    let len: u32 = len.parse().ok()?;
    if len > 32 {
        return None;
    }
    let mask = if len == 0 { 0 } else { u32::MAX << (32 - len) };
    Some((u32::from(addr) & mask, len))
}

/// FNV-1a, so a task's IP is stable for its ID across builds and platforms.
fn fnv1a(seed: &str) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in seed.bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0100_0000_01b3);
    }
    hash
}

/// A free private IP in `cidr` for the task `seed`. AWS reserves the first
/// four addresses of every subnet (network, VPC router, DNS, future use) and
/// the last (broadcast), so those are never handed out. The scan starts at an
/// offset derived from `seed` and skips `in_use`; `None` when the subnet is
/// too small or full.
pub(crate) fn allocate_eni_ip(
    cidr: &str,
    seed: &str,
    in_use: &HashSet<Ipv4Addr>,
) -> Option<Ipv4Addr> {
    let (network, len) = parse_cidr(cidr)?;
    // AWS subnets are /16../28; anything smaller has no usable address.
    if len > 28 {
        return None;
    }
    let size: u64 = 1u64 << (32 - len);
    let usable = size - 5;
    let start = fnv1a(seed) % usable;
    (0..usable)
        .map(|i| Ipv4Addr::from(network + 4 + ((start + i) % usable) as u32))
        .find(|ip| !in_use.contains(ip))
}

/// The subnet an `awsvpc` task's ENI lives in: the first subnet of its
/// `awsvpcConfiguration` (carried by a precreated ENI attachment, or by the
/// owning service's network configuration).
pub(crate) fn task_subnet(
    st: &crate::state::EcsState,
    task: &crate::state::Task,
) -> Option<String> {
    if let Some(subnet) = task
        .attachments
        .iter()
        .filter(|a| a.attachment_type == "eni")
        .flat_map(|a| a.details.iter())
        .find(|d| d.name == "subnetId")
        .map(|d| d.value.clone())
    {
        return Some(subnet);
    }
    let group = task.group.as_deref()?;
    let service_name = group.strip_prefix("service:")?;
    let key = crate::state::EcsState::service_key(&task.cluster_name, service_name);
    st.services
        .get(&key)?
        .network_configuration
        .as_ref()
        .and_then(first_subnet)
}

/// First subnet of a `networkConfiguration` value.
pub(crate) fn first_subnet(network_configuration: &serde_json::Value) -> Option<String> {
    network_configuration
        .get("awsvpcConfiguration")?
        .get("subnets")?
        .as_array()?
        .iter()
        .find_map(|s| s.as_str())
        .map(str::to_string)
}

/// ENI private IPs held by the account's other tasks that are not stopped.
pub(crate) fn eni_ips_in_use(st: &crate::state::EcsState, except_task: &str) -> HashSet<Ipv4Addr> {
    st.tasks
        .values()
        .filter(|t| t.task_id != except_task && t.last_status != "STOPPED")
        .flat_map(|t| t.attachments.iter())
        .filter(|a| a.attachment_type == "eni")
        .flat_map(|a| a.details.iter())
        .filter(|d| d.name == "privateIPv4Address")
        .filter_map(|d| d.value.parse().ok())
        .collect()
}

/// The task's ENI private IP, once attached.
pub(crate) fn task_eni_ip(task: &crate::state::Task) -> Option<String> {
    task.attachments
        .iter()
        .filter(|a| a.attachment_type == "eni")
        .flat_map(|a| a.details.iter())
        .find(|d| d.name == "privateIPv4Address")
        .map(|d| d.value.clone())
}

/// Record the attached ENI on the task: fill in a precreated attachment, or
/// add one. Real ECS reports `subnetId`, `networkInterfaceId`, `macAddress`,
/// `privateDnsName` and `privateIPv4Address`.
pub(crate) fn attach_eni(
    task: &mut crate::state::Task,
    eni_id: &str,
    subnet: &str,
    ip: Ipv4Addr,
    mac: &str,
) {
    use crate::state::AttachmentDetail;
    let details = vec![
        AttachmentDetail {
            name: "subnetId".into(),
            value: subnet.to_string(),
        },
        AttachmentDetail {
            name: "networkInterfaceId".into(),
            value: eni_id.to_string(),
        },
        AttachmentDetail {
            name: "macAddress".into(),
            value: mac.to_string(),
        },
        AttachmentDetail {
            name: "privateDnsName".into(),
            value: format!("ip-{}.ec2.internal", ip.to_string().replace('.', "-")),
        },
        AttachmentDetail {
            name: "privateIPv4Address".into(),
            value: ip.to_string(),
        },
    ];
    if let Some(existing) = task
        .attachments
        .iter_mut()
        .find(|a| a.attachment_type == "eni")
    {
        existing.status = "ATTACHED".into();
        existing.details = details;
    } else {
        task.attachments.push(crate::state::TaskAttachment {
            id: uuid::Uuid::new_v4().to_string(),
            attachment_type: "eni".into(),
            status: "ATTACHED".into(),
            details,
        });
    }
}

/// The ENI attachment a task gets at creation when it names `awsvpc`
/// subnets: `PRECREATED` with its `subnetId`, as ECS reports before the ENI
/// is attached.
pub(crate) fn precreated_attachment(
    network_configuration: Option<&serde_json::Value>,
) -> Option<crate::state::TaskAttachment> {
    let subnet = first_subnet(network_configuration?)?;
    Some(crate::state::TaskAttachment {
        id: uuid::Uuid::new_v4().to_string(),
        attachment_type: "eni".into(),
        status: "PRECREATED".into(),
        details: vec![crate::state::AttachmentDetail {
            name: "subnetId".into(),
            value: subnet,
        }],
    })
}

impl super::EcsRuntime {
    /// Attach the task's ENI: allocate its private IP from the subnet CIDR and
    /// record the attachment. Returns the IP.
    pub(crate) fn attach_task_eni(
        &self,
        state: &crate::state::SharedEcsState,
        account_id: &str,
        task_id: &str,
    ) -> String {
        let (subnet, in_use) = {
            let accounts = state.read();
            let Some(st) = accounts.get(account_id) else {
                return String::new();
            };
            let subnet = st.tasks.get(task_id).and_then(|t| task_subnet(st, t));
            (subnet, eni_ips_in_use(st, task_id))
        };
        // Lock order: the ECS state lock is released before the EC2 lookup.
        let cidr = subnet.as_deref().and_then(|s| {
            self.delivery_bus
                .as_ref()
                .and_then(|bus| bus.ec2_subnet_cidr(account_id, s))
        });
        let ip = cidr
            .as_deref()
            .and_then(|c| allocate_eni_ip(c, task_id, &in_use))
            .or_else(|| allocate_eni_ip(FALLBACK_CIDR, task_id, &in_use))
            .unwrap_or(Ipv4Addr::new(10, 0, 0, 4));
        let eni_id = format!("eni-{}", &uuid::Uuid::new_v4().simple().to_string()[..17]);
        let mac = format!(
            "02:{:02x}:{:02x}:{:02x}:{:02x}:{:02x}",
            rand::random::<u8>(),
            rand::random::<u8>(),
            rand::random::<u8>(),
            rand::random::<u8>(),
            rand::random::<u8>()
        );
        let subnet = subnet.unwrap_or_else(|| FALLBACK_SUBNET.to_string());
        let mut accounts = state.write();
        if let Some(task) = accounts
            .get_mut(account_id)
            .and_then(|st| st.tasks.get_mut(task_id))
        {
            attach_eni(task, &eni_id, &subnet, ip, &mac);
        }
        tracing::info!(task = %task_id, eni = %eni_id, ip = %ip, subnet = %subnet, "attached awsvpc ENI");
        ip.to_string()
    }

    /// Read back the host port each of `plan`'s TCP container ports was
    /// published on (by `owner`, the container holding the namespace) and
    /// register `eni_ip:<containerPort>` -> `sibling_host:<hostPort>` with the
    /// shared data-plane resolution. A port that can't be read stays
    /// unregistered (the target then resolves verbatim and fails health
    /// checks, rather than routing somewhere wrong).
    pub(crate) async fn publish_awsvpc_ports(
        &self,
        account_id: &str,
        eni_ip: &str,
        owner: &str,
        plan: &super::ContainerPlan,
    ) {
        for pm in plan
            .port_mappings
            .iter()
            .filter(|pm| pm.protocol.eq_ignore_ascii_case("tcp"))
        {
            let spec = format!("{}/tcp", pm.container_port);
            let out = tokio::process::Command::new(&self.cli)
                .args(["port", owner, &spec])
                .output()
                .await;
            let host_port = out.ok().filter(|o| o.status.success()).and_then(|o| {
                fakecloud_core::container_net::parse_published_port(&String::from_utf8_lossy(
                    &o.stdout,
                ))
            });
            let Some(host_port) = host_port else {
                tracing::warn!(
                    container = %plan.container_name,
                    port = pm.container_port,
                    "could not read the published host port of an awsvpc container port; \
                     load balancers cannot reach it"
                );
                continue;
            };
            fakecloud_core::dataplane::register_target(
                account_id,
                eni_ip,
                pm.container_port,
                fakecloud_core::dataplane::Endpoint::new(self.net.sibling_host.clone(), host_port),
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allocated_ip_is_inside_the_subnet_and_skips_reserved_addresses() {
        let none = HashSet::new();
        for seed in ["a", "b", "task-123", "0123456789abcdef"] {
            let ip = allocate_eni_ip("10.0.1.0/24", seed, &none).unwrap();
            let o = ip.octets();
            assert_eq!(&o[..3], &[10, 0, 1], "{ip}");
            assert!(
                (4..=254).contains(&o[3]),
                "reserved address handed out: {ip}"
            );
        }
        // Stable per task.
        assert_eq!(
            allocate_eni_ip("172.31.16.0/20", "t1", &none),
            allocate_eni_ip("172.31.16.0/20", "t1", &none)
        );
    }

    #[test]
    fn allocation_skips_ips_in_use_and_fails_when_full() {
        // A /28 has 16 addresses, 11 usable.
        let mut used = HashSet::new();
        for _ in 0..11 {
            let ip = allocate_eni_ip("10.1.0.0/28", "same-seed", &used).unwrap();
            assert!(used.insert(ip), "{ip} handed out twice");
        }
        assert_eq!(allocate_eni_ip("10.1.0.0/28", "same-seed", &used), None);
        assert_eq!(allocate_eni_ip("10.1.0.0/30", "x", &HashSet::new()), None);
        assert_eq!(allocate_eni_ip("not-a-cidr", "x", &HashSet::new()), None);
    }

    #[test]
    fn first_subnet_reads_awsvpc_configuration() {
        let nc = serde_json::json!({
            "awsvpcConfiguration": {"subnets": ["subnet-aaa", "subnet-bbb"]}
        });
        assert_eq!(first_subnet(&nc).as_deref(), Some("subnet-aaa"));
        assert_eq!(first_subnet(&serde_json::json!({})), None);
        let pre = precreated_attachment(Some(&nc)).unwrap();
        assert_eq!(pre.status, "PRECREATED");
        assert_eq!(pre.details[0].value, "subnet-aaa");
        assert!(precreated_attachment(None).is_none());
    }
}
