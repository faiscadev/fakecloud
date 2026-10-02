//! Backing-container runtime for EC2 instances.
//!
//! `RunInstances` spins a real container per instance; the instance
//! lifecycle (`Start`/`Stop`/`Reboot`/`Terminate`) maps onto the container
//! lifecycle, and `DescribeInstances` reports the container's real private
//! IP. The container can run either as a local Docker/Podman container (the
//! default) or as a native Kubernetes Pod (`FAKECLOUD_EC2_BACKEND=k8s` or the
//! global `FAKECLOUD_CONTAINER_BACKEND=k8s`).
//!
//! Operations are keyed by **instance id**, not the backend handle: a
//! Kubernetes Pod can't be stopped and restarted in place, so `Stop` deletes
//! the Pod and `Start`/`Reboot` recreate it. The runtime therefore keeps,
//! per instance, the handle plus enough of the original request (image,
//! user-data) to recreate the backing container deterministically.
//!
//! The runtime is strictly additive: when no container backend is available
//! the control plane keeps its metadata-faithful behaviour (synthesized IPs,
//! state transitions) so every API call still succeeds. Real container
//! backing is best-effort fidelity layered on top.

pub mod firewall;
mod imds;
mod k8s;
pub mod netpolicy;

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use parking_lot::RwLock;

use firewall::{
    render_bridge_ruleset, render_ruleset, resolve_enforcement_mode, EnforcementMode,
    InstanceRules, SubnetFirewall,
};

/// Default base image an instance's container runs. AMIs don't map to a
/// concrete OS image, so we boot a real Amazon Linux container by default
/// (overridable via `FAKECLOUD_EC2_DEFAULT_IMAGE`, e.g. to a lighter image
/// in CI). The container is kept alive with `tail -f /dev/null` — EC2
/// instances are long-running hosts, not one-shot tasks. `tail` is used
/// rather than `sleep infinity` so any base image works (busybox `sleep`
/// rejects `infinity`).
const DEFAULT_IMAGE_ENV: &str = "FAKECLOUD_EC2_DEFAULT_IMAGE";
const DEFAULT_IMAGE: &str = "amazonlinux:2023";

#[derive(Debug, thiserror::Error)]
pub enum RuntimeError {
    #[error("container failed to start: {0}")]
    ContainerStartFailed(String),
}

/// Error initializing the Kubernetes backend at startup. Surfaced to the
/// operator so a misconfigured cluster fails fast rather than silently
/// falling back to Docker.
#[derive(Debug, thiserror::Error)]
pub enum BackendInitError {
    #[error(transparent)]
    Env(#[from] fakecloud_k8s::K8sEnvError),
    #[error(transparent)]
    PodConfig(#[from] fakecloud_k8s::K8sPodConfigError),
    #[error("failed to connect to the Kubernetes cluster: {0}")]
    Connect(String),
}

/// A running instance's backing container.
#[derive(Debug, Clone)]
pub struct RunningInstance {
    /// Backend-specific handle: a Docker container id, or a Pod name.
    pub container_id: String,
    /// The instance's private IP — the container's address on the daemon
    /// network (Docker) or the Pod IP (k8s).
    pub private_ip: String,
    /// Name of the backing daemon network the container was attached to
    /// (`fakecloud-subnet-<id>`), or `None` when it ran on the default bridge
    /// (no network spec, or creation failed and we fell back). Surfaced for
    /// introspection (#1745 phase 5).
    pub network: Option<String>,
}

/// The L3 placement of an instance's backing container: which subnet it lands
/// in and whether that subnet is private.
///
/// Per-subnet networks give the isolation #1745 wants for free: two instances
/// in the same subnet share a bridge and can talk; instances in different
/// subnets / VPCs land on different bridges and cannot route to each other.
#[derive(Debug, Clone)]
pub struct InstanceNetwork {
    /// The EC2 subnet id the instance launched into.
    pub subnet_id: String,
    /// True when the subnet has no `0.0.0.0/0 -> igw` route (private): the
    /// backing network is created `--internal` (no NAT to host/internet).
    pub internal: bool,
}

/// The daemon network name backing an EC2 subnet. Stable per subnet so every
/// instance in the subnet attaches to the same bridge.
pub fn subnet_network_name(subnet_id: &str) -> String {
    format!("fakecloud-subnet-{subnet_id}")
}

/// How this runtime isolates instance traffic, surfaced by the
/// `/_fakecloud/ec2/instance-networks` introspection endpoint so users can
/// answer "why can't X reach Y" — which backend, which SG-enforcement
/// mechanism, and whether it's actually active vs degraded to metadata-only.
#[derive(Debug, Clone)]
pub struct NetworkIsolationSummary {
    /// `docker` | `podman` | `kubernetes`.
    pub backend: &'static str,
    /// `nftables` (Docker host firewall) | `networkpolicy` (k8s) | `disabled`.
    pub sg_enforcement: &'static str,
    /// Whether security-group rules are actually enforced. False means rules
    /// are tracked but not applied (no `CAP_NET_ADMIN`, or a CNI that ignores
    /// NetworkPolicy) — phase-2 L3 isolation still holds.
    pub enforced: bool,
}

/// What the runtime remembers per instance so it can drive the backing
/// container's lifecycle and recreate it (k8s `Start`/`Reboot`).
#[derive(Debug, Clone)]
struct InstanceRecord {
    /// Docker container id, or Pod name.
    handle: String,
    /// Resolved base image, captured at `RunInstances` so a recreate is
    /// identical even if `FAKECLOUD_EC2_DEFAULT_IMAGE` later changes.
    image: String,
    /// Base64 user-data to re-run on recreate, if any.
    user_data: Option<String>,
    /// The instance's tags, captured at `RunInstances`. Reserved
    /// `fakecloud-k8s/*` entries drive per-instance Pod scheduling and must
    /// survive a k8s `Start`/`Reboot` recreate, so they're stored here
    /// rather than re-read from the control plane.
    tags: BTreeMap<String, String>,
    /// The instance's subnet placement, captured at `RunInstances` so a k8s
    /// `Start`/`Reboot` recreate re-applies the same network and phase-5
    /// introspection can report the backing network. `None` in metadata-only
    /// network mode.
    network: Option<InstanceNetwork>,
    /// The backing container's current private IP (refreshed on start and
    /// k8s recreate), which load balancer forwarders connect to.
    private_ip: String,
}

/// The selected backing-container backend.
#[derive(Debug, Clone)]
enum InstanceBackend {
    Docker(DockerInstances),
    K8s(k8s::K8sInstances),
}

/// Host firewall enforcement for security groups + NACLs (#1745 phase 3).
///
/// The network-driver abstraction the issue asks for: today there is one real
/// driver (nftables) plus the degraded no-op, selected once at construction.
/// Branching on podman vs docker isn't needed explicitly — rootless podman
/// can't touch the host firewall, so the `nft list ruleset` capability probe
/// already degrades it; rootful podman with netavark passes the same probe.
#[derive(Debug, Clone)]
pub struct FirewallEnforcer {
    mode: EnforcementMode,
}

impl FirewallEnforcer {
    /// Resolve the enforcement mode from `FAKECLOUD_EC2_SG_ENFORCEMENT` and an
    /// `nft` capability probe, warning once when enforcement was requested but
    /// can't be backed (so the operator knows it degraded, not silently).
    fn detect() -> Self {
        let requested = std::env::var("FAKECLOUD_EC2_SG_ENFORCEMENT").ok();
        let mode = resolve_enforcement_mode(
            requested.as_deref(),
            firewall::host_shares_daemon_netns(),
            firewall::nft_available,
        );
        if requested.is_some() && mode == EnforcementMode::Disabled {
            tracing::warn!(
                "EC2 security-group enforcement was requested but it can't take effect here \
                 (needs nftables + CAP_NET_ADMIN on a native-Linux host whose daemon shares this \
                 network namespace — Docker Desktop / podman-machine run the daemon in a VM); \
                 falling back to metadata-only (phase-2 L3 isolation stays active, security-group \
                 rules are tracked but not enforced)"
            );
        } else if mode == EnforcementMode::Nftables {
            tracing::info!("EC2 security-group enforcement active via nftables");
        }
        Self { mode }
    }

    /// Disabled enforcer (k8s backend, or no container runtime).
    fn disabled() -> Self {
        Self {
            mode: EnforcementMode::Disabled,
        }
    }

    pub fn mode(&self) -> EnforcementMode {
        self.mode
    }

    pub fn enabled(&self) -> bool {
        self.mode != EnforcementMode::Disabled
    }

    /// Atomically swap in the rendered ruleset via `nft -f -`. No-op when
    /// disabled. Best-effort: a failed apply logs and leaves the previous
    /// ruleset in place rather than erroring the originating API call.
    async fn reconcile(&self, subnets: &[SubnetFirewall]) {
        if self.mode == EnforcementMode::Disabled {
            return;
        }
        // Instances in the same subnet share one Linux bridge; their traffic is
        // L2-switched and only traverses the `forward` chain (where our SG rules
        // live) when bridge netfilter is enabled. Without this, same-subnet SG
        // rules silently filter nothing — exactly what the real-packet E2E
        // caught. Needs CAP_NET_ADMIN (which the enforcer holds) and the
        // `modprobe`/`sysctl` binaries (shipped via kmod/procps in the image).
        // Warn rather than swallow the error: a missing binary or a failed call
        // means enforcement degrades to filtering nothing, and the operator who
        // opted in deserves to know (bug-audit 2026-06-20, 0.B1).
        match tokio::process::Command::new("modprobe")
            .arg("br_netfilter")
            .output()
            .await
        {
            Ok(o) if o.status.success() => {}
            Ok(o) => tracing::warn!(
                stderr = %String::from_utf8_lossy(&o.stderr).trim(),
                "modprobe br_netfilter failed; same-subnet security-group enforcement may filter nothing"
            ),
            Err(e) => tracing::warn!(
                error = %e,
                "could not run modprobe (is kmod installed?); same-subnet security-group enforcement may filter nothing"
            ),
        }
        match tokio::process::Command::new("sysctl")
            .args(["-w", "net.bridge.bridge-nf-call-iptables=1"])
            .output()
            .await
        {
            Ok(o) if o.status.success() => {}
            Ok(o) => tracing::warn!(
                stderr = %String::from_utf8_lossy(&o.stderr).trim(),
                "sysctl bridge-nf-call-iptables=1 failed; same-subnet security-group enforcement may filter nothing"
            ),
            Err(e) => tracing::warn!(
                error = %e,
                "could not run sysctl (is procps installed?); same-subnet security-group enforcement may filter nothing"
            ),
        }
        use tokio::io::AsyncWriteExt;
        // Load a rendered ruleset via `nft -f -`. `required=false` marks the
        // best-effort same-subnet bridge table: a kernel without
        // `nf_conntrack_bridge` rejects its `ct state` line, and since the inet
        // table is applied independently first, that rejection is logged at
        // debug (degraded same-subnet enforcement) rather than warn.
        async fn load_nft(label: &str, ruleset: &str, subnets: usize, required: bool) {
            let mut child = match tokio::process::Command::new("nft")
                .args(["-f", "-"])
                .stdin(std::process::Stdio::piped())
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::piped())
                .spawn()
            {
                Ok(c) => c,
                Err(e) => {
                    tracing::warn!(error = %e, table = label, "failed to spawn nft; security-group ruleset not applied");
                    return;
                }
            };
            if let Some(mut stdin) = child.stdin.take() {
                let _ = stdin.write_all(ruleset.as_bytes()).await;
                let _ = stdin.shutdown().await;
            }
            match child.wait_with_output().await {
                Ok(out) if out.status.success() => {
                    tracing::debug!(
                        subnets,
                        table = label,
                        "applied EC2 security-group nft ruleset"
                    );
                }
                Ok(out) => {
                    let stderr = String::from_utf8_lossy(&out.stderr);
                    let stderr = stderr.trim();
                    if required {
                        tracing::warn!(table = label, stderr = %stderr, "nft rejected the security-group ruleset; leaving the previous ruleset in place");
                    } else {
                        tracing::debug!(table = label, stderr = %stderr, "bridge-family SG ruleset not applied (kernel may lack nf_conntrack_bridge); same-subnet enforcement degraded to inet table only");
                    }
                }
                Err(e) => tracing::warn!(error = %e, table = label, "nft apply failed"),
            }
        }
        let n = subnets.len();
        // inet: cross-subnet routed enforcement (required). bridge: same-subnet
        // L2 enforcement that the inet forward hook misses for bridged frames.
        load_nft("inet fakecloud_ec2", &render_ruleset(subnets), n, true).await;
        load_nft(
            "bridge fakecloud_ec2_l2",
            &render_bridge_ruleset(subnets),
            n,
            false,
        )
        .await;
    }
}

#[derive(Debug, Clone)]
pub struct Ec2Runtime {
    backend: InstanceBackend,
    /// Per-instance backing records, keyed by EC2 instance id, so the
    /// lifecycle operations and reset/shutdown teardown work without
    /// consulting service state.
    instances: Arc<RwLock<HashMap<String, InstanceRecord>>>,
    /// Host firewall enforcer for security groups + NACLs.
    firewall: FirewallEnforcer,
    /// Serializes firewall reconciles. Reconcile is fired from many concurrent
    /// background tasks (per SG/NACL/lifecycle event); without this, two
    /// reconciles built from divergent state could interleave so the k8s
    /// apply+prune of one deletes a policy the other just applied (bug-hunt
    /// 2026-06-18 finding 4.3). Holding it across the whole reconcile makes the
    /// last-started reconcile the last-applied for both backends.
    reconcile_lock: Arc<tokio::sync::Mutex<()>>,
    /// Per-instance lifecycle locks. Every container-lifecycle task (boot,
    /// start, stop, terminate, reboot, recovery) holds its instance's lock
    /// across the runtime call and the state reconcile that follows, and
    /// re-reads the instance's desired state once it holds it. So a stop
    /// decided against a `stopped` row can never land on a container a later
    /// StartInstances just started: that start waits, then sees the row it
    /// is asked to converge to.
    lifecycle_locks: Arc<parking_lot::Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>>,
}

impl Ec2Runtime {
    /// Construct the Docker/Podman backend. Returns `None` when no container
    /// CLI is available — callers then run in metadata-only mode.
    /// `server_port` is fakecloud's bound port, which instance IMDS proxies
    /// forward to.
    pub fn new(server_port: u16) -> Option<Self> {
        let cli = fakecloud_core::container_net::detect_container_cli()?;
        let podman = fakecloud_core::container_net::is_podman(&cli);
        let net = fakecloud_core::container_net::HostNetworking::detect(&cli);
        Some(Self {
            backend: InstanceBackend::Docker(DockerInstances {
                cli,
                podman,
                instance_id: format!("fakecloud-{}", std::process::id()),
                net,
                server_port,
                helper_image: Arc::new(tokio::sync::Mutex::new(None)),
                forwarders: Arc::new(parking_lot::Mutex::new(HashMap::new())),
            }),
            instances: Arc::new(RwLock::new(HashMap::new())),
            firewall: FirewallEnforcer::detect(),
            reconcile_lock: Arc::new(tokio::sync::Mutex::new(())),
            lifecycle_locks: Arc::new(parking_lot::Mutex::new(HashMap::new())),
        })
    }

    /// Construct the Kubernetes backend. `server_port` is fakecloud's bound
    /// port (used when `FAKECLOUD_K8S_SELF_URL` omits one). Fails fast on
    /// misconfiguration — never silently degrades to Docker.
    pub async fn new_k8s(server_port: u16) -> Result<Self, BackendInitError> {
        let backend = k8s::K8sInstances::from_env(server_port).await?;
        Ok(Self {
            backend: InstanceBackend::K8s(backend),
            instances: Arc::new(RwLock::new(HashMap::new())),
            // k8s isolation is a NetworkPolicy concern (phase 4), not host nft.
            firewall: FirewallEnforcer::disabled(),
            reconcile_lock: Arc::new(tokio::sync::Mutex::new(())),
            lifecycle_locks: Arc::new(parking_lot::Mutex::new(HashMap::new())),
        })
    }

    /// The firewall enforcer, so the control plane can skip building the model
    /// when enforcement is disabled and report the mode for introspection.
    pub fn firewall(&self) -> &FirewallEnforcer {
        &self.firewall
    }

    /// Re-render and atomically apply the security-group/NACL ruleset for the
    /// given per-subnet model. No-op (cheap) when enforcement is disabled.
    /// Serialized against other reconciles (finding 4.3).
    pub async fn reconcile_firewall(&self, subnets: Vec<SubnetFirewall>) {
        let _guard = self.reconcile_lock.lock().await;
        self.firewall.reconcile(&subnets).await;
    }

    /// Whether this runtime backs network isolation with real enforcement —
    /// host nftables (Docker, opt-in) or k8s NetworkPolicy. Lets the control
    /// plane skip building the firewall model entirely when neither applies.
    pub fn network_isolation_enforced(&self) -> bool {
        self.firewall.enabled() || self.is_k8s()
    }

    /// True for the Kubernetes backend (isolation via NetworkPolicy).
    pub fn is_k8s(&self) -> bool {
        matches!(self.backend, InstanceBackend::K8s(_))
    }

    /// Apply one NetworkPolicy per instance for the k8s backend. No-op on the
    /// Docker backend (which uses nftables instead). Serialized against other
    /// reconciles so a concurrent apply+prune can't delete a just-applied
    /// policy (finding 4.3).
    pub async fn reconcile_network_policies(&self, rules: Vec<InstanceRules>) {
        if let InstanceBackend::K8s(k) = &self.backend {
            let _guard = self.reconcile_lock.lock().await;
            k.reconcile_network_policies(&rules).await;
        }
    }

    /// A snapshot of how this runtime isolates instance traffic, for the
    /// `/_fakecloud/ec2/instance-networks` introspection endpoint (#1745 ph5).
    pub fn network_isolation_summary(&self) -> NetworkIsolationSummary {
        match &self.backend {
            InstanceBackend::Docker(d) => NetworkIsolationSummary {
                backend: if d.podman { "podman" } else { "docker" },
                sg_enforcement: match self.firewall.mode() {
                    EnforcementMode::Nftables => "nftables",
                    EnforcementMode::Disabled => "disabled",
                },
                enforced: self.firewall.enabled(),
            },
            InstanceBackend::K8s(k) => NetworkIsolationSummary {
                backend: "kubernetes",
                sg_enforcement: "networkpolicy",
                // NetworkPolicies are always created; "enforced" reflects
                // whether the detected CNI actually applies them.
                enforced: k.cni_enforces(),
            },
        }
    }

    /// Name of the active backend, for logging.
    pub fn cli_name(&self) -> &str {
        match &self.backend {
            InstanceBackend::Docker(d) => &d.cli,
            InstanceBackend::K8s(_) => "kubernetes",
        }
    }

    /// Boot a container for an instance. `user_data` is the base64-encoded
    /// user-data as received on the wire (RunInstances `UserData`), run at
    /// boot the way cloud-init would, if present.
    pub async fn run_instance(
        &self,
        account_id: &str,
        instance_id: &str,
        user_data: Option<&str>,
        tags: &BTreeMap<String, String>,
        network: Option<&InstanceNetwork>,
    ) -> Result<RunningInstance, RuntimeError> {
        let image = default_image();
        let running = match &self.backend {
            // Docker attaches the container to the subnet's per-VPC bridge for
            // L3 isolation. k8s pods share a flat network; isolation there is a
            // NetworkPolicy concern handled separately (#1745 phase 4).
            InstanceBackend::Docker(d) => {
                d.run_instance(account_id, instance_id, &image, user_data, network)
                    .await?
            }
            InstanceBackend::K8s(k) => k.spawn_pod(instance_id, &image, user_data, tags).await?,
        };
        self.instances.write().insert(
            instance_id.to_string(),
            InstanceRecord {
                handle: running.container_id.clone(),
                image,
                user_data: user_data.map(str::to_string),
                tags: tags.clone(),
                network: network.cloned(),
                private_ip: running.private_ip.clone(),
            },
        );
        Ok(running)
    }

    /// Stop an instance's backing container (maps to `StopInstances`).
    /// Docker stops the container in place; k8s deletes the Pod (recreated
    /// on the next `Start`).
    pub async fn stop_instance(&self, instance_id: &str) {
        let Some(handle) = self.handle_of(instance_id) else {
            return;
        };
        match &self.backend {
            InstanceBackend::Docker(d) => {
                d.stop(&handle).await;
                // The namespace the sidecar joined and the IP the forwarders
                // target are gone with the container's network.
                d.remove_sidecars(instance_id).await;
            }
            InstanceBackend::K8s(k) => k.delete_pod(&handle).await,
        }
    }

    /// Start a previously-stopped instance (maps to `StartInstances`).
    /// Returns the running container's (possibly new) handle and private IP.
    /// Docker starts the existing container; k8s recreates the Pod under a new
    /// unique name, so the handle changes — callers should persist it.
    pub async fn start_instance(&self, instance_id: &str) -> Option<RunningInstance> {
        let record = self.instances.read().get(instance_id)?.clone();
        match &self.backend {
            InstanceBackend::Docker(d) => {
                // Same container; only the IP may change. The subnet network the
                // container was created on persists across stop/start.
                let private_ip = d.start(&record.handle).await?;
                // A started container has a fresh network namespace: give it
                // IMDS again.
                d.remove_sidecars(instance_id).await;
                d.start_imds(instance_id, &record.handle).await;
                self.update_ip(instance_id, &private_ip);
                Some(RunningInstance {
                    container_id: record.handle,
                    private_ip,
                    network: record
                        .network
                        .as_ref()
                        .map(|n| subnet_network_name(&n.subnet_id)),
                })
            }
            InstanceBackend::K8s(k) => {
                let running = k
                    .spawn_pod(
                        instance_id,
                        &record.image,
                        record.user_data.as_deref(),
                        &record.tags,
                    )
                    .await
                    .ok()?;
                self.update_handle(instance_id, &running.container_id);
                self.update_ip(instance_id, &running.private_ip);
                Some(running)
            }
        }
    }

    /// Restart an instance's backing container (maps to `RebootInstances`).
    /// Docker restarts in place; k8s deletes and recreates the Pod under a new
    /// name. Returns the running container's handle + IP when it changed (k8s),
    /// so callers can persist the new handle; `None` when nothing to update.
    pub async fn reboot_instance(&self, instance_id: &str) -> Option<RunningInstance> {
        let record = self.instances.read().get(instance_id).cloned()?;
        match &self.backend {
            InstanceBackend::Docker(d) => {
                d.reboot(&record.handle).await;
                // The restart replaced the network namespace the sidecar had
                // joined; the IP can change with it.
                d.remove_sidecars(instance_id).await;
                d.start_imds(instance_id, &record.handle).await;
                if let Some(ip) = d.inspect_ip(&record.handle).await {
                    self.update_ip(instance_id, &ip);
                }
                None
            }
            InstanceBackend::K8s(k) => {
                k.delete_pod(&record.handle).await;
                let running = k
                    .spawn_pod(
                        instance_id,
                        &record.image,
                        record.user_data.as_deref(),
                        &record.tags,
                    )
                    .await
                    .ok()?;
                self.update_handle(instance_id, &running.container_id);
                self.update_ip(instance_id, &running.private_ip);
                Some(running)
            }
        }
    }

    /// Serialize lifecycle work on one instance; see `lifecycle_locks`.
    pub async fn lock_lifecycle(&self, instance_id: &str) -> tokio::sync::OwnedMutexGuard<()> {
        let lock = self
            .lifecycle_locks
            .lock()
            .entry(instance_id.to_string())
            .or_default()
            .clone();
        lock.lock_owned().await
    }

    /// Remove an instance's backing container and its durable root-disk
    /// volume (maps to `TerminateInstances`). The volume is removed by the
    /// instance's account and id even when no runtime record exists: a boot
    /// that failed after creating it, or an instance recovered from disk as
    /// `stopped`, has a volume but no record. Instance ids are unique, so this
    /// never reaches another instance's volume.
    pub async fn terminate_instance(&self, account_id: &str, instance_id: &str) {
        let record = self.instances.write().remove(instance_id);
        // Terminated is final, so its lifecycle lock is never needed again; a
        // late task for the id creates a fresh one and only ever reaps.
        self.lifecycle_locks.lock().remove(instance_id);
        match &self.backend {
            InstanceBackend::Docker(d) => {
                d.remove_sidecars(instance_id).await;
                if let Some(record) = record {
                    d.remove(&record.handle).await;
                }
                // Terminate = volume gone, matching a deleted EBS root volume.
                // No-op when volumes are disabled.
                d.remove_data_volume(account_id, instance_id).await;
            }
            InstanceBackend::K8s(k) => {
                if let Some(record) = record {
                    k.delete_pod(&record.handle).await;
                }
            }
        }
    }

    /// Tear down every container this runtime spawned (used on reset and
    /// shutdown). The Docker backend leans on the shared reaper for any
    /// container it loses track of.
    pub async fn stop_all(&self) {
        let records: Vec<(String, InstanceRecord)> = {
            let mut instances = self.instances.write();
            instances.drain().collect()
        };
        for (instance_id, record) in records {
            match &self.backend {
                InstanceBackend::Docker(d) => {
                    d.remove_sidecars(&instance_id).await;
                    d.remove(&record.handle).await
                }
                InstanceBackend::K8s(k) => k.delete_pod(&record.handle).await,
            }
        }
    }

    /// Tear down the given `(account, instance_id)`s for a reset: their
    /// containers and data volumes. Instance ids are unique per instance, so
    /// this never reaches an instance launched after the reset; a boot still in
    /// flight for a reset instance reaps itself once it finds its row gone.
    /// Covers stopped instances recovered from disk too, which have no
    /// runtime record but still have a volume.
    pub async fn remove_instances(&self, instances: Vec<(String, String)>) {
        for (account_id, instance_id) in instances {
            // Wait out any lifecycle task in flight on this instance (a
            // reboot recreating its Pod, a boot), so the teardown removes
            // what that task leaves rather than racing it.
            let lifecycle = self.lock_lifecycle(&instance_id).await;
            let record = self.instances.write().remove(&instance_id);
            if let InstanceBackend::Docker(d) = &self.backend {
                d.remove_sidecars(&instance_id).await;
            }
            if let Some(record) = record {
                match &self.backend {
                    InstanceBackend::Docker(d) => d.remove(&record.handle).await,
                    InstanceBackend::K8s(k) => k.delete_pod(&record.handle).await,
                }
            }
            if let InstanceBackend::Docker(d) = &self.backend {
                d.remove_data_volume(&account_id, &instance_id).await;
            }
            drop(lifecycle);
            self.lifecycle_locks.lock().remove(&instance_id);
        }
    }

    /// Sweep instance Pods orphaned by a previous fakecloud process (k8s
    /// only; the Docker backend relies on the shared reaper).
    pub async fn reap_stale(&self) {
        if let InstanceBackend::K8s(k) = &self.backend {
            k.reap_stale().await;
        }
    }

    /// The backing container's console log — its combined stdout/stderr, which
    /// includes anything user-data printed at boot (maps to `GetConsoleOutput`).
    /// `None` for an unbacked instance or when logs can't be read.
    pub async fn console_output(&self, instance_id: &str) -> Option<Vec<u8>> {
        let handle = self.handle_of(instance_id)?;
        match &self.backend {
            InstanceBackend::Docker(d) => d.logs(&handle).await,
            InstanceBackend::K8s(k) => k.logs(&handle).await,
        }
    }

    /// Whether the runtime holds an in-memory backing record for this instance.
    /// After a fakecloud restart the registry is rebuilt only for instances that
    /// persisted as `running`/`pending` (see `recover_persisted_containers`), so
    /// an instance that persisted as `stopped` has no record here. `StartInstances`
    /// uses this to distinguish "reattach the existing container" (`start_instance`)
    /// from "the registry was lost across a restart, boot a fresh container"
    /// (`run_instance`) — without it, starting a stopped-then-restarted instance
    /// flips it to `running` with no backing container (the EC2 analogue of the
    /// RDS restart-recovery bug).
    pub fn is_registered(&self, instance_id: &str) -> bool {
        self.instances.read().contains_key(instance_id)
    }

    fn handle_of(&self, instance_id: &str) -> Option<String> {
        self.instances
            .read()
            .get(instance_id)
            .map(|r| r.handle.clone())
    }

    fn update_handle(&self, instance_id: &str, handle: &str) {
        if let Some(record) = self.instances.write().get_mut(instance_id) {
            record.handle = handle.to_string();
        }
    }

    fn update_ip(&self, instance_id: &str, ip: &str) {
        if let Some(record) = self.instances.write().get_mut(instance_id) {
            record.private_ip = ip.to_string();
        }
    }

    /// The resolver the ELBv2 data plane uses to reach `i-...` targets:
    /// install it with [`fakecloud_core::dataplane::set_instance_resolver`].
    pub fn endpoint_resolver(&self) -> Arc<dyn fakecloud_core::dataplane::InstanceResolver> {
        Arc::new(InstanceEndpoints(self.clone()))
    }

    /// Where `port` of a running instance is reachable from fakecloud: a
    /// forwarder's published host port (Docker), or the Pod IP (k8s, which
    /// the data plane reaches in-cluster). `None` for an unbacked instance.
    async fn instance_endpoint(
        &self,
        instance_id: &str,
        port: u16,
    ) -> Option<fakecloud_core::dataplane::Endpoint> {
        let record = self.instances.read().get(instance_id).cloned()?;
        match &self.backend {
            InstanceBackend::Docker(d) => d.forward(instance_id, &record, port).await,
            InstanceBackend::K8s(_) => Some(fakecloud_core::dataplane::Endpoint::new(
                record.private_ip,
                port,
            )),
        }
    }
}

/// [`fakecloud_core::dataplane::InstanceResolver`] over the EC2 runtime.
struct InstanceEndpoints(Ec2Runtime);

impl fakecloud_core::dataplane::InstanceResolver for InstanceEndpoints {
    fn resolve(
        &self,
        _account_id: &str,
        instance_id: &str,
        port: u16,
    ) -> fakecloud_core::dataplane::ResolveFuture {
        // Instance ids are globally unique, so the id alone picks the record.
        let rt = self.0.clone();
        let instance_id = instance_id.to_string();
        Box::pin(async move { rt.instance_endpoint(&instance_id, port).await })
    }
}

fn default_image() -> String {
    std::env::var(DEFAULT_IMAGE_ENV).unwrap_or_else(|_| DEFAULT_IMAGE.to_string())
}

/// The in-instance directory backed by the durable data volume. Defaults to
/// `/var/lib/fakecloud/ec2`; override with `FAKECLOUD_EC2_INSTANCE_DATA_DIR`
/// to capture whichever path the instance's workload writes its long-lived
/// state to. This is fakecloud's persistent-instance-data convention rather
/// than a full root-filesystem snapshot: data written here survives restart
/// and stop/start; the rest of the container's ephemeral filesystem does not.
fn instance_data_dir() -> String {
    std::env::var("FAKECLOUD_EC2_INSTANCE_DATA_DIR")
        .unwrap_or_else(|_| "/var/lib/fakecloud/ec2".to_string())
}

/// Whether EC2 instance data should survive a container being recreated via a
/// durable named volume. The server defaults `FAKECLOUD_PERSIST_EC2_VOLUMES`
/// on in persistent mode (volumes scoped to the data dir); unset means off, as
/// in memory mode, where an explicit opt-in gets process-scoped volumes.
fn ec2_volumes_enabled() -> bool {
    std::env::var("FAKECLOUD_PERSIST_EC2_VOLUMES")
        .map(|v| matches!(v.as_str(), "1" | "true" | "TRUE" | "yes"))
        .unwrap_or(false)
}

/// Docker volume name for an instance's data dir in a volume scope
/// (`fakecloud_core::data_volume`): stable for the same data dir across
/// restarts, so the recovered container reattaches it, and distinct for any
/// other data dir or memory-mode process.
fn scoped_data_volume_name(scope_tag: &str, account_id: &str, instance_id: &str) -> String {
    fakecloud_core::data_volume::scoped_volume_name("ec2", scope_tag, &[account_id, instance_id])
}

/// The unscoped name builds before #2630 gave an instance's data volume.
fn legacy_data_volume_name(account_id: &str, instance_id: &str) -> String {
    fakecloud_core::data_volume::legacy_volume_name("ec2", &[account_id, instance_id])
}

/// Pick the volume to mount: the scoped one once it exists, else a legacy one
/// left by a pre-scoping build for this (randomly-id'd) instance, else a new
/// scoped one.
fn choose_data_volume(
    scoped: String,
    legacy: String,
    scoped_exists: bool,
    legacy_exists: bool,
) -> String {
    if !scoped_exists && legacy_exists {
        legacy
    } else {
        scoped
    }
}

/// Keep-alive command + user-data wrapper for a base image. Shared by both
/// backends so they boot identical containers. When `user_data` (base64) is
/// present it is decoded and run as a root shell script, backgrounded so a
/// slow script never blocks readiness, then the container tails forever.
fn boot_command(user_data: Option<&str>) -> Vec<String> {
    match user_data.filter(|s| !s.is_empty()) {
        Some(b64) => {
            // Wait (bounded) for the IMDS proxy, so a boot script that reads
            // `169.254.169.254` first thing finds it, as cloud-init would.
            let script = format!(
                "(i=0; while [ ! -e {ready} ] && [ $i -lt {wait} ]; do sleep 1; i=$((i+1)); done; \
                 printf %s '{b64}' | base64 -d | sh) & exec tail -f /dev/null",
                ready = imds::READY_FILE,
                wait = imds::BOOT_WAIT_SECS,
            );
            vec!["sh".to_string(), "-c".to_string(), script]
        }
        None => vec![
            "tail".to_string(),
            "-f".to_string(),
            "/dev/null".to_string(),
        ],
    }
}

/// Docker/Podman backend: shells out to the container CLI.
#[derive(Debug, Clone)]
struct DockerInstances {
    cli: String,
    /// Whether `cli` drives Podman, probed once at construction (by engine,
    /// so the `podman-docker` shim counts) for the introspection summary.
    podman: bool,
    instance_id: String,
    /// Container-to-host networking: the host alias instances (and their IMDS
    /// proxy) reach fakecloud at, and the address fakecloud reaches published
    /// ports at.
    net: fakecloud_core::container_net::HostNetworking,
    /// fakecloud's bound port.
    server_port: u16,
    /// The IMDS / forwarder helper image (see [`imds`]), or its last failure.
    helper_image: Arc<tokio::sync::Mutex<Option<imds::HelperImage>>>,
    /// Live load balancer forwarders: `(instance, port)` -> the forwarder's
    /// container, published host port and the instance IP it targets.
    forwarders: Arc<parking_lot::Mutex<HashMap<(String, u16), Forwarder>>>,
}

/// A forwarder publishing one instance port on the host (see [`imds`]).
#[derive(Debug, Clone)]
struct Forwarder {
    container: String,
    host_port: u16,
    target_ip: String,
}

impl DockerInstances {
    async fn run_instance(
        &self,
        account_id: &str,
        instance_id: &str,
        image: &str,
        user_data: Option<&str>,
        network: Option<&InstanceNetwork>,
    ) -> Result<RunningInstance, RuntimeError> {
        // Ensure the subnet's bridge exists and attach to it for L3 isolation.
        // Network creation is best-effort: on failure we fall back to the
        // default bridge so the instance still boots (no regression vs today).
        let attached_network = match network {
            Some(net) => self.ensure_subnet_network(net).await,
            None => None,
        };

        let mut args: Vec<String> = vec![
            "run".to_string(),
            "-d".to_string(),
            "--label".to_string(),
            format!("fakecloud-ec2={instance_id}"),
            "--label".to_string(),
            format!("fakecloud-instance={}", self.instance_id),
        ];
        // Optionally back the instance's writable data directory with a durable
        // named volume keyed on the data dir + account + instance id, so the
        // filesystem state an instance writes there survives a fakecloud
        // restart (the recovery path recreates the container, which reattaches
        // the same volume) and a stop/start (Docker reuses the same container,
        // so the volume persists regardless). Default-on in persistent storage
        // mode, off in memory mode, `FAKECLOUD_PERSIST_EC2_VOLUMES` overrides.
        // The volume is dropped on TerminateInstances. See
        // [`scoped_data_volume_name`] / [`instance_data_dir`].
        if ec2_volumes_enabled() {
            let volume = self.resolve_data_volume(account_id, instance_id).await?;
            fakecloud_core::data_volume::ensure_volume(
                &self.cli,
                &volume,
                fakecloud_core::data_volume::current_scope(),
                &[format!("fakecloud-ec2={instance_id}")],
            )
            .await;
            args.push("-v".to_string());
            args.push(format!("{volume}:{}", instance_data_dir()));
        }
        if let Some(name) = &attached_network {
            args.push("--network".to_string());
            args.push(name.clone());
        }
        // The host alias resolves inside the instance (and its IMDS sidecar,
        // which shares its /etc/hosts), so workloads can reach fakecloud.
        self.net.push_add_host_args(&mut args);
        // IMDS answers at 169.254.169.254 through the sidecar; without a
        // helper image, point SDKs at fakecloud's per-instance IMDS instead.
        let helper = self.ensure_helper_image().await;
        if helper.is_none() {
            let (k, v) =
                imds::metadata_endpoint_env(&self.net.host_alias, self.server_port, instance_id);
            args.push("-e".to_string());
            args.push(format!("{k}={v}"));
        }
        args.push(image.to_string());
        args.extend(boot_command(user_data));

        // `run` pulls a missing image itself but gives up on the first
        // throttled pull; make it available first, retrying transient
        // registry failures.
        fakecloud_core::container_image::ensure_image(&self.cli, None, image)
            .await
            .map_err(RuntimeError::ContainerStartFailed)?;

        let output = tokio::process::Command::new(&self.cli)
            .args(&args)
            .output()
            .await
            .map_err(|e| RuntimeError::ContainerStartFailed(e.to_string()))?;

        if !output.status.success() {
            return Err(RuntimeError::ContainerStartFailed(
                String::from_utf8_lossy(&output.stderr).trim().to_string(),
            ));
        }

        let container_id = String::from_utf8_lossy(&output.stdout).trim().to_string();
        let private_ip = self
            .inspect_ip(&container_id)
            .await
            .unwrap_or_else(|| "10.0.0.1".to_string());
        if helper.is_some() {
            self.start_imds(instance_id, &container_id).await;
        } else {
            self.mark_boot_ready(&container_id).await;
        }

        Ok(RunningInstance {
            container_id,
            private_ip,
            network: attached_network,
        })
    }

    /// The helper image for IMDS sidecars and forwarders: the operator's
    /// override, or a locally built Alpine + nftables + nginx + socat image
    /// (built once per host and reused). `None` when it can't be had; a failure
    /// is remembered for [`imds::HELPER_RETRY_AFTER`].
    async fn ensure_helper_image(&self) -> Option<String> {
        let mut cached = self.helper_image.lock().await;
        match cached.as_ref() {
            Some(imds::HelperImage::Ready(image)) if self.image_present(image).await => {
                return Some(image.clone());
            }
            Some(imds::HelperImage::Failed { at, .. })
                if at.elapsed() < imds::HELPER_RETRY_AFTER =>
            {
                return None;
            }
            _ => {}
        }
        let resolved = match imds::helper_image_override() {
            Some(image) => fakecloud_core::container_image::ensure_image(&self.cli, None, &image)
                .await
                .map(|_| image),
            None => {
                let tag = imds::local_helper_tag();
                if self.image_present(&tag).await {
                    Ok(tag)
                } else {
                    self.build_local_helper(&tag).await.map(|()| tag)
                }
            }
        };
        *cached = Some(match &resolved {
            Ok(image) => imds::HelperImage::Ready(image.clone()),
            Err(error) => {
                tracing::warn!(
                    error = %error,
                    "EC2 helper image unavailable; instances get AWS_EC2_METADATA_SERVICE_ENDPOINT \
                     instead of 169.254.169.254, and load balancers cannot reach instance ports"
                );
                imds::HelperImage::Failed {
                    at: std::time::Instant::now(),
                }
            }
        });
        resolved.ok()
    }

    async fn image_present(&self, tag: &str) -> bool {
        tokio::process::Command::new(&self.cli)
            .args(["image", "inspect", tag])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .await
            .is_ok_and(|s| s.success())
    }

    async fn build_local_helper(&self, tag: &str) -> Result<(), String> {
        // Pull the base with the shared retry so a registry 429 doesn't fail
        // the build's implicit pull.
        fakecloud_core::container_image::ensure_image(&self.cli, None, imds::HELPER_BASE_IMAGE)
            .await?;
        let dir = tempfile::tempdir().map_err(|e| format!("helper build dir: {e}"))?;
        std::fs::write(dir.path().join("Dockerfile"), imds::helper_dockerfile())
            .map_err(|e| format!("helper Dockerfile: {e}"))?;
        let mut last_err = String::new();
        for attempt in 0..3u64 {
            if attempt > 0 {
                tokio::time::sleep(std::time::Duration::from_secs(2 * attempt)).await;
            }
            let out = tokio::process::Command::new(&self.cli)
                .args(["build", "-q", "-t", tag])
                .arg(dir.path())
                .output()
                .await
                .map_err(|e| format!("{} build: {e}", self.cli))?;
            if out.status.success() {
                tracing::info!(image = %tag, "built EC2 helper image");
                return Ok(());
            }
            last_err = String::from_utf8_lossy(&out.stderr).trim().to_string();
        }
        Err(format!("building {tag}: {last_err}"))
    }

    /// Give a running instance container IMDS at `169.254.169.254`: start
    /// its sidecar, wait for the redirect, then release the boot (user-data
    /// waits on [`imds::READY_FILE`]). Best-effort: on failure the boot is
    /// released anyway and the instance runs without link-local IMDS.
    async fn start_imds(&self, instance_id: &str, container_id: &str) {
        let Some(helper) = self.ensure_helper_image().await else {
            self.mark_boot_ready(container_id).await;
            return;
        };
        let name = imds::sidecar_name(instance_id);
        let _ = tokio::process::Command::new(&self.cli)
            .args(["rm", "-f", &name])
            .output()
            .await;
        let argv = imds::sidecar_argv(
            instance_id,
            container_id,
            &self.instance_id,
            &helper,
            &self.net.host_alias,
            self.server_port,
        );
        let result = match tokio::process::Command::new(&self.cli)
            .args(&argv)
            .output()
            .await
        {
            Ok(out) if out.status.success() => {
                let id = String::from_utf8_lossy(&out.stdout).trim().to_string();
                self.wait_for_sidecar(&id).await
            }
            Ok(out) => Err(String::from_utf8_lossy(&out.stderr).trim().to_string()),
            Err(e) => Err(e.to_string()),
        };
        if let Err(error) = result {
            tracing::warn!(
                instance = %instance_id,
                error = %error,
                "could not route 169.254.169.254 to fakecloud IMDS inside the instance"
            );
            let _ = tokio::process::Command::new(&self.cli)
                .args(["rm", "-f", &name])
                .output()
                .await;
        }
        self.mark_boot_ready(container_id).await;
    }

    async fn wait_for_sidecar(&self, id: &str) -> Result<(), String> {
        let deadline = std::time::Instant::now() + imds::SIDECAR_READY_TIMEOUT;
        let mut poll = std::time::Duration::from_millis(100);
        loop {
            let logs = tokio::process::Command::new(&self.cli)
                .args(["logs", id])
                .output()
                .await
                .map_err(|e| e.to_string())?;
            let stdout = String::from_utf8_lossy(&logs.stdout);
            if stdout.contains(imds::READY_MARKER) {
                return Ok(());
            }
            let running = tokio::process::Command::new(&self.cli)
                .args(["inspect", "-f", "{{.State.Running}}", id])
                .output()
                .await
                .map(|o| String::from_utf8_lossy(&o.stdout).trim() == "true")
                .unwrap_or(false);
            if !running {
                return Err(format!(
                    "IMDS sidecar exited during setup; stderr: {:?}",
                    String::from_utf8_lossy(&logs.stderr).trim()
                ));
            }
            if std::time::Instant::now() >= deadline {
                return Err("IMDS sidecar did not become ready in time".into());
            }
            tokio::time::sleep(poll).await;
            poll = (poll * 2).min(std::time::Duration::from_secs(1));
        }
    }

    /// Release a boot waiting on IMDS (see [`boot_command`]).
    async fn mark_boot_ready(&self, container_id: &str) {
        let script = format!("mkdir -p {} && : > {}", imds::READY_DIR, imds::READY_FILE);
        let _ = tokio::process::Command::new(&self.cli)
            .args(["exec", container_id, "sh", "-c", &script])
            .output()
            .await;
    }

    /// Remove an instance's IMDS sidecar and load balancer forwarders.
    async fn remove_sidecars(&self, instance_id: &str) {
        let mut names = vec![imds::sidecar_name(instance_id)];
        {
            let mut fwd = self.forwarders.lock();
            fwd.retain(|(id, _), f| {
                if id == instance_id {
                    names.push(f.container.clone());
                    false
                } else {
                    true
                }
            });
        }
        for name in names {
            let _ = tokio::process::Command::new(&self.cli)
                .args(["rm", "-f", &name])
                .output()
                .await;
        }
    }

    /// The host endpoint serving `port` of an instance, starting a forwarder
    /// for it on first use (or when the instance's IP changed).
    async fn forward(
        &self,
        instance_id: &str,
        record: &InstanceRecord,
        port: u16,
    ) -> Option<fakecloud_core::dataplane::Endpoint> {
        let key = (instance_id.to_string(), port);
        // The record's IP is refreshed on start / reboot (the only times a
        // container's address changes, and they also drop its forwarders), so
        // a request through a live forwarder costs no daemon round trip.
        let current_ip = if record.private_ip.is_empty() {
            self.inspect_ip(&record.handle).await?
        } else {
            record.private_ip.clone()
        };
        if let Some(f) = self.forwarders.lock().get(&key).cloned() {
            if f.target_ip == current_ip {
                return Some(fakecloud_core::dataplane::Endpoint::new(
                    self.net.sibling_host.clone(),
                    f.host_port,
                ));
            }
        }
        let helper = self.ensure_helper_image().await?;
        let name = imds::forwarder_name(instance_id, port);
        let _ = tokio::process::Command::new(&self.cli)
            .args(["rm", "-f", &name])
            .output()
            .await;
        let argv = imds::forwarder_argv(instance_id, port, &current_ip, &self.instance_id, &helper);
        let out = tokio::process::Command::new(&self.cli)
            .args(&argv)
            .output()
            .await
            .ok()?;
        if !out.status.success() {
            tracing::warn!(
                instance = %instance_id,
                port,
                error = %String::from_utf8_lossy(&out.stderr).trim(),
                "could not start a load balancer forwarder for the instance"
            );
            return None;
        }
        let container = String::from_utf8_lossy(&out.stdout).trim().to_string();
        // Reach the instance over its subnet network (the forwarder sits on
        // the default bridge so its port can be published).
        if let Some(net) = record.network.as_ref() {
            let _ = tokio::process::Command::new(&self.cli)
                .args([
                    "network",
                    "connect",
                    &subnet_network_name(&net.subnet_id),
                    &container,
                ])
                .output()
                .await;
        }
        let spec = format!("{port}/tcp");
        let host_port = tokio::process::Command::new(&self.cli)
            .args(["port", &container, &spec])
            .output()
            .await
            .ok()
            .filter(|o| o.status.success())
            .and_then(|o| {
                fakecloud_core::container_net::parse_published_port(&String::from_utf8_lossy(
                    &o.stdout,
                ))
            });
        let Some(host_port) = host_port else {
            let _ = tokio::process::Command::new(&self.cli)
                .args(["rm", "-f", &container])
                .output()
                .await;
            return None;
        };
        self.forwarders.lock().insert(
            key,
            Forwarder {
                container,
                host_port,
                target_ip: current_ip,
            },
        );
        Some(fakecloud_core::dataplane::Endpoint::new(
            self.net.sibling_host.clone(),
            host_port,
        ))
    }

    /// Create (idempotently) the daemon network backing a subnet and return its
    /// name, or `None` if creation failed (caller falls back to the default
    /// bridge). The network carries the shared `fakecloud-instance` ownership
    /// label so the startup reaper prunes it after an ungraceful restart, plus
    /// a `fakecloud-subnet=<id>` label for introspection. Private subnets get
    /// an `--internal` network (no NAT to the host/internet).
    async fn ensure_subnet_network(&self, net: &InstanceNetwork) -> Option<String> {
        let name = subnet_network_name(&net.subnet_id);
        let mut args = vec!["network".to_string(), "create".to_string()];
        if net.internal {
            args.push("--internal".to_string());
        }
        args.push("--label".to_string());
        args.push(format!("fakecloud-subnet={}", net.subnet_id));
        args.push("--label".to_string());
        args.push(format!("fakecloud-instance={}", self.instance_id));
        args.push(name.clone());

        let output = tokio::process::Command::new(&self.cli)
            .args(&args)
            .output()
            .await;
        match output {
            // Created fresh.
            Ok(out) if out.status.success() => Some(name),
            // Already exists (another instance in the same subnet created it):
            // a benign race — the network is there, so attach to it.
            Ok(out) => {
                let err = String::from_utf8_lossy(&out.stderr);
                if err.contains("already exists") || err.contains("exists") {
                    Some(name)
                } else {
                    tracing::warn!(
                        subnet = %net.subnet_id,
                        network = %name,
                        error = %err.trim(),
                        "subnet network creation failed; falling back to default bridge"
                    );
                    None
                }
            }
            Err(e) => {
                tracing::warn!(
                    subnet = %net.subnet_id,
                    network = %name,
                    error = %e,
                    "subnet network creation failed; falling back to default bridge"
                );
                None
            }
        }
    }

    /// Read the container's private IP from `inspect`. Returns `None` if the
    /// container has no address (e.g. host networking) — the caller falls
    /// back to a synthesized IP.
    async fn inspect_ip(&self, container_id: &str) -> Option<String> {
        let output = tokio::process::Command::new(&self.cli)
            .args([
                "inspect",
                "-f",
                "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}",
                container_id,
            ])
            .output()
            .await
            .ok()?;
        if !output.status.success() {
            return None;
        }
        let ip = String::from_utf8_lossy(&output.stdout).trim().to_string();
        if ip.is_empty() {
            None
        } else {
            Some(ip)
        }
    }

    async fn stop(&self, container_id: &str) {
        let _ = tokio::process::Command::new(&self.cli)
            .args(["stop", container_id])
            .output()
            .await;
    }

    async fn start(&self, container_id: &str) -> Option<String> {
        let started = tokio::process::Command::new(&self.cli)
            .args(["start", container_id])
            .output()
            .await
            .map(|o| o.status.success())
            .unwrap_or(false);
        if !started {
            return None;
        }
        self.inspect_ip(container_id).await
    }

    async fn reboot(&self, container_id: &str) {
        let _ = tokio::process::Command::new(&self.cli)
            .args(["restart", container_id])
            .output()
            .await;
    }

    async fn remove(&self, container_id: &str) {
        let _ = tokio::process::Command::new(&self.cli)
            .args(["rm", "-f", container_id])
            .output()
            .await;
    }

    /// Remove the durable data volume for an instance (called on
    /// `TerminateInstances`) so a later instance reusing the same id starts
    /// clean rather than inheriting the terminated instance's filesystem. A
    /// no-op when volume persistence is disabled (no such volume was created).
    async fn remove_data_volume(&self, account_id: &str, instance_id: &str) {
        if !ec2_volumes_enabled() {
            return;
        }
        // Instance ids are random, so a legacy volume named after this id can
        // only be this instance's own (adopted by `resolve_data_volume`).
        for name in [
            scoped_data_volume_name(
                fakecloud_core::data_volume::current_scope().tag(),
                account_id,
                instance_id,
            ),
            legacy_data_volume_name(account_id, instance_id),
        ] {
            fakecloud_core::data_volume::remove_volume(&self.cli, &name).await;
        }
    }

    /// The data volume an instance mounts. Instance ids are random, so the
    /// unscoped name a build before data-dir scoping gave this instance can
    /// only belong to it: an instance restored from such a data dir keeps
    /// that legacy volume, unless its scoped one already exists. Fails when
    /// the daemon can't list its volumes: guessing "absent" would mount an
    /// empty scoped volume that from then on shadows the legacy data.
    async fn resolve_data_volume(
        &self,
        account_id: &str,
        instance_id: &str,
    ) -> Result<String, RuntimeError> {
        let scoped = scoped_data_volume_name(
            fakecloud_core::data_volume::current_scope().tag(),
            account_id,
            instance_id,
        );
        let legacy = legacy_data_volume_name(account_id, instance_id);
        let existing = fakecloud_core::data_volume::list_volumes(&self.cli)
            .await
            .ok_or_else(|| {
                RuntimeError::ContainerStartFailed(
                    "could not list container volumes to find the instance's data volume"
                        .to_string(),
                )
            })?;
        let scoped_exists = existing.contains(&scoped);
        let legacy_exists = existing.contains(&legacy);
        Ok(choose_data_volume(
            scoped,
            legacy,
            scoped_exists,
            legacy_exists,
        ))
    }

    /// The container's combined stdout+stderr (`docker logs`). `None` if the
    /// command fails; an empty log is `Some(vec![])`.
    async fn logs(&self, container_id: &str) -> Option<Vec<u8>> {
        let output = tokio::process::Command::new(&self.cli)
            .args(["logs", container_id])
            .output()
            .await
            .ok()?;
        if !output.status.success() {
            return None;
        }
        // `docker logs` writes the container's stdout to ours and its stderr to
        // ours; concatenate so the console output carries both streams.
        let mut buf = output.stdout;
        buf.extend_from_slice(&output.stderr);
        Some(buf)
    }
}

#[cfg(test)]
mod volume_tests {
    use super::*;

    #[test]
    fn data_volume_name_is_scoped_stable_and_sanitized() {
        assert_eq!(
            scoped_data_volume_name("d0123456789ab", "123456789012", "i-0abc123"),
            "fakecloud-ec2-data-d0123456789ab-123456789012-i-0abc123"
        );
        // Two data dirs never share a volume.
        assert_ne!(
            scoped_data_volume_name("d0123456789ab", "123456789012", "i-0abc123"),
            scoped_data_volume_name("dba9876543210", "123456789012", "i-0abc123")
        );
        // Characters outside Docker's volume-name set become '-'.
        assert_eq!(
            scoped_data_volume_name("d0", "1234/5678", "i-0abc:1"),
            "fakecloud-ec2-data-d0-1234-5678-i-0abc-1"
        );
        // The legacy name is exactly what pre-scoping builds created.
        assert_eq!(
            legacy_data_volume_name("123456789012", "i-0abc123"),
            "fakecloud-ec2-data-123456789012-i-0abc123"
        );
    }

    #[test]
    fn distinct_instances_get_distinct_volumes() {
        // Two instances in the same account never share a data volume, so
        // terminating one cannot wipe another's filesystem.
        assert_ne!(
            scoped_data_volume_name("d0", "123456789012", "i-aaaa"),
            scoped_data_volume_name("d0", "123456789012", "i-bbbb")
        );
    }

    #[test]
    fn legacy_volume_kept_only_until_a_scoped_one_exists() {
        let pick = |s, l| choose_data_volume("scoped".into(), "legacy".into(), s, l);
        // Fresh instance: a new scoped volume.
        assert_eq!(pick(false, false), "scoped");
        // Instance restored from a pre-scoping data dir: keep its data.
        assert_eq!(pick(false, true), "legacy");
        // The scoped volume always wins once it exists.
        assert_eq!(pick(true, false), "scoped");
        assert_eq!(pick(true, true), "scoped");
    }
}
