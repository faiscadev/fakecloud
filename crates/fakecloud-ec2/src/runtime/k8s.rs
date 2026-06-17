//! Kubernetes backend for EC2 instances.
//!
//! Runs each instance as a native Pod (one container off the instance's
//! base image, kept alive with `tail -f /dev/null`) instead of a Docker
//! container. Shared client / lifecycle / reaping plumbing comes from the
//! `fakecloud-k8s` crate; this module only builds the instance Pod spec and
//! maps the EC2 runtime operations onto Pod operations.
//!
//! A Pod can't be stopped and restarted in place, so the runtime models
//! `Stop` as a Pod delete and `Start`/`Reboot` as a recreate under the same
//! deterministic Pod name — see [`super::Ec2Runtime`]. User-data is baked
//! into the container command (decoded and run at boot, backgrounded), the
//! same script the Docker backend runs.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use k8s_openapi::api::core::v1::{Container, LocalObjectReference, Pod, PodSpec};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;

use fakecloud_k8s::{labels, names, K8sClient, K8sEnv, K8sPodConfig};

use super::firewall::InstanceRules;
use super::netpolicy::{self, CniDriver};
use super::{boot_command, BackendInitError, RunningInstance, RuntimeError};

/// Which `fakecloud-service` label instance Pods carry.
const SERVICE: &str = "ec2";
/// Pod-name prefix for instance Pods (kept short so the instance slug + hash
/// suffix fit the 63-char DNS label limit).
const POD_PREFIX: &str = "fakecloud-i";
/// Container name inside each instance Pod.
const CONTAINER: &str = "instance";

#[derive(Clone)]
pub(super) struct K8sInstances {
    client: K8sClient,
    /// Optional `imagePullSecrets` name for private registries.
    pull_secret: Option<String>,
    /// Global + EC2-service node selector / tolerations / annotations
    /// applied to every instance Pod.
    pod_config: K8sPodConfig,
    /// Monotonic counter making each spawned Pod name unique. `Start`/`Reboot`
    /// recreate an instance's Pod while the previous one may still be
    /// `Terminating`; a fresh name avoids a name collision with it (the old
    /// Pod terminates on its own).
    spawn_seq: Arc<AtomicU64>,
    /// The detected cluster CNI, deciding whether NetworkPolicy is enforced
    /// (#1745 phase 4). Policies are always created; this only governs the
    /// degrade warning.
    cni: CniDriver,
}

impl std::fmt::Debug for K8sInstances {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("K8sInstances")
            .field("namespace", &self.client.namespace())
            .finish_non_exhaustive()
    }
}

impl K8sInstances {
    pub(super) async fn from_env(server_port: u16) -> Result<Self, BackendInitError> {
        let env = K8sEnv::from_env(server_port)?;
        let client = K8sClient::connect(env.namespace.clone())
            .await
            .map_err(|e| BackendInitError::Connect(e.to_string()))?;
        tracing::info!(
            namespace = %env.namespace,
            self_url = %env.self_url,
            "K8s EC2 backend initialized"
        );
        let pod_config = K8sPodConfig::resolved_base("FAKECLOUD_EC2_K8S")?;
        // Detect the CNI so we can warn when NetworkPolicy won't be enforced.
        let cni = CniDriver::from_components(client.kube_system_pod_names().await);
        if cni.enforces() {
            tracing::info!(?cni, "k8s CNI enforces NetworkPolicy; EC2 security groups will be applied as NetworkPolicies");
        } else {
            tracing::warn!(
                "k8s CNI does not appear to enforce NetworkPolicy (detected: {cni:?}); EC2 \
                 security-group NetworkPolicies will be created but may not be enforced by the \
                 cluster -- install a NetworkPolicy-enforcing CNI (e.g. Calico) for real isolation"
            );
        }
        Ok(Self {
            client,
            pull_secret: env.pull_secret,
            pod_config,
            spawn_seq: Arc::new(AtomicU64::new(0)),
            cni,
        })
    }

    /// (Re)apply one NetworkPolicy per running instance and prune policies for
    /// instances that no longer exist. Always creates the policies; whether
    /// they're enforced is up to the cluster CNI (warned about at startup).
    pub(super) async fn reconcile_network_policies(&self, rules: &[InstanceRules]) {
        let namespace = self.client.namespace().to_string();
        let owner = self.client.instance_id().to_string();
        let policies = netpolicy::build_policies(rules, &namespace, &owner);
        let keep: std::collections::HashSet<String> = policies
            .iter()
            .filter_map(|p| p.metadata.name.clone())
            .collect();
        for policy in &policies {
            self.client.apply_network_policy(policy).await;
        }
        self.client.prune_network_policies(&keep).await;
        tracing::debug!(
            count = policies.len(),
            enforced = self.cni.enforces(),
            "applied EC2 NetworkPolicies"
        );
    }

    /// Spawn (or recreate) the Pod backing an instance. Each spawn gets a
    /// unique Pod name (instance-id slug + a per-spawn sequence hash) so a
    /// recreate never collides with the previous, still-`Terminating` Pod.
    pub(super) async fn spawn_pod(
        &self,
        instance_id: &str,
        image: &str,
        user_data: Option<&str>,
        tags: &std::collections::BTreeMap<String, String>,
    ) -> Result<RunningInstance, RuntimeError> {
        let seq = self.spawn_seq.fetch_add(1, Ordering::Relaxed);
        let pod_name = names::pod_name(POD_PREFIX, instance_id, &format!("{instance_id}-{seq}"));
        let mut pod = build_instance_pod(InstancePodContext {
            pod_name: &pod_name,
            namespace: self.client.namespace(),
            instance_id_label: self.client.instance_id(),
            ec2_instance_id: instance_id,
            image,
            user_data,
            pull_secret: self.pull_secret.as_deref(),
        });
        // Operator-configured global + EC2-service base, with this
        // instance's reserved `fakecloud-k8s/*` tag overrides merged over it
        // (per-instance wins). Every spawn (RunInstances, and Start/Reboot
        // recreate) goes through here, so this covers them all.
        self.pod_config
            .clone()
            .merge(K8sPodConfig::from_tags(tags))
            .apply(&mut pod);

        self.client
            .create_pod(&pod)
            .await
            .map_err(|e| RuntimeError::ContainerStartFailed(format!("create instance pod: {e}")))?;

        let pod_ip = match self
            .client
            .wait_for_pod_ip(&pod_name, Duration::from_secs(90))
            .await
        {
            Ok(ip) => ip,
            Err(e) => {
                self.client.delete_pod(&pod_name).await;
                return Err(RuntimeError::ContainerStartFailed(e.to_string()));
            }
        };

        Ok(RunningInstance {
            container_id: pod_name,
            private_ip: pod_ip,
            // k8s pods share a flat network; per-subnet isolation is a
            // NetworkPolicy concern (#1745 phase 4), not a daemon network.
            network: None,
        })
    }

    pub(super) async fn delete_pod(&self, pod_name: &str) {
        self.client.delete_pod(pod_name).await;
    }

    /// The instance container's logs (the k8s equivalent of `docker logs`).
    /// `None` if the Pod log can't be read.
    pub(super) async fn logs(&self, pod_name: &str) -> Option<Vec<u8>> {
        self.client
            .pod_logs(pod_name, Some(CONTAINER))
            .await
            .ok()
            .map(String::into_bytes)
    }

    pub(super) async fn reap_stale(&self) {
        self.client.reap_stale(SERVICE).await;
    }
}

/// Inputs for [`build_instance_pod`].
struct InstancePodContext<'a> {
    pod_name: &'a str,
    namespace: &'a str,
    /// The `fakecloud-instance` ownership label (this fakecloud process).
    instance_id_label: &'a str,
    /// The EC2 instance id this Pod backs.
    ec2_instance_id: &'a str,
    image: &'a str,
    user_data: Option<&'a str>,
    pull_secret: Option<&'a str>,
}

/// Build the Pod spec for one EC2 instance. Pure — no cluster I/O — so it's
/// unit-testable.
fn build_instance_pod(ctx: InstancePodContext<'_>) -> Pod {
    let mut pod_labels = std::collections::BTreeMap::new();
    pod_labels.insert(
        labels::MANAGED_BY.to_string(),
        labels::MANAGED_BY_VALUE.to_string(),
    );
    pod_labels.insert(
        labels::INSTANCE.to_string(),
        ctx.instance_id_label.to_string(),
    );
    pod_labels.insert(labels::SERVICE.to_string(), SERVICE.to_string());
    pod_labels.insert(
        "fakecloud-ec2".to_string(),
        names::label_safe(ctx.ec2_instance_id),
    );

    let container = Container {
        name: CONTAINER.to_string(),
        image: Some(ctx.image.to_string()),
        command: Some(boot_command(ctx.user_data)),
        ..Container::default()
    };

    let pull_secrets = ctx.pull_secret.map(|name| {
        vec![LocalObjectReference {
            name: name.to_string(),
        }]
    });

    Pod {
        metadata: ObjectMeta {
            name: Some(ctx.pod_name.to_string()),
            namespace: Some(ctx.namespace.to_string()),
            labels: Some(pod_labels),
            ..ObjectMeta::default()
        },
        spec: Some(PodSpec {
            // We manage lifecycle explicitly (reboot recreates the Pod), so
            // the kubelet shouldn't restart the container itself.
            restart_policy: Some("Never".to_string()),
            containers: vec![container],
            image_pull_secrets: pull_secrets,
            ..PodSpec::default()
        }),
        ..Pod::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx(user_data: Option<&'static str>) -> InstancePodContext<'static> {
        InstancePodContext {
            pod_name: "fakecloud-i-i0abc-abc123",
            namespace: "fakecloud",
            instance_id_label: "fakecloud-1234",
            ec2_instance_id: "i-0ABC123",
            image: "amazonlinux:2023",
            user_data,
            pull_secret: None,
        }
    }

    #[test]
    fn pod_has_ownership_labels() {
        let pod = build_instance_pod(ctx(None));
        let l = pod.metadata.labels.unwrap();
        assert_eq!(l.get(labels::MANAGED_BY).unwrap(), labels::MANAGED_BY_VALUE);
        assert_eq!(l.get(labels::SERVICE).unwrap(), "ec2");
        assert_eq!(l.get(labels::INSTANCE).unwrap(), "fakecloud-1234");
        // instance id slug is DNS-safe (lowercased).
        assert_eq!(l.get("fakecloud-ec2").unwrap(), "i-0abc123");
    }

    #[test]
    fn container_uses_image_and_keepalive_command() {
        let pod = build_instance_pod(ctx(None));
        let c = &pod.spec.unwrap().containers[0];
        assert_eq!(c.image.as_deref(), Some("amazonlinux:2023"));
        assert_eq!(
            c.command.as_ref().unwrap(),
            &vec![
                "tail".to_string(),
                "-f".to_string(),
                "/dev/null".to_string()
            ]
        );
    }

    #[test]
    fn user_data_is_decoded_and_backgrounded() {
        let pod = build_instance_pod(ctx(Some("ZWNobyBoaQ=="))); // "echo hi"
        let c = &pod.spec.unwrap().containers[0];
        let cmd = c.command.as_ref().unwrap();
        assert_eq!(cmd[0], "sh");
        assert_eq!(cmd[1], "-c");
        let script = &cmd[2];
        assert!(script.contains("base64 -d"), "decodes: {script}");
        assert!(
            script.contains("ZWNobyBoaQ=="),
            "embeds user-data: {script}"
        );
        // backgrounded so a slow script never blocks readiness, then tails.
        assert!(
            script.contains("& exec tail -f /dev/null"),
            "tails: {script}"
        );
    }

    #[test]
    fn restart_policy_never() {
        let pod = build_instance_pod(ctx(None));
        assert_eq!(pod.spec.unwrap().restart_policy.as_deref(), Some("Never"));
    }

    #[test]
    fn pull_secret_attached_when_set() {
        let mut c = ctx(None);
        c.pull_secret = Some("reg-secret");
        let pod = build_instance_pod(c);
        let secrets = pod.spec.unwrap().image_pull_secrets.unwrap();
        assert_eq!(secrets[0].name, "reg-secret");
    }

    #[test]
    fn pod_config_base_applies_to_built_pod() {
        use std::collections::BTreeMap;
        // The global + service env base (resolved at from_env) is applied
        // to every instance Pod in spawn_pod; this asserts the apply
        // contract over a built instance Pod.
        let mut pod = build_instance_pod(ctx(None));
        let cfg = K8sPodConfig {
            node_selector: BTreeMap::from([("pool".to_string(), "ec2".to_string())]),
            annotations: BTreeMap::from([("team".to_string(), "infra".to_string())]),
            ..Default::default()
        };
        cfg.apply(&mut pod);
        let spec = pod.spec.unwrap();
        assert_eq!(
            spec.node_selector.unwrap().get("pool").map(String::as_str),
            Some("ec2")
        );
        assert_eq!(
            pod.metadata
                .annotations
                .unwrap()
                .get("team")
                .map(String::as_str),
            Some("infra")
        );
    }

    #[test]
    fn pod_config_overrides_apply_to_built_pod() {
        use std::collections::BTreeMap;
        // Mirrors the spawn_pod wiring: EC2-service base merged with the
        // instance's reserved-tag overrides, applied to the built Pod.
        let mut pod = build_instance_pod(ctx(None));
        let base = K8sPodConfig {
            node_selector: BTreeMap::from([("pool".to_string(), "ec2".to_string())]),
            ..Default::default()
        };
        let tags = BTreeMap::from([
            (
                "fakecloud-k8s/node-selector".to_string(),
                "pool=spot,disktype=ssd".to_string(),
            ),
            (
                "fakecloud-k8s/annotations".to_string(),
                "team=infra".to_string(),
            ),
        ]);
        base.merge(K8sPodConfig::from_tags(&tags)).apply(&mut pod);

        let spec = pod.spec.unwrap();
        let sel = spec.node_selector.unwrap();
        // Per-instance tag overrides the base on `pool`; base-only keys survive.
        assert_eq!(sel.get("pool").map(String::as_str), Some("spot"));
        assert_eq!(sel.get("disktype").map(String::as_str), Some("ssd"));
        assert_eq!(
            pod.metadata
                .annotations
                .unwrap()
                .get("team")
                .map(String::as_str),
            Some("infra")
        );
    }
}
