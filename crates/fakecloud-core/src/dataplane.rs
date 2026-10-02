//! Where fakecloud actually connects for an AWS-visible data-plane address.
//!
//! AWS reports addresses that only exist inside a VPC: an `awsvpc` ECS task is
//! an ENI private IP from its subnet, an EC2 target is an instance id. Locally
//! those addresses are not routable from fakecloud (Docker Desktop and podman
//! machine keep container IPs inside a VM, and a containerized fakecloud sits
//! on another network), so the runtimes publish the workload's port on the
//! host and record here where it went. The ELBv2 data plane and health prober
//! keep the AWS-visible target id and port in every API response and consult
//! this module only to pick the socket to open.
//!
//! Two sources feed it:
//!
//! - **static endpoints**, registered by a runtime that already published the
//!   port (ECS publishes each `awsvpc` container port when the task starts);
//! - an **instance resolver**, installed by the EC2 runtime, which can publish
//!   an arbitrary port of an instance on demand -- an instance's listening
//!   ports are only known once a target group names one.
//!
//! A second, smaller registry maps a host-visible data-plane port to the port
//! sibling containers must use instead, for services whose protocol embeds the
//! address in its responses (a Kafka broker advertises its listener, so a
//! container needs a listener that advertises the host alias). The container
//! env rewriting in [`crate::container_net::rewrite_loopback_value`] applies
//! it.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};

use parking_lot::RwLock;

/// A socket address fakecloud can open: host (name or IP) and port.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Endpoint {
    pub host: String,
    pub port: u16,
}

impl Endpoint {
    pub fn new(host: impl Into<String>, port: u16) -> Self {
        Self {
            host: host.into(),
            port,
        }
    }
}

/// Key of a static endpoint: account, AWS-visible target id, target port.
type TargetKey = (String, String, u16);

fn targets() -> &'static RwLock<HashMap<TargetKey, Endpoint>> {
    static TARGETS: OnceLock<RwLock<HashMap<TargetKey, Endpoint>>> = OnceLock::new();
    TARGETS.get_or_init(|| RwLock::new(HashMap::new()))
}

/// Future returned by an [`InstanceResolver`].
pub type ResolveFuture = Pin<Box<dyn Future<Output = Option<Endpoint>> + Send>>;

/// Publishes a port of an EC2 instance on demand and says where it landed.
pub trait InstanceResolver: Send + Sync {
    /// The endpoint serving `port` of `instance_id` in `account_id`, or
    /// `None` when the instance has no running backing container.
    /// `source_groups` are the security groups of the load balancer the
    /// traffic comes from, so the instance's security-group rules can admit
    /// it the way they admit the load balancer on AWS.
    fn resolve(
        &self,
        account_id: &str,
        instance_id: &str,
        port: u16,
        source_groups: Vec<String>,
    ) -> ResolveFuture;
}

fn instance_resolver() -> &'static RwLock<Option<Arc<dyn InstanceResolver>>> {
    static RESOLVER: OnceLock<RwLock<Option<Arc<dyn InstanceResolver>>>> = OnceLock::new();
    RESOLVER.get_or_init(|| RwLock::new(None))
}

/// Install the EC2 instance resolver (one per process; a later call replaces
/// the earlier one).
pub fn set_instance_resolver(resolver: Arc<dyn InstanceResolver>) {
    *instance_resolver().write() = Some(resolver);
}

/// Record that `target_id:port` in `account_id` is served at `endpoint`.
pub fn register_target(account_id: &str, target_id: &str, port: u16, endpoint: Endpoint) {
    targets().write().insert(
        (account_id.to_string(), target_id.to_string(), port),
        endpoint,
    );
}

/// Forget every endpoint of `target_id` in `account_id`.
pub fn unregister_target(account_id: &str, target_id: &str) {
    targets()
        .write()
        .retain(|(acct, id, _), _| !(acct == account_id && id == target_id));
}

/// The static endpoint for `target_id:port`, if a runtime registered one.
pub fn registered_target(account_id: &str, target_id: &str, port: u16) -> Option<Endpoint> {
    targets()
        .read()
        .get(&(account_id.to_string(), target_id.to_string(), port))
        .cloned()
}

/// The socket to open for an ELBv2 target. In order:
///
/// 1. a static endpoint a runtime registered for it;
/// 2. for an instance id (`i-*`), the instance resolver, when installed;
/// 3. the historical fallbacks: `i-*` and ECS bridge-mode tasks (registered
///    as `127.0.0.1`) publish on the daemon's host, reached at
///    `sibling_host`; any other id is used verbatim.
///
/// `source_groups` are the security groups of the load balancer connecting
/// (see [`InstanceResolver::resolve`]).
pub async fn resolve_target(
    account_id: &str,
    target_id: &str,
    port: u16,
    sibling_host: &str,
    source_groups: &[String],
) -> Endpoint {
    if let Some(ep) = registered_target(account_id, target_id, port) {
        return ep;
    }
    if target_id.starts_with("i-") {
        let resolver = instance_resolver().read().clone();
        if let Some(resolver) = resolver {
            if let Some(ep) = resolver
                .resolve(account_id, target_id, port, source_groups.to_vec())
                .await
            {
                return ep;
            }
        }
    }
    Endpoint::new(fallback_host(target_id, sibling_host), port)
}

/// Host for a target nothing registered: see [`resolve_target`].
pub fn fallback_host(target_id: &str, sibling_host: &str) -> String {
    if target_id.starts_with("i-") || target_id == "127.0.0.1" {
        sibling_host.to_string()
    } else {
        target_id.to_string()
    }
}

/// The account id in an ARN (`arn:<partition>:<service>:<region>:<account>:...`).
pub fn account_of_arn(arn: &str) -> Option<&str> {
    arn.split(':').nth(4).filter(|a| !a.is_empty())
}

fn container_ports() -> &'static RwLock<HashMap<u16, u16>> {
    static PORTS: OnceLock<RwLock<HashMap<u16, u16>>> = OnceLock::new();
    PORTS.get_or_init(|| RwLock::new(HashMap::new()))
}

/// Record that sibling containers must use `container_port` (on the host
/// alias) where host clients use loopback `host_port`.
pub fn register_container_port(host_port: u16, container_port: u16) {
    container_ports().write().insert(host_port, container_port);
}

/// Drop the container-view mapping of `host_port`.
pub fn unregister_container_port(host_port: u16) {
    container_ports().write().remove(&host_port);
}

/// The port sibling containers use for loopback `host_port`, when a service
/// registered a different one.
pub fn container_port_for(host_port: u16) -> Option<u16> {
    container_ports().read().get(&host_port).copied()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fallback_host_keeps_historical_routing() {
        assert_eq!(
            fallback_host("i-0abc", "host.docker.internal"),
            "host.docker.internal"
        );
        assert_eq!(fallback_host("127.0.0.1", "127.0.0.1"), "127.0.0.1");
        assert_eq!(
            fallback_host("10.0.4.7", "host.docker.internal"),
            "10.0.4.7"
        );
    }

    #[tokio::test]
    async fn registered_endpoint_wins_over_the_verbatim_ip() {
        let acct = "111111111111";
        register_target(acct, "10.9.8.7", 80, Endpoint::new("127.0.0.1", 49153));
        assert_eq!(
            resolve_target(acct, "10.9.8.7", 80, "127.0.0.1", &[]).await,
            Endpoint::new("127.0.0.1", 49153)
        );
        // Another port of the same target, or another account, is not mapped.
        assert_eq!(
            resolve_target(acct, "10.9.8.7", 81, "127.0.0.1", &[]).await,
            Endpoint::new("10.9.8.7", 81)
        );
        assert_eq!(
            resolve_target("222222222222", "10.9.8.7", 80, "127.0.0.1", &[]).await,
            Endpoint::new("10.9.8.7", 80)
        );
        unregister_target(acct, "10.9.8.7");
        assert_eq!(
            resolve_target(acct, "10.9.8.7", 80, "127.0.0.1", &[]).await,
            Endpoint::new("10.9.8.7", 80)
        );
    }

    struct FixedResolver;
    impl InstanceResolver for FixedResolver {
        fn resolve(
            &self,
            _account: &str,
            instance_id: &str,
            port: u16,
            source_groups: Vec<String>,
        ) -> ResolveFuture {
            // The load balancer's groups reach the resolver.
            let known = instance_id == "i-resolvable" && source_groups == ["sg-alb"];
            Box::pin(async move { known.then(|| Endpoint::new("127.0.0.1", port + 1000)) })
        }
    }

    #[tokio::test]
    async fn instance_resolver_publishes_instance_ports() {
        set_instance_resolver(Arc::new(FixedResolver));
        assert_eq!(
            resolve_target(
                "333333333333",
                "i-resolvable",
                8080,
                "host.docker.internal",
                &["sg-alb".to_string()]
            )
            .await,
            Endpoint::new("127.0.0.1", 9080)
        );
        // An instance the resolver doesn't know keeps the sibling-host route.
        assert_eq!(
            resolve_target(
                "333333333333",
                "i-unknown",
                8080,
                "host.docker.internal",
                &[]
            )
            .await,
            Endpoint::new("host.docker.internal", 8080)
        );
    }

    #[test]
    fn account_is_read_from_the_arn() {
        assert_eq!(
            account_of_arn(
                "arn:aws:elasticloadbalancing:us-east-1:123456789012:targetgroup/tg/abc"
            ),
            Some("123456789012")
        );
        assert_eq!(account_of_arn("not-an-arn"), None);
    }

    #[test]
    fn container_port_map_round_trips() {
        register_container_port(40001, 40002);
        assert_eq!(container_port_for(40001), Some(40002));
        unregister_container_port(40001);
        assert_eq!(container_port_for(40001), None);
    }
}
