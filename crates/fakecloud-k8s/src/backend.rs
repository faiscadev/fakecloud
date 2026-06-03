//! Backend selection: Docker (default) vs native Kubernetes Pods.
//!
//! A FakeCloud server can run every container-backed service on Docker
//! (the default) or on Kubernetes. The choice is per-service with a
//! global fallback:
//!
//! - `FAKECLOUD_<SERVICE>_BACKEND=k8s|docker` — explicit per-service
//!   override (e.g. `FAKECLOUD_ELASTICACHE_BACKEND`,
//!   `FAKECLOUD_LAMBDA_BACKEND`). An explicit value always wins.
//! - `FAKECLOUD_CONTAINER_BACKEND=k8s|docker` — global default applied
//!   to any service whose own variable is unset.
//! - Unset everywhere -> [`Backend::Docker`].
//!
//! This lets an operator flip the whole stack to Kubernetes with one
//! variable while still pinning an individual service back to Docker.

/// Which execution backend a service should use.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Backend {
    /// Shell out to a local Docker/Podman daemon (the default).
    Docker,
    /// Spawn native Pods in a Kubernetes cluster.
    K8s,
}

/// The global default backend variable, consulted when a service's own
/// variable is unset.
pub const GLOBAL_BACKEND_ENV: &str = "FAKECLOUD_CONTAINER_BACKEND";

/// Resolve the backend for a service given the name of its per-service
/// override variable (e.g. `"FAKECLOUD_ELASTICACHE_BACKEND"`).
///
/// An explicit per-service value wins over the global default; only
/// `k8s` and `docker` are recognized (case-insensitive), and any other
/// value is ignored (treated as unset) so a typo can't silently leave a
/// service on an unexpected backend without falling through to the
/// documented precedence.
pub fn backend_choice(per_service_env: &str) -> Backend {
    match read_choice(per_service_env) {
        Some(b) => b,
        None => read_choice(GLOBAL_BACKEND_ENV).unwrap_or(Backend::Docker),
    }
}

/// Parse a single backend variable. Returns `None` when unset or set to
/// an unrecognized value so the caller can fall through to the next
/// level of precedence.
fn read_choice(var: &str) -> Option<Backend> {
    match std::env::var(var)
        .ok()?
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "k8s" | "kubernetes" => Some(Backend::K8s),
        "docker" | "podman" => Some(Backend::Docker),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    // Env vars are process-global; serialize the tests that mutate them.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn with_env(vars: &[(&str, Option<&str>)], f: impl FnOnce()) {
        let _g = ENV_LOCK.lock().unwrap();
        let saved: Vec<(String, Option<String>)> = vars
            .iter()
            .map(|(k, _)| (k.to_string(), std::env::var(k).ok()))
            .collect();
        for (k, v) in vars {
            match v {
                Some(v) => std::env::set_var(k, v),
                None => std::env::remove_var(k),
            }
        }
        f();
        for (k, v) in saved {
            match v {
                Some(v) => std::env::set_var(&k, v),
                None => std::env::remove_var(&k),
            }
        }
    }

    const SVC: &str = "FAKECLOUD_ELASTICACHE_BACKEND";

    #[test]
    fn defaults_to_docker_when_unset() {
        with_env(&[(SVC, None), (GLOBAL_BACKEND_ENV, None)], || {
            assert_eq!(backend_choice(SVC), Backend::Docker);
        });
    }

    #[test]
    fn per_service_k8s_wins() {
        with_env(&[(SVC, Some("k8s")), (GLOBAL_BACKEND_ENV, None)], || {
            assert_eq!(backend_choice(SVC), Backend::K8s);
        });
    }

    #[test]
    fn global_k8s_applies_when_service_unset() {
        with_env(&[(SVC, None), (GLOBAL_BACKEND_ENV, Some("k8s"))], || {
            assert_eq!(backend_choice(SVC), Backend::K8s);
        });
    }

    #[test]
    fn per_service_docker_overrides_global_k8s() {
        with_env(
            &[(SVC, Some("docker")), (GLOBAL_BACKEND_ENV, Some("k8s"))],
            || {
                assert_eq!(backend_choice(SVC), Backend::Docker);
            },
        );
    }

    #[test]
    fn case_insensitive_and_trimmed() {
        with_env(
            &[(SVC, Some("  K8s  ")), (GLOBAL_BACKEND_ENV, None)],
            || {
                assert_eq!(backend_choice(SVC), Backend::K8s);
            },
        );
    }

    #[test]
    fn unrecognized_service_value_falls_through_to_global() {
        with_env(
            &[(SVC, Some("banana")), (GLOBAL_BACKEND_ENV, Some("k8s"))],
            || {
                assert_eq!(backend_choice(SVC), Backend::K8s);
            },
        );
    }

    #[test]
    fn unrecognized_everywhere_is_docker() {
        with_env(
            &[(SVC, Some("banana")), (GLOBAL_BACKEND_ENV, Some("nope"))],
            || {
                assert_eq!(backend_choice(SVC), Backend::Docker);
            },
        );
    }
}
