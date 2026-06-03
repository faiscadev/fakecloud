//! Parsing of the `FAKECLOUD_K8S_*` operator configuration.
//!
//! When FakeCloud runs on Kubernetes it needs to know how Pods reach it
//! back (`FAKECLOUD_K8S_SELF_URL`), which namespace to create Pods in
//! (`FAKECLOUD_K8S_NAMESPACE`), where its in-cluster ECR endpoint lives
//! (`FAKECLOUD_K8S_ECR_URL`), and optionally an `imagePullSecrets` name
//! for private images (`FAKECLOUD_K8S_PULL_SECRET`). Every k8s backend
//! shares this configuration, so it's parsed once here.

/// Parsed `FAKECLOUD_K8S_*` configuration.
#[derive(Debug, Clone)]
pub struct K8sEnv {
    /// Namespace Pods are created in. Defaults to `default`.
    pub namespace: String,
    /// In-cluster URL of the FakeCloud server, e.g.
    /// `http://fakecloud.fakecloud.svc.cluster.local:4566`. Pods fetch
    /// artifacts from and call back to this URL.
    pub self_url: String,
    /// Host part of [`self_url`](Self::self_url) — used to rewrite
    /// `localhost`/`127.0.0.1` env values so workloads inside a Pod can
    /// reach FakeCloud.
    pub self_host: String,
    /// Port part of [`self_url`](Self::self_url).
    pub self_port: u16,
    /// Host of the in-cluster ECR endpoint (for image URI translation).
    /// Defaults to [`self_host`](Self::self_host).
    pub ecr_host: String,
    /// Port of the in-cluster ECR endpoint. Defaults to
    /// [`self_port`](Self::self_port).
    pub ecr_port: u16,
    /// Optional name of a `kubernetes.io/dockerconfigjson` Secret used as
    /// `imagePullSecrets` for Pods pulling private images.
    pub pull_secret: Option<String>,
}

/// Errors parsing the `FAKECLOUD_K8S_*` configuration.
#[derive(Debug, thiserror::Error)]
pub enum K8sEnvError {
    #[error("FAKECLOUD_K8S_SELF_URL must be set when using the Kubernetes backend")]
    MissingSelfUrl,
    #[error("FAKECLOUD_K8S_SELF_URL is not a valid URL: {0}")]
    InvalidSelfUrl(String),
    #[error("FAKECLOUD_K8S_ECR_URL is not a valid URL: {0}")]
    InvalidEcrUrl(String),
}

impl K8sEnv {
    /// Read configuration from the environment. `default_port` is
    /// FakeCloud's bound port, used as the self/ECR port when the URL
    /// omits one. Fails fast on missing/invalid required config — never
    /// silently degrades.
    pub fn from_env(default_port: u16) -> Result<Self, K8sEnvError> {
        let self_url =
            std::env::var("FAKECLOUD_K8S_SELF_URL").map_err(|_| K8sEnvError::MissingSelfUrl)?;
        let parsed = reqwest::Url::parse(&self_url)
            .map_err(|e| K8sEnvError::InvalidSelfUrl(e.to_string()))?;
        let self_host = parsed
            .host_str()
            .ok_or_else(|| K8sEnvError::InvalidSelfUrl("missing host".into()))?
            .to_string();
        // When the URL omits a port, fall back to fakecloud's actual bound
        // port — not the scheme default (80/443), which fakecloud doesn't
        // serve on in-cluster.
        let self_port = parsed.port().unwrap_or(default_port);

        let (ecr_host, ecr_port) = match std::env::var("FAKECLOUD_K8S_ECR_URL").ok() {
            Some(raw) => {
                let u = reqwest::Url::parse(&raw)
                    .map_err(|e| K8sEnvError::InvalidEcrUrl(e.to_string()))?;
                let h = u
                    .host_str()
                    .ok_or_else(|| K8sEnvError::InvalidEcrUrl("missing host".into()))?
                    .to_string();
                let p = u.port().unwrap_or(default_port);
                (h, p)
            }
            None => (self_host.clone(), self_port),
        };

        // An explicitly-empty value is treated as unset: an empty namespace
        // is invalid, and an empty pull-secret name would attach a bogus
        // imagePullSecrets reference.
        let namespace = std::env::var("FAKECLOUD_K8S_NAMESPACE")
            .ok()
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| "default".to_string());
        let pull_secret = std::env::var("FAKECLOUD_K8S_PULL_SECRET")
            .ok()
            .filter(|s| !s.trim().is_empty());

        Ok(Self {
            namespace,
            self_url,
            self_host,
            self_port,
            ecr_host,
            ecr_port,
            pull_secret,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    // from_env reads process-global vars; serialize the tests touching them.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    const VARS: &[&str] = &[
        "FAKECLOUD_K8S_SELF_URL",
        "FAKECLOUD_K8S_ECR_URL",
        "FAKECLOUD_K8S_NAMESPACE",
        "FAKECLOUD_K8S_PULL_SECRET",
    ];

    /// Restores saved env vars on drop, so a panicking test closure can't
    /// leak its mutated environment into the next test.
    struct EnvGuard(Vec<(String, Option<String>)>);
    impl Drop for EnvGuard {
        fn drop(&mut self) {
            for (k, v) in &self.0 {
                match v {
                    Some(v) => std::env::set_var(k, v),
                    None => std::env::remove_var(k),
                }
            }
        }
    }

    fn with_env(set: &[(&str, &str)], f: impl FnOnce()) {
        let _g = ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        let _restore = EnvGuard(
            VARS.iter()
                .map(|k| (k.to_string(), std::env::var(k).ok()))
                .collect(),
        );
        for k in VARS {
            std::env::remove_var(k);
        }
        for (k, v) in set {
            std::env::set_var(k, v);
        }
        f();
    }

    #[test]
    fn missing_self_url_errors() {
        with_env(&[], || {
            assert!(matches!(
                K8sEnv::from_env(4566),
                Err(K8sEnvError::MissingSelfUrl)
            ));
        });
    }

    #[test]
    fn portless_url_falls_back_to_default_port_not_scheme_default() {
        with_env(
            &[("FAKECLOUD_K8S_SELF_URL", "http://fakecloud.svc")],
            || {
                let env = K8sEnv::from_env(4566).unwrap();
                // Not 80 (the http scheme default) — fakecloud's bound port.
                assert_eq!(env.self_port, 4566);
                assert_eq!(env.self_host, "fakecloud.svc");
                // ECR defaults to the self host:port when its URL is unset.
                assert_eq!(env.ecr_host, "fakecloud.svc");
                assert_eq!(env.ecr_port, 4566);
            },
        );
    }

    #[test]
    fn explicit_port_is_respected() {
        with_env(
            &[("FAKECLOUD_K8S_SELF_URL", "http://fakecloud.svc:4566")],
            || {
                let env = K8sEnv::from_env(9999).unwrap();
                assert_eq!(env.self_port, 4566);
            },
        );
    }

    #[test]
    fn ecr_url_overrides_and_defaults_namespace_pull_secret() {
        with_env(
            &[
                ("FAKECLOUD_K8S_SELF_URL", "http://fakecloud.svc:4566"),
                ("FAKECLOUD_K8S_ECR_URL", "http://registry.svc:5000"),
                ("FAKECLOUD_K8S_NAMESPACE", "fc"),
                ("FAKECLOUD_K8S_PULL_SECRET", "ecr-secret"),
            ],
            || {
                let env = K8sEnv::from_env(4566).unwrap();
                assert_eq!(env.ecr_host, "registry.svc");
                assert_eq!(env.ecr_port, 5000);
                assert_eq!(env.namespace, "fc");
                assert_eq!(env.pull_secret.as_deref(), Some("ecr-secret"));
            },
        );
    }

    #[test]
    fn empty_namespace_and_pull_secret_treated_as_unset() {
        with_env(
            &[
                ("FAKECLOUD_K8S_SELF_URL", "http://fakecloud.svc:4566"),
                ("FAKECLOUD_K8S_NAMESPACE", "  "),
                ("FAKECLOUD_K8S_PULL_SECRET", ""),
            ],
            || {
                let env = K8sEnv::from_env(4566).unwrap();
                assert_eq!(env.namespace, "default");
                assert_eq!(env.pull_secret, None);
            },
        );
    }

    #[test]
    fn namespace_defaults_to_default() {
        with_env(
            &[("FAKECLOUD_K8S_SELF_URL", "http://fakecloud.svc:4566")],
            || {
                assert_eq!(K8sEnv::from_env(4566).unwrap().namespace, "default");
            },
        );
    }
}
