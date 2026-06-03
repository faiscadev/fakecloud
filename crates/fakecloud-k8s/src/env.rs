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
        let self_port = parsed.port_or_known_default().unwrap_or(default_port);

        let (ecr_host, ecr_port) = match std::env::var("FAKECLOUD_K8S_ECR_URL").ok() {
            Some(raw) => {
                let u = reqwest::Url::parse(&raw)
                    .map_err(|e| K8sEnvError::InvalidEcrUrl(e.to_string()))?;
                let h = u
                    .host_str()
                    .ok_or_else(|| K8sEnvError::InvalidEcrUrl("missing host".into()))?
                    .to_string();
                let p = u.port_or_known_default().unwrap_or(default_port);
                (h, p)
            }
            None => (self_host.clone(), self_port),
        };

        let namespace =
            std::env::var("FAKECLOUD_K8S_NAMESPACE").unwrap_or_else(|_| "default".to_string());
        let pull_secret = std::env::var("FAKECLOUD_K8S_PULL_SECRET").ok();

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
