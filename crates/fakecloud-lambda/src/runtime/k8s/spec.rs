//! Pod spec construction for the Kubernetes [`super::K8sBackend`].
//!
//! Pure functions over `k8s_openapi::api::core::v1::Pod` — no cluster
//! interaction, fully unit-testable.

use std::collections::BTreeMap;

use k8s_openapi::api::core::v1::{
    Container, EmptyDirVolumeSource, EnvVar, LocalObjectReference, Pod, PodSpec,
    ResourceRequirements, Volume, VolumeMount,
};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;

use fakecloud_k8s::names::label_safe;

use super::super::docker::runtime_to_image;
use super::super::env_rewrite::rewrite_localhost_envs;
use crate::state::LambdaFunction;

/// Inputs that don't come from the function itself — instance identity,
/// the in-cluster fakecloud URL, the bearer token the init container
/// uses to fetch code/layers, etc.
pub struct PodSpecContext<'a> {
    pub instance_id: &'a str,
    pub namespace: &'a str,
    /// In-cluster URL of the fakecloud server (e.g.
    /// `http://fakecloud.fakecloud.svc.cluster.local:4566`). Init
    /// containers fetch code + layers from this host.
    pub self_url: &'a str,
    /// Host part of `self_url` — used to rewrite `localhost`/`127.0.0.1`
    /// env values so user code can reach fakecloud from inside the Pod.
    pub self_host: &'a str,
    /// Host:port for the fakecloud ECR endpoint (for `PackageType=Image`
    /// functions that reference AWS private-ECR URIs).
    pub ecr_host: &'a str,
    pub ecr_port: u16,
    /// Bearer token the init container presents when fetching code +
    /// layers from `self_url`. Never logged.
    pub internal_token: &'a str,
    /// Account ID owning the function. Embedded in init-container URLs
    /// so the artifact endpoint can find the right LambdaState.
    pub account_id: &'a str,
    /// Optional name of a Kubernetes `Secret` of type
    /// `kubernetes.io/dockerconfigjson` used as `imagePullSecrets` for
    /// container-image functions.
    pub pull_secret: Option<&'a str>,
}

/// Resource cap for the `/tmp` `emptyDir` (`medium: Memory`) — sized
/// from the function's `ephemeral_storage_size`. AWS defaults to 512
/// MiB; clamp at 64 MiB so wildly stale snapshots still produce a spec
/// the kubelet accepts.
fn ephemeral_storage_mib(size: Option<i64>) -> i64 {
    size.unwrap_or(512).max(64)
}

/// Build the Pod spec for a single Lambda function invocation runtime.
/// The Pod has one init container (busybox) that downloads code + layers
/// from fakecloud over HTTP, and one main container running the AWS RIE
/// image (zip functions) or the user-supplied image (image functions).
pub fn build_pod_spec(
    func: &LambdaFunction,
    deploy_id: &str,
    ctx: &PodSpecContext<'_>,
) -> Result<Pod, String> {
    let is_image = func.package_type == "Image";

    let main_image = if is_image {
        let raw = func
            .image_uri
            .as_deref()
            .ok_or_else(|| "PackageType=Image function has no ImageUri".to_string())?;
        fakecloud_core::ecr_uri::translate_to_local_at(raw, ctx.ecr_host, ctx.ecr_port)
            .unwrap_or_else(|| raw.to_string())
    } else {
        runtime_to_image(&func.runtime)
            .ok_or_else(|| format!("unsupported runtime: {}", func.runtime))?
    };

    let pod_name = pod_name_for(&func.function_name, deploy_id);

    let mut labels = BTreeMap::new();
    labels.insert(
        fakecloud_k8s::labels::MANAGED_BY.into(),
        fakecloud_k8s::labels::MANAGED_BY_VALUE.into(),
    );
    labels.insert(
        fakecloud_k8s::labels::INSTANCE.into(),
        ctx.instance_id.into(),
    );
    labels.insert(fakecloud_k8s::labels::SERVICE.into(), super::SERVICE.into());
    labels.insert("fakecloud-lambda".into(), label_safe(&func.function_name));
    labels.insert(
        "fakecloud-deploy-id".into(),
        label_safe(&deploy_id[..deploy_id.len().min(40)]),
    );

    // Env vars — user-supplied (with localhost rewritten to in-cluster
    // fakecloud host) + the AWS_LAMBDA_FUNCTION_TIMEOUT the RIE honors.
    let mut env: Vec<EnvVar> = rewrite_localhost_envs(&func.environment, ctx.self_host)
        .into_iter()
        .map(|(k, v)| EnvVar {
            name: k,
            value: Some(v),
            value_from: None,
        })
        .collect();
    env.push(EnvVar {
        name: "AWS_LAMBDA_FUNCTION_TIMEOUT".into(),
        value: Some(func.timeout.to_string()),
        value_from: None,
    });

    // Shared volumes — init container writes /var/task, /opt, layers;
    // main container reads them. `/tmp` is sized from ephemeral_storage.
    let tmp_mib = ephemeral_storage_mib(func.ephemeral_storage_size);
    let volumes = vec![
        Volume {
            name: "fakecloud-task".into(),
            empty_dir: Some(EmptyDirVolumeSource::default()),
            ..Volume::default()
        },
        Volume {
            name: "fakecloud-opt".into(),
            empty_dir: Some(EmptyDirVolumeSource::default()),
            ..Volume::default()
        },
        Volume {
            name: "fakecloud-tmp".into(),
            empty_dir: Some(EmptyDirVolumeSource {
                medium: Some("Memory".into()),
                size_limit: Some(Quantity(format!("{tmp_mib}Mi"))),
            }),
            ..Volume::default()
        },
    ];

    let common_mounts = vec![
        VolumeMount {
            name: "fakecloud-task".into(),
            mount_path: "/var/task".into(),
            ..VolumeMount::default()
        },
        VolumeMount {
            name: "fakecloud-opt".into(),
            mount_path: "/opt".into(),
            ..VolumeMount::default()
        },
        VolumeMount {
            name: "fakecloud-tmp".into(),
            mount_path: "/tmp".into(),
            ..VolumeMount::default()
        },
    ];

    // Init container: busybox + wget. Downloads code zip + layers tar
    // and unpacks them into the shared emptyDir volumes. For
    // image-package functions the code+layers endpoint short-circuits
    // (image already contains code) so we only fetch the layers tar.
    let init_script = if is_image {
        "set -eu; \
         wget -q --header=\"authorization: Bearer $FAKECLOUD_INTERNAL_TOKEN\" \
              -O /tmp/layers.tar \
              \"$FAKECLOUD_SELF_URL/_fakecloud/lambda/_internal/layers/$ACCT/$FN/$DEPLOY_ID.tar\"; \
         if [ -s /tmp/layers.tar ]; then tar -xf /tmp/layers.tar -C /opt; fi"
            .to_string()
    } else {
        "set -eu; \
         wget -q --header=\"authorization: Bearer $FAKECLOUD_INTERNAL_TOKEN\" \
              -O /tmp/code.zip \
              \"$FAKECLOUD_SELF_URL/_fakecloud/lambda/_internal/code/$ACCT/$FN/$DEPLOY_ID.zip\"; \
         unzip -q /tmp/code.zip -d /var/task; \
         wget -q --header=\"authorization: Bearer $FAKECLOUD_INTERNAL_TOKEN\" \
              -O /tmp/layers.tar \
              \"$FAKECLOUD_SELF_URL/_fakecloud/lambda/_internal/layers/$ACCT/$FN/$DEPLOY_ID.tar\"; \
         if [ -s /tmp/layers.tar ]; then tar -xf /tmp/layers.tar -C /opt; fi"
            .to_string()
    };

    let init_env = vec![
        EnvVar {
            name: "FAKECLOUD_SELF_URL".into(),
            value: Some(ctx.self_url.into()),
            value_from: None,
        },
        EnvVar {
            name: "FAKECLOUD_INTERNAL_TOKEN".into(),
            value: Some(ctx.internal_token.into()),
            value_from: None,
        },
        EnvVar {
            name: "ACCT".into(),
            value: Some(ctx.account_id.into()),
            value_from: None,
        },
        EnvVar {
            name: "FN".into(),
            value: Some(func.function_name.clone()),
            value_from: None,
        },
        EnvVar {
            name: "DEPLOY_ID".into(),
            value: Some(deploy_id.into()),
            value_from: None,
        },
    ];

    let init_container = Container {
        name: "fakecloud-init".into(),
        // busybox ships `wget`, `unzip`, `tar` — covers everything the
        // bootstrap script needs without depending on what's in the
        // runtime image.
        image: Some("busybox:1.36".into()),
        command: Some(vec!["sh".into(), "-c".into(), init_script]),
        env: Some(init_env),
        volume_mounts: Some(common_mounts.clone()),
        ..Container::default()
    };

    // Main container — runs the RIE image (zip) or the user's image.
    // For zip functions the handler is passed as the cmd argument, same
    // as Docker (`docker create <image> <handler>`).
    let mut main_container = Container {
        name: "fakecloud-lambda".into(),
        image: Some(main_image.clone()),
        env: Some(env),
        volume_mounts: Some(common_mounts),
        resources: Some(memory_resources(func.memory_size)),
        ..Container::default()
    };
    if !is_image {
        main_container.command = None;
        main_container.args = Some(vec![func.handler.clone()]);
    }

    let pull_secrets = ctx.pull_secret.map(|name| {
        vec![LocalObjectReference {
            name: name.to_string(),
        }]
    });

    Ok(Pod {
        metadata: ObjectMeta {
            name: Some(pod_name),
            namespace: Some(ctx.namespace.to_string()),
            labels: Some(labels),
            ..ObjectMeta::default()
        },
        spec: Some(PodSpec {
            restart_policy: Some("Never".into()),
            init_containers: Some(vec![init_container]),
            containers: vec![main_container],
            volumes: Some(volumes),
            image_pull_secrets: pull_secrets,
            ..PodSpec::default()
        }),
        ..Pod::default()
    })
}

/// Build a deterministic, DNS-1123-safe Pod name for the given
/// function + deploy_id. Truncated/lowercased so it fits the 63-char
/// label limit. The suffix is a stable hash of `deploy_id` so a new
/// deploy gets a fresh, non-colliding Pod name.
pub fn pod_name_for(function_name: &str, deploy_id: &str) -> String {
    fakecloud_k8s::names::pod_name("fakecloud-lambda", function_name, deploy_id)
}

/// Build a per-launch *unique* DNS-1123-safe Pod name. The deterministic
/// [`pod_name_for`] name collides whenever the warm pool needs more than one
/// concurrent instance of a function (the RIE serves one invocation at a time),
/// and a still-terminating Pod blocks its replacement (`AlreadyExists` /
/// "object is being deleted" / `NotFound` races). Salting the hashed id with a
/// process-monotonic counter yields a fresh, length-bounded name per launch, so
/// instances never collide and a dying Pod never wedges a new one.
pub fn unique_pod_name(function_name: &str, deploy_id: &str) -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let n = SEQ.fetch_add(1, Ordering::Relaxed);
    let salted = format!("{deploy_id}-{n}");
    fakecloud_k8s::names::pod_name("fakecloud-lambda", function_name, &salted)
}

/// Map the function's `memory_size` (MiB) onto both `requests` and
/// `limits`. Mirrors real Lambda: CPU is implicitly proportional to
/// memory, but k8s requires explicit values, so we set memory only and
/// leave CPU unlimited (clusters with LimitRange policies can layer
/// their own defaults).
fn memory_resources(memory_size: i64) -> ResourceRequirements {
    let mut req = BTreeMap::new();
    req.insert(
        "memory".to_string(),
        Quantity(format!("{}Mi", memory_size.max(128))),
    );
    let mut lim = BTreeMap::new();
    lim.insert(
        "memory".to_string(),
        Quantity(format!("{}Mi", memory_size.max(128))),
    );
    ResourceRequirements {
        requests: Some(req),
        limits: Some(lim),
        claims: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::LambdaFunction;
    use chrono::Utc;

    fn ctx<'a>() -> PodSpecContext<'a> {
        PodSpecContext {
            instance_id: "fakecloud-1234",
            namespace: "fakecloud",
            self_url: "http://fakecloud.fakecloud.svc.cluster.local:4566",
            self_host: "fakecloud.fakecloud.svc.cluster.local",
            ecr_host: "fakecloud.fakecloud.svc.cluster.local",
            ecr_port: 4566,
            internal_token: "secret-token-xyz",
            account_id: "000000000000",
            pull_secret: None,
        }
    }

    fn zip_function(name: &str) -> LambdaFunction {
        let mut f = LambdaFunction {
            function_name: name.into(),
            function_arn: format!("arn:aws:lambda:us-east-1:000000000000:function:{name}"),
            runtime: "python3.12".into(),
            role: "arn:aws:iam::000000000000:role/test".into(),
            handler: "lambda_function.lambda_handler".into(),
            description: String::new(),
            timeout: 30,
            memory_size: 256,
            code_sha256: "abc".into(),
            code_size: 0,
            version: "$LATEST".into(),
            last_modified: Utc::now(),
            tags: BTreeMap::new(),
            environment: BTreeMap::new(),
            architectures: vec!["x86_64".into()],
            package_type: "Zip".into(),
            code_zip: None,
            image_uri: None,
            policy: None,
            layers: Vec::new(),
            revision_id: "rev-1".into(),
            tracing_mode: None,
            kms_key_arn: None,
            ephemeral_storage_size: Some(1024),
            vpc_config: None,
            snap_start: None,
            dead_letter_config_arn: None,
            file_system_configs: Vec::new(),
            logging_config: None,
            image_config: None,
            durable_config: None,
            signing_profile_version_arn: None,
            signing_job_arn: None,
            runtime_version_config: None,
            master_arn: None,
            state_reason: None,
            state_reason_code: None,
            last_update_status_reason: None,
            last_update_status_reason_code: None,
        };
        f.environment
            .insert("FAKECLOUD_URL".into(), "http://localhost:4566".into());
        f
    }

    #[test]
    fn zip_pod_has_init_container_with_code_download() {
        let f = zip_function("my-fn");
        let pod = build_pod_spec(&f, "deploy-xyz", &ctx()).unwrap();
        let spec = pod.spec.unwrap();
        let init = &spec.init_containers.unwrap()[0];
        let script = init.command.as_ref().unwrap().last().unwrap();
        assert!(
            script.contains("code/$ACCT/$FN/$DEPLOY_ID.zip"),
            "init script must include code download for zip functions: {script}"
        );
        assert!(
            script.contains("layers/$ACCT/$FN/$DEPLOY_ID.tar"),
            "init script must include layers download: {script}"
        );
        // Bearer header references env, not the literal token, so we
        // don't leak the secret into describe output / logs.
        assert!(script.contains("$FAKECLOUD_INTERNAL_TOKEN"));
        assert!(!script.contains("secret-token-xyz"));
    }

    #[test]
    fn image_pod_skips_code_download() {
        let mut f = zip_function("img-fn");
        f.package_type = "Image".into();
        f.image_uri = Some("public.ecr.aws/test/img:1".into());
        let pod = build_pod_spec(&f, "d", &ctx()).unwrap();
        let init = &pod.spec.unwrap().init_containers.unwrap()[0];
        let script = init.command.as_ref().unwrap().last().unwrap();
        assert!(!script.contains("code/"));
        assert!(script.contains("layers/"));
    }

    #[test]
    fn image_pod_translates_aws_ecr_uri() {
        let mut f = zip_function("img-fn");
        f.package_type = "Image".into();
        f.image_uri = Some("123456789012.dkr.ecr.us-east-1.amazonaws.com/repo:tag".into());
        let pod = build_pod_spec(&f, "d", &ctx()).unwrap();
        let image = pod.spec.unwrap().containers[0].image.clone().unwrap();
        assert_eq!(image, "fakecloud.fakecloud.svc.cluster.local:4566/repo:tag");
    }

    #[test]
    fn image_pod_leaves_non_ecr_uri_alone() {
        let mut f = zip_function("img-fn");
        f.package_type = "Image".into();
        f.image_uri = Some("public.ecr.aws/test/img:1".into());
        let pod = build_pod_spec(&f, "d", &ctx()).unwrap();
        let image = pod.spec.unwrap().containers[0].image.clone().unwrap();
        assert_eq!(image, "public.ecr.aws/test/img:1");
    }

    #[test]
    fn zip_pod_uses_runtime_to_image_for_main_container() {
        let f = zip_function("my-fn");
        let pod = build_pod_spec(&f, "d", &ctx()).unwrap();
        let image = pod.spec.unwrap().containers[0].image.clone().unwrap();
        assert_eq!(image, "public.ecr.aws/lambda/python:3.12");
    }

    #[test]
    fn handler_passed_as_args_for_zip_pod() {
        let f = zip_function("my-fn");
        let pod = build_pod_spec(&f, "d", &ctx()).unwrap();
        let args = pod.spec.unwrap().containers[0].args.clone().unwrap();
        assert_eq!(args, vec!["lambda_function.lambda_handler"]);
    }

    #[test]
    fn env_vars_rewrite_localhost_to_self_host() {
        let f = zip_function("my-fn");
        let pod = build_pod_spec(&f, "d", &ctx()).unwrap();
        let env = pod.spec.unwrap().containers[0].env.clone().unwrap();
        let url = env
            .iter()
            .find(|e| e.name == "FAKECLOUD_URL")
            .unwrap()
            .value
            .clone()
            .unwrap();
        assert_eq!(url, "http://fakecloud.fakecloud.svc.cluster.local:4566");
    }

    #[test]
    fn function_timeout_env_set() {
        let f = zip_function("my-fn");
        let pod = build_pod_spec(&f, "d", &ctx()).unwrap();
        let env = pod.spec.unwrap().containers[0].env.clone().unwrap();
        let t = env
            .iter()
            .find(|e| e.name == "AWS_LAMBDA_FUNCTION_TIMEOUT")
            .unwrap()
            .value
            .clone()
            .unwrap();
        assert_eq!(t, "30");
    }

    #[test]
    fn ephemeral_storage_maps_to_emptydir_memory_with_size_limit() {
        let f = zip_function("my-fn");
        let pod = build_pod_spec(&f, "d", &ctx()).unwrap();
        let vol = pod
            .spec
            .unwrap()
            .volumes
            .unwrap()
            .into_iter()
            .find(|v| v.name == "fakecloud-tmp")
            .unwrap();
        let ed = vol.empty_dir.unwrap();
        assert_eq!(ed.medium.as_deref(), Some("Memory"));
        assert_eq!(ed.size_limit.unwrap().0, "1024Mi");
    }

    #[test]
    fn ephemeral_storage_defaults_to_512mib() {
        let mut f = zip_function("my-fn");
        f.ephemeral_storage_size = None;
        let pod = build_pod_spec(&f, "d", &ctx()).unwrap();
        let vol = pod
            .spec
            .unwrap()
            .volumes
            .unwrap()
            .into_iter()
            .find(|v| v.name == "fakecloud-tmp")
            .unwrap();
        assert_eq!(vol.empty_dir.unwrap().size_limit.unwrap().0, "512Mi");
    }

    #[test]
    fn memory_size_maps_to_resources_requests_and_limits() {
        let f = zip_function("my-fn");
        let pod = build_pod_spec(&f, "d", &ctx()).unwrap();
        let res = pod.spec.unwrap().containers[0].resources.clone().unwrap();
        assert_eq!(res.requests.unwrap().get("memory").unwrap().0, "256Mi");
        assert_eq!(res.limits.unwrap().get("memory").unwrap().0, "256Mi");
    }

    #[test]
    fn labels_identify_fakecloud_ownership() {
        let f = zip_function("my-fn");
        let pod = build_pod_spec(&f, "deploy-xyz", &ctx()).unwrap();
        let labels = pod.metadata.labels.unwrap();
        assert_eq!(
            labels.get("fakecloud-managed-by"),
            Some(&"fakecloud".into())
        );
        assert_eq!(
            labels.get("fakecloud-instance"),
            Some(&"fakecloud-1234".into())
        );
        assert_eq!(labels.get("fakecloud-lambda"), Some(&"my-fn".into()));
        assert!(labels.contains_key("fakecloud-deploy-id"));
    }

    #[test]
    fn pull_secret_attached_when_configured() {
        let f = zip_function("my-fn");
        let mut c = ctx();
        c.pull_secret = Some("fakecloud-ecr-secret");
        let pod = build_pod_spec(&f, "d", &c).unwrap();
        let secrets = pod.spec.unwrap().image_pull_secrets.unwrap();
        assert_eq!(secrets.len(), 1);
        assert_eq!(secrets[0].name, "fakecloud-ecr-secret");
    }

    #[test]
    fn no_pull_secret_when_unset() {
        let f = zip_function("my-fn");
        let pod = build_pod_spec(&f, "d", &ctx()).unwrap();
        assert!(pod.spec.unwrap().image_pull_secrets.is_none());
    }

    #[test]
    fn pod_name_is_dns1123_safe() {
        let name = pod_name_for("My_Awesome_Function", "abc/123+def==");
        assert!(name.chars().all(|c| c.is_ascii_alphanumeric() || c == '-'));
        assert!(!name.ends_with('-'));
        assert!(name.len() <= 63);
    }

    #[test]
    fn pod_name_is_stable_for_same_inputs() {
        let a = pod_name_for("fn", "deploy");
        let b = pod_name_for("fn", "deploy");
        assert_eq!(a, b);
    }

    #[test]
    fn pod_name_differs_when_deploy_id_changes() {
        let a = pod_name_for("fn", "deploy-1");
        let b = pod_name_for("fn", "deploy-2");
        assert_ne!(a, b);
    }

    #[test]
    fn unique_pod_name_differs_across_calls() {
        // Same function + deploy_id must still yield distinct names so the
        // warm pool can run more than one concurrent instance and a
        // still-terminating Pod never blocks its replacement.
        let a = unique_pod_name("fn", "deploy");
        let b = unique_pod_name("fn", "deploy");
        let c = unique_pod_name("fn", "deploy");
        assert_ne!(a, b);
        assert_ne!(b, c);
        assert_ne!(a, c);
    }

    #[test]
    fn unique_pod_name_is_dns1123_safe_and_bounded() {
        // Hostile inputs and a large counter must stay DNS-1123-safe and
        // within the 63-char Pod-name limit.
        for _ in 0..1000 {
            let name = unique_pod_name("My_Awesome_Function", "abc/123+def==");
            assert!(name.chars().all(|c| c.is_ascii_alphanumeric() || c == '-'));
            assert!(!name.starts_with('-'));
            assert!(!name.ends_with('-'));
            assert!(name.len() <= 63);
        }
    }

    #[test]
    fn per_function_tags_override_base_pod_config() {
        use fakecloud_k8s::K8sPodConfig;
        use std::collections::BTreeMap;

        // Backend base (global + service env): one node-selector key.
        let base = K8sPodConfig {
            node_selector: BTreeMap::from([
                ("disktype".to_string(), "ssd".to_string()),
                ("zone".to_string(), "a".to_string()),
            ]),
            ..Default::default()
        };

        // Function carries a reserved tag overriding disktype and adding
        // an annotation.
        let mut f = zip_function("my-fn");
        f.tags
            .insert("fakecloud-k8s/node-selector".into(), "disktype=nvme".into());
        f.tags
            .insert("fakecloud-k8s/annotations".into(), "team=core".into());

        let mut pod = build_pod_spec(&f, "d", &ctx()).unwrap();
        // Mirror the launch() contract: base merged with per-function tags.
        base.merge(K8sPodConfig::from_tags(&f.tags)).apply(&mut pod);

        let selector = pod.spec.unwrap().node_selector.unwrap();
        // Tag wins on disktype; the base-only zone key survives.
        assert_eq!(selector.get("disktype").map(String::as_str), Some("nvme"));
        assert_eq!(selector.get("zone").map(String::as_str), Some("a"));
        assert_eq!(
            pod.metadata
                .annotations
                .unwrap()
                .get("team")
                .map(String::as_str),
            Some("core")
        );
    }

    #[test]
    fn restart_policy_never() {
        let f = zip_function("my-fn");
        let pod = build_pod_spec(&f, "d", &ctx()).unwrap();
        assert_eq!(pod.spec.unwrap().restart_policy.as_deref(), Some("Never"));
    }

    #[test]
    fn unsupported_runtime_errors() {
        let mut f = zip_function("my-fn");
        f.runtime = "cobol1.0".into();
        let err = build_pod_spec(&f, "d", &ctx()).unwrap_err();
        assert!(err.contains("unsupported runtime"));
    }

    #[test]
    fn image_function_without_uri_errors() {
        let mut f = zip_function("my-fn");
        f.package_type = "Image".into();
        f.image_uri = None;
        let err = build_pod_spec(&f, "d", &ctx()).unwrap_err();
        assert!(err.contains("ImageUri"));
    }
}
