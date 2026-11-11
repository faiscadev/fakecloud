//! Pod spec construction for the Kubernetes [`super::K8sBackend`].
//!
//! Pure functions over `k8s_openapi::api::core::v1::Pod` — no cluster
//! interaction, fully unit-testable.

use std::collections::BTreeMap;

use k8s_openapi::api::core::v1::{
    Container, EmptyDirVolumeSource, EnvVar, EnvVarSource, HostAlias, LocalObjectReference, Pod,
    PodSpec, ResourceRequirements, Secret, SecretKeySelector, Volume, VolumeMount,
};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;

use fakecloud_k8s::names::label_safe;

use super::super::docker::runtime_to_image;
use super::super::environment::{credential_envs, function_environment, CREDENTIAL_ENV_KEYS};
use crate::state::LambdaFunction;
use fakecloud_core::auth::SessionCredentials;

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
    /// Name of the Secret holding this instance's execution-role
    /// credentials (see [`build_credentials_secret`]); `None` exports none.
    pub credentials_secret: Option<&'a str>,
    /// This instance's Runtime API host name (see
    /// [`crate::runtime::api::sandbox_host`]).
    pub runtime_api_host: &'a str,
    /// The address the Runtime API host is mapped to (`hostAliases`): the
    /// fakecloud Service the Pod reaches `self_url` through.
    pub runtime_api_ip: &'a str,
    /// Port of `self_url`.
    pub self_port: u16,
    /// The function's entrypoint and command, run through the extension
    /// launcher (see [`crate::runtime::extensions`]). `None` for an image
    /// whose command could not be read: it then runs as is, without its
    /// extensions.
    pub launch_command: Option<Vec<String>>,
}

/// `value` as Kubernetes passes it on unchanged: the kubelet expands
/// `$(VAR)` references in a container's `command`, `args` and `env` values
/// (and reduces `$$` to `$`), so every `$` is doubled.
fn verbatim(value: &str) -> String {
    value.replace('$', "$$")
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
/// from fakecloud over HTTP, and one main container running the AWS base
/// image (zip functions) or the user-supplied image (image functions), whose
/// runtime reaches the Runtime API at its own host name, mapped to the
/// fakecloud Service with `hostAliases`.
pub fn build_pod_spec(
    func: &LambdaFunction,
    deploy_id: &str,
    ctx: &PodSpecContext<'_>,
) -> Result<Pod, String> {
    let is_image = func.package_type == "Image";

    let main_image = main_image(func, ctx.ecr_host, ctx.ecr_port)?;

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

    // The function's execution environment: the reserved Lambda variables
    // plus the user's own (localhost rewritten to the in-cluster fakecloud
    // host), with SDK calls pointed at the same in-cluster fakecloud URL the
    // init container fetches code from.
    // Credentials never sit in the Pod spec: they are read from the
    // per-Pod Secret (see [`build_credentials_secret`]).
    let endpoint_url = ctx.self_url.trim_end_matches('/');
    let runtime_api = format!("{}:{}", ctx.runtime_api_host, ctx.self_port);
    let mut env: Vec<EnvVar> =
        function_environment(func, endpoint_url, ctx.self_host, &runtime_api, None)
            .into_iter()
            .map(|(k, v)| EnvVar {
                name: k,
                value: Some(verbatim(&v)),
                value_from: None,
            })
            .collect();
    if let Some(secret) = ctx.credentials_secret {
        env.extend(CREDENTIAL_ENV_KEYS.iter().map(|key| EnvVar {
            name: (*key).to_string(),
            value: None,
            value_from: Some(EnvVarSource {
                secret_key_ref: Some(SecretKeySelector {
                    name: secret.to_string(),
                    key: (*key).to_string(),
                    optional: Some(false),
                }),
                ..EnvVarSource::default()
            }),
        }));
    }

    // Shared volumes — init container writes /var/task, /opt, layers;
    // main container reads them. `/tmp` is sized from ephemeral_storage.
    let tmp_mib = ephemeral_storage_mib(func.ephemeral_storage_size);
    let mut volumes = vec![
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

    let mut common_mounts = vec![
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

    // An image keeps its own `/opt` (its extensions live there) unless
    // layers are attached; a zip function's comes from its layers.
    if is_image && func.layers.is_empty() {
        volumes.retain(|v| v.name != "fakecloud-opt");
        common_mounts.retain(|m| m.name != "fakecloud-opt");
    }
    if ctx.launch_command.is_some() {
        volumes.push(Volume {
            name: "fakecloud-launcher".into(),
            empty_dir: Some(EmptyDirVolumeSource::default()),
            ..Volume::default()
        });
        common_mounts.push(VolumeMount {
            name: "fakecloud-launcher".into(),
            mount_path: crate::runtime::extensions::LAUNCHER_DIR.into(),
            ..VolumeMount::default()
        });
    }

    // Init container: busybox + wget. Downloads code zip + layers tar
    // and unpacks them into the shared emptyDir volumes. For
    // image-package functions the code+layers endpoint short-circuits
    // (image already contains code) so we only fetch the layers tar.
    let init_script = if is_image {
        "set -eu; \
         wget -q --header=\"authorization: Bearer $FAKECLOUD_INTERNAL_TOKEN\" \
              -O /tmp/layers.tar \
              \"$FAKECLOUD_SELF_URL/_fakecloud/lambda/_internal/layers/$ACCT/$FN_REGION/$FN/$DEPLOY_ID.tar\"; \
         if [ -s /tmp/layers.tar ]; then tar -xf /tmp/layers.tar -C /opt; fi"
            .to_string()
    } else {
        "set -eu; \
         wget -q --header=\"authorization: Bearer $FAKECLOUD_INTERNAL_TOKEN\" \
              -O /tmp/code.zip \
              \"$FAKECLOUD_SELF_URL/_fakecloud/lambda/_internal/code/$ACCT/$FN_REGION/$FN/$DEPLOY_ID.zip\"; \
         unzip -q /tmp/code.zip -d /var/task; \
         wget -q --header=\"authorization: Bearer $FAKECLOUD_INTERNAL_TOKEN\" \
              -O /tmp/layers.tar \
              \"$FAKECLOUD_SELF_URL/_fakecloud/lambda/_internal/layers/$ACCT/$FN_REGION/$FN/$DEPLOY_ID.tar\"; \
         if [ -s /tmp/layers.tar ]; then tar -xf /tmp/layers.tar -C /opt; fi"
            .to_string()
    };

    // The launcher's directory takes its bookkeeping files whatever user
    // the function's image runs as (an `emptyDir` is created world
    // writable; the sticky bit is added where the init user may).
    let init_script = if ctx.launch_command.is_some() {
        format!(
            "{init_script}; printf '%s' \"$FAKECLOUD_LAUNCHER\" > {path}; chmod 755 {path}; chmod 1777 {dir} 2>/dev/null || true",
            path = crate::runtime::extensions::LAUNCHER_PATH,
            dir = crate::runtime::extensions::LAUNCHER_DIR
        )
    } else {
        init_script
    };
    let mut init_env = vec![
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
            // Functions are regional: the same name can exist in several
            // regions of one account, so the artifact routes take the
            // region the function's ARN names.
            name: "FN_REGION".into(),
            value: Some(
                fakecloud_aws::arn::region_of(&func.function_arn)
                    .unwrap_or_default()
                    .to_string(),
            ),
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

    if ctx.launch_command.is_some() {
        init_env.push(EnvVar {
            name: "FAKECLOUD_LAUNCHER".into(),
            value: Some(verbatim(crate::runtime::extensions::LAUNCHER_SCRIPT)),
            value_from: None,
        });
    }
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

    // Main container: runs the runtime's base image (zip) or the user's image.
    // For zip functions the handler is passed as the cmd argument, same
    // as Docker (`docker create <image> <handler>`).
    let mut main_container = Container {
        name: super::MAIN_CONTAINER.into(),
        image: Some(main_image.clone()),
        env: Some(env),
        volume_mounts: Some(common_mounts),
        resources: Some(memory_resources(func.memory_size)),
        ..Container::default()
    };
    main_container.working_dir =
        crate::runtime::extensions::image_working_directory(func.image_config.as_ref());
    match &ctx.launch_command {
        // The launcher starts the extensions, then runs the function.
        Some(command) => {
            main_container.command =
                Some(vec![crate::runtime::extensions::LAUNCHER_PATH.to_string()]);
            main_container.args = Some(command.iter().map(|a| verbatim(a)).collect());
        }
        // An image run as is: `ImageConfig` still overrides its command.
        None => {
            let list = |key: &str| {
                func.image_config
                    .as_ref()
                    .and_then(|c| c.get(key))
                    .and_then(|v| v.as_array())
                    .map(|a| {
                        a.iter()
                            .filter_map(|v| v.as_str().map(str::to_string))
                            .collect::<Vec<_>>()
                    })
                    .filter(|v| !v.is_empty())
            };
            main_container.command =
                list("EntryPoint").map(|c| c.iter().map(|a| verbatim(a)).collect());
            main_container.args = list("Command").map(|c| c.iter().map(|a| verbatim(a)).collect());
        }
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
            host_aliases: Some(vec![HostAlias {
                ip: ctx.runtime_api_ip.to_string(),
                hostnames: Some(vec![ctx.runtime_api_host.to_string()]),
            }]),
            ..PodSpec::default()
        }),
        ..Pod::default()
    })
}

/// The image a function's main container runs: its base image (zip) or
/// its `ImageUri`, AWS private-ECR URIs translated to fakecloud's registry.
pub fn main_image(func: &LambdaFunction, ecr_host: &str, ecr_port: u16) -> Result<String, String> {
    if func.package_type == "Image" {
        let raw = func
            .image_uri
            .as_deref()
            .ok_or_else(|| "PackageType=Image function has no ImageUri".to_string())?;
        Ok(
            fakecloud_core::ecr_uri::translate_to_local_at(raw, ecr_host, ecr_port)
                .unwrap_or_else(|| raw.to_string()),
        )
    } else {
        runtime_to_image(&func.runtime)
            .ok_or_else(|| format!("unsupported runtime: {}", func.runtime))
    }
}

/// The Secret carrying one instance's execution-role credentials, named
/// `name` (the instance's Pod name) and labelled like the Pod so the same
/// ownership rules apply. The Pod references it through `secretKeyRef`, so
/// the credentials never appear in the Pod spec.
pub fn build_credentials_secret(
    name: &str,
    credentials: &SessionCredentials,
    instance_id: &str,
) -> Secret {
    let mut labels = BTreeMap::new();
    labels.insert(
        fakecloud_k8s::labels::MANAGED_BY.into(),
        fakecloud_k8s::labels::MANAGED_BY_VALUE.into(),
    );
    labels.insert(fakecloud_k8s::labels::INSTANCE.into(), instance_id.into());
    labels.insert(fakecloud_k8s::labels::SERVICE.into(), super::SERVICE.into());
    Secret {
        metadata: ObjectMeta {
            name: Some(name.to_string()),
            labels: Some(labels),
            ..ObjectMeta::default()
        },
        type_: Some("Opaque".into()),
        string_data: Some(
            credential_envs(credentials)
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect(),
        ),
        ..Secret::default()
    }
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
            credentials_secret: None,
            runtime_api_host: "fakecloud-runtime-abc123",
            runtime_api_ip: "10.96.0.15",
            self_port: 4566,
            launch_command: Some(vec![
                "/lambda-entrypoint.sh".to_string(),
                "lambda_function.lambda_handler".to_string(),
            ]),
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
            capacity_provider_config: None,
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
            script.contains("code/$ACCT/$FN_REGION/$FN/$DEPLOY_ID.zip"),
            "init script must include code download for zip functions: {script}"
        );
        assert!(
            script.contains("layers/$ACCT/$FN_REGION/$FN/$DEPLOY_ID.tar"),
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
        assert_eq!(args.last().unwrap(), "lambda_function.lambda_handler");
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
    fn the_function_runs_through_the_extension_launcher() {
        let f = zip_function("my-fn");
        let pod = build_pod_spec(&f, "d", &ctx()).unwrap();
        let spec = pod.spec.unwrap();
        let main = &spec.containers[0];
        assert_eq!(main.command.clone().unwrap(), vec!["/var/fakecloud/launch"]);
        assert_eq!(
            main.args.clone().unwrap(),
            vec!["/lambda-entrypoint.sh", "lambda_function.lambda_handler"]
        );
        let mounted = |c: &Container| {
            c.volume_mounts
                .as_ref()
                .unwrap()
                .iter()
                .any(|m| m.mount_path == "/var/fakecloud")
        };
        assert!(mounted(main));
        let init = &spec.init_containers.unwrap()[0];
        assert!(mounted(init));
        let script = &init.command.as_ref().unwrap()[2];
        assert!(script.contains("> /var/fakecloud/launch"), "{script}");
        // The launcher's bookkeeping directory is writable whatever user the
        // image runs as.
        assert!(script.contains("chmod 1777 /var/fakecloud"), "{script}");
        let launcher = init
            .env
            .as_ref()
            .unwrap()
            .iter()
            .find(|e| e.name == "FAKECLOUD_LAUNCHER")
            .and_then(|e| e.value.clone())
            .unwrap();
        // What the kubelet hands the init container is the script itself.
        assert_eq!(
            kubelet_expand(&launcher),
            crate::runtime::extensions::LAUNCHER_SCRIPT
        );
    }

    /// The kubelet's `$(VAR)` expansion of `command`, `args` and `env`
    /// values (`third_party/forked/golang/expansion`), with `VAR` and `HOME`
    /// defined: `$$` is reduced to `$`, `$(NAME)` of a defined variable is
    /// replaced, and anything else is kept.
    fn kubelet_expand(input: &str) -> String {
        let defined = |name: &str| match name {
            "VAR" => Some("expanded".to_string()),
            "HOME" => Some("/root".to_string()),
            _ => None,
        };
        let bytes = input.as_bytes();
        let mut out = String::new();
        let mut checkpoint = 0;
        let mut cursor = 0;
        while cursor < bytes.len() {
            if bytes[cursor] == b'$' && cursor + 1 < bytes.len() {
                out.push_str(&input[checkpoint..cursor]);
                let rest = &input[cursor + 1..];
                let advance = match rest.as_bytes()[0] {
                    b'$' => {
                        out.push('$');
                        1
                    }
                    b'(' => match rest.find(')') {
                        Some(close) => {
                            let name = &rest[1..close];
                            out.push_str(&defined(name).unwrap_or_else(|| format!("$({name})")));
                            close + 1
                        }
                        None => {
                            out.push_str("$(");
                            1
                        }
                    },
                    _ => {
                        let c = rest.chars().next().unwrap();
                        out.push('$');
                        out.push(c);
                        c.len_utf8()
                    }
                };
                cursor += advance;
                checkpoint = cursor + 1;
            }
            cursor += 1;
        }
        out.push_str(&input[checkpoint.min(input.len())..]);
        out
    }

    #[test]
    fn the_expansion_model_matches_the_kubelet() {
        assert_eq!(kubelet_expand("$(VAR)"), "expanded");
        assert_eq!(kubelet_expand("$(UNSET)"), "$(UNSET)");
        assert_eq!(kubelet_expand("$$(VAR)"), "$(VAR)");
        assert_eq!(kubelet_expand("a$$b"), "a$b");
        assert_eq!(kubelet_expand("$HOME $"), "$HOME $");
        assert_eq!(kubelet_expand("$(VAR"), "$(VAR");
    }

    #[test]
    fn verbatim_survives_the_kubelet_expansion() {
        for s in [
            "plain",
            "$(VAR)",
            "echo $(HOME) $$ $(UNSET) $HOME",
            "trailing $",
            "$$(VAR)",
            "$(VAR",
            "ünï$(VAR)cødé",
        ] {
            assert_eq!(kubelet_expand(&verbatim(s)), s, "{s}");
        }
    }

    /// `ImageConfig` command and arguments, the launcher's arguments and the
    /// function's environment reach the container as written: the kubelet
    /// does not expand `$(VAR)` in them (Lambda never does).
    #[test]
    fn container_command_args_and_env_are_not_expanded() {
        let tricky = "$(HOME)/x $$ $(VAR)";
        let mut f = zip_function("dollar-fn");
        f.environment.insert("LITERAL".into(), tricky.into());
        let mut c = ctx();
        c.launch_command = Some(vec!["/bin/app".into(), tricky.into()]);
        let pod = build_pod_spec(&f, "d", &c).unwrap();
        let main = &pod.spec.unwrap().containers[0];
        let args: Vec<String> = main
            .args
            .clone()
            .unwrap()
            .iter()
            .map(|a| kubelet_expand(a))
            .collect();
        assert_eq!(args, vec!["/bin/app".to_string(), tricky.to_string()]);
        let env = main.env.clone().unwrap();
        let literal = env.iter().find(|e| e.name == "LITERAL").unwrap();
        assert_eq!(kubelet_expand(literal.value.as_deref().unwrap()), tricky);

        // An image run without the launcher: `ImageConfig` EntryPoint and
        // Command become the container's command and args.
        let mut f = zip_function("dollar-img");
        f.package_type = "Image".into();
        f.image_uri = Some("public.ecr.aws/test/img:1".into());
        f.image_config = Some(serde_json::json!({
            "EntryPoint": ["/bin/sh", "-c"],
            "Command": [format!("echo {tricky}")],
        }));
        let mut c = ctx();
        c.launch_command = None;
        let pod = build_pod_spec(&f, "d", &c).unwrap();
        let main = &pod.spec.unwrap().containers[0];
        let expand = |v: &Option<Vec<String>>| {
            v.clone()
                .unwrap()
                .iter()
                .map(|a| kubelet_expand(a))
                .collect::<Vec<_>>()
        };
        assert_eq!(expand(&main.command), vec!["/bin/sh", "-c"]);
        assert_eq!(expand(&main.args), vec![format!("echo {tricky}")]);
    }

    #[test]
    fn runtime_reaches_its_own_runtime_api_host_mapped_to_the_service() {
        let f = zip_function("my-fn");
        let pod = build_pod_spec(&f, "d", &ctx()).unwrap();
        let spec = pod.spec.unwrap();
        let env = spec.containers[0].env.clone().unwrap();
        let api = env
            .iter()
            .find(|e| e.name == "AWS_LAMBDA_RUNTIME_API")
            .and_then(|e| e.value.clone());
        assert_eq!(api.as_deref(), Some("fakecloud-runtime-abc123:4566"));
        assert!(env.iter().all(|e| e.name != "AWS_LAMBDA_FUNCTION_TIMEOUT"));
        let aliases = spec.host_aliases.unwrap();
        assert_eq!(aliases.len(), 1);
        assert_eq!(aliases[0].ip, "10.96.0.15");
        assert_eq!(
            aliases[0].hostnames.as_deref(),
            Some(&["fakecloud-runtime-abc123".to_string()][..])
        );
    }

    #[test]
    fn main_container_gets_the_aws_environment_pointed_at_the_cluster() {
        let f = zip_function("my-fn");
        let mut c = ctx();
        c.credentials_secret = Some("my-fn-pod");
        let pod = build_pod_spec(&f, "d", &c).unwrap();
        let env = pod.spec.unwrap().containers[0].env.clone().unwrap();
        let get = |k: &str| {
            env.iter()
                .find(|e| e.name == k)
                .and_then(|e| e.value.clone())
        };
        assert_eq!(
            get("AWS_ENDPOINT_URL").as_deref(),
            Some("http://fakecloud.fakecloud.svc.cluster.local:4566")
        );
        assert_eq!(get("AWS_REGION").as_deref(), Some("us-east-1"));
        assert_eq!(get("AWS_LAMBDA_FUNCTION_NAME").as_deref(), Some("my-fn"));

        // Credentials come from the per-Pod Secret, never inline values.
        for key in CREDENTIAL_ENV_KEYS {
            let var = env.iter().find(|e| e.name == key).expect(key);
            assert_eq!(var.value, None, "{key} must not be inline");
            let r = var
                .value_from
                .as_ref()
                .and_then(|v| v.secret_key_ref.as_ref())
                .expect("secretKeyRef");
            assert_eq!(r.name, "my-fn-pod");
            assert_eq!(r.key, key);
        }
        // Kubernetes keeps duplicate names ambiguous; each is set once.
        let mut names: Vec<&str> = env.iter().map(|e| e.name.as_str()).collect();
        let total = names.len();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), total);
    }

    #[test]
    fn no_credential_vars_without_a_secret() {
        let pod = build_pod_spec(&zip_function("my-fn"), "d", &ctx()).unwrap();
        let env = pod.spec.unwrap().containers[0].env.clone().unwrap();
        assert!(env
            .iter()
            .all(|e| !CREDENTIAL_ENV_KEYS.contains(&e.name.as_str())));
    }

    #[test]
    fn credentials_secret_carries_every_referenced_key() {
        let creds = SessionCredentials {
            access_key_id: "FSIAEXAMPLE".into(),
            secret_access_key: "secret".into(),
            session_token: "token".into(),
            expiration: Utc::now(),
            account_id: "000000000000".into(),
        };
        let secret = build_credentials_secret("my-fn-pod", &creds, "fakecloud-1234");
        assert_eq!(secret.metadata.name.as_deref(), Some("my-fn-pod"));
        let labels = secret.metadata.labels.unwrap();
        assert_eq!(
            labels
                .get(fakecloud_k8s::labels::INSTANCE)
                .map(String::as_str),
            Some("fakecloud-1234")
        );
        let data = secret.string_data.unwrap();
        for key in CREDENTIAL_ENV_KEYS {
            assert!(data.contains_key(key), "{key}");
        }
        assert_eq!(data["AWS_ACCESS_KEY_ID"], "FSIAEXAMPLE");
        assert_eq!(data["AWS_SECRET_ACCESS_KEY"], "secret");
        assert_eq!(data["AWS_SESSION_TOKEN"], "token");
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
