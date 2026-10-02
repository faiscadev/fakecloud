//! What an instance container reaches over the network that a real instance
//! would: IMDS at `169.254.169.254`, and load balancers reaching its ports.
//!
//! **IMDS.** On EC2 every instance answers `http://169.254.169.254` with its
//! own metadata and the credentials of its instance profile. fakecloud
//! reproduces the address inside the instance's network namespace, the way the
//! ECS runtime reproduces the agent's `169.254.170.2`: a NAT rule redirects
//! `169.254.169.254:80` to a small proxy listening on loopback in the same
//! namespace, which forwards to fakecloud as
//! `/_fakecloud/ec2/imds/<instance-id>/latest/...`. The instance id in the
//! path is what identifies the caller (the address a container reaches
//! fakecloud from says nothing about which instance it is).
//!
//! - Docker / Podman: a sidecar container joins the instance's namespace
//!   (`--network container:<instance>`) with `NET_ADMIN`, installs the rule,
//!   and runs the proxy. It is recreated whenever the instance container
//!   (and with it the namespace) restarts.
//! - Kubernetes: the same sidecar is a second container of the instance Pod.
//!
//! User-data waits (bounded) for the proxy before it runs, so a boot script
//! that reads IMDS first thing finds it. When no helper image is available the
//! instance gets `AWS_EC2_METADATA_SERVICE_ENDPOINT` pointing at fakecloud
//! instead, which SDK credential chains honour.
//!
//! **Load balancer targets.** An instance's listening ports are not known
//! when it boots, so nothing is published then. When an ELBv2 target group
//! first routes to `i-...:<port>`, a forwarder container (socat on the
//! default bridge, attached to the instance's subnet network) publishes an
//! ephemeral host port to `<instance IP>:<port>`, and the data plane connects
//! there (see [`fakecloud_core::dataplane`]).

use std::time::Duration;

/// Overrides the helper image. It must have `sh`, `getent`, `awk`, `nginx`,
/// `socat` and either `nft` or `iptables` (or be Alpine, which installs what
/// is missing at start).
pub(crate) const HELPER_IMAGE_ENV: &str = "FAKECLOUD_EC2_HELPER_IMAGE";

/// Base image of the locally built helper (and the default Kubernetes helper,
/// which installs its packages at start).
pub(crate) const HELPER_BASE_IMAGE: &str = "public.ecr.aws/docker/library/alpine:3.20";

/// Repository of the locally built Docker / Podman helper image.
const HELPER_LOCAL_REPO: &str = "fakecloud-ec2-helper";

/// Loopback port the IMDS proxy listens on inside the instance's namespace.
pub(crate) const PROXY_PORT: u16 = 61169;

/// Line the IMDS setup script prints once the rule and proxy are up.
pub(crate) const READY_MARKER: &str = "FAKECLOUD_EC2_IMDS_READY";

/// Directory, inside the instance, holding the IMDS readiness marker.
pub(crate) const READY_DIR: &str = "/run/fakecloud";

/// The readiness marker user-data waits for.
pub(crate) const READY_FILE: &str = "/run/fakecloud/imds-ready";

/// `run` flags putting [`READY_DIR`] on a tmpfs: every start / restart of the
/// instance container begins without the readiness marker, so user-data waits
/// for the sidecar of *this* boot rather than finding a stale marker in the
/// writable layer.
pub(crate) fn readiness_tmpfs_args() -> [String; 2] {
    ["--tmpfs".to_string(), READY_DIR.to_string()]
}

/// How long user-data waits for IMDS before running anyway (seconds).
pub(crate) const BOOT_WAIT_SECS: u32 = 60;

/// How long the sidecar may take to come up.
pub(crate) const SIDECAR_READY_TIMEOUT: Duration = Duration::from_secs(60);

/// How long a failed helper-image resolution is remembered.
pub(crate) const HELPER_RETRY_AFTER: Duration = Duration::from_secs(300);

/// The locally built helper: the base image plus nftables, nginx and socat.
pub(crate) fn helper_dockerfile() -> String {
    format!("FROM {HELPER_BASE_IMAGE}\nRUN apk add --no-cache nftables nginx socat\n")
}

/// Tag of the locally built helper, keyed on its Dockerfile so a change
/// builds a fresh image instead of reusing a stale one.
pub(crate) fn local_helper_tag() -> String {
    // FNV-1a: stable across builds and platforms, unlike `DefaultHasher`.
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in helper_dockerfile().bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0100_0000_01b3);
    }
    format!("{HELPER_LOCAL_REPO}:{:012x}", hash & 0xffff_ffff_ffff)
}

/// The operator's helper image override, if set.
pub(crate) fn helper_image_override() -> Option<String> {
    std::env::var(HELPER_IMAGE_ENV)
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

/// The resolved helper image, or the last failure to resolve it.
#[derive(Debug, Clone)]
pub(crate) enum HelperImage {
    Ready(String),
    Failed { at: std::time::Instant },
}

/// Installs the `169.254.169.254:80` redirect to the loopback proxy and runs
/// the proxy, which forwards to `http://$1:$2/_fakecloud/ec2/imds/$3/`.
/// `$READY_FILE`, when set, is created once IMDS answers -- or when setup
/// fails, so a boot waiting on it never waits out its whole bound for nothing.
pub(crate) const SETUP_SCRIPT: &str = r#"set -eu
host="$1"; port="$2"; instance="$3"; proxy_port="$4"
ready() { if [ -n "${READY_FILE:-}" ]; then mkdir -p "$(dirname "$READY_FILE")" && : > "$READY_FILE"; fi; }
trap ready EXIT
have() { command -v "$1" >/dev/null 2>&1; }
if { ! have nginx || { ! have nft && ! have iptables; }; } && have apk; then
  apk add --no-cache -q nftables nginx >/dev/null
fi
ip=$(getent ahostsv4 "$host" 2>/dev/null | awk 'NR==1{print $1}')
if [ -z "$ip" ]; then
  echo "fakecloud-ec2-imds: cannot resolve $host" >&2
  exit 1
fi
conf=/tmp/fakecloud-ec2-imds.conf
cat > "$conf" <<EOF
worker_processes 1;
pid /tmp/fakecloud-ec2-imds.pid;
error_log stderr warn;
events { worker_connections 256; }
http {
  access_log off;
  client_body_temp_path /tmp/fakecloud-ec2-imds-body;
  proxy_temp_path /tmp/fakecloud-ec2-imds-proxy;
  server {
    listen 127.0.0.1:$proxy_port;
    location / {
      proxy_pass http://$ip:$port/_fakecloud/ec2/imds/$instance/;
      proxy_set_header Host 169.254.169.254;
      proxy_http_version 1.1;
      proxy_set_header Connection "";
    }
  }
}
EOF
nginx -c "$conf"
# nft first; iptables when nft is missing or the kernel refuses its rule.
if have nft && printf 'table ip fakecloud_ec2_imds\ndelete table ip fakecloud_ec2_imds\ntable ip fakecloud_ec2_imds {\n chain output {\n  type nat hook output priority -100; policy accept;\n  ip daddr 169.254.169.254 tcp dport 80 redirect to :%s\n }\n}\n' "$proxy_port" | nft -f -; then
  :
elif have iptables && rule="-d 169.254.169.254/32 -p tcp --dport 80 -j REDIRECT --to-ports $proxy_port" \
  && { iptables -t nat -C OUTPUT $rule 2>/dev/null || iptables -t nat -A OUTPUT $rule; }; then
  :
else
  echo "fakecloud-ec2-imds: could not install the NAT rule with nft or iptables" >&2
  exit 1
fi
echo "FAKECLOUD_EC2_IMDS_READY 169.254.169.254:80 -> $ip:$port ($instance)"
ready
trap - EXIT
trap 'nginx -c "$conf" -s quit 2>/dev/null; exit 0' TERM INT
while :; do sleep 3600 & wait $!; done
"#;

/// Docker name of an instance's IMDS sidecar.
pub(crate) fn sidecar_name(instance_id: &str) -> String {
    format!("fakecloud-ec2-imds-{instance_id}")
}

/// Docker name of the forwarder publishing `port` of an instance.
pub(crate) fn forwarder_name(instance_id: &str, port: u16) -> String {
    format!("fakecloud-ec2-fwd-{instance_id}-{port}")
}

/// `docker run` argv for an instance's IMDS sidecar, joined to the instance
/// container's network namespace. It inherits that container's `/etc/hosts`,
/// which carries the host alias.
pub(crate) fn sidecar_argv(
    instance_id: &str,
    instance_container: &str,
    owner_label: &str,
    helper_image: &str,
    fakecloud_host: &str,
    fakecloud_port: u16,
) -> Vec<String> {
    vec![
        "run".into(),
        "-d".into(),
        "--name".into(),
        sidecar_name(instance_id),
        "--network".into(),
        format!("container:{instance_container}"),
        "--cap-add".into(),
        "NET_ADMIN".into(),
        "--label".into(),
        format!("fakecloud-ec2-imds={instance_id}"),
        "--label".into(),
        format!("fakecloud-instance={owner_label}"),
        "--entrypoint".into(),
        "sh".into(),
        helper_image.to_string(),
        "-c".into(),
        SETUP_SCRIPT.to_string(),
        "fakecloud-ec2-imds".into(),
        fakecloud_host.to_string(),
        fakecloud_port.to_string(),
        instance_id.to_string(),
        PROXY_PORT.to_string(),
    ]
}

/// `docker run` argv for a forwarder publishing `port` of an instance at
/// `instance_ip` on an ephemeral host port. It runs on the default bridge (so
/// its port can be published even when the subnet network is `--internal`)
/// and is attached to the subnet network afterwards.
pub(crate) fn forwarder_argv(
    instance_id: &str,
    port: u16,
    instance_ip: &str,
    owner_label: &str,
    helper_image: &str,
) -> Vec<String> {
    vec![
        "run".into(),
        "-d".into(),
        "--name".into(),
        forwarder_name(instance_id, port),
        "--publish".into(),
        format!("{port}/tcp"),
        "--label".into(),
        format!("fakecloud-ec2-fwd={instance_id}"),
        "--label".into(),
        format!("fakecloud-instance={owner_label}"),
        "--entrypoint".into(),
        "socat".into(),
        helper_image.to_string(),
        format!("TCP-LISTEN:{port},fork,reuseaddr"),
        format!("TCP:{instance_ip}:{port}"),
    ]
}

/// The IMDS endpoint an instance's SDKs use when the link-local address can't
/// be provided: fakecloud's per-instance IMDS, through the host alias.
pub(crate) fn metadata_endpoint_env(
    fakecloud_host: &str,
    fakecloud_port: u16,
    instance_id: &str,
) -> (String, String) {
    (
        "AWS_EC2_METADATA_SERVICE_ENDPOINT".into(),
        format!("http://{fakecloud_host}:{fakecloud_port}/_fakecloud/ec2/imds/{instance_id}/"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sidecar_joins_the_instance_namespace_with_net_admin() {
        let argv = sidecar_argv(
            "i-0abc",
            "cid123",
            "fakecloud-42",
            "helper:1",
            "host.docker.internal",
            4566,
        );
        let joined = argv.join(" ");
        assert!(joined.starts_with("run -d --name fakecloud-ec2-imds-i-0abc "));
        assert!(joined.contains("--network container:cid123"), "{joined}");
        assert!(joined.contains("--cap-add NET_ADMIN"), "{joined}");
        assert!(
            joined.contains("--label fakecloud-instance=fakecloud-42"),
            "{joined}"
        );
        // A joined container can't carry its own --add-host / --publish.
        assert!(!joined.contains("--add-host"), "{joined}");
        assert!(!joined.contains("--publish"), "{joined}");
        let tail = &argv[argv.len() - 4..];
        assert_eq!(tail, ["host.docker.internal", "4566", "i-0abc", "61169"]);
    }

    #[test]
    fn forwarder_publishes_an_ephemeral_port_to_the_instance() {
        let argv = forwarder_argv("i-0abc", 8080, "172.20.0.5", "fakecloud-42", "helper:1");
        let joined = argv.join(" ");
        assert!(joined.contains("--publish 8080/tcp"), "{joined}");
        assert!(joined.contains("--entrypoint socat helper:1"), "{joined}");
        assert!(
            joined.ends_with("TCP-LISTEN:8080,fork,reuseaddr TCP:172.20.0.5:8080"),
            "{joined}"
        );
    }

    #[test]
    fn readiness_marker_does_not_survive_a_restart() {
        assert_eq!(readiness_tmpfs_args(), ["--tmpfs", READY_DIR]);
        assert!(READY_FILE.starts_with(READY_DIR));
    }

    #[test]
    fn metadata_endpoint_names_the_instance() {
        assert_eq!(
            metadata_endpoint_env("host.docker.internal", 4566, "i-0abc").1,
            "http://host.docker.internal:4566/_fakecloud/ec2/imds/i-0abc/"
        );
    }

    #[test]
    fn helper_tag_is_stable() {
        assert_eq!(local_helper_tag(), local_helper_tag());
        assert!(local_helper_tag().starts_with("fakecloud-ec2-helper:"));
    }

    /// Run the setup script under `sh` with fake `getent`, `nginx` and `nft`
    /// on `PATH`: it must start the proxy before installing the redirect, use
    /// the resolved fakecloud address in the proxy target, report readiness
    /// and create the readiness file.
    #[cfg(unix)]
    #[test]
    fn setup_script_starts_the_proxy_then_installs_the_redirect() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let log = dir.path().join("calls");
        let tool = |name: &str, body: &str| {
            let path = dir.path().join(name);
            std::fs::write(&path, format!("#!/bin/sh\n{body}\n")).unwrap();
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        };
        let logged = |name: &str| {
            format!(
                "echo {name} \"$@\" >> {}; cat >> {} 2>/dev/null || true",
                log.display(),
                log.display()
            )
        };
        tool(
            "getent",
            "echo '192.168.65.254 STREAM host.docker.internal'",
        );
        tool("nginx", &format!("echo nginx \"$@\" >> {}", log.display()));
        tool("nft", &logged("nft"));
        // The script holds forever after READY; stop it with a fake `sleep`.
        tool("sleep", "kill -TERM $PPID");
        let ready = dir.path().join("run/imds-ready");
        let path = format!("{}:/usr/bin:/bin", dir.path().display());
        let out = std::process::Command::new("sh")
            .args([
                "-c",
                SETUP_SCRIPT,
                "x",
                "host.docker.internal",
                "4566",
                "i-0abc",
                "61169",
            ])
            .env("PATH", &path)
            .env("READY_FILE", &ready)
            .output()
            .unwrap();
        let stdout = String::from_utf8_lossy(&out.stdout);
        assert!(
            stdout.contains(READY_MARKER),
            "stdout: {stdout}; stderr: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        assert!(ready.exists(), "readiness file not created");
        let calls = std::fs::read_to_string(&log).unwrap();
        let nginx_at = calls.find("nginx -c").expect(&calls);
        let nft_at = calls.find("nft -f").expect(&calls);
        assert!(
            nginx_at < nft_at,
            "proxy must be up before the redirect: {calls}"
        );
        assert!(calls.contains("redirect to :61169"), "{calls}");
        let conf = std::fs::read_to_string("/tmp/fakecloud-ec2-imds.conf").unwrap_or_default();
        assert!(
            conf.contains("proxy_pass http://192.168.65.254:4566/_fakecloud/ec2/imds/i-0abc/"),
            "{conf}"
        );
    }
}
