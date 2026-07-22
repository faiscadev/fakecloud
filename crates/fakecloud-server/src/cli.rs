use std::path::PathBuf;

use clap::{Parser, ValueEnum};
use fakecloud_core::auth::IamMode;
use fakecloud_persistence::{PersistenceConfig, StorageMode};

#[derive(Clone, Copy, Debug, ValueEnum)]
#[clap(rename_all = "lowercase")]
pub(crate) enum IamModeArg {
    Off,
    Soft,
    Strict,
}

impl From<IamModeArg> for IamMode {
    fn from(value: IamModeArg) -> Self {
        match value {
            IamModeArg::Off => IamMode::Off,
            IamModeArg::Soft => IamMode::Soft,
            IamModeArg::Strict => IamMode::Strict,
        }
    }
}

#[derive(Clone, Copy, Debug, ValueEnum)]
#[clap(rename_all = "lowercase")]
pub(crate) enum StorageModeArg {
    Memory,
    Persistent,
}

impl From<StorageModeArg> for StorageMode {
    fn from(value: StorageModeArg) -> Self {
        match value {
            StorageModeArg::Memory => StorageMode::Memory,
            StorageModeArg::Persistent => StorageMode::Persistent,
        }
    }
}

const DEFAULT_S3_CACHE_BYTES: u64 = 256 * 1024 * 1024;

#[derive(Parser)]
#[command(name = "fakecloud")]
#[command(about = "FakeCloud — local AWS cloud emulator")]
#[command(version)]
pub(crate) struct Cli {
    /// Listen address
    #[arg(long, default_value = "0.0.0.0:4566", env = "FAKECLOUD_ADDR")]
    pub addr: String,

    /// AWS region to advertise
    #[arg(long, default_value = "us-east-1", env = "FAKECLOUD_REGION")]
    pub region: String,

    /// AWS account ID to use
    #[arg(long, default_value = "123456789012", env = "FAKECLOUD_ACCOUNT_ID")]
    pub account_id: String,

    /// Log level (trace, debug, info, warn, error)
    #[arg(long, default_value = "info", env = "FAKECLOUD_LOG")]
    pub log_level: String,

    /// Storage mode. `memory` (default) keeps all state in RAM; `persistent`
    /// mirrors supported services to `--data-path` on disk.
    #[arg(
        long,
        value_enum,
        default_value_t = StorageModeArg::Memory,
        env = "FAKECLOUD_STORAGE_MODE",
    )]
    pub storage_mode: StorageModeArg,

    /// Directory to persist state to. Required when `--storage-mode=persistent`.
    #[arg(long, env = "FAKECLOUD_DATA_PATH")]
    pub data_path: Option<PathBuf>,

    /// Bulk-load a single AWS-format DynamoDB export at startup. Requires
    /// --dynamodb-import-describe-table. See
    /// /docs/services/dynamodb#importing-an-aws-export-at-startup.
    #[arg(
        long,
        env = "FAKECLOUD_DYNAMODB_IMPORT_PATH",
        conflicts_with = "dynamodb_import_dir"
    )]
    pub dynamodb_import_path: Option<PathBuf>,

    /// `aws dynamodb describe-table` JSON for --dynamodb-import-path.
    #[arg(
        long,
        env = "FAKECLOUD_DYNAMODB_DESCRIBE_TABLE",
        conflicts_with = "dynamodb_import_dir"
    )]
    pub dynamodb_import_describe_table: Option<PathBuf>,

    /// Bulk-load many AWS-format DynamoDB exports at startup from a root
    /// directory of per-table subdirectories, each self-contained with its
    /// own describe-table.json. See
    /// /docs/services/dynamodb#importing-an-aws-export-at-startup.
    #[arg(long, env = "FAKECLOUD_DYNAMODB_IMPORT_DIR")]
    pub dynamodb_import_dir: Option<PathBuf>,

    /// In-memory LRU cache for S3 object bodies in persistent mode. Plain bytes,
    /// no SI/IEC suffix parsing. Default 256 MiB.
    #[arg(long, default_value_t = DEFAULT_S3_CACHE_BYTES, env = "FAKECLOUD_S3_CACHE_SIZE")]
    pub s3_cache_size: u64,

    /// Cryptographically verify SigV4 signatures on incoming requests.
    /// Off by default — fakecloud parses SigV4 for routing regardless. When
    /// enabled, requests with invalid signatures are rejected with
    /// `SignatureDoesNotMatch`. The reserved `test`/`test` root identity
    /// always bypasses verification. See `/docs/reference/security`.
    #[arg(long, default_value_t = false, env = "FAKECLOUD_VERIFY_SIGV4")]
    pub verify_sigv4: bool,

    /// IAM identity-policy evaluation mode.
    ///
    /// - `off` (default): policies are stored but never consulted.
    /// - `soft`: evaluate and audit-log denied decisions via the
    ///   `fakecloud::iam::audit` tracing target, but allow the request.
    /// - `strict`: evaluate and return `AccessDeniedException` on denied
    ///   decisions.
    ///
    /// Phase 1 scope: identity policies, Allow/Deny with Deny precedence,
    /// Action/Resource wildcards. Condition blocks, resource-based policies,
    /// permission boundaries, SCPs, and ABAC are explicitly not evaluated
    /// yet. The reserved `test`/`test` root identity always bypasses
    /// enforcement. See `/docs/reference/security`.
    #[arg(
        long = "iam",
        value_enum,
        default_value_t = IamModeArg::Off,
        env = "FAKECLOUD_IAM",
    )]
    pub iam_mode: IamModeArg,

    /// IAM role ARN that the container/instance credential endpoint
    /// (`GET /_fakecloud/credentials`, consumed via
    /// `AWS_CONTAINER_CREDENTIALS_FULL_URI`) vends credentials for. Lets an
    /// app running under an instance/task role resolve the AWS SDK default
    /// credential chain locally with no code change. Defaults to
    /// `arn:aws:iam::<account>:role/fakecloud`.
    #[arg(long, env = "FAKECLOUD_CREDENTIALS_ROLE_ARN")]
    pub credentials_role_arn: Option<String>,

    /// Instance ID the EC2 instance metadata service (IMDS, `/latest/*`)
    /// reports (`meta-data/instance-id`, the instance identity document).
    /// Defaults to a stable synthetic `i-…` ID. IMDS is consumed by pointing
    /// an app's SDK at `AWS_EC2_METADATA_SERVICE_ENDPOINT=http://<host>:<port>`.
    #[arg(long, env = "FAKECLOUD_IMDS_INSTANCE_ID")]
    pub imds_instance_id: Option<String>,

    /// Also bind the AWS link-local metadata addresses so apps that hardcode
    /// them (rather than honoring `AWS_EC2_METADATA_SERVICE_ENDPOINT` /
    /// `AWS_CONTAINER_CREDENTIALS_*`) resolve credentials unmodified:
    /// IMDS at `169.254.169.254:80` and ECS container credentials at
    /// `169.254.170.2:80/creds`. Off by default. Requires running fakecloud as
    /// root (to bind port 80) with those addresses already assigned to the
    /// loopback interface: fakecloud binds them but never creates or deletes the
    /// alias, and logs the exact manual command if binding fails (the main
    /// server is unaffected). See `/docs/guides/instance-credentials`.
    #[arg(long, default_value_t = false, env = "FAKECLOUD_IMDS_LINK_LOCAL")]
    pub imds_link_local: bool,

    /// Run a DNS resolver that answers `A`/`AAAA`/`CNAME`/`MX`/`TXT` (and any
    /// other stored type) from the Route 53 records created in fakecloud. Point
    /// a container's resolver at fakecloud (compose `dns:` / `/etc/resolv.conf`)
    /// and created records resolve to their local targets. Off by default. See
    /// `/docs/guides/dns`.
    #[arg(long, default_value_t = false, env = "FAKECLOUD_DNS")]
    pub dns: bool,

    /// Address the DNS resolver binds (UDP + TCP). Defaults to `0.0.0.0:53`;
    /// binding port 53 needs root, so pass e.g. `127.0.0.1:15353` for an
    /// unprivileged run. Only used when `--dns` is set.
    #[arg(long, env = "FAKECLOUD_DNS_ADDR")]
    pub dns_addr: Option<String>,

    /// Upstream resolver for names in no Route 53 zone, so a container can use
    /// fakecloud as its sole resolver and still reach the outside world.
    /// Defaults to the first `nameserver` in `/etc/resolv.conf`, else
    /// `8.8.8.8:53`. Only used when `--dns` is set.
    #[arg(long, env = "FAKECLOUD_DNS_UPSTREAM")]
    pub dns_upstream: Option<String>,

    #[command(subcommand)]
    pub command: Option<Command>,
}

/// Optional subcommands. Absent (the default) starts the server.
#[derive(clap::Subcommand)]
pub(crate) enum Command {
    /// Probe a running server's `/_fakecloud/health` endpoint and exit 0 if
    /// healthy, non-zero otherwise. Used by the container HEALTHCHECK so the
    /// published image needs no extra tooling (curl/wget are not installed).
    /// Targets `127.0.0.1:<port>`, where the port is taken from `--addr`.
    Healthcheck,
}

impl Cli {
    /// Resolve the IAM mode as the cross-crate [`IamMode`] type.
    pub fn iam_mode(&self) -> IamMode {
        self.iam_mode.into()
    }

    /// Resolve the role ARN the container/instance credential endpoint vends
    /// credentials for, defaulting to
    /// `arn:<partition>:iam::<account>:role/fakecloud` with the partition
    /// derived from the configured region (so aws-cn / aws-us-gov servers get a
    /// correctly-partitioned default principal).
    pub fn credentials_role_arn(&self, account_id: &str) -> String {
        self.credentials_role_arn.clone().unwrap_or_else(|| {
            let partition = fakecloud_aws::arn::partition_for(&self.region);
            format!("arn:{partition}:iam::{account_id}:role/fakecloud")
        })
    }

    /// Address the DNS resolver binds, defaulting to `0.0.0.0:53`. A bare host
    /// (no `:port`) gets the default DNS port 53 appended.
    pub fn dns_addr(&self) -> std::net::SocketAddr {
        let raw = self
            .dns_addr
            .clone()
            .unwrap_or_else(|| "0.0.0.0:53".to_string());
        parse_socket_addr(&raw, 53)
            .unwrap_or_else(|| std::net::SocketAddr::from((std::net::Ipv4Addr::UNSPECIFIED, 53)))
    }

    /// Upstream resolver for names in no local zone, defaulting to the first
    /// `nameserver` in `/etc/resolv.conf`, else `8.8.8.8:53`. `None` only if an
    /// explicit value fails to parse (forwarding is then disabled).
    pub fn dns_upstream(&self) -> Option<std::net::SocketAddr> {
        if let Some(raw) = &self.dns_upstream {
            return parse_socket_addr(raw, 53);
        }
        resolv_conf_nameserver()
            .and_then(|ns| parse_socket_addr(&ns, 53))
            .or_else(|| parse_socket_addr("8.8.8.8:53", 53))
    }

    /// The instance ID IMDS reports, defaulting to a stable synthetic
    /// `i-<17 hex>` derived from the account ID (so it is deterministic across
    /// restarts without a flag).
    pub fn imds_instance_id(&self) -> String {
        self.imds_instance_id.clone().unwrap_or_else(|| {
            // Deterministic 17-hex-char suffix seeded by the account ID, via the
            // shared generator (also used for the assumed-role ID).
            format!(
                "i-{}",
                fakecloud_iam::sts_service::container_creds::deterministic_suffix(
                    &self.account_id,
                    b"0123456789abcdef",
                    17,
                )
            )
        })
    }

    pub fn persistence_config(&self) -> Result<PersistenceConfig, String> {
        let mode: StorageMode = self.storage_mode.into();
        let config = PersistenceConfig {
            mode,
            data_path: self.data_path.clone(),
            s3_cache_bytes: self.s3_cache_size,
        };
        config.validate()?;
        Ok(config)
    }
}

/// Parse `raw` as a numeric socket address (`IP` or `IP:port`), appending
/// `:default_port` for a bare IP. IP-only on purpose: the DNS bind and upstream
/// are always addresses, so this never does a blocking hostname lookup (which
/// would stall async startup). Returns `None` for anything that isn't a numeric
/// address.
fn parse_socket_addr(raw: &str, default_port: u16) -> Option<std::net::SocketAddr> {
    let raw = raw.trim();
    if let Ok(addr) = raw.parse::<std::net::SocketAddr>() {
        return Some(addr);
    }
    let ip = raw.parse::<std::net::IpAddr>().ok()?;
    Some(std::net::SocketAddr::new(ip, default_port))
}

/// First `nameserver` address from `/etc/resolv.conf`, if readable. Skips
/// loopback entries: on many hosts `resolv.conf` points at a local stub
/// (127.0.0.53 / 127.0.0.11) that would just loop back into fakecloud when
/// fakecloud is itself the resolver.
fn resolv_conf_nameserver() -> Option<String> {
    let contents = std::fs::read_to_string("/etc/resolv.conf").ok()?;
    for line in contents.lines() {
        let line = line.trim();
        if let Some(addr) = line.strip_prefix("nameserver") {
            let addr = addr.trim();
            let is_loopback = addr
                .parse::<std::net::IpAddr>()
                .map(|ip| ip.is_loopback())
                .unwrap_or(false);
            if !addr.is_empty() && !is_loopback {
                return Some(addr.to_string());
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn defaults_leave_security_features_off() {
        let cli = Cli::try_parse_from(["fakecloud"]).unwrap();
        assert!(!cli.verify_sigv4);
        assert_eq!(cli.iam_mode(), IamMode::Off);
    }

    #[test]
    fn verify_sigv4_flag_parses() {
        let cli = Cli::try_parse_from(["fakecloud", "--verify-sigv4"]).unwrap();
        assert!(cli.verify_sigv4);
    }

    #[test]
    fn iam_flag_parses_all_variants() {
        let cli = Cli::try_parse_from(["fakecloud", "--iam", "off"]).unwrap();
        assert_eq!(cli.iam_mode(), IamMode::Off);
        let cli = Cli::try_parse_from(["fakecloud", "--iam", "soft"]).unwrap();
        assert_eq!(cli.iam_mode(), IamMode::Soft);
        let cli = Cli::try_parse_from(["fakecloud", "--iam", "strict"]).unwrap();
        assert_eq!(cli.iam_mode(), IamMode::Strict);
    }

    #[test]
    fn iam_flag_rejects_garbage() {
        assert!(Cli::try_parse_from(["fakecloud", "--iam", "allow"]).is_err());
    }

    #[test]
    fn iam_mode_arg_conversion_covers_all_variants() {
        assert_eq!(IamMode::from(IamModeArg::Off), IamMode::Off);
        assert_eq!(IamMode::from(IamModeArg::Soft), IamMode::Soft);
        assert_eq!(IamMode::from(IamModeArg::Strict), IamMode::Strict);
    }

    #[test]
    fn storage_mode_arg_conversion_covers_all_variants() {
        assert!(matches!(
            StorageMode::from(StorageModeArg::Memory),
            StorageMode::Memory
        ));
        assert!(matches!(
            StorageMode::from(StorageModeArg::Persistent),
            StorageMode::Persistent
        ));
    }

    #[test]
    fn persistence_config_memory_ok_without_data_path() {
        let cli = Cli::try_parse_from(["fakecloud"]).unwrap();
        let cfg = cli.persistence_config().unwrap();
        assert!(matches!(cfg.mode, StorageMode::Memory));
    }

    #[test]
    fn persistence_config_persistent_requires_data_path() {
        let cli = Cli::try_parse_from(["fakecloud", "--storage-mode", "persistent"]).unwrap();
        assert!(cli.persistence_config().is_err());
    }

    #[test]
    fn persistence_config_persistent_with_data_path() {
        let cli = Cli::try_parse_from([
            "fakecloud",
            "--storage-mode",
            "persistent",
            "--data-path",
            "/tmp/fc-test",
        ])
        .unwrap();
        let cfg = cli.persistence_config().unwrap();
        assert!(matches!(cfg.mode, StorageMode::Persistent));
        assert_eq!(
            cfg.data_path.as_deref(),
            Some(std::path::Path::new("/tmp/fc-test"))
        );
    }

    #[test]
    fn parse_socket_addr_appends_default_port() {
        assert_eq!(
            parse_socket_addr("8.8.8.8", 53).unwrap().to_string(),
            "8.8.8.8:53"
        );
        assert_eq!(
            parse_socket_addr("127.0.0.1:15353", 53)
                .unwrap()
                .to_string(),
            "127.0.0.1:15353"
        );
        assert!(parse_socket_addr("not an addr", 53).is_none());
    }

    #[test]
    fn dns_addr_defaults_to_port_53() {
        let cli = Cli::try_parse_from(["fakecloud"]).unwrap();
        assert_eq!(cli.dns_addr().to_string(), "0.0.0.0:53");
        let cli = Cli::try_parse_from(["fakecloud", "--dns-addr", "127.0.0.1:15353"]).unwrap();
        assert_eq!(cli.dns_addr().to_string(), "127.0.0.1:15353");
    }

    #[test]
    fn dns_upstream_explicit_bare_ip_gets_port() {
        let cli = Cli::try_parse_from(["fakecloud", "--dns-upstream", "1.1.1.1"]).unwrap();
        assert_eq!(cli.dns_upstream().unwrap().to_string(), "1.1.1.1:53");
    }

    #[test]
    fn s3_cache_size_default_and_override() {
        let cli = Cli::try_parse_from(["fakecloud"]).unwrap();
        assert_eq!(cli.s3_cache_size, DEFAULT_S3_CACHE_BYTES);
        let cli = Cli::try_parse_from(["fakecloud", "--s3-cache-size", "1024"]).unwrap();
        assert_eq!(cli.s3_cache_size, 1024);
    }
}
