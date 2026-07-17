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

    /// Bulk-load an AWS-format DynamoDB export at startup (additive; no API
    /// round-trip). Points at the local `AWSDynamoDB/{export-id}/` folder that
    /// holds `manifest-summary.json`. Requires `--dynamodb-import-describe-table`.
    #[arg(long, env = "FAKECLOUD_DYNAMODB_IMPORT_PATH")]
    pub dynamodb_import_path: Option<PathBuf>,

    /// Path to an `aws dynamodb describe-table` JSON dump supplying the table
    /// shape (key schema, indexes, billing mode) for `--dynamodb-import-path`.
    #[arg(long, env = "FAKECLOUD_DYNAMODB_DESCRIBE_TABLE")]
    pub dynamodb_import_describe_table: Option<PathBuf>,

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
    fn s3_cache_size_default_and_override() {
        let cli = Cli::try_parse_from(["fakecloud"]).unwrap();
        assert_eq!(cli.s3_cache_size, DEFAULT_S3_CACHE_BYTES);
        let cli = Cli::try_parse_from(["fakecloud", "--s3-cache-size", "1024"]).unwrap();
        assert_eq!(cli.s3_cache_size, 1024);
    }
}
