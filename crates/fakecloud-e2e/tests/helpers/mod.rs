use std::net::TcpListener;
use std::process::{Child, Command, Output, Stdio};
use std::time::Duration;

use aws_config::BehaviorVersion;
use aws_credential_types::Credentials;
use aws_types::region::Region;

/// A test server that spawns fakecloud on a random port.
#[allow(dead_code)]
pub struct TestServer {
    child: Option<Child>,
    port: u16,
    endpoint: String,
    container_cli: String,
}

#[allow(dead_code)]
impl TestServer {
    /// Start a new FakeCloud server on a random available port.
    pub async fn start() -> Self {
        Self::start_with_env(&[]).await
    }

    /// Start with extra environment variables passed to the server process.
    pub async fn start_with_env(env: &[(&str, &str)]) -> Self {
        let bin = find_binary();

        let container_cli = env
            .iter()
            .find(|(k, _)| *k == "FAKECLOUD_CONTAINER_CLI")
            .map(|(_, v)| v.to_string())
            .unwrap_or_else(detect_container_cli);

        for _ in 0..3 {
            let port = find_available_port();
            let endpoint = format!("http://127.0.0.1:{port}");

            let mut cmd = Command::new(&bin);
            cmd.arg("--addr")
                .arg(format!("127.0.0.1:{port}"))
                .arg("--log-level")
                .arg("warn")
                .stdout(Stdio::piped())
                .stderr(Stdio::piped());

            for (key, value) in env {
                cmd.env(key, value);
            }

            let mut child = cmd.spawn().expect("failed to start fakecloud");

            if wait_for_port(&mut child, port).await {
                return Self {
                    child: Some(child),
                    port,
                    endpoint,
                    container_cli,
                };
            }

            let _ = child.kill();
            let _ = child.wait();
        }

        panic!("fakecloud failed to start after 3 attempts");
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    /// Create a shared AWS SDK config pointing at this test server.
    pub async fn aws_config(&self) -> aws_config::SdkConfig {
        aws_config::defaults(BehaviorVersion::latest())
            .endpoint_url(self.endpoint())
            .region(Region::new("us-east-1"))
            .credentials_provider(Credentials::new(
                "AKIAIOSFODNN7EXAMPLE",
                "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
                None,
                None,
                "test",
            ))
            .load()
            .await
    }

    /// Create an SQS client.
    pub async fn sqs_client(&self) -> aws_sdk_sqs::Client {
        aws_sdk_sqs::Client::new(&self.aws_config().await)
    }

    /// Create an SNS client.
    pub async fn sns_client(&self) -> aws_sdk_sns::Client {
        aws_sdk_sns::Client::new(&self.aws_config().await)
    }

    /// Create an EventBridge client.
    pub async fn eventbridge_client(&self) -> aws_sdk_eventbridge::Client {
        aws_sdk_eventbridge::Client::new(&self.aws_config().await)
    }

    /// Create an IAM client.
    pub async fn iam_client(&self) -> aws_sdk_iam::Client {
        aws_sdk_iam::Client::new(&self.aws_config().await)
    }

    /// Create an STS client.
    pub async fn sts_client(&self) -> aws_sdk_sts::Client {
        aws_sdk_sts::Client::new(&self.aws_config().await)
    }

    /// Create an SSM client.
    pub async fn ssm_client(&self) -> aws_sdk_ssm::Client {
        aws_sdk_ssm::Client::new(&self.aws_config().await)
    }

    /// Create a DynamoDB client.
    pub async fn dynamodb_client(&self) -> aws_sdk_dynamodb::Client {
        aws_sdk_dynamodb::Client::new(&self.aws_config().await)
    }

    /// Create a Lambda client.
    pub async fn lambda_client(&self) -> aws_sdk_lambda::Client {
        aws_sdk_lambda::Client::new(&self.aws_config().await)
    }

    /// Create a Secrets Manager client.
    pub async fn secretsmanager_client(&self) -> aws_sdk_secretsmanager::Client {
        aws_sdk_secretsmanager::Client::new(&self.aws_config().await)
    }

    /// Create a CloudWatch Logs client.
    pub async fn logs_client(&self) -> aws_sdk_cloudwatchlogs::Client {
        aws_sdk_cloudwatchlogs::Client::new(&self.aws_config().await)
    }

    /// Create a KMS client.
    pub async fn kms_client(&self) -> aws_sdk_kms::Client {
        aws_sdk_kms::Client::new(&self.aws_config().await)
    }

    pub async fn kinesis_client(&self) -> aws_sdk_kinesis::Client {
        aws_sdk_kinesis::Client::new(&self.aws_config().await)
    }

    pub async fn rds_client(&self) -> aws_sdk_rds::Client {
        aws_sdk_rds::Client::new(&self.aws_config().await)
    }

    pub async fn elasticache_client(&self) -> aws_sdk_elasticache::Client {
        aws_sdk_elasticache::Client::new(&self.aws_config().await)
    }

    /// Create a CloudFormation client.
    pub async fn cloudformation_client(&self) -> aws_sdk_cloudformation::Client {
        aws_sdk_cloudformation::Client::new(&self.aws_config().await)
    }

    /// Create an SES v1 client.
    pub async fn ses_client(&self) -> aws_sdk_ses::Client {
        aws_sdk_ses::Client::new(&self.aws_config().await)
    }

    /// Create an SES v2 client.
    pub async fn sesv2_client(&self) -> aws_sdk_sesv2::Client {
        aws_sdk_sesv2::Client::new(&self.aws_config().await)
    }

    /// Create a Cognito Identity Provider client.
    pub async fn cognito_client(&self) -> aws_sdk_cognitoidentityprovider::Client {
        aws_sdk_cognitoidentityprovider::Client::new(&self.aws_config().await)
    }

    /// Create an S3 client (path-style addressing for single-endpoint emulator).
    pub async fn s3_client(&self) -> aws_sdk_s3::Client {
        let config = self.aws_config().await;
        let s3_config = aws_sdk_s3::config::Builder::from(&config)
            .force_path_style(true)
            .build();
        aws_sdk_s3::Client::from_conf(s3_config)
    }

    /// Run an AWS CLI command against this test server.
    pub async fn aws_cli(&self, args: &[&str]) -> CliOutput {
        let output = Command::new("aws")
            .args(args)
            .arg("--endpoint-url")
            .arg(self.endpoint())
            .arg("--region")
            .arg("us-east-1")
            .env("AWS_ACCESS_KEY_ID", "test")
            .env("AWS_SECRET_ACCESS_KEY", "test")
            .env("AWS_DEFAULT_REGION", "us-east-1")
            .output()
            .expect("failed to run aws cli");

        CliOutput(output)
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let pid = child.id();
            let _ = child.kill();
            let _ = child.wait();

            // Clean up any Lambda containers spawned by this server instance
            let label = format!("fakecloud-instance=fakecloud-{}", pid);
            let cli = &self.container_cli;
            let output = Command::new(cli)
                .args(["ps", "-aq", "--filter", &format!("label={}", label)])
                .output();
            if let Ok(output) = output {
                let ids = String::from_utf8_lossy(&output.stdout);
                for id in ids.split_whitespace() {
                    let _ = Command::new(cli)
                        .args(["rm", "-f", id])
                        .stdout(Stdio::null())
                        .stderr(Stdio::null())
                        .status();
                }
            }
        }
    }
}

/// Output from an AWS CLI invocation.
pub struct CliOutput(Output);

#[allow(dead_code)]
impl CliOutput {
    pub fn success(&self) -> bool {
        self.0.status.success()
    }

    pub fn stdout_text(&self) -> String {
        String::from_utf8_lossy(&self.0.stdout).to_string()
    }

    pub fn stderr_text(&self) -> String {
        String::from_utf8_lossy(&self.0.stderr).to_string()
    }

    pub fn stdout_json(&self) -> serde_json::Value {
        serde_json::from_slice(&self.0.stdout).unwrap_or(serde_json::Value::Null)
    }
}

fn find_available_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind to random port");
    listener.local_addr().unwrap().port()
}

fn find_binary() -> String {
    let debug_path = concat!(env!("CARGO_MANIFEST_DIR"), "/../../target/debug/fakecloud");
    let release_path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../target/release/fakecloud"
    );

    if std::path::Path::new(debug_path).exists() {
        return debug_path.to_string();
    }
    if std::path::Path::new(release_path).exists() {
        return release_path.to_string();
    }

    panic!(
        "fakecloud binary not found. Run `cargo build` first.\n\
         Looked in:\n  {debug_path}\n  {release_path}"
    );
}

fn detect_container_cli() -> String {
    if cli_available("docker") {
        "docker".to_string()
    } else if cli_available("podman") {
        "podman".to_string()
    } else {
        "docker".to_string()
    }
}

fn cli_available(cli: &str) -> bool {
    Command::new(cli)
        .arg("info")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

async fn wait_for_port(child: &mut Child, port: u16) -> bool {
    let addr = format!("127.0.0.1:{port}");
    for _ in 0..300 {
        if std::net::TcpStream::connect(&addr).is_ok() {
            return true;
        }
        if child.try_wait().ok().flatten().is_some() {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    false
}
