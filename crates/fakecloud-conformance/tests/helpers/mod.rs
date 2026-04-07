use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use aws_config::BehaviorVersion;
use aws_credential_types::Credentials;
use aws_types::region::Region;

#[allow(dead_code)]
pub struct TestServer {
    child: Option<Child>,
    port: u16,
    endpoint: String,
}

#[allow(dead_code)]
impl TestServer {
    pub async fn start() -> Self {
        let port = find_available_port();
        let endpoint = format!("http://127.0.0.1:{port}");

        let bin = find_binary();

        let child = Command::new(bin)
            .arg("--addr")
            .arg(format!("127.0.0.1:{port}"))
            .arg("--log-level")
            .arg("error")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("failed to start fakecloud");

        wait_for_port(port).await;

        Self {
            child: Some(child),
            port,
            endpoint,
        }
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

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

    pub async fn sqs_client(&self) -> aws_sdk_sqs::Client {
        aws_sdk_sqs::Client::new(&self.aws_config().await)
    }

    pub async fn sns_client(&self) -> aws_sdk_sns::Client {
        aws_sdk_sns::Client::new(&self.aws_config().await)
    }

    pub async fn eventbridge_client(&self) -> aws_sdk_eventbridge::Client {
        aws_sdk_eventbridge::Client::new(&self.aws_config().await)
    }

    pub async fn iam_client(&self) -> aws_sdk_iam::Client {
        aws_sdk_iam::Client::new(&self.aws_config().await)
    }

    pub async fn sts_client(&self) -> aws_sdk_sts::Client {
        aws_sdk_sts::Client::new(&self.aws_config().await)
    }

    pub async fn ssm_client(&self) -> aws_sdk_ssm::Client {
        aws_sdk_ssm::Client::new(&self.aws_config().await)
    }

    pub async fn dynamodb_client(&self) -> aws_sdk_dynamodb::Client {
        aws_sdk_dynamodb::Client::new(&self.aws_config().await)
    }

    pub async fn lambda_client(&self) -> aws_sdk_lambda::Client {
        aws_sdk_lambda::Client::new(&self.aws_config().await)
    }

    pub async fn secretsmanager_client(&self) -> aws_sdk_secretsmanager::Client {
        aws_sdk_secretsmanager::Client::new(&self.aws_config().await)
    }

    pub async fn logs_client(&self) -> aws_sdk_cloudwatchlogs::Client {
        aws_sdk_cloudwatchlogs::Client::new(&self.aws_config().await)
    }

    pub async fn kms_client(&self) -> aws_sdk_kms::Client {
        aws_sdk_kms::Client::new(&self.aws_config().await)
    }

    pub async fn cloudformation_client(&self) -> aws_sdk_cloudformation::Client {
        aws_sdk_cloudformation::Client::new(&self.aws_config().await)
    }

    pub async fn sesv2_client(&self) -> aws_sdk_sesv2::Client {
        aws_sdk_sesv2::Client::new(&self.aws_config().await)
    }

    pub async fn s3_client(&self) -> aws_sdk_s3::Client {
        let config = self.aws_config().await;
        let s3_config = aws_sdk_s3::config::Builder::from(&config)
            .force_path_style(true)
            .build();
        aws_sdk_s3::Client::from_conf(s3_config)
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

fn find_available_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind to random port")
        .local_addr()
        .unwrap()
        .port()
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

async fn wait_for_port(port: u16) {
    let addr = format!("127.0.0.1:{port}");
    for _ in 0..150 {
        if std::net::TcpStream::connect(&addr).is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("fakecloud did not start within 15 seconds on port {port}");
}
