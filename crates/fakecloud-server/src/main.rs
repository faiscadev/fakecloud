use std::sync::Arc;

use axum::extract::Extension;
use axum::Router;
use clap::Parser;
use tokio::net::TcpListener;
use tower_http::trace::TraceLayer;

use fakecloud_core::dispatch::{self, DispatchConfig};
use fakecloud_core::registry::ServiceRegistry;

use fakecloud_eventbridge::service::EventBridgeService;
use fakecloud_iam::iam_service::IamService;
use fakecloud_iam::sts_service::StsService;
use fakecloud_sns::service::SnsService;
use fakecloud_sqs::service::SqsService;
use fakecloud_ssm::service::SsmService;

#[derive(Parser)]
#[command(name = "fakecloud-server")]
#[command(about = "FakeCloud — local AWS cloud emulator")]
#[command(version)]
struct Cli {
    /// Listen address
    #[arg(long, default_value = "0.0.0.0:4566", env = "FAKECLOUD_ADDR")]
    addr: String,

    /// AWS region to advertise
    #[arg(long, default_value = "us-east-1", env = "FAKECLOUD_REGION")]
    region: String,

    /// AWS account ID to use
    #[arg(long, default_value = "000000000000", env = "FAKECLOUD_ACCOUNT_ID")]
    account_id: String,

    /// Log level (trace, debug, info, warn, error)
    #[arg(long, default_value = "info", env = "FAKECLOUD_LOG")]
    log_level: String,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_new(&cli.log_level)
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let mut registry = ServiceRegistry::new();
    registry.register(Arc::new(SqsService::new()));
    registry.register(Arc::new(SnsService::new()));
    registry.register(Arc::new(EventBridgeService::new()));
    registry.register(Arc::new(IamService::new()));
    registry.register(Arc::new(StsService::new()));
    registry.register(Arc::new(SsmService::new()));

    let services: Vec<&str> = registry.service_names();
    tracing::info!(services = ?services, "registered services");

    let config = DispatchConfig {
        region: cli.region,
        account_id: cli.account_id,
    };

    let app = Router::new()
        .fallback(dispatch::dispatch)
        .layer(Extension(Arc::new(registry)))
        .layer(Extension(Arc::new(config)))
        .layer(TraceLayer::new_for_http());

    let listener = TcpListener::bind(&cli.addr).await.unwrap();
    tracing::info!(addr = %cli.addr, "fakecloud is ready");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .unwrap();
}

async fn shutdown_signal() {
    tokio::signal::ctrl_c()
        .await
        .expect("failed to install Ctrl+C handler");
    tracing::info!("shutting down");
}
