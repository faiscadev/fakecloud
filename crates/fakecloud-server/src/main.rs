use std::sync::Arc;

use axum::extract::Extension;
use axum::Router;
use clap::Parser;
use tokio::net::TcpListener;
use tower_http::trace::TraceLayer;

use fakecloud_core::delivery::DeliveryBus;
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
    #[arg(long, default_value = "123456789012", env = "FAKECLOUD_ACCOUNT_ID")]
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

    // Shared state
    let iam_state = Arc::new(parking_lot::RwLock::new(
        fakecloud_iam::state::IamState::new(&cli.account_id),
    ));
    let sqs_state = Arc::new(parking_lot::RwLock::new(
        fakecloud_sqs::state::SqsState::new(&cli.account_id, &cli.region, "http://localhost:4566"),
    ));
    let sns_state = Arc::new(parking_lot::RwLock::new(
        fakecloud_sns::state::SnsState::new(&cli.account_id, &cli.region),
    ));
    let eb_state = Arc::new(parking_lot::RwLock::new(
        fakecloud_eventbridge::state::EventBridgeState::new(&cli.account_id, &cli.region),
    ));
    let ssm_state = Arc::new(parking_lot::RwLock::new(
        fakecloud_ssm::state::SsmState::new(&cli.account_id, &cli.region),
    ));

    // Cross-service delivery bus
    // Step 1: SQS delivery (SNS and EventBridge can push messages into SQS queues)
    let sqs_delivery = Arc::new(fakecloud_sqs::delivery::SqsDeliveryImpl::new(
        sqs_state.clone(),
    ));
    let delivery_for_sns = Arc::new(DeliveryBus::new().with_sqs(sqs_delivery.clone()));

    // Step 2: SNS delivery (EventBridge can publish to SNS topics, which then fan out to SQS)
    let sns_delivery = Arc::new(fakecloud_sns::delivery::SnsDeliveryImpl::new(
        sns_state.clone(),
        delivery_for_sns.clone(),
    ));
    let delivery_for_eb = Arc::new(
        DeliveryBus::new()
            .with_sqs(sqs_delivery)
            .with_sns(sns_delivery),
    );

    // Register services
    let mut registry = ServiceRegistry::new();
    registry.register(Arc::new(SqsService::new(sqs_state)));
    registry.register(Arc::new(SnsService::new(sns_state, delivery_for_sns)));
    registry.register(Arc::new(EventBridgeService::new(
        eb_state.clone(),
        delivery_for_eb.clone(),
    )));

    // Spawn the EventBridge scheduler as a background task
    let scheduler = fakecloud_eventbridge::scheduler::Scheduler::new(eb_state, delivery_for_eb);
    tokio::spawn(scheduler.run());
    registry.register(Arc::new(IamService::new(iam_state.clone())));
    registry.register(Arc::new(StsService::new(iam_state)));
    registry.register(Arc::new(SsmService::new(ssm_state)));

    let services: Vec<&str> = registry.service_names();
    tracing::info!(services = ?services, "registered services");

    let config = DispatchConfig {
        region: cli.region,
        account_id: cli.account_id,
    };

    let service_names: Vec<String> = registry
        .service_names()
        .iter()
        .map(|s| s.to_string())
        .collect();

    let app = Router::new()
        .route(
            "/_fakecloud/health",
            axum::routing::get({
                let services = service_names.clone();
                move || async move {
                    axum::Json(serde_json::json!({
                        "status": "ok",
                        "version": env!("CARGO_PKG_VERSION"),
                        "services": services,
                    }))
                }
            }),
        )
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
