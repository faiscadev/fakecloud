use std::sync::Arc;

use axum::extract::Extension;
use axum::Router;
use clap::Parser;
use tokio::net::TcpListener;
use tower_http::trace::TraceLayer;

use fakecloud_core::delivery::DeliveryBus;
use fakecloud_core::dispatch::{self, DispatchConfig};
use fakecloud_core::registry::ServiceRegistry;
use fakecloud_sdk::types;

mod kinesis_lambda_poller;
mod lambda_delivery;
mod sqs_lambda_poller;
use kinesis_lambda_poller::KinesisLambdaPoller;
use sqs_lambda_poller::SqsLambdaPoller;

use fakecloud_cloudformation::service::CloudFormationService;
use fakecloud_cognito::service::CognitoService;
use fakecloud_dynamodb::service::DynamoDbService;
use fakecloud_elasticache::service::ElastiCacheService;
use fakecloud_eventbridge::service::EventBridgeService;
use fakecloud_iam::iam_service::IamService;
use fakecloud_iam::sts_service::StsService;
use fakecloud_kinesis::service::KinesisService;
use fakecloud_kms::service::KmsService;
use fakecloud_lambda::service::LambdaService;
use fakecloud_logs::service::LogsService;
use fakecloud_rds::service::RdsService;
use fakecloud_s3::service::S3Service;
use fakecloud_secretsmanager::service::SecretsManagerService;
use fakecloud_ses::service::SesV2Service;
use fakecloud_sns::service::SnsService;
use fakecloud_sqs::service::SqsService;
use fakecloud_ssm::service::SsmService;

#[derive(Parser)]
#[command(name = "fakecloud")]
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

    // Derive endpoint URL from the configured bind address.
    // Use rsplit to handle IPv6 addresses (e.g., "[::1]:4566").
    let endpoint_url = {
        let addr = &cli.addr;
        let port = addr.rsplit(':').next().unwrap_or("4566");
        let host = addr.rsplit_once(':').map(|(h, _)| h).unwrap_or("0.0.0.0");
        let host = if host == "0.0.0.0" || host == "[::]" {
            "localhost"
        } else {
            host
        };
        format!("http://{host}:{port}")
    };

    // Shared state
    let iam_state = Arc::new(parking_lot::RwLock::new(
        fakecloud_iam::state::IamState::new(&cli.account_id),
    ));
    let sqs_state = Arc::new(parking_lot::RwLock::new(
        fakecloud_sqs::state::SqsState::new(&cli.account_id, &cli.region, &endpoint_url),
    ));
    let sns_state = Arc::new(parking_lot::RwLock::new({
        let mut s =
            fakecloud_sns::state::SnsState::new(&cli.account_id, &cli.region, &endpoint_url);
        s.seed_default_opted_out();
        s
    }));
    let eb_state = Arc::new(parking_lot::RwLock::new(
        fakecloud_eventbridge::state::EventBridgeState::new(&cli.account_id, &cli.region),
    ));
    let ssm_state = Arc::new(parking_lot::RwLock::new(
        fakecloud_ssm::state::SsmState::new(&cli.account_id, &cli.region),
    ));
    let dynamodb_state = Arc::new(parking_lot::RwLock::new(
        fakecloud_dynamodb::state::DynamoDbState::new(&cli.account_id, &cli.region),
    ));
    let lambda_state = Arc::new(parking_lot::RwLock::new(
        fakecloud_lambda::state::LambdaState::new(&cli.account_id, &cli.region),
    ));

    // Auto-detect Docker/Podman for Lambda execution
    let container_runtime = fakecloud_lambda::runtime::ContainerRuntime::new().map(Arc::new);
    if let Some(ref rt) = container_runtime {
        tracing::info!(
            cli = rt.cli_name(),
            "Lambda execution enabled via container runtime"
        );
    } else {
        tracing::info!("Docker/Podman not available — Lambda Invoke will return errors for functions with code");
    }

    let secretsmanager_state = Arc::new(parking_lot::RwLock::new(
        fakecloud_secretsmanager::state::SecretsManagerState::new(&cli.account_id, &cli.region),
    ));
    let s3_state = Arc::new(parking_lot::RwLock::new(fakecloud_s3::state::S3State::new(
        &cli.account_id,
        &cli.region,
    )));
    let logs_state = Arc::new(parking_lot::RwLock::new(
        fakecloud_logs::state::LogsState::new(&cli.account_id, &cli.region),
    ));
    let kms_state = Arc::new(parking_lot::RwLock::new(
        fakecloud_kms::state::KmsState::new(&cli.account_id, &cli.region),
    ));
    let cloudformation_state = Arc::new(parking_lot::RwLock::new(
        fakecloud_cloudformation::state::CloudFormationState::new(&cli.account_id, &cli.region),
    ));
    let ses_state = Arc::new(parking_lot::RwLock::new(
        fakecloud_ses::state::SesState::new(&cli.account_id, &cli.region),
    ));
    let cognito_state = Arc::new(parking_lot::RwLock::new(
        fakecloud_cognito::state::CognitoState::new(&cli.account_id, &cli.region),
    ));
    let kinesis_state = Arc::new(parking_lot::RwLock::new(
        fakecloud_kinesis::state::KinesisState::new(&cli.account_id, &cli.region),
    ));
    let rds_state = Arc::new(parking_lot::RwLock::new(
        fakecloud_rds::state::RdsState::new(&cli.account_id, &cli.region),
    ));
    let elasticache_state = Arc::new(parking_lot::RwLock::new(
        fakecloud_elasticache::state::ElastiCacheState::new(&cli.account_id, &cli.region),
    ));

    let rds_runtime = fakecloud_rds::runtime::RdsRuntime::new().map(Arc::new);
    if let Some(ref rt) = rds_runtime {
        tracing::info!(
            cli = rt.cli_name(),
            "RDS execution enabled via container runtime"
        );
    } else {
        tracing::info!("Docker/Podman not available — RDS CreateDBInstance will return errors");
    }

    let elasticache_runtime =
        fakecloud_elasticache::runtime::ElastiCacheRuntime::new().map(Arc::new);
    if let Some(ref rt) = elasticache_runtime {
        tracing::info!(
            cli = rt.cli_name(),
            "ElastiCache execution enabled via container runtime"
        );
    } else {
        tracing::info!(
            "Docker/Podman not available — ElastiCache CreateReplicationGroup will return errors"
        );
    }

    // Cross-service delivery bus
    // Step 1: SQS delivery (SNS and EventBridge can push messages into SQS queues)
    let sqs_delivery = Arc::new(fakecloud_sqs::delivery::SqsDeliveryImpl::new(
        sqs_state.clone(),
    ));

    // Lambda delivery (SNS can invoke Lambda functions via container runtime)
    let lambda_delivery: Option<Arc<dyn fakecloud_core::delivery::LambdaDelivery>> =
        container_runtime.as_ref().map(|rt| {
            Arc::new(lambda_delivery::LambdaDeliveryImpl::new(
                lambda_state.clone(),
                rt.clone(),
            )) as Arc<dyn fakecloud_core::delivery::LambdaDelivery>
        });

    let delivery_for_sns = {
        let mut bus = DeliveryBus::new().with_sqs(sqs_delivery.clone());
        if let Some(ref ld) = lambda_delivery {
            bus = bus.with_lambda(ld.clone());
        }
        Arc::new(bus)
    };

    // Step 2: SNS delivery (EventBridge can publish to SNS topics, which then fan out to SQS)
    let sns_delivery = Arc::new(fakecloud_sns::delivery::SnsDeliveryImpl::new(
        sns_state.clone(),
        delivery_for_sns.clone(),
    ));
    let delivery_for_eb = Arc::new(
        DeliveryBus::new()
            .with_sqs(sqs_delivery.clone())
            .with_sns(sns_delivery.clone()),
    );

    // Step 3: S3 delivery (S3 notifications can push to SQS, SNS, and Lambda)
    let sns_delivery_for_ses = sns_delivery.clone();
    let delivery_for_s3 = {
        let mut bus = DeliveryBus::new()
            .with_sqs(sqs_delivery.clone())
            .with_sns(sns_delivery);
        if let Some(ref ld) = lambda_delivery {
            bus = bus.with_lambda(ld.clone());
        }
        Arc::new(bus)
    };

    // Step 4: Logs delivery (subscription filters can push to SQS)
    let sqs_delivery_for_ses = sqs_delivery.clone();
    let delivery_for_logs = Arc::new(DeliveryBus::new().with_sqs(sqs_delivery));

    // Clone state refs for internal endpoints
    let lambda_invocations_state = lambda_state.clone();
    let ses_emails_state = ses_state.clone();
    let ses_inbound_state = ses_state.clone();
    let sns_introspection_state = sns_state.clone();
    let sqs_introspection_state = sqs_state.clone();
    let eb_introspection_state = eb_state.clone();
    let s3_introspection_state = s3_state.clone();
    let rds_introspection_state = rds_state.clone();
    let dynamodb_ttl_state = dynamodb_state.clone();
    let secretsmanager_rotation_state = secretsmanager_state.clone();

    // Clone state refs for simulation endpoints
    let sqs_sim_expiration_state = sqs_state.clone();
    let sqs_sim_force_dlq_state = sqs_state.clone();
    let eb_sim_state = eb_state.clone();
    let eb_sim_delivery = delivery_for_eb.clone();
    let eb_sim_lambda_state = Some(lambda_state.clone());
    let eb_sim_logs_state = Some(logs_state.clone());
    let eb_sim_container_runtime = container_runtime.clone();
    let s3_sim_lifecycle_state = s3_state.clone();
    let lambda_sim_warm_state = lambda_state.clone();
    let lambda_sim_warm_runtime = container_runtime.clone();
    let lambda_sim_evict_runtime = container_runtime.clone();
    let sns_sim_pending_state = sns_state.clone();
    let sns_sim_confirm_state = sns_state.clone();

    // Clone state refs for Cognito simulation endpoints
    let cognito_codes_state = cognito_state.clone();
    let cognito_confirm_state = cognito_state.clone();
    let cognito_tokens_state = cognito_state.clone();
    let cognito_expire_state = cognito_state.clone();
    let cognito_events_state = cognito_state.clone();

    // Clone state for reset endpoint before moving into services
    let reset_state = ResetState {
        iam: iam_state.clone(),
        sqs: sqs_state.clone(),
        sns: sns_state.clone(),
        eb: eb_state.clone(),
        ssm: ssm_state.clone(),
        dynamodb: dynamodb_state.clone(),
        lambda: lambda_state.clone(),
        secretsmanager: secretsmanager_state.clone(),
        s3: s3_state.clone(),
        logs: logs_state.clone(),
        kms: kms_state.clone(),
        cloudformation: cloudformation_state.clone(),
        ses: ses_state.clone(),
        cognito: cognito_state.clone(),
        kinesis: kinesis_state.clone(),
        rds: rds_state.clone(),
        elasticache: elasticache_state.clone(),
        container_runtime: container_runtime.clone(),
        rds_runtime: rds_runtime.clone(),
        elasticache_runtime: elasticache_runtime.clone(),
    };

    // Step 5: CloudFormation delivery (custom resources can invoke Lambda)
    let delivery_for_cf = {
        let mut bus = DeliveryBus::new();
        if let Some(ref ld) = lambda_delivery {
            bus = bus.with_lambda(ld.clone());
        }
        Arc::new(bus)
    };

    // Register services
    let mut registry = ServiceRegistry::new();
    registry.register(Arc::new(CloudFormationService::new(
        cloudformation_state,
        sqs_state.clone(),
        sns_state.clone(),
        ssm_state.clone(),
        iam_state.clone(),
        s3_state.clone(),
        eb_state.clone(),
        dynamodb_state.clone(),
        logs_state.clone(),
        delivery_for_cf,
    )));
    registry.register(Arc::new(SqsService::new(sqs_state.clone())));
    registry.register(Arc::new(SnsService::new(sns_state, delivery_for_sns)));
    let mut eb_service = EventBridgeService::new(eb_state.clone(), delivery_for_eb.clone())
        .with_lambda(lambda_state.clone())
        .with_logs(logs_state.clone());
    if let Some(ref rt) = container_runtime {
        eb_service = eb_service.with_runtime(rt.clone());
    }
    registry.register(Arc::new(eb_service));

    // Spawn the EventBridge scheduler as a background task
    let eb_state_for_ses = eb_state.clone();
    let mut scheduler = fakecloud_eventbridge::scheduler::Scheduler::new(eb_state, delivery_for_eb)
        .with_lambda(lambda_state.clone())
        .with_logs(logs_state.clone());
    if let Some(ref rt) = container_runtime {
        scheduler = scheduler.with_runtime(rt.clone());
    }
    tokio::spawn(scheduler.run());
    registry.register(Arc::new(IamService::new(iam_state.clone())));
    registry.register(Arc::new(StsService::new(iam_state)));
    registry.register(Arc::new(
        SsmService::new(ssm_state).with_secretsmanager(secretsmanager_state.clone()),
    ));
    registry.register(Arc::new(
        DynamoDbService::new(dynamodb_state).with_s3(s3_state.clone()),
    ));
    let mut lambda_service = LambdaService::new(lambda_state.clone());
    if let Some(ref rt) = container_runtime {
        lambda_service = lambda_service.with_runtime(rt.clone());
    }
    registry.register(Arc::new(lambda_service));
    // SecretsManager delivery bus (rotation Lambda invocation)
    let delivery_for_secretsmanager = {
        let mut bus = DeliveryBus::new();
        if let Some(ref ld) = lambda_delivery {
            bus = bus.with_lambda(ld.clone());
        }
        Arc::new(bus)
    };
    let delivery_for_rotation_scheduler = delivery_for_secretsmanager.clone();
    registry.register(Arc::new(
        SecretsManagerService::new(secretsmanager_state).with_delivery(delivery_for_secretsmanager),
    ));
    registry.register(Arc::new(LogsService::new(logs_state, delivery_for_logs)));
    registry.register(Arc::new(KmsService::new(kms_state.clone())));
    registry.register(Arc::new(
        S3Service::new(s3_state.clone(), delivery_for_s3).with_kms(kms_state),
    ));
    // SES delivery bus (event fanout to SNS topics and EventBridge buses)
    let eb_delivery_for_ses = Arc::new(
        fakecloud_eventbridge::delivery::EventBridgeDeliveryImpl::new(
            eb_state_for_ses,
            Arc::new(DeliveryBus::new().with_sqs(sqs_delivery_for_ses)),
        ),
    );
    let delivery_for_ses = Arc::new(
        DeliveryBus::new()
            .with_sns(sns_delivery_for_ses)
            .with_eventbridge(eb_delivery_for_ses),
    );
    let ses_delivery_ctx = fakecloud_ses::fanout::SesDeliveryContext {
        ses_state: ses_state.clone(),
        delivery_bus: delivery_for_ses,
    };
    registry.register(Arc::new(
        SesV2Service::new(ses_state).with_delivery(ses_delivery_ctx),
    ));
    let delivery_for_cognito = {
        let mut bus = DeliveryBus::new();
        if let Some(ref ld) = lambda_delivery {
            bus = bus.with_lambda(ld.clone());
        }
        Arc::new(bus)
    };
    let cognito_delivery_ctx = fakecloud_cognito::triggers::CognitoDeliveryContext {
        delivery_bus: delivery_for_cognito,
    };
    registry.register(Arc::new(
        CognitoService::new(cognito_state.clone()).with_delivery(cognito_delivery_ctx),
    ));
    registry.register(Arc::new(KinesisService::new(kinesis_state.clone())));
    registry.register(Arc::new(KinesisService::new(kinesis_state.clone())));
    let mut rds_service = RdsService::new(rds_state);
    if let Some(ref rt) = rds_runtime {
        rds_service = rds_service.with_runtime(rt.clone());
    }
    registry.register(Arc::new(rds_service));
    let mut elasticache_service = ElastiCacheService::new(elasticache_state);
    if let Some(ref rt) = elasticache_runtime {
        elasticache_service = elasticache_service.with_runtime(rt.clone());
    }
    registry.register(Arc::new(elasticache_service));

    // Spawn background tasks
    let lifecycle_processor = fakecloud_s3::lifecycle::LifecycleProcessor::new(s3_state);
    tokio::spawn(lifecycle_processor.run());

    let mut sqs_lambda_poller = SqsLambdaPoller::new(sqs_state, lambda_state);
    if let Some(ref ld) = lambda_delivery {
        sqs_lambda_poller = sqs_lambda_poller.with_lambda_delivery(ld.clone());
    }
    tokio::spawn(sqs_lambda_poller.run());

    let mut kinesis_lambda_poller =
        KinesisLambdaPoller::new(kinesis_state, lambda_invocations_state.clone());
    if let Some(ref ld) = lambda_delivery {
        kinesis_lambda_poller = kinesis_lambda_poller.with_lambda_delivery(ld.clone());
    }
    tokio::spawn(kinesis_lambda_poller.run());

    if let Some(ref rt) = container_runtime {
        let rt = rt.clone();
        tokio::spawn(rt.run_cleanup_loop(std::time::Duration::from_secs(300)));
    }

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
                    axum::Json(types::HealthResponse {
                        status: "ok".to_string(),
                        version: env!("CARGO_PKG_VERSION").to_string(),
                        services,
                    })
                }
            }),
        )
        .route(
            "/_reset",
            axum::routing::post({
                let s = reset_state.clone();
                move || async move { s.reset() }
            }),
        )
        .route(
            "/_fakecloud/lambda/invocations",
            axum::routing::get({
                let ls = lambda_invocations_state.clone();
                move || async move {
                    let state = ls.read();
                    let invocations = state
                        .invocations
                        .iter()
                        .map(|inv| types::LambdaInvocation {
                            function_arn: inv.function_arn.clone(),
                            payload: inv.payload.clone(),
                            source: inv.source.clone(),
                            timestamp: inv.timestamp.to_rfc3339(),
                        })
                        .collect();
                    axum::Json(types::LambdaInvocationsResponse { invocations })
                }
            }),
        )
        .route(
            "/_fakecloud/ses/emails",
            axum::routing::get({
                let ss = ses_emails_state.clone();
                move || async move {
                    let state = ss.read();
                    let emails = state
                        .sent_emails
                        .iter()
                        .map(|email| types::SentEmail {
                            message_id: email.message_id.clone(),
                            from: email.from.clone(),
                            to: email.to.clone(),
                            cc: email.cc.clone(),
                            bcc: email.bcc.clone(),
                            subject: email.subject.clone(),
                            html_body: email.html_body.clone(),
                            text_body: email.text_body.clone(),
                            raw_data: email.raw_data.clone(),
                            template_name: email.template_name.clone(),
                            template_data: email.template_data.clone(),
                            timestamp: email.timestamp.to_rfc3339(),
                        })
                        .collect();
                    axum::Json(types::SesEmailsResponse { emails })
                }
            }),
        )
        .route(
            "/_fakecloud/ses/inbound",
            axum::routing::post({
                let ss = ses_inbound_state.clone();
                move |axum::Json(body): axum::Json<types::InboundEmailRequest>| async move {
                    let (message_id, matched_rules, actions) =
                        fakecloud_ses::v1::evaluate_inbound_email(
                            &ss,
                            &body.from,
                            &body.to,
                            &body.subject,
                            &body.body,
                        );

                    let actions_executed = actions
                        .iter()
                        .map(|(rule, action)| types::InboundActionExecuted {
                            rule: rule.clone(),
                            action_type: match action {
                                fakecloud_ses::state::ReceiptAction::S3 { .. } => "S3",
                                fakecloud_ses::state::ReceiptAction::Sns { .. } => "SNS",
                                fakecloud_ses::state::ReceiptAction::Lambda { .. } => "Lambda",
                                fakecloud_ses::state::ReceiptAction::Bounce { .. } => "Bounce",
                                fakecloud_ses::state::ReceiptAction::AddHeader { .. } => {
                                    "AddHeader"
                                }
                                fakecloud_ses::state::ReceiptAction::Stop { .. } => "Stop",
                            }
                            .to_string(),
                        })
                        .collect();

                    axum::Json(types::InboundEmailResponse {
                        message_id,
                        matched_rules,
                        actions_executed,
                    })
                }
            }),
        )
        .route(
            "/_fakecloud/sns/messages",
            axum::routing::get({
                let ss = sns_introspection_state;
                move || async move {
                    let state = ss.read();
                    let messages = state
                        .published
                        .iter()
                        .map(|msg| types::SnsMessage {
                            message_id: msg.message_id.clone(),
                            topic_arn: msg.topic_arn.clone(),
                            message: msg.message.clone(),
                            subject: msg.subject.clone(),
                            timestamp: msg.timestamp.to_rfc3339(),
                        })
                        .collect();
                    axum::Json(types::SnsMessagesResponse { messages })
                }
            }),
        )
        .route(
            "/_fakecloud/sqs/messages",
            axum::routing::get({
                let ss = sqs_introspection_state;
                move || async move {
                    let state = ss.read();
                    let queues = state
                        .queues
                        .values()
                        .map(|queue| {
                            let mut messages: Vec<types::SqsMessageInfo> = queue
                                .messages
                                .iter()
                                .map(|msg| types::SqsMessageInfo {
                                    message_id: msg.message_id.clone(),
                                    body: msg.body.clone(),
                                    receive_count: msg.receive_count as u64,
                                    in_flight: false,
                                    created_at: msg.created_at.to_rfc3339(),
                                })
                                .collect();
                            let inflight: Vec<types::SqsMessageInfo> = queue
                                .inflight
                                .iter()
                                .map(|msg| types::SqsMessageInfo {
                                    message_id: msg.message_id.clone(),
                                    body: msg.body.clone(),
                                    receive_count: msg.receive_count as u64,
                                    in_flight: true,
                                    created_at: msg.created_at.to_rfc3339(),
                                })
                                .collect();
                            messages.extend(inflight);
                            types::SqsQueueMessages {
                                queue_url: queue.queue_url.clone(),
                                queue_name: queue.queue_name.clone(),
                                messages,
                            }
                        })
                        .collect();
                    axum::Json(types::SqsMessagesResponse { queues })
                }
            }),
        )
        .route(
            "/_fakecloud/events/history",
            axum::routing::get({
                let es = eb_introspection_state;
                move || async move {
                    let state = es.read();
                    let events = state
                        .events
                        .iter()
                        .map(|evt| types::EventBridgeEvent {
                            event_id: evt.event_id.clone(),
                            source: evt.source.clone(),
                            detail_type: evt.detail_type.clone(),
                            detail: evt.detail.clone(),
                            bus_name: evt.event_bus_name.clone(),
                            timestamp: evt.time.to_rfc3339(),
                        })
                        .collect();
                    let lambda = state
                        .lambda_invocations
                        .iter()
                        .map(|inv| types::EventBridgeLambdaDelivery {
                            function_arn: inv.function_arn.clone(),
                            payload: inv.payload.clone(),
                            timestamp: inv.timestamp.to_rfc3339(),
                        })
                        .collect();
                    let logs = state
                        .log_deliveries
                        .iter()
                        .map(|ld| types::EventBridgeLogDelivery {
                            log_group_arn: ld.log_group_arn.clone(),
                            payload: ld.payload.clone(),
                            timestamp: ld.timestamp.to_rfc3339(),
                        })
                        .collect();
                    axum::Json(types::EventHistoryResponse {
                        events,
                        deliveries: types::EventBridgeDeliveries { lambda, logs },
                    })
                }
            }),
        )
        .route(
            "/_fakecloud/sqs/expiration-processor/tick",
            axum::routing::post({
                let ss = sqs_sim_expiration_state;
                move || async move {
                    let expired = fakecloud_sqs::simulation::tick_expiration(&ss);
                    axum::Json(types::ExpirationTickResponse {
                        expired_messages: expired,
                    })
                }
            }),
        )
        .route(
            "/_fakecloud/sqs/{queue_name}/force-dlq",
            axum::routing::post({
                let ss = sqs_sim_force_dlq_state;
                move |axum::extract::Path(queue_name): axum::extract::Path<String>| async move {
                    let moved = fakecloud_sqs::simulation::force_dlq(&ss, &queue_name);
                    axum::Json(types::ForceDlqResponse {
                        moved_messages: moved,
                    })
                }
            }),
        )
        .route(
            "/_fakecloud/events/fire-rule",
            axum::routing::post({
                let es = eb_sim_state;
                let delivery = eb_sim_delivery;
                let lambda_state = eb_sim_lambda_state;
                let logs_state = eb_sim_logs_state;
                let container_runtime = eb_sim_container_runtime;
                move |axum::Json(body): axum::Json<types::FireRuleRequest>| async move {
                    let bus_name = body.bus_name.as_deref().unwrap_or("default");

                    match fakecloud_eventbridge::simulation::fire_rule(
                        &es,
                        &delivery,
                        &lambda_state,
                        &logs_state,
                        &container_runtime,
                        bus_name,
                        &body.rule_name,
                    ) {
                        Ok(targets) => {
                            let target_list = targets
                                .iter()
                                .map(|t| types::FireRuleTarget {
                                    target_type: t.target_type.clone(),
                                    arn: t.arn.clone(),
                                })
                                .collect();
                            (
                                axum::http::StatusCode::OK,
                                axum::Json(serde_json::json!(types::FireRuleResponse {
                                    targets: target_list
                                })),
                            )
                        }
                        Err(msg) => (
                            axum::http::StatusCode::NOT_FOUND,
                            axum::Json(serde_json::json!({ "error": msg })),
                        ),
                    }
                }
            }),
        )
        .route(
            "/_fakecloud/s3/notifications",
            axum::routing::get({
                let ss = s3_introspection_state;
                move || async move {
                    let state = ss.read();
                    let notifications = state
                        .notification_events
                        .iter()
                        .map(|evt| types::S3Notification {
                            bucket: evt.bucket.clone(),
                            key: evt.key.clone(),
                            event_type: evt.event_type.clone(),
                            timestamp: evt.timestamp.to_rfc3339(),
                        })
                        .collect();
                    axum::Json(types::S3NotificationsResponse { notifications })
                }
            }),
        )
        .route(
            "/_fakecloud/dynamodb/ttl-processor/tick",
            axum::routing::post({
                let ds = dynamodb_ttl_state;
                move || async move {
                    let count = fakecloud_dynamodb::ttl::process_ttl_expirations(&ds);
                    axum::Json(types::TtlTickResponse {
                        expired_items: count as u64,
                    })
                }
            }),
        )
        .route(
            "/_fakecloud/secretsmanager/rotation-scheduler/tick",
            axum::routing::post({
                let ss = secretsmanager_rotation_state;
                let bus = delivery_for_rotation_scheduler;
                move || async move {
                    let rotated =
                        fakecloud_secretsmanager::rotation::check_and_rotate(&ss, Some(&bus)).await;
                    axum::Json(types::RotationTickResponse {
                        rotated_secrets: rotated,
                    })
                }
            }),
        )
        .route(
            "/_fakecloud/cognito/confirmation-codes/{pool_id}/{username}",
            axum::routing::get({
                let cs = cognito_state.clone();
                move |axum::extract::Path((pool_id, username)): axum::extract::Path<(
                    String,
                    String,
                )>| {
                    let cs = cs.clone();
                    async move {
                        let state = cs.read();
                        let user = state
                            .users
                            .get(&pool_id)
                            .and_then(|users| users.get(&username));
                        let code = user.and_then(|u| u.confirmation_code.clone());
                        let attr_codes = user
                            .map(|u| serde_json::json!(u.attribute_verification_codes))
                            .unwrap_or(serde_json::json!({}));
                        axum::Json(types::UserConfirmationCodes {
                            confirmation_code: code,
                            attribute_verification_codes: attr_codes,
                        })
                    }
                }
            }),
        )
        .route(
            "/_fakecloud/cognito/confirmation-codes",
            axum::routing::get({
                let cs = cognito_codes_state;
                move || {
                    let cs = cs.clone();
                    async move {
                        let state = cs.read();
                        let mut codes = Vec::new();
                        for (pool_id, users) in &state.users {
                            for (username, user) in users {
                                if let Some(code) = &user.confirmation_code {
                                    codes.push(types::ConfirmationCode {
                                        pool_id: pool_id.clone(),
                                        username: username.clone(),
                                        code: code.clone(),
                                        code_type: "signup".to_string(),
                                        attribute: None,
                                    });
                                }
                                for (attr, code) in &user.attribute_verification_codes {
                                    codes.push(types::ConfirmationCode {
                                        pool_id: pool_id.clone(),
                                        username: username.clone(),
                                        code: code.clone(),
                                        code_type: "attribute_verification".to_string(),
                                        attribute: Some(attr.clone()),
                                    });
                                }
                            }
                        }
                        axum::Json(types::ConfirmationCodesResponse { codes })
                    }
                }
            }),
        )
        .route(
            "/_fakecloud/cognito/confirm-user",
            axum::routing::post({
                let cs = cognito_confirm_state;
                move |axum::Json(body): axum::Json<types::ConfirmUserRequest>| {
                    let cs = cs.clone();
                    async move {
                        let mut state = cs.write();
                        let user = state
                            .users
                            .get_mut(&body.user_pool_id)
                            .and_then(|users| users.get_mut(&body.username));
                        match user {
                            Some(user) => {
                                user.user_status = "CONFIRMED".to_string();
                                user.confirmation_code = None;
                                user.user_last_modified_date = chrono::Utc::now();
                                (
                                    axum::http::StatusCode::OK,
                                    axum::Json(serde_json::json!(types::ConfirmUserResponse {
                                        confirmed: true,
                                        error: None,
                                    })),
                                )
                            }
                            None => (
                                axum::http::StatusCode::NOT_FOUND,
                                axum::Json(serde_json::json!(types::ConfirmUserResponse {
                                    confirmed: false,
                                    error: Some("User not found".to_string()),
                                })),
                            ),
                        }
                    }
                }
            }),
        )
        .route(
            "/_fakecloud/cognito/tokens",
            axum::routing::get({
                let cs = cognito_tokens_state;
                move || {
                    let cs = cs.clone();
                    async move {
                        let state = cs.read();
                        let mut tokens = Vec::new();
                        for data in state.access_tokens.values() {
                            tokens.push(types::TokenInfo {
                                token_type: "access".to_string(),
                                username: data.username.clone(),
                                pool_id: data.user_pool_id.clone(),
                                client_id: data.client_id.clone(),
                                issued_at: data.issued_at.timestamp() as f64,
                            });
                        }
                        for data in state.refresh_tokens.values() {
                            tokens.push(types::TokenInfo {
                                token_type: "refresh".to_string(),
                                username: data.username.clone(),
                                pool_id: data.user_pool_id.clone(),
                                client_id: data.client_id.clone(),
                                issued_at: data.issued_at.timestamp() as f64,
                            });
                        }
                        axum::Json(types::TokensResponse { tokens })
                    }
                }
            }),
        )
        .route(
            "/_fakecloud/cognito/expire-tokens",
            axum::routing::post({
                let cs = cognito_expire_state;
                move |axum::Json(body): axum::Json<types::ExpireTokensRequest>| {
                    let cs = cs.clone();
                    async move {
                        let mut state = cs.write();
                        let mut expired = 0usize;

                        let matches = |p: &str, u: &str| -> bool {
                            body.user_pool_id.as_ref().is_none_or(|pid| pid == p)
                                && body.username.as_ref().is_none_or(|un| un == u)
                        };

                        let before_access = state.access_tokens.len();
                        state
                            .access_tokens
                            .retain(|_, v| !matches(&v.user_pool_id, &v.username));
                        expired += before_access - state.access_tokens.len();

                        let before_refresh = state.refresh_tokens.len();
                        state
                            .refresh_tokens
                            .retain(|_, v| !matches(&v.user_pool_id, &v.username));
                        expired += before_refresh - state.refresh_tokens.len();

                        let before_sessions = state.sessions.len();
                        state
                            .sessions
                            .retain(|_, v| !matches(&v.user_pool_id, &v.username));
                        expired += before_sessions - state.sessions.len();

                        axum::Json(types::ExpireTokensResponse {
                            expired_tokens: expired as u64,
                        })
                    }
                }
            }),
        )
        .route(
            "/_fakecloud/cognito/auth-events",
            axum::routing::get({
                let cs = cognito_events_state;
                move || {
                    let cs = cs.clone();
                    async move {
                        let state = cs.read();
                        let events = state
                            .auth_events
                            .iter()
                            .map(|e| types::AuthEvent {
                                event_type: e.event_type.clone(),
                                username: e.username.clone(),
                                user_pool_id: e.user_pool_id.clone(),
                                client_id: e.client_id.clone(),
                                timestamp: e.timestamp.timestamp() as f64,
                                success: e.success,
                            })
                            .collect();
                        axum::Json(types::AuthEventsResponse { events })
                    }
                }
            }),
        )
        .route(
            "/_fakecloud/s3/lifecycle-processor/tick",
            axum::routing::post({
                let ss = s3_sim_lifecycle_state;
                move || async move {
                    let result = fakecloud_s3::simulation::tick_lifecycle(&ss);
                    axum::Json(types::LifecycleTickResponse {
                        processed_buckets: result.processed_buckets,
                        expired_objects: result.expired_objects,
                        transitioned_objects: result.transitioned_objects,
                    })
                }
            }),
        )
        .route(
            "/_fakecloud/lambda/warm-containers",
            axum::routing::get({
                let ls = lambda_sim_warm_state;
                let rt = lambda_sim_warm_runtime;
                move || async move {
                    let containers: Vec<serde_json::Value> = if let Some(ref rt) = rt {
                        rt.list_warm_containers(&ls)
                    } else {
                        Vec::new()
                    };
                    // list_warm_containers returns Vec<serde_json::Value>, so we
                    // deserialize into our typed struct for consistency.
                    let containers: Vec<types::WarmContainer> = containers
                        .into_iter()
                        .filter_map(|v| serde_json::from_value(v).ok())
                        .collect();
                    axum::Json(types::WarmContainersResponse { containers })
                }
            }),
        )
        .route(
            "/_fakecloud/rds/instances",
            axum::routing::get({
                let rs = rds_introspection_state;
                move || {
                    let rs = rs.clone();
                    async move {
                        let state = rs.read();
                        let mut instances: Vec<types::RdsInstance> = state
                            .instances
                            .values()
                            .map(rds_instance_response)
                            .collect();
                        instances.sort_by(|a, b| {
                            a.db_instance_identifier.cmp(&b.db_instance_identifier)
                        });
                        axum::Json(types::RdsInstancesResponse { instances })
                    }
                }
            }),
        )
        .route(
            "/_fakecloud/lambda/{function_name}/evict-container",
            axum::routing::post({
                let rt = lambda_sim_evict_runtime;
                move |axum::extract::Path(function_name): axum::extract::Path<String>| async move {
                    let evicted = if let Some(ref rt) = rt {
                        rt.evict_container(&function_name).await
                    } else {
                        false
                    };
                    axum::Json(types::EvictContainerResponse { evicted })
                }
            }),
        )
        .route(
            "/_fakecloud/sns/pending-confirmations",
            axum::routing::get({
                let ss = sns_sim_pending_state;
                move || async move {
                    let pending = fakecloud_sns::simulation::list_pending_confirmations(&ss);
                    let pending_confirmations = pending
                        .into_iter()
                        .map(|p| types::PendingConfirmation {
                            subscription_arn: p.subscription_arn,
                            topic_arn: p.topic_arn,
                            protocol: p.protocol,
                            endpoint: p.endpoint,
                            token: p.token,
                        })
                        .collect();
                    axum::Json(types::PendingConfirmationsResponse {
                        pending_confirmations,
                    })
                }
            }),
        )
        .route(
            "/_fakecloud/sns/confirm-subscription",
            axum::routing::post({
                let ss = sns_sim_confirm_state;
                move |axum::Json(body): axum::Json<types::ConfirmSubscriptionRequest>| async move {
                    let confirmed = fakecloud_sns::simulation::confirm_subscription(
                        &ss,
                        &body.subscription_arn,
                    );
                    axum::Json(types::ConfirmSubscriptionResponse { confirmed })
                }
            }),
        )
        .route(
            "/_fakecloud/reset/{service}",
            axum::routing::post({
                let s = reset_state.clone();
                move |axum::extract::Path(service): axum::extract::Path<String>| async move {
                    match s.reset_service(&service) {
                        Ok(()) => (
                            axum::http::StatusCode::OK,
                            axum::Json(serde_json::json!(types::ResetServiceResponse {
                                reset: service
                            })),
                        ),
                        Err(msg) => (
                            axum::http::StatusCode::NOT_FOUND,
                            axum::Json(serde_json::json!({ "error": msg })),
                        ),
                    }
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

    // Clean up Lambda containers on shutdown
    if let Some(rt) = container_runtime {
        rt.stop_all().await;
    }
    if let Some(rt) = rds_runtime {
        rt.stop_all().await;
    }
    if let Some(rt) = elasticache_runtime {
        rt.stop_all().await;
    }
}

#[derive(Clone)]
struct ResetState {
    iam: fakecloud_iam::state::SharedIamState,
    sqs: fakecloud_sqs::state::SharedSqsState,
    sns: fakecloud_sns::state::SharedSnsState,
    eb: fakecloud_eventbridge::state::SharedEventBridgeState,
    ssm: fakecloud_ssm::state::SharedSsmState,
    dynamodb: fakecloud_dynamodb::state::SharedDynamoDbState,
    lambda: fakecloud_lambda::state::SharedLambdaState,
    secretsmanager: fakecloud_secretsmanager::state::SharedSecretsManagerState,
    s3: fakecloud_s3::state::SharedS3State,
    logs: fakecloud_logs::state::SharedLogsState,
    kms: fakecloud_kms::state::SharedKmsState,
    cloudformation: fakecloud_cloudformation::state::SharedCloudFormationState,
    ses: fakecloud_ses::state::SharedSesState,
    cognito: fakecloud_cognito::state::SharedCognitoState,
    kinesis: fakecloud_kinesis::state::SharedKinesisState,
    rds: fakecloud_rds::state::SharedRdsState,
    elasticache: fakecloud_elasticache::state::SharedElastiCacheState,
    container_runtime: Option<Arc<fakecloud_lambda::runtime::ContainerRuntime>>,
    rds_runtime: Option<Arc<fakecloud_rds::runtime::RdsRuntime>>,
    elasticache_runtime: Option<Arc<fakecloud_elasticache::runtime::ElastiCacheRuntime>>,
}

impl ResetState {
    fn reset_service(&self, service: &str) -> Result<(), String> {
        match service {
            "iam" | "sts" => {
                self.iam.write().reset();
            }
            "sqs" => {
                let mut s = self.sqs.write();
                s.queues.clear();
                s.name_to_url.clear();
            }
            "sns" => {
                let mut s = self.sns.write();
                s.reset();
                s.seed_default_opted_out();
            }
            "events" | "eventbridge" => {
                let mut eb = self.eb.write();
                eb.rules.clear();
                eb.events.clear();
                eb.archives.clear();
                eb.connections.clear();
                eb.api_destinations.clear();
                eb.replays.clear();
                eb.buses.retain(|name, _| name == "default");
                eb.lambda_invocations.clear();
                eb.log_deliveries.clear();
                eb.step_function_executions.clear();
            }
            "ssm" => {
                self.ssm.write().reset();
            }
            "dynamodb" => {
                self.dynamodb.write().reset();
            }
            "lambda" => {
                self.lambda.write().reset();
                if let Some(ref rt) = self.container_runtime {
                    let rt = rt.clone();
                    tokio::spawn(async move { rt.stop_all().await });
                }
            }
            "secretsmanager" => {
                self.secretsmanager.write().reset();
            }
            "s3" => {
                self.s3.write().reset();
            }
            "logs" => {
                self.logs.write().reset();
            }
            "kms" => {
                self.kms.write().reset();
            }
            "cloudformation" => {
                self.cloudformation.write().reset();
            }
            "ses" => {
                self.ses.write().reset();
            }
            "cognito" => {
                self.cognito.write().reset();
            }
            "kinesis" => {
                self.kinesis.write().reset();
            }
            "rds" => {
                self.rds.write().reset();
                if let Some(ref rt) = self.rds_runtime {
                    let rt = rt.clone();
                    tokio::spawn(async move { rt.stop_all().await });
                }
            }
            "elasticache" => {
                self.elasticache.write().reset();
                if let Some(ref rt) = self.elasticache_runtime {
                    let rt = rt.clone();
                    tokio::spawn(async move { rt.stop_all().await });
                }
            }
            _ => {
                return Err(format!("Unknown service: {service}"));
            }
        }
        tracing::info!(service = %service, "service state reset via per-service reset API");
        Ok(())
    }

    fn reset(&self) -> axum::Json<types::ResetResponse> {
        self.iam.write().reset();
        self.sqs.write().queues.clear();
        self.sqs.write().name_to_url.clear();
        {
            let mut sns = self.sns.write();
            sns.reset();
            sns.seed_default_opted_out();
        }
        {
            let mut eb = self.eb.write();
            eb.rules.clear();
            eb.events.clear();
            eb.archives.clear();
            eb.connections.clear();
            eb.api_destinations.clear();
            eb.replays.clear();
            eb.buses.retain(|name, _| name == "default");
            eb.lambda_invocations.clear();
            eb.log_deliveries.clear();
            eb.step_function_executions.clear();
        }
        self.ssm.write().reset();
        self.dynamodb.write().reset();
        self.lambda.write().reset();
        // Stop all Lambda containers on reset
        if let Some(ref rt) = self.container_runtime {
            let rt = rt.clone();
            tokio::spawn(async move { rt.stop_all().await });
        }
        self.secretsmanager.write().reset();
        self.s3.write().reset();
        self.logs.write().reset();
        self.kms.write().reset();
        self.cloudformation.write().reset();
        self.ses.write().reset();
        self.cognito.write().reset();
        self.kinesis.write().reset();
        self.rds.write().reset();
        if let Some(ref rt) = self.rds_runtime {
            let rt = rt.clone();
            tokio::spawn(async move { rt.stop_all().await });
        }
        self.elasticache.write().reset();
        if let Some(ref rt) = self.elasticache_runtime {
            let rt = rt.clone();
            tokio::spawn(async move { rt.stop_all().await });
        }
        tracing::info!("state reset via reset API");
        axum::Json(types::ResetResponse {
            status: "ok".to_string(),
        })
    }
}

async fn shutdown_signal() {
    tokio::signal::ctrl_c()
        .await
        .expect("failed to install Ctrl+C handler");
    tracing::info!("shutting down");
}

fn rds_instance_response(instance: &fakecloud_rds::state::DbInstance) -> types::RdsInstance {
    types::RdsInstance {
        db_instance_identifier: instance.db_instance_identifier.clone(),
        db_instance_arn: instance.db_instance_arn.clone(),
        db_instance_class: instance.db_instance_class.clone(),
        engine: instance.engine.clone(),
        engine_version: instance.engine_version.clone(),
        db_instance_status: instance.db_instance_status.clone(),
        master_username: instance.master_username.clone(),
        db_name: instance.db_name.clone(),
        endpoint_address: instance.endpoint_address.clone(),
        port: instance.port,
        allocated_storage: instance.allocated_storage,
        publicly_accessible: instance.publicly_accessible,
        deletion_protection: instance.deletion_protection,
        created_at: instance.created_at.to_rfc3339(),
        dbi_resource_id: instance.dbi_resource_id.clone(),
        container_id: instance.container_id.clone(),
        host_port: instance.host_port,
        tags: instance
            .tags
            .iter()
            .map(|tag| types::RdsTag {
                key: tag.key.clone(),
                value: tag.value.clone(),
            })
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use chrono::Utc;
    use fakecloud_rds::state::{DbInstance, RdsState};

    use super::{rds_instance_response, ResetState};

    #[test]
    fn reset_service_clears_rds_state() {
        let mut rds = RdsState::new("123456789012", "us-east-1");
        rds.instances.insert(
            "db-1".to_string(),
            DbInstance {
                db_instance_identifier: "db-1".to_string(),
                db_instance_arn: "arn:aws:rds:us-east-1:123456789012:db:db-1".to_string(),
                db_instance_class: "db.t3.micro".to_string(),
                engine: "postgres".to_string(),
                engine_version: "16.3".to_string(),
                db_instance_status: "available".to_string(),
                master_username: "admin".to_string(),
                db_name: Some("postgres".to_string()),
                endpoint_address: "127.0.0.1".to_string(),
                port: 5432,
                allocated_storage: 20,
                publicly_accessible: true,
                deletion_protection: false,
                created_at: Utc::now(),
                dbi_resource_id: "db-test".to_string(),
                master_user_password: "secret123".to_string(),
                container_id: "container-id".to_string(),
                host_port: 15432,
                tags: Vec::new(),
                read_replica_source_db_instance_identifier: None,
                read_replica_db_instance_identifiers: Vec::new(),
            },
        );

        let state = ResetState {
            iam: Arc::new(parking_lot::RwLock::new(
                fakecloud_iam::state::IamState::new("123456789012"),
            )),
            sqs: Arc::new(parking_lot::RwLock::new(
                fakecloud_sqs::state::SqsState::new(
                    "123456789012",
                    "us-east-1",
                    "http://localhost:4566",
                ),
            )),
            sns: Arc::new(parking_lot::RwLock::new(
                fakecloud_sns::state::SnsState::new(
                    "123456789012",
                    "us-east-1",
                    "http://localhost:4566",
                ),
            )),
            eb: Arc::new(parking_lot::RwLock::new(
                fakecloud_eventbridge::state::EventBridgeState::new("123456789012", "us-east-1"),
            )),
            ssm: Arc::new(parking_lot::RwLock::new(
                fakecloud_ssm::state::SsmState::new("123456789012", "us-east-1"),
            )),
            dynamodb: Arc::new(parking_lot::RwLock::new(
                fakecloud_dynamodb::state::DynamoDbState::new("123456789012", "us-east-1"),
            )),
            lambda: Arc::new(parking_lot::RwLock::new(
                fakecloud_lambda::state::LambdaState::new("123456789012", "us-east-1"),
            )),
            secretsmanager: Arc::new(parking_lot::RwLock::new(
                fakecloud_secretsmanager::state::SecretsManagerState::new(
                    "123456789012",
                    "us-east-1",
                ),
            )),
            s3: Arc::new(parking_lot::RwLock::new(fakecloud_s3::state::S3State::new(
                "123456789012",
                "us-east-1",
            ))),
            logs: Arc::new(parking_lot::RwLock::new(
                fakecloud_logs::state::LogsState::new("123456789012", "us-east-1"),
            )),
            kms: Arc::new(parking_lot::RwLock::new(
                fakecloud_kms::state::KmsState::new("123456789012", "us-east-1"),
            )),
            cloudformation: Arc::new(parking_lot::RwLock::new(
                fakecloud_cloudformation::state::CloudFormationState::new(
                    "123456789012",
                    "us-east-1",
                ),
            )),
            ses: Arc::new(parking_lot::RwLock::new(
                fakecloud_ses::state::SesState::new("123456789012", "us-east-1"),
            )),
            cognito: Arc::new(parking_lot::RwLock::new(
                fakecloud_cognito::state::CognitoState::new("123456789012", "us-east-1"),
            )),
            kinesis: Arc::new(parking_lot::RwLock::new(
                fakecloud_kinesis::state::KinesisState::new("123456789012", "us-east-1"),
            )),
            rds: Arc::new(parking_lot::RwLock::new(rds)),
            elasticache: Arc::new(parking_lot::RwLock::new(
                fakecloud_elasticache::state::ElastiCacheState::new("123456789012", "us-east-1"),
            )),
            container_runtime: None,
            rds_runtime: None,
            elasticache_runtime: None,
        };

        state.reset_service("rds").expect("reset rds");

        assert!(state.rds.read().instances.is_empty());
    }

    #[test]
    fn rds_instance_response_omits_password_but_keeps_runtime_metadata() {
        let instance = DbInstance {
            db_instance_identifier: "db-1".to_string(),
            db_instance_arn: "arn:aws:rds:us-east-1:123456789012:db:db-1".to_string(),
            db_instance_class: "db.t3.micro".to_string(),
            engine: "postgres".to_string(),
            engine_version: "16.3".to_string(),
            db_instance_status: "available".to_string(),
            master_username: "admin".to_string(),
            db_name: Some("appdb".to_string()),
            endpoint_address: "127.0.0.1".to_string(),
            port: 15432,
            allocated_storage: 20,
            publicly_accessible: true,
            deletion_protection: false,
            created_at: Utc::now(),
            dbi_resource_id: "db-test".to_string(),
            master_user_password: "secret123".to_string(),
            container_id: "container-id".to_string(),
            host_port: 15432,
            tags: vec![fakecloud_rds::state::RdsTag {
                key: "env".to_string(),
                value: "test".to_string(),
            }],
        };

        let response = rds_instance_response(&instance);

        assert_eq!(response.db_instance_identifier, "db-1");
        assert_eq!(response.container_id, "container-id");
        assert_eq!(response.host_port, 15432);
        assert_eq!(response.tags.len(), 1);
    }
}
