use std::sync::Arc;

use axum::extract::Extension;
use axum::Router;
use clap::Parser;
use tokio::net::TcpListener;
use tower_http::trace::TraceLayer;

use fakecloud_core::delivery::DeliveryBus;
use fakecloud_core::dispatch::{self, DispatchConfig};
use fakecloud_core::registry::ServiceRegistry;

mod lambda_delivery;
mod sqs_lambda_poller;
use sqs_lambda_poller::SqsLambdaPoller;

use fakecloud_cloudformation::service::CloudFormationService;
use fakecloud_cognito::service::CognitoService;
use fakecloud_dynamodb::service::DynamoDbService;
use fakecloud_eventbridge::service::EventBridgeService;
use fakecloud_iam::iam_service::IamService;
use fakecloud_iam::sts_service::StsService;
use fakecloud_kms::service::KmsService;
use fakecloud_lambda::service::LambdaService;
use fakecloud_logs::service::LogsService;
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
        container_runtime: container_runtime.clone(),
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
    registry.register(Arc::new(CognitoService::new(cognito_state.clone())));

    // Spawn background tasks
    let lifecycle_processor = fakecloud_s3::lifecycle::LifecycleProcessor::new(s3_state);
    tokio::spawn(lifecycle_processor.run());

    let mut sqs_lambda_poller = SqsLambdaPoller::new(sqs_state, lambda_state);
    if let Some(ref ld) = lambda_delivery {
        sqs_lambda_poller = sqs_lambda_poller.with_lambda_delivery(ld.clone());
    }
    tokio::spawn(sqs_lambda_poller.run());

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
                    axum::Json(serde_json::json!({
                        "status": "ok",
                        "version": env!("CARGO_PKG_VERSION"),
                        "services": services,
                    }))
                }
            }),
        )
        .route(
            "/_reset",
            axum::routing::post({
                let s = reset_state;
                move || async move { s.reset() }
            }),
        )
        .route(
            "/_fakecloud/lambda/invocations",
            axum::routing::get({
                let ls = lambda_invocations_state.clone();
                move || async move {
                    let state = ls.read();
                    let invocations: Vec<serde_json::Value> = state
                        .invocations
                        .iter()
                        .map(|inv| {
                            serde_json::json!({
                                "functionArn": inv.function_arn,
                                "payload": inv.payload,
                                "source": inv.source,
                                "timestamp": inv.timestamp.to_rfc3339(),
                            })
                        })
                        .collect();
                    axum::Json(serde_json::json!({ "invocations": invocations }))
                }
            }),
        )
        .route(
            "/_fakecloud/ses/emails",
            axum::routing::get({
                let ss = ses_emails_state.clone();
                move || async move {
                    let state = ss.read();
                    let emails: Vec<serde_json::Value> = state
                        .sent_emails
                        .iter()
                        .map(|email| {
                            serde_json::json!({
                                "messageId": email.message_id,
                                "from": email.from,
                                "to": email.to,
                                "cc": email.cc,
                                "bcc": email.bcc,
                                "subject": email.subject,
                                "htmlBody": email.html_body,
                                "textBody": email.text_body,
                                "rawData": email.raw_data,
                                "templateName": email.template_name,
                                "templateData": email.template_data,
                                "timestamp": email.timestamp.to_rfc3339(),
                            })
                        })
                        .collect();
                    axum::Json(serde_json::json!({ "emails": emails }))
                }
            }),
        )
        .route(
            "/_fakecloud/ses/inbound",
            axum::routing::post({
                let ss = ses_inbound_state.clone();
                move |axum::Json(body): axum::Json<serde_json::Value>| async move {
                    let from = body["from"].as_str().unwrap_or("").to_string();
                    let to: Vec<String> = body["to"]
                        .as_array()
                        .map(|a| {
                            a.iter()
                                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                                .collect()
                        })
                        .unwrap_or_default();
                    let subject = body["subject"].as_str().unwrap_or("").to_string();
                    let email_body = body["body"].as_str().unwrap_or("").to_string();

                    let (message_id, matched_rules, actions) =
                        fakecloud_ses::v1::evaluate_inbound_email(
                            &ss, &from, &to, &subject, &email_body,
                        );

                    let actions_executed: Vec<serde_json::Value> = actions
                        .iter()
                        .map(|(rule, action)| {
                            serde_json::json!({
                                "rule": rule,
                                "actionType": match action {
                                    fakecloud_ses::state::ReceiptAction::S3 { .. } => "S3",
                                    fakecloud_ses::state::ReceiptAction::Sns { .. } => "SNS",
                                    fakecloud_ses::state::ReceiptAction::Lambda { .. } => "Lambda",
                                    fakecloud_ses::state::ReceiptAction::Bounce { .. } => "Bounce",
                                    fakecloud_ses::state::ReceiptAction::AddHeader { .. } => "AddHeader",
                                    fakecloud_ses::state::ReceiptAction::Stop { .. } => "Stop",
                                },
                            })
                        })
                        .collect();

                    axum::Json(serde_json::json!({
                        "messageId": message_id,
                        "matchedRules": matched_rules,
                        "actionsExecuted": actions_executed,
                    }))
                }
            }),
        )
        .route(
            "/_fakecloud/sns/messages",
            axum::routing::get({
                let ss = sns_introspection_state;
                move || async move {
                    let state = ss.read();
                    let messages: Vec<serde_json::Value> = state
                        .published
                        .iter()
                        .map(|msg| {
                            serde_json::json!({
                                "messageId": msg.message_id,
                                "topicArn": msg.topic_arn,
                                "message": msg.message,
                                "subject": msg.subject,
                                "timestamp": msg.timestamp.to_rfc3339(),
                            })
                        })
                        .collect();
                    axum::Json(serde_json::json!({ "messages": messages }))
                }
            }),
        )
        .route(
            "/_fakecloud/sqs/messages",
            axum::routing::get({
                let ss = sqs_introspection_state;
                move || async move {
                    let state = ss.read();
                    let queues: Vec<serde_json::Value> = state
                        .queues
                        .values()
                        .map(|queue| {
                            let mut messages: Vec<serde_json::Value> = queue
                                .messages
                                .iter()
                                .map(|msg| {
                                    serde_json::json!({
                                        "messageId": msg.message_id,
                                        "body": msg.body,
                                        "receiveCount": msg.receive_count,
                                        "inFlight": false,
                                        "createdAt": msg.created_at.to_rfc3339(),
                                    })
                                })
                                .collect();
                            let inflight: Vec<serde_json::Value> = queue
                                .inflight
                                .iter()
                                .map(|msg| {
                                    serde_json::json!({
                                        "messageId": msg.message_id,
                                        "body": msg.body,
                                        "receiveCount": msg.receive_count,
                                        "inFlight": true,
                                        "createdAt": msg.created_at.to_rfc3339(),
                                    })
                                })
                                .collect();
                            messages.extend(inflight);
                            serde_json::json!({
                                "queueUrl": queue.queue_url,
                                "queueName": queue.queue_name,
                                "messages": messages,
                            })
                        })
                        .collect();
                    axum::Json(serde_json::json!({ "queues": queues }))
                }
            }),
        )
        .route(
            "/_fakecloud/events/history",
            axum::routing::get({
                let es = eb_introspection_state;
                move || async move {
                    let state = es.read();
                    let events: Vec<serde_json::Value> = state
                        .events
                        .iter()
                        .map(|evt| {
                            serde_json::json!({
                                "eventId": evt.event_id,
                                "source": evt.source,
                                "detailType": evt.detail_type,
                                "detail": evt.detail,
                                "busName": evt.event_bus_name,
                                "timestamp": evt.time.to_rfc3339(),
                            })
                        })
                        .collect();
                    let lambda_deliveries: Vec<serde_json::Value> = state
                        .lambda_invocations
                        .iter()
                        .map(|inv| {
                            serde_json::json!({
                                "functionArn": inv.function_arn,
                                "payload": inv.payload,
                                "timestamp": inv.timestamp.to_rfc3339(),
                            })
                        })
                        .collect();
                    let log_deliveries: Vec<serde_json::Value> = state
                        .log_deliveries
                        .iter()
                        .map(|ld| {
                            serde_json::json!({
                                "logGroupArn": ld.log_group_arn,
                                "payload": ld.payload,
                                "timestamp": ld.timestamp.to_rfc3339(),
                            })
                        })
                        .collect();
                    axum::Json(serde_json::json!({
                        "events": events,
                        "deliveries": {
                            "lambda": lambda_deliveries,
                            "logs": log_deliveries,
                        },
                    }))
                }
            }),
        )
        .route(
            "/_fakecloud/s3/notifications",
            axum::routing::get({
                let ss = s3_introspection_state;
                move || async move {
                    let state = ss.read();
                    let notifications: Vec<serde_json::Value> = state
                        .notification_events
                        .iter()
                        .map(|evt| {
                            serde_json::json!({
                                "bucket": evt.bucket,
                                "key": evt.key,
                                "eventType": evt.event_type,
                                "timestamp": evt.timestamp.to_rfc3339(),
                            })
                        })
                        .collect();
                    axum::Json(serde_json::json!({ "notifications": notifications }))
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
    container_runtime: Option<Arc<fakecloud_lambda::runtime::ContainerRuntime>>,
}

impl ResetState {
    fn reset(&self) -> axum::Json<serde_json::Value> {
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
        tracing::info!("state reset via reset API");
        axum::Json(serde_json::json!({"status": "ok"}))
    }
}

async fn shutdown_signal() {
    tokio::signal::ctrl_c()
        .await
        .expect("failed to install Ctrl+C handler");
    tracing::info!("shutting down");
}
