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
            "/_fakecloud/sqs/expiration-processor/tick",
            axum::routing::post({
                let ss = sqs_sim_expiration_state;
                move || async move {
                    let expired = fakecloud_sqs::simulation::tick_expiration(&ss);
                    axum::Json(serde_json::json!({ "expiredMessages": expired }))
                }
            }),
        )
        .route(
            "/_fakecloud/sqs/{queue_name}/force-dlq",
            axum::routing::post({
                let ss = sqs_sim_force_dlq_state;
                move |axum::extract::Path(queue_name): axum::extract::Path<String>| async move {
                    let moved = fakecloud_sqs::simulation::force_dlq(&ss, &queue_name);
                    axum::Json(serde_json::json!({ "movedMessages": moved }))
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
                move |axum::Json(body): axum::Json<serde_json::Value>| async move {
                    let bus_name = body["busName"].as_str().unwrap_or("default");
                    let rule_name = match body["ruleName"].as_str() {
                        Some(n) => n,
                        None => {
                            return (
                                axum::http::StatusCode::BAD_REQUEST,
                                axum::Json(serde_json::json!({ "error": "ruleName is required" })),
                            );
                        }
                    };

                    match fakecloud_eventbridge::simulation::fire_rule(
                        &es,
                        &delivery,
                        &lambda_state,
                        &logs_state,
                        &container_runtime,
                        bus_name,
                        rule_name,
                    ) {
                        Ok(targets) => {
                            let target_list: Vec<serde_json::Value> = targets
                                .iter()
                                .map(|t| {
                                    serde_json::json!({
                                        "type": t.target_type,
                                        "arn": t.arn,
                                    })
                                })
                                .collect();
                            (
                                axum::http::StatusCode::OK,
                                axum::Json(serde_json::json!({ "targets": target_list })),
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
        .route(
            "/_fakecloud/dynamodb/ttl-processor/tick",
            axum::routing::post({
                let ds = dynamodb_ttl_state;
                move || async move {
                    let count = fakecloud_dynamodb::ttl::process_ttl_expirations(&ds);
                    axum::Json(serde_json::json!({ "expiredItems": count }))
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
                        fakecloud_secretsmanager::rotation::check_and_rotate(&ss, Some(&bus))
                            .await;
                    axum::Json(serde_json::json!({ "rotatedSecrets": rotated }))
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
                        let attr_codes: serde_json::Value = user
                            .map(|u| serde_json::json!(u.attribute_verification_codes))
                            .unwrap_or(serde_json::json!({}));
                        axum::Json(serde_json::json!({
                            "confirmationCode": code,
                            "attributeVerificationCodes": attr_codes
                        }))
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
                                    codes.push(serde_json::json!({
                                        "poolId": pool_id,
                                        "username": username,
                                        "code": code,
                                        "type": "signup"
                                    }));
                                }
                                for (attr, code) in &user.attribute_verification_codes {
                                    codes.push(serde_json::json!({
                                        "poolId": pool_id,
                                        "username": username,
                                        "code": code,
                                        "type": "attribute_verification",
                                        "attribute": attr
                                    }));
                                }
                            }
                        }
                        axum::Json(serde_json::json!({ "codes": codes }))
                    }
                }
            }),
        )
        .route(
            "/_fakecloud/cognito/confirm-user",
            axum::routing::post({
                let cs = cognito_confirm_state;
                move |axum::Json(body): axum::Json<serde_json::Value>| {
                    let cs = cs.clone();
                    async move {
                        let pool_id = body["userPoolId"].as_str().unwrap_or("").to_string();
                        let username = body["username"].as_str().unwrap_or("").to_string();
                        let mut state = cs.write();
                        let user = state
                            .users
                            .get_mut(&pool_id)
                            .and_then(|users| users.get_mut(&username));
                        match user {
                            Some(user) => {
                                user.user_status = "CONFIRMED".to_string();
                                user.confirmation_code = None;
                                user.user_last_modified_date = chrono::Utc::now();
                                (
                                    axum::http::StatusCode::OK,
                                    axum::Json(serde_json::json!({ "confirmed": true })),
                                )
                            }
                            None => (
                                axum::http::StatusCode::NOT_FOUND,
                                axum::Json(serde_json::json!({
                                    "confirmed": false,
                                    "error": "User not found"
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
                            tokens.push(serde_json::json!({
                                "type": "access",
                                "username": data.username,
                                "poolId": data.user_pool_id,
                                "clientId": data.client_id,
                                "issuedAt": data.issued_at.timestamp()
                            }));
                        }
                        for data in state.refresh_tokens.values() {
                            tokens.push(serde_json::json!({
                                "type": "refresh",
                                "username": data.username,
                                "poolId": data.user_pool_id,
                                "clientId": data.client_id,
                                "issuedAt": data.issued_at.timestamp()
                            }));
                        }
                        axum::Json(serde_json::json!({ "tokens": tokens }))
                    }
                }
            }),
        )
        .route(
            "/_fakecloud/cognito/expire-tokens",
            axum::routing::post({
                let cs = cognito_expire_state;
                move |axum::Json(body): axum::Json<serde_json::Value>| {
                    let cs = cs.clone();
                    async move {
                        let pool_id = body["userPoolId"].as_str().map(|s| s.to_string());
                        let username = body["username"].as_str().map(|s| s.to_string());
                        let mut state = cs.write();
                        let mut expired = 0usize;

                        let matches = |p: &str, u: &str| -> bool {
                            pool_id.as_ref().is_none_or(|pid| pid == p)
                                && username.as_ref().is_none_or(|un| un == u)
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

                        axum::Json(serde_json::json!({ "expiredTokens": expired }))
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
                        let events: Vec<serde_json::Value> = state
                            .auth_events
                            .iter()
                            .map(|e| {
                                serde_json::json!({
                                    "eventType": e.event_type,
                                    "username": e.username,
                                    "userPoolId": e.user_pool_id,
                                    "clientId": e.client_id,
                                    "timestamp": e.timestamp.timestamp(),
                                    "success": e.success
                                })
                            })
                            .collect();
                        axum::Json(serde_json::json!({ "events": events }))
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
                    axum::Json(serde_json::json!({
                        "processedBuckets": result.processed_buckets,
                        "expiredObjects": result.expired_objects,
                        "transitionedObjects": result.transitioned_objects,
                    }))
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
                    axum::Json(serde_json::json!({ "containers": containers }))
                }
            }),
        )
        .route(
            "/_fakecloud/lambda/{function_name}/evict-container",
            axum::routing::post({
                let rt = lambda_sim_evict_runtime;
                move |axum::extract::Path(function_name): axum::extract::Path<String>| async move {
                    if let Some(ref rt) = rt {
                        let evicted = rt.evict_container(&function_name).await;
                        axum::Json(serde_json::json!({ "evicted": evicted }))
                    } else {
                        axum::Json(serde_json::json!({ "evicted": false }))
                    }
                }
            }),
        )
        .route(
            "/_fakecloud/sns/pending-confirmations",
            axum::routing::get({
                let ss = sns_sim_pending_state;
                move || async move {
                    let pending = fakecloud_sns::simulation::list_pending_confirmations(&ss);
                    let items: Vec<serde_json::Value> = pending
                        .iter()
                        .map(|p| {
                            serde_json::json!({
                                "subscriptionArn": p.subscription_arn,
                                "topicArn": p.topic_arn,
                                "protocol": p.protocol,
                                "endpoint": p.endpoint,
                                "token": p.token,
                            })
                        })
                        .collect();
                    axum::Json(serde_json::json!({ "pendingConfirmations": items }))
                }
            }),
        )
        .route(
            "/_fakecloud/sns/confirm-subscription",
            axum::routing::post({
                let ss = sns_sim_confirm_state;
                move |axum::Json(body): axum::Json<serde_json::Value>| async move {
                    let sub_arn = body["subscriptionArn"].as_str().unwrap_or("");
                    let confirmed = fakecloud_sns::simulation::confirm_subscription(&ss, sub_arn);
                    axum::Json(serde_json::json!({ "confirmed": confirmed }))
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
                            axum::Json(serde_json::json!({ "reset": service })),
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
            _ => {
                return Err(format!("Unknown service: {service}"));
            }
        }
        tracing::info!(service = %service, "service state reset via per-service reset API");
        Ok(())
    }

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
