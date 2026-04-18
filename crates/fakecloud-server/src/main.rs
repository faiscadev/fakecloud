use std::sync::Arc;

use axum::extract::Extension;
use axum::Router;
use clap::Parser;
use md5::Digest;
use tokio::net::TcpListener;
use tower_http::trace::TraceLayer;

use fakecloud_core::delivery::DeliveryBus;
use fakecloud_core::dispatch::{self, DispatchConfig};
use fakecloud_core::registry::ServiceRegistry;
use fakecloud_sdk::types;

mod cli;
mod dynamodb_streams_lambda_poller;
mod introspection;
mod kinesis_lambda_poller;
mod lambda_delivery;
mod reaper;
mod reset;
mod sqs_lambda_poller;
mod stepfunctions_delivery;
use cli::Cli;
use dynamodb_streams_lambda_poller::DynamoDbStreamsLambdaPoller;
use introspection::{
    elasticache_cluster_response, elasticache_replication_group_response,
    elasticache_serverless_cache_response, rds_instance_response,
};
use kinesis_lambda_poller::KinesisLambdaPoller;
use reset::ResetState;
use sqs_lambda_poller::SqsLambdaPoller;

use fakecloud_apigatewayv2::service::ApiGatewayV2Service;
use fakecloud_bedrock::service::BedrockService;
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
use fakecloud_scheduler::service::SchedulerService;
use fakecloud_secretsmanager::service::SecretsManagerService;
use fakecloud_ses::service::SesV2Service;
use fakecloud_sns::service::SnsService;
use fakecloud_sqs::service::SqsService;
use fakecloud_ssm::service::SsmService;
use fakecloud_stepfunctions::service::StepFunctionsService;

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_new(&cli.log_level)
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_writer(std::io::stderr)
        .init();

    install_panic_hook();

    let persistence_config = match cli.persistence_config() {
        Ok(cfg) => cfg,
        Err(err) => fatal_exit(format_args!("invalid persistence configuration: {err}")),
    };

    if persistence_config.mode == fakecloud_persistence::StorageMode::Persistent {
        if let Some(ref data_path) = persistence_config.data_path {
            if let Err(err) = std::fs::create_dir_all(data_path) {
                fatal_exit(format_args!(
                    "failed to create persistence data directory {}: {err}",
                    data_path.display()
                ));
            }
            if let Err(err) = fakecloud_persistence::version::ensure_version_file(
                data_path,
                env!("CARGO_PKG_VERSION"),
            ) {
                fatal_exit(format_args!(
                    "persistence version file check failed at {}/fakecloud.version.toml: {err}",
                    data_path.display()
                ));
            }
        }
    }

    let endpoint_url = cli.endpoint_url();

    // Shared state
    let iam_state = Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new(
            &cli.account_id,
            &cli.region,
            &endpoint_url,
        ),
    ));
    let sqs_state = Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new(
            &cli.account_id,
            &cli.region,
            &endpoint_url,
        ),
    ));
    let sns_state = Arc::new(parking_lot::RwLock::new({
        let mut mas: fakecloud_core::multi_account::MultiAccountState<
            fakecloud_sns::state::SnsState,
        > = fakecloud_core::multi_account::MultiAccountState::new(
            &cli.account_id,
            &cli.region,
            &endpoint_url,
        );
        mas.default_mut().seed_default_opted_out();
        mas
    }));
    let eb_state = Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new(
            &cli.account_id,
            &cli.region,
            &endpoint_url,
        ),
    ));
    let ssm_state = Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new(
            &cli.account_id,
            &cli.region,
            &endpoint_url,
        ),
    ));
    let dynamodb_state = Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new(
            &cli.account_id,
            &cli.region,
            &endpoint_url,
        ),
    ));
    let lambda_state = Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new(
            &cli.account_id,
            &cli.region,
            &endpoint_url,
        ),
    ));

    // Reap any backing containers left behind by a previous fakecloud process
    // that was killed before it could run its own cleanup (SIGKILL, crash, OOM).
    reaper::reap_stale_containers();

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
        fakecloud_core::multi_account::MultiAccountState::new(
            &cli.account_id,
            &cli.region,
            &endpoint_url,
        ),
    ));
    let s3_state = Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new(
            &cli.account_id,
            &cli.region,
            &endpoint_url,
        ),
    ));
    let logs_state = Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new(
            &cli.account_id,
            &cli.region,
            &endpoint_url,
        ),
    ));
    let kms_state = Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new(
            &cli.account_id,
            &cli.region,
            &endpoint_url,
        ),
    ));
    let cloudformation_state = Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new(
            &cli.account_id,
            &cli.region,
            &endpoint_url,
        ),
    ));
    let ses_state = Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new(
            &cli.account_id,
            &cli.region,
            &endpoint_url,
        ),
    ));
    let cognito_state = Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new(
            &cli.account_id,
            &cli.region,
            &endpoint_url,
        ),
    ));
    let kinesis_state = Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new(
            &cli.account_id,
            &cli.region,
            &endpoint_url,
        ),
    ));
    let rds_state = Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new(
            &cli.account_id,
            &cli.region,
            &endpoint_url,
        ),
    ));
    let elasticache_state = Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new(
            &cli.account_id,
            &cli.region,
            &endpoint_url,
        ),
    ));

    let stepfunctions_state = Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new(
            &cli.account_id,
            &cli.region,
            &endpoint_url,
        ),
    ));

    let apigatewayv2_state = Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new(
            &cli.account_id,
            &cli.region,
            &endpoint_url,
        ),
    ));

    let bedrock_state = Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new(
            &cli.account_id,
            &cli.region,
            &endpoint_url,
        ),
    ));

    let scheduler_state: fakecloud_scheduler::state::SharedSchedulerState = Arc::new(
        parking_lot::RwLock::new(fakecloud_core::multi_account::MultiAccountState::new(
            &cli.account_id,
            &cli.region,
            &endpoint_url,
        )),
    );

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
    let kinesis_delivery_for_eb =
        fakecloud_kinesis::delivery::KinesisDeliveryImpl::new(kinesis_state.clone());

    // Step Functions delivery (EventBridge/Scheduler can start executions)
    let sfn_delivery_for_eb: Arc<dyn fakecloud_core::delivery::StepFunctionsDelivery> = {
        // Build a full delivery bus for the SFN interpreter so task states
        // (SNS Publish, EventBridge PutEvents, etc.) actually deliver.
        let mut sns_fanout_for_sfn = DeliveryBus::new().with_sqs(sqs_delivery.clone());
        if let Some(ref ld) = lambda_delivery {
            sns_fanout_for_sfn = sns_fanout_for_sfn.with_lambda(ld.clone());
        }
        let sns_for_sfn_delivery = Arc::new(fakecloud_sns::delivery::SnsDeliveryImpl::new(
            sns_state.clone(),
            Arc::new(sns_fanout_for_sfn),
        ));
        let eb_for_sfn_delivery = Arc::new(
            fakecloud_eventbridge::delivery::EventBridgeDeliveryImpl::new(
                eb_state.clone(),
                Arc::new(DeliveryBus::new().with_sqs(sqs_delivery.clone())),
            ),
        );
        let mut sfn_interpreter_bus = DeliveryBus::new()
            .with_sqs(sqs_delivery.clone())
            .with_sns(sns_for_sfn_delivery)
            .with_eventbridge(eb_for_sfn_delivery);
        if let Some(ref ld) = lambda_delivery {
            sfn_interpreter_bus = sfn_interpreter_bus.with_lambda(ld.clone());
        }
        Arc::new(stepfunctions_delivery::StepFunctionsDeliveryImpl::new(
            stepfunctions_state.clone(),
            Some(Arc::new(sfn_interpreter_bus)),
            Some(dynamodb_state.clone()),
        ))
    };

    let delivery_for_eb = Arc::new(
        DeliveryBus::new()
            .with_sqs(sqs_delivery.clone())
            .with_sns(sns_delivery.clone())
            .with_kinesis(kinesis_delivery_for_eb)
            .with_stepfunctions(sfn_delivery_for_eb),
    );

    // Step 3: S3 delivery (S3 notifications can push to SQS, SNS, Lambda, and EventBridge)
    let sns_delivery_for_ses = sns_delivery.clone();
    let sns_delivery_for_cf = sns_delivery.clone();
    let sns_delivery_for_scheduler = sns_delivery.clone();
    let sns_delivery_for_scheduler_eb = sns_delivery.clone();
    let sns_delivery_for_scheduler_sfn_eb = sns_delivery.clone();
    let eb_delivery_for_s3 = Arc::new(
        fakecloud_eventbridge::delivery::EventBridgeDeliveryImpl::new(
            eb_state.clone(),
            Arc::new(DeliveryBus::new().with_sqs(sqs_delivery.clone())),
        ),
    );
    let delivery_for_s3 = {
        let mut bus = DeliveryBus::new()
            .with_sqs(sqs_delivery.clone())
            .with_sns(sns_delivery)
            .with_eventbridge(eb_delivery_for_s3);
        if let Some(ref ld) = lambda_delivery {
            bus = bus.with_lambda(ld.clone());
        }
        Arc::new(bus)
    };

    // Step 4: Logs delivery (subscription filters can push to SQS, Lambda, and Kinesis)
    let sqs_delivery_for_ses = sqs_delivery.clone();
    let kinesis_delivery =
        fakecloud_kinesis::delivery::KinesisDeliveryImpl::new(kinesis_state.clone());
    let kinesis_delivery_for_dynamodb =
        fakecloud_kinesis::delivery::KinesisDeliveryImpl::new(kinesis_state.clone());
    let mut delivery_for_logs = DeliveryBus::new()
        .with_sqs(sqs_delivery.clone())
        .with_kinesis(kinesis_delivery);
    if let Some(ref ld) = lambda_delivery {
        delivery_for_logs = delivery_for_logs.with_lambda(ld.clone());
    }
    let delivery_for_logs = Arc::new(delivery_for_logs);

    // Step 4b: DynamoDB delivery (Kinesis streaming destinations)
    let delivery_for_dynamodb =
        Arc::new(DeliveryBus::new().with_kinesis(kinesis_delivery_for_dynamodb));

    // Clone state refs for internal endpoints
    let lambda_invocations_state = lambda_state.clone();
    let ses_emails_state = ses_state.clone();
    let ses_inbound_state = ses_state.clone();
    let sns_introspection_state = sns_state.clone();
    let sqs_introspection_state = sqs_state.clone();
    let eb_introspection_state = eb_state.clone();
    let s3_introspection_state = s3_state.clone();
    let rds_introspection_state = rds_state.clone();
    let elasticache_introspection_state = elasticache_state.clone();
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
        stepfunctions: stepfunctions_state.clone(),
        scheduler: scheduler_state.clone(),
        apigatewayv2: apigatewayv2_state.clone(),
        bedrock: bedrock_state.clone(),
        container_runtime: container_runtime.clone(),
        rds_runtime: rds_runtime.clone(),
        elasticache_runtime: elasticache_runtime.clone(),
    };

    // Step 5: CloudFormation delivery (custom resources can invoke Lambda)
    let delivery_for_cf = {
        let mut bus = DeliveryBus::new().with_sns(sns_delivery_for_cf);
        if let Some(ref ld) = lambda_delivery {
            bus = bus.with_lambda(ld.clone());
        }
        Arc::new(bus)
    };

    // Register services
    let mut registry = ServiceRegistry::new();
    let cloudformation_snapshot_store: Option<Arc<dyn fakecloud_persistence::SnapshotStore>> =
        if persistence_config.mode == fakecloud_persistence::StorageMode::Persistent {
            let data_path = persistence_config
                .data_path
                .as_ref()
                .expect("validated above")
                .clone();
            let path = data_path.join("cloudformation").join("snapshot.json");
            let store = fakecloud_persistence::DiskSnapshotStore::new(path);
            match fakecloud_persistence::SnapshotStore::load(&store) {
                Ok(Some(bytes)) => {
                    match serde_json::from_slice::<
                        fakecloud_cloudformation::state::CloudFormationSnapshot,
                    >(&bytes)
                    {
                        Ok(snapshot) => {
                            if snapshot.schema_version
                                > fakecloud_cloudformation::state::CLOUDFORMATION_SNAPSHOT_SCHEMA_VERSION
                            {
                                fatal_exit(format_args!(
                                    "cloudformation persistence schema mismatch: on-disk={}, expected={}",
                                    snapshot.schema_version,
                                    fakecloud_cloudformation::state::CLOUDFORMATION_SNAPSHOT_SCHEMA_VERSION,
                                ));
                            }
                            if let Some(accounts) = snapshot.accounts {
                                let account_count = accounts.account_count();
                                *cloudformation_state.write() = accounts;
                                tracing::info!(
                                    accounts = account_count,
                                    "loaded cloudformation persistence snapshot (multi-account)"
                                );
                            } else if let Some(single_state) = snapshot.state {
                                let stack_count = single_state.stacks.len();
                                let account_id = single_state.account_id.clone();
                                let mut mas = cloudformation_state.write();
                                *mas.get_or_create(&account_id) = single_state;
                                tracing::info!(
                                    stacks = stack_count,
                                    "loaded cloudformation persistence snapshot (migrated from v1)"
                                );
                            }
                        }
                        Err(err) => fatal_exit(format_args!(
                            "failed to parse cloudformation persistence snapshot: {err}"
                        )),
                    }
                }
                Ok(None) => {
                    tracing::info!("no cloudformation persistence snapshot found; starting empty");
                }
                Err(err) => fatal_exit(format_args!(
                    "failed to read cloudformation persistence snapshot: {err}"
                )),
            }
            Some(Arc::new(store) as Arc<dyn fakecloud_persistence::SnapshotStore>)
        } else {
            None
        };
    let mut cloudformation_service = CloudFormationService::new(
        cloudformation_state.clone(),
        fakecloud_cloudformation::service::CloudFormationDeps {
            sqs: sqs_state.clone(),
            sns: sns_state.clone(),
            ssm: ssm_state.clone(),
            iam: iam_state.clone(),
            s3: s3_state.clone(),
            eventbridge: eb_state.clone(),
            dynamodb: dynamodb_state.clone(),
            logs: logs_state.clone(),
            delivery: delivery_for_cf,
        },
    );
    if let Some(store) = cloudformation_snapshot_store {
        cloudformation_service = cloudformation_service.with_snapshot_store(store);
    }
    registry.register(Arc::new(cloudformation_service));
    let sqs_snapshot_store: Option<Arc<dyn fakecloud_persistence::SnapshotStore>> =
        if persistence_config.mode == fakecloud_persistence::StorageMode::Persistent {
            let data_path = persistence_config
                .data_path
                .as_ref()
                .expect("validated above")
                .clone();
            let path = data_path.join("sqs").join("snapshot.json");
            let store = fakecloud_persistence::DiskSnapshotStore::new(path);
            match fakecloud_persistence::SnapshotStore::load(&store) {
                Ok(Some(bytes)) => {
                    match serde_json::from_slice::<fakecloud_sqs::state::SqsSnapshot>(&bytes) {
                        Ok(snapshot) => {
                            if snapshot.schema_version
                                > fakecloud_sqs::state::SQS_SNAPSHOT_SCHEMA_VERSION
                            {
                                fatal_exit(format_args!(
                                    "sqs persistence schema too new: on-disk={}, max supported={}",
                                    snapshot.schema_version,
                                    fakecloud_sqs::state::SQS_SNAPSHOT_SCHEMA_VERSION,
                                ));
                            }
                            if let Some(accounts) = snapshot.accounts {
                                let account_count = accounts.account_count();
                                *sqs_state.write() = accounts;
                                tracing::info!(
                                    accounts = account_count,
                                    "loaded sqs persistence snapshot (multi-account)"
                                );
                            } else if let Some(single_state) = snapshot.state {
                                let queue_count = single_state.queues.len();
                                let account_id = single_state.account_id.clone();
                                let mut mas = sqs_state.write();
                                *mas.get_or_create(&account_id) = single_state;
                                tracing::info!(
                                    queues = queue_count,
                                    "loaded sqs persistence snapshot (migrated from v1)"
                                );
                            }
                        }
                        Err(err) => fatal_exit(format_args!(
                            "failed to parse sqs persistence snapshot: {err}"
                        )),
                    }
                }
                Ok(None) => {
                    tracing::info!("no sqs persistence snapshot found; starting empty");
                }
                Err(err) => fatal_exit(format_args!(
                    "failed to read sqs persistence snapshot: {err}"
                )),
            }
            Some(Arc::new(store) as Arc<dyn fakecloud_persistence::SnapshotStore>)
        } else {
            None
        };
    let mut sqs_service = SqsService::new(sqs_state.clone());
    if let Some(store) = sqs_snapshot_store {
        sqs_service = sqs_service.with_snapshot_store(store);
    }
    registry.register(Arc::new(sqs_service));
    let sns_state_for_sfn = sns_state.clone();
    let delivery_for_sns_sfn = delivery_for_sns.clone();
    let sns_snapshot_store: Option<Arc<dyn fakecloud_persistence::SnapshotStore>> =
        if persistence_config.mode == fakecloud_persistence::StorageMode::Persistent {
            let data_path = persistence_config
                .data_path
                .as_ref()
                .expect("validated above")
                .clone();
            let path = data_path.join("sns").join("snapshot.json");
            let store = fakecloud_persistence::DiskSnapshotStore::new(path);
            match fakecloud_persistence::SnapshotStore::load(&store) {
                Ok(Some(bytes)) => {
                    match serde_json::from_slice::<fakecloud_sns::state::SnsSnapshot>(&bytes) {
                        Ok(snapshot) => {
                            if snapshot.schema_version
                                > fakecloud_sns::state::SNS_SNAPSHOT_SCHEMA_VERSION
                            {
                                fatal_exit(format_args!(
                                    "sns persistence schema too new: on-disk={}, max supported={}",
                                    snapshot.schema_version,
                                    fakecloud_sns::state::SNS_SNAPSHOT_SCHEMA_VERSION,
                                ));
                            }
                            if let Some(accounts) = snapshot.accounts {
                                let account_count = accounts.account_count();
                                *sns_state.write() = accounts;
                                tracing::info!(
                                    accounts = account_count,
                                    "loaded sns persistence snapshot (multi-account)"
                                );
                            } else if let Some(single_state) = snapshot.state {
                                let topic_count = single_state.topics.len();
                                let account_id = single_state.account_id.clone();
                                let mut mas = sns_state.write();
                                *mas.get_or_create(&account_id) = single_state;
                                tracing::info!(
                                    topics = topic_count,
                                    "loaded sns persistence snapshot (migrated from v1)"
                                );
                            }
                        }
                        Err(err) => fatal_exit(format_args!(
                            "failed to parse sns persistence snapshot: {err}"
                        )),
                    }
                }
                Ok(None) => {
                    tracing::info!("no sns persistence snapshot found; starting empty");
                }
                Err(err) => fatal_exit(format_args!(
                    "failed to read sns persistence snapshot: {err}"
                )),
            }
            Some(Arc::new(store) as Arc<dyn fakecloud_persistence::SnapshotStore>)
        } else {
            None
        };
    let mut sns_service = SnsService::new(sns_state.clone(), delivery_for_sns);
    if let Some(store) = sns_snapshot_store {
        sns_service = sns_service.with_snapshot_store(store);
    }
    registry.register(Arc::new(sns_service));
    let eb_snapshot_store: Option<Arc<dyn fakecloud_persistence::SnapshotStore>> =
        if persistence_config.mode == fakecloud_persistence::StorageMode::Persistent {
            let data_path = persistence_config
                .data_path
                .as_ref()
                .expect("validated above")
                .clone();
            let path = data_path.join("eventbridge").join("snapshot.json");
            let store = fakecloud_persistence::DiskSnapshotStore::new(path);
            match fakecloud_persistence::SnapshotStore::load(&store) {
                Ok(Some(bytes)) => {
                    match serde_json::from_slice::<fakecloud_eventbridge::state::EventBridgeSnapshot>(
                        &bytes,
                    ) {
                        Ok(snapshot) => {
                            if snapshot.schema_version
                                > fakecloud_eventbridge::state::EVENTBRIDGE_SNAPSHOT_SCHEMA_VERSION
                            {
                                fatal_exit(format_args!(
                                    "eventbridge persistence schema too new: on-disk={}, max supported={}",
                                    snapshot.schema_version,
                                    fakecloud_eventbridge::state::EVENTBRIDGE_SNAPSHOT_SCHEMA_VERSION,
                                ));
                            }
                            if let Some(accounts) = snapshot.accounts {
                                let account_count = accounts.account_count();
                                *eb_state.write() = accounts;
                                tracing::info!(
                                    accounts = account_count,
                                    "loaded eventbridge persistence snapshot (multi-account)"
                                );
                            } else if let Some(single_state) = snapshot.state {
                                let bus_count = single_state.buses.len();
                                let account_id = single_state.account_id.clone();
                                let mut mas = eb_state.write();
                                *mas.get_or_create(&account_id) = single_state;
                                tracing::info!(
                                    buses = bus_count,
                                    "loaded eventbridge persistence snapshot (migrated from v1)"
                                );
                            }
                        }
                        Err(err) => fatal_exit(format_args!(
                            "failed to parse eventbridge persistence snapshot: {err}"
                        )),
                    }
                }
                Ok(None) => {
                    tracing::info!("no eventbridge persistence snapshot found; starting empty");
                }
                Err(err) => fatal_exit(format_args!(
                    "failed to read eventbridge persistence snapshot: {err}"
                )),
            }
            Some(Arc::new(store) as Arc<dyn fakecloud_persistence::SnapshotStore>)
        } else {
            None
        };
    let mut eb_service = EventBridgeService::new(eb_state.clone(), delivery_for_eb.clone())
        .with_lambda(lambda_state.clone())
        .with_logs(logs_state.clone());
    if let Some(ref rt) = container_runtime {
        eb_service = eb_service.with_runtime(rt.clone());
    }
    if let Some(store) = eb_snapshot_store {
        eb_service = eb_service.with_snapshot_store(store);
    }
    registry.register(Arc::new(eb_service));

    // Spawn the EventBridge scheduler as a background task
    let eb_state_for_ses = eb_state.clone();
    let eb_state_for_sfn = eb_state.clone();
    let eb_state_for_scheduler = eb_state.clone();
    let mut scheduler = fakecloud_eventbridge::scheduler::Scheduler::new(eb_state, delivery_for_eb)
        .with_lambda(lambda_state.clone())
        .with_logs(logs_state.clone());
    if let Some(ref rt) = container_runtime {
        scheduler = scheduler.with_runtime(rt.clone());
    }
    tokio::spawn(scheduler.run());
    let iam_snapshot_store: Option<Arc<dyn fakecloud_persistence::SnapshotStore>> =
        if persistence_config.mode == fakecloud_persistence::StorageMode::Persistent {
            let data_path = persistence_config
                .data_path
                .as_ref()
                .expect("validated above")
                .clone();
            let path = data_path.join("iam").join("snapshot.json");
            let store = fakecloud_persistence::DiskSnapshotStore::new(path);
            match fakecloud_persistence::SnapshotStore::load(&store) {
                Ok(Some(bytes)) => {
                    match serde_json::from_slice::<fakecloud_iam::state::IamSnapshot>(&bytes) {
                        Ok(snapshot) => {
                            if snapshot.schema_version
                                > fakecloud_iam::state::IAM_SNAPSHOT_SCHEMA_VERSION
                            {
                                fatal_exit(format_args!(
                                    "iam persistence schema too new: on-disk={}, max supported={}",
                                    snapshot.schema_version,
                                    fakecloud_iam::state::IAM_SNAPSHOT_SCHEMA_VERSION,
                                ));
                            }
                            // v2: multi-account state in `accounts` field
                            // v1: single-account state in `state` field, migrated by wrapping
                            if let Some(accounts) = snapshot.accounts {
                                let account_count = accounts.account_count();
                                *iam_state.write() = accounts;
                                tracing::info!(
                                    accounts = account_count,
                                    "loaded iam persistence snapshot (multi-account)",
                                );
                            } else if let Some(single_state) = snapshot.state {
                                let user_count = single_state.users.len();
                                let role_count = single_state.roles.len();
                                let account_id = single_state.account_id.clone();
                                let mut mas = iam_state.write();
                                *mas.get_or_create(&account_id) = single_state;
                                tracing::info!(
                                    users = user_count,
                                    roles = role_count,
                                    "loaded iam persistence snapshot (migrated from v1)",
                                );
                            } else {
                                tracing::warn!(
                                    "iam persistence snapshot has neither accounts nor state field; starting empty"
                                );
                            }
                        }
                        Err(err) => fatal_exit(format_args!(
                            "failed to parse iam persistence snapshot: {err}"
                        )),
                    }
                }
                Ok(None) => {
                    tracing::info!("no iam persistence snapshot found; starting empty");
                }
                Err(err) => fatal_exit(format_args!(
                    "failed to read iam persistence snapshot: {err}"
                )),
            }
            Some(Arc::new(store) as Arc<dyn fakecloud_persistence::SnapshotStore>)
        } else {
            None
        };
    let mut iam_service = IamService::new(iam_state.clone());
    if let Some(ref store) = iam_snapshot_store {
        iam_service = iam_service.with_snapshot_store(store.clone());
    }
    // Share the snapshot lock between IamService and StsService so
    // writes from both services mutually serialize through one lock.
    let iam_snapshot_lock = iam_service.snapshot_lock();
    let mut sts_service = StsService::new(iam_state.clone()).with_snapshot_lock(iam_snapshot_lock);
    if let Some(store) = iam_snapshot_store {
        sts_service = sts_service.with_snapshot_store(store);
    }
    registry.register(Arc::new(iam_service));
    registry.register(Arc::new(sts_service));
    let ssm_snapshot_store: Option<Arc<dyn fakecloud_persistence::SnapshotStore>> =
        if persistence_config.mode == fakecloud_persistence::StorageMode::Persistent {
            let data_path = persistence_config
                .data_path
                .as_ref()
                .expect("validated above")
                .clone();
            let path = data_path.join("ssm").join("snapshot.json");
            let store = fakecloud_persistence::DiskSnapshotStore::new(path);
            match fakecloud_persistence::SnapshotStore::load(&store) {
                Ok(Some(bytes)) => {
                    match serde_json::from_slice::<fakecloud_ssm::state::SsmSnapshot>(&bytes) {
                        Ok(snapshot) => {
                            if snapshot.schema_version
                                > fakecloud_ssm::state::SSM_SNAPSHOT_SCHEMA_VERSION
                            {
                                fatal_exit(format_args!(
                                    "ssm persistence schema mismatch: on-disk={}, expected={}",
                                    snapshot.schema_version,
                                    fakecloud_ssm::state::SSM_SNAPSHOT_SCHEMA_VERSION,
                                ));
                            }
                            if let Some(accounts) = snapshot.accounts {
                                let account_count = accounts.account_count();
                                *ssm_state.write() = accounts;
                                tracing::info!(
                                    accounts = account_count,
                                    "loaded ssm persistence snapshot (multi-account)"
                                );
                            } else if let Some(single_state) = snapshot.state {
                                let param_count = single_state.parameters.len();
                                let account_id = single_state.account_id.clone();
                                let mut mas = ssm_state.write();
                                *mas.get_or_create(&account_id) = single_state;
                                tracing::info!(
                                    parameters = param_count,
                                    "loaded ssm persistence snapshot (migrated from v1)"
                                );
                            }
                        }
                        Err(err) => fatal_exit(format_args!(
                            "failed to parse ssm persistence snapshot: {err}"
                        )),
                    }
                }
                Ok(None) => {
                    tracing::info!("no ssm persistence snapshot found; starting empty");
                }
                Err(err) => fatal_exit(format_args!(
                    "failed to read ssm persistence snapshot: {err}"
                )),
            }
            Some(Arc::new(store) as Arc<dyn fakecloud_persistence::SnapshotStore>)
        } else {
            None
        };
    let mut ssm_service =
        SsmService::new(ssm_state).with_secretsmanager(secretsmanager_state.clone());
    if let Some(store) = ssm_snapshot_store {
        ssm_service = ssm_service.with_snapshot_store(store);
    }
    registry.register(Arc::new(ssm_service));
    // DynamoDB is registered later, after s3_store is constructed, so the
    // export path can persist result objects through the S3 store.
    let dynamodb_state_for_register = dynamodb_state.clone();
    let delivery_for_dynamodb_register = delivery_for_dynamodb;
    let mut lambda_service = LambdaService::new(lambda_state.clone());
    if let Some(ref rt) = container_runtime {
        lambda_service = lambda_service.with_runtime(rt.clone());
    }
    let lambda_snapshot_store: Option<Arc<dyn fakecloud_persistence::SnapshotStore>> =
        if persistence_config.mode == fakecloud_persistence::StorageMode::Persistent {
            let data_path = persistence_config
                .data_path
                .as_ref()
                .expect("validated above")
                .clone();
            let path = data_path.join("lambda").join("snapshot.json");
            let store = fakecloud_persistence::DiskSnapshotStore::new(path);
            match fakecloud_persistence::SnapshotStore::load(&store) {
                Ok(Some(bytes)) => {
                    match serde_json::from_slice::<fakecloud_lambda::state::LambdaSnapshot>(&bytes)
                    {
                        Ok(snapshot) => {
                            if snapshot.schema_version
                                > fakecloud_lambda::state::LAMBDA_SNAPSHOT_SCHEMA_VERSION
                            {
                                fatal_exit(format_args!(
                                    "lambda persistence schema too new: on-disk={}, max supported={}",
                                    snapshot.schema_version,
                                    fakecloud_lambda::state::LAMBDA_SNAPSHOT_SCHEMA_VERSION,
                                ));
                            }
                            if let Some(accounts) = snapshot.accounts {
                                let account_count = accounts.account_count();
                                *lambda_state.write() = accounts;
                                tracing::info!(
                                    accounts = account_count,
                                    "loaded lambda persistence snapshot (multi-account)"
                                );
                            } else if let Some(single_state) = snapshot.state {
                                let fn_count = single_state.functions.len();
                                let account_id = single_state.account_id.clone();
                                let mut mas = lambda_state.write();
                                *mas.get_or_create(&account_id) = single_state;
                                tracing::info!(
                                    functions = fn_count,
                                    "loaded lambda persistence snapshot (migrated from v1)"
                                );
                            } else {
                                tracing::warn!("lambda persistence snapshot has neither accounts nor state; starting empty");
                            }
                        }
                        Err(err) => fatal_exit(format_args!(
                            "failed to parse lambda persistence snapshot: {err}"
                        )),
                    }
                }
                Ok(None) => {
                    tracing::info!("no lambda persistence snapshot found; starting empty");
                }
                Err(err) => fatal_exit(format_args!(
                    "failed to read lambda persistence snapshot: {err}"
                )),
            }
            Some(Arc::new(store) as Arc<dyn fakecloud_persistence::SnapshotStore>)
        } else {
            None
        };
    if let Some(store) = lambda_snapshot_store {
        lambda_service = lambda_service.with_snapshot_store(store);
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
    let secretsmanager_snapshot_store: Option<Arc<dyn fakecloud_persistence::SnapshotStore>> =
        if persistence_config.mode == fakecloud_persistence::StorageMode::Persistent {
            let data_path = persistence_config
                .data_path
                .as_ref()
                .expect("validated above")
                .clone();
            let path = data_path.join("secretsmanager").join("snapshot.json");
            let store = fakecloud_persistence::DiskSnapshotStore::new(path);
            match fakecloud_persistence::SnapshotStore::load(&store) {
                Ok(Some(bytes)) => {
                    match serde_json::from_slice::<
                        fakecloud_secretsmanager::state::SecretsManagerSnapshot,
                    >(&bytes)
                    {
                        Ok(snapshot) => {
                            if snapshot.schema_version
                                > fakecloud_secretsmanager::state::SECRETSMANAGER_SNAPSHOT_SCHEMA_VERSION
                            {
                                fatal_exit(format_args!(
                                    "secretsmanager persistence schema too new: on-disk={}, max supported={}",
                                    snapshot.schema_version,
                                    fakecloud_secretsmanager::state::SECRETSMANAGER_SNAPSHOT_SCHEMA_VERSION,
                                ));
                            }
                            if let Some(accounts) = snapshot.accounts {
                                let account_count = accounts.account_count();
                                *secretsmanager_state.write() = accounts;
                                tracing::info!(
                                    accounts = account_count,
                                    "loaded secretsmanager persistence snapshot (multi-account)"
                                );
                            } else if let Some(single_state) = snapshot.state {
                                let secret_count = single_state.secrets.len();
                                let account_id = single_state.account_id.clone();
                                let mut mas = secretsmanager_state.write();
                                *mas.get_or_create(&account_id) = single_state;
                                tracing::info!(
                                    secrets = secret_count,
                                    "loaded secretsmanager persistence snapshot (migrated from v1)"
                                );
                            }
                        }
                        Err(err) => fatal_exit(format_args!(
                            "failed to parse secretsmanager persistence snapshot: {err}"
                        )),
                    }
                }
                Ok(None) => {
                    tracing::info!("no secretsmanager persistence snapshot found; starting empty");
                }
                Err(err) => fatal_exit(format_args!(
                    "failed to read secretsmanager persistence snapshot: {err}"
                )),
            }
            Some(Arc::new(store) as Arc<dyn fakecloud_persistence::SnapshotStore>)
        } else {
            None
        };
    let mut secretsmanager_service =
        SecretsManagerService::new(secretsmanager_state).with_delivery(delivery_for_secretsmanager);
    if let Some(store) = secretsmanager_snapshot_store {
        secretsmanager_service = secretsmanager_service.with_snapshot_store(store);
    }
    registry.register(Arc::new(secretsmanager_service));
    let logs_snapshot_store: Option<Arc<dyn fakecloud_persistence::SnapshotStore>> =
        if persistence_config.mode == fakecloud_persistence::StorageMode::Persistent {
            let data_path = persistence_config
                .data_path
                .as_ref()
                .expect("validated above")
                .clone();
            let path = data_path.join("logs").join("snapshot.json");
            let store = fakecloud_persistence::DiskSnapshotStore::new(path);
            match fakecloud_persistence::SnapshotStore::load(&store) {
                Ok(Some(bytes)) => {
                    match serde_json::from_slice::<fakecloud_logs::state::LogsSnapshot>(&bytes) {
                        Ok(snapshot) => {
                            if snapshot.schema_version
                                > fakecloud_logs::state::LOGS_SNAPSHOT_SCHEMA_VERSION
                            {
                                fatal_exit(format_args!(
                                    "logs persistence schema too new: on-disk={}, max supported={}",
                                    snapshot.schema_version,
                                    fakecloud_logs::state::LOGS_SNAPSHOT_SCHEMA_VERSION,
                                ));
                            }
                            if let Some(accounts) = snapshot.accounts {
                                let account_count = accounts.account_count();
                                *logs_state.write() = accounts;
                                tracing::info!(
                                    accounts = account_count,
                                    "loaded logs persistence snapshot (multi-account)"
                                );
                            } else if let Some(single_state) = snapshot.state {
                                let group_count = single_state.log_groups.len();
                                let account_id = single_state.account_id.clone();
                                let mut mas = logs_state.write();
                                *mas.get_or_create(&account_id) = single_state;
                                tracing::info!(
                                    log_groups = group_count,
                                    "loaded logs persistence snapshot (migrated from v1)"
                                );
                            } else {
                                tracing::warn!("logs persistence snapshot has neither accounts nor state; starting empty");
                            }
                        }
                        Err(err) => fatal_exit(format_args!(
                            "failed to parse logs persistence snapshot: {err}"
                        )),
                    }
                }
                Ok(None) => {
                    tracing::info!("no logs persistence snapshot found; starting empty");
                }
                Err(err) => fatal_exit(format_args!(
                    "failed to read logs persistence snapshot: {err}"
                )),
            }
            Some(Arc::new(store) as Arc<dyn fakecloud_persistence::SnapshotStore>)
        } else {
            None
        };
    let mut logs_service = LogsService::new(logs_state, delivery_for_logs);
    if let Some(store) = logs_snapshot_store {
        logs_service = logs_service.with_snapshot_store(store);
    }
    registry.register(Arc::new(logs_service));
    let kms_snapshot_store: Option<Arc<dyn fakecloud_persistence::SnapshotStore>> =
        if persistence_config.mode == fakecloud_persistence::StorageMode::Persistent {
            let data_path = persistence_config
                .data_path
                .as_ref()
                .expect("validated above")
                .clone();
            let path = data_path.join("kms").join("snapshot.json");
            let store = fakecloud_persistence::DiskSnapshotStore::new(path);
            match fakecloud_persistence::SnapshotStore::load(&store) {
                Ok(Some(bytes)) => {
                    match serde_json::from_slice::<fakecloud_kms::state::KmsSnapshot>(&bytes) {
                        Ok(snapshot) => {
                            if snapshot.schema_version
                                > fakecloud_kms::state::KMS_SNAPSHOT_SCHEMA_VERSION
                            {
                                fatal_exit(format_args!(
                                    "kms persistence schema too new: on-disk={}, max supported={}",
                                    snapshot.schema_version,
                                    fakecloud_kms::state::KMS_SNAPSHOT_SCHEMA_VERSION,
                                ));
                            }
                            if let Some(accounts) = snapshot.accounts {
                                let account_count = accounts.account_count();
                                *kms_state.write() = accounts;
                                tracing::info!(
                                    accounts = account_count,
                                    "loaded kms persistence snapshot (multi-account)"
                                );
                            } else if let Some(single_state) = snapshot.state {
                                let key_count = single_state.keys.len();
                                let account_id = single_state.account_id.clone();
                                let mut mas = kms_state.write();
                                *mas.get_or_create(&account_id) = single_state;
                                tracing::info!(
                                    keys = key_count,
                                    "loaded kms persistence snapshot (migrated from v1)"
                                );
                            }
                        }
                        Err(err) => fatal_exit(format_args!(
                            "failed to parse kms persistence snapshot: {err}"
                        )),
                    }
                }
                Ok(None) => {
                    tracing::info!("no kms persistence snapshot found; starting empty");
                }
                Err(err) => fatal_exit(format_args!(
                    "failed to read kms persistence snapshot: {err}"
                )),
            }
            Some(Arc::new(store) as Arc<dyn fakecloud_persistence::SnapshotStore>)
        } else {
            None
        };
    let mut kms_service = KmsService::new(kms_state.clone());
    if let Some(store) = kms_snapshot_store {
        kms_service = kms_service.with_snapshot_store(store);
    }
    registry.register(Arc::new(kms_service));
    let mut shared_body_cache: Option<Arc<fakecloud_persistence::cache::BodyCache>> = None;
    let s3_store: Arc<dyn fakecloud_persistence::S3Store> = match persistence_config.mode {
        fakecloud_persistence::StorageMode::Persistent => {
            let data_path = persistence_config
                .data_path
                .as_ref()
                .expect("validated above")
                .clone();
            let s3_root = data_path.join("s3");
            if let Err(err) = std::fs::create_dir_all(&s3_root) {
                fatal_exit(format_args!(
                    "failed to create s3 persistence dir {}: {err}",
                    s3_root.display()
                ));
            }
            let cache = Arc::new(fakecloud_persistence::cache::BodyCache::new(
                persistence_config.s3_cache_bytes,
            ));
            shared_body_cache = Some(cache.clone());
            let disk = fakecloud_persistence::s3::DiskS3Store::new(s3_root, cache);
            match <fakecloud_persistence::s3::DiskS3Store as fakecloud_persistence::S3Store>::load(
                &disk,
            ) {
                Ok(snapshot) => {
                    let bucket_count = snapshot.buckets.len();
                    let object_count: usize =
                        snapshot.buckets.values().map(|b| b.objects.len()).sum();
                    let hydrated = match fakecloud_s3::persistence::hydrate_s3_state(
                        snapshot,
                        &cli.account_id,
                        &cli.region,
                    ) {
                        Ok(h) => h,
                        Err(err) => fatal_exit(format_args!(
                            "failed to hydrate s3 persistence snapshot: {err}"
                        )),
                    };
                    {
                        let account_id = hydrated.account_id.clone();
                        let mut mas = s3_state.write();
                        *mas.get_or_create(&account_id) = hydrated;
                    }
                    tracing::info!(
                        buckets = bucket_count,
                        objects = object_count,
                        "loaded s3 persistence snapshot",
                    );
                }
                Err(err) => fatal_exit(format_args!(
                    "failed to load s3 persistence snapshot: {err}"
                )),
            }
            Arc::new(disk)
        }
        fakecloud_persistence::StorageMode::Memory => {
            Arc::new(fakecloud_persistence::s3::MemoryS3Store::new())
        }
    };
    let s3_store_for_inbound = s3_store.clone();
    if let Some(ref cache) = shared_body_cache {
        // Share the cache between the S3Store and S3State so read_body honors
        // the persistent LRU on every read site, not just open_object_body.
        s3_state.write().default_mut().set_body_cache(cache.clone());
    }
    registry.register(Arc::new(
        S3Service::with_store(s3_state.clone(), delivery_for_s3, s3_store.clone())
            .with_kms(kms_state.clone()),
    ));
    // Snapshot store is only wired in persistent mode. In memory mode we
    // leave it unset so the service doesn't pay the per-mutation
    // serialization cost for a store that would just drop the bytes.
    let dynamodb_snapshot_store: Option<Arc<dyn fakecloud_persistence::SnapshotStore>> =
        if persistence_config.mode == fakecloud_persistence::StorageMode::Persistent {
            let data_path = persistence_config
                .data_path
                .as_ref()
                .expect("validated above")
                .clone();
            let path = data_path.join("dynamodb").join("snapshot.json");
            let store = fakecloud_persistence::DiskSnapshotStore::new(path);
            match fakecloud_persistence::SnapshotStore::load(&store) {
                Ok(Some(bytes)) => {
                    match serde_json::from_slice::<fakecloud_dynamodb::state::DynamoDbSnapshot>(
                        &bytes,
                    ) {
                        Ok(snapshot) => {
                            if snapshot.schema_version
                                > fakecloud_dynamodb::state::DYNAMODB_SNAPSHOT_SCHEMA_VERSION
                            {
                                fatal_exit(format_args!(
                                    "dynamodb persistence schema too new: on-disk={}, max supported={}",
                                    snapshot.schema_version,
                                    fakecloud_dynamodb::state::DYNAMODB_SNAPSHOT_SCHEMA_VERSION,
                                ));
                            }
                            if let Some(accounts) = snapshot.accounts {
                                let account_count = accounts.account_count();
                                *dynamodb_state_for_register.write() = accounts;
                                tracing::info!(
                                    accounts = account_count,
                                    "loaded dynamodb persistence snapshot (multi-account)",
                                );
                            } else if let Some(single_state) = snapshot.state {
                                let table_count = single_state.tables.len();
                                let account_id = single_state.account_id.clone();
                                let mut mas = dynamodb_state_for_register.write();
                                *mas.get_or_create(&account_id) = single_state;
                                tracing::info!(
                                    tables = table_count,
                                    "loaded dynamodb persistence snapshot (migrated from v1)",
                                );
                            }
                        }
                        Err(err) => fatal_exit(format_args!(
                            "failed to parse dynamodb persistence snapshot: {err}"
                        )),
                    }
                }
                Ok(None) => {
                    tracing::info!("no dynamodb persistence snapshot found; starting empty");
                }
                Err(err) => fatal_exit(format_args!(
                    "failed to read dynamodb persistence snapshot: {err}"
                )),
            }
            Some(Arc::new(store) as Arc<dyn fakecloud_persistence::SnapshotStore>)
        } else {
            None
        };
    let mut dynamodb_service = DynamoDbService::new(dynamodb_state_for_register)
        .with_s3(s3_state.clone())
        .with_s3_store(s3_store.clone())
        .with_delivery(delivery_for_dynamodb_register);
    if let Some(store) = dynamodb_snapshot_store {
        dynamodb_service = dynamodb_service.with_snapshot_store(store);
    }
    registry.register(Arc::new(dynamodb_service));
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
    let ses_snapshot_store: Option<Arc<dyn fakecloud_persistence::SnapshotStore>> =
        if persistence_config.mode == fakecloud_persistence::StorageMode::Persistent {
            let data_path = persistence_config
                .data_path
                .as_ref()
                .expect("validated above")
                .clone();
            let path = data_path.join("ses").join("snapshot.json");
            let store = fakecloud_persistence::DiskSnapshotStore::new(path);
            match fakecloud_persistence::SnapshotStore::load(&store) {
                Ok(Some(bytes)) => {
                    match serde_json::from_slice::<fakecloud_ses::state::SesSnapshot>(&bytes) {
                        Ok(snapshot) => {
                            if snapshot.schema_version
                                > fakecloud_ses::state::SES_SNAPSHOT_SCHEMA_VERSION
                            {
                                fatal_exit(format_args!(
                                    "ses persistence schema too new: on-disk={}, max supported={}",
                                    snapshot.schema_version,
                                    fakecloud_ses::state::SES_SNAPSHOT_SCHEMA_VERSION,
                                ));
                            }
                            if let Some(accounts) = snapshot.accounts {
                                let account_count = accounts.account_count();
                                *ses_state.write() = accounts;
                                tracing::info!(
                                    accounts = account_count,
                                    "loaded ses persistence snapshot (multi-account)",
                                );
                            } else if let Some(single_state) = snapshot.state {
                                let identity_count = single_state.identities.len();
                                let account_id = single_state.account_id.clone();
                                let mut mas = ses_state.write();
                                *mas.get_or_create(&account_id) = single_state;
                                tracing::info!(
                                    identities = identity_count,
                                    "loaded ses persistence snapshot (migrated from v1)",
                                );
                            }
                        }
                        Err(err) => fatal_exit(format_args!(
                            "failed to parse ses persistence snapshot: {err}"
                        )),
                    }
                }
                Ok(None) => {
                    tracing::info!("no ses persistence snapshot found; starting empty");
                }
                Err(err) => fatal_exit(format_args!(
                    "failed to read ses persistence snapshot: {err}"
                )),
            }
            Some(Arc::new(store) as Arc<dyn fakecloud_persistence::SnapshotStore>)
        } else {
            None
        };
    let mut ses_service = SesV2Service::new(ses_state).with_delivery(ses_delivery_ctx);
    if let Some(store) = ses_snapshot_store {
        ses_service = ses_service.with_snapshot_store(store);
    }
    registry.register(Arc::new(ses_service));
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
    let cognito_snapshot_store: Option<Arc<dyn fakecloud_persistence::SnapshotStore>> =
        if persistence_config.mode == fakecloud_persistence::StorageMode::Persistent {
            let data_path = persistence_config
                .data_path
                .as_ref()
                .expect("validated above")
                .clone();
            let path = data_path.join("cognito-idp").join("snapshot.json");
            let store = fakecloud_persistence::DiskSnapshotStore::new(path);
            match fakecloud_persistence::SnapshotStore::load(&store) {
                Ok(Some(bytes)) => {
                    match serde_json::from_slice::<fakecloud_cognito::state::CognitoSnapshot>(
                        &bytes,
                    ) {
                        Ok(snapshot) => {
                            if snapshot.schema_version
                                > fakecloud_cognito::state::COGNITO_SNAPSHOT_SCHEMA_VERSION
                            {
                                fatal_exit(format_args!(
                                    "cognito persistence schema too new: on-disk={}, max supported={}",
                                    snapshot.schema_version,
                                    fakecloud_cognito::state::COGNITO_SNAPSHOT_SCHEMA_VERSION,
                                ));
                            }
                            if let Some(accounts) = snapshot.accounts {
                                let account_count = accounts.account_count();
                                *cognito_state.write() = accounts;
                                tracing::info!(
                                    accounts = account_count,
                                    "loaded cognito persistence snapshot (multi-account)",
                                );
                            } else if let Some(single_state) = snapshot.state {
                                let pool_count = single_state.user_pools.len();
                                let account_id = single_state.account_id.clone();
                                let mut mas = cognito_state.write();
                                *mas.get_or_create(&account_id) = single_state;
                                tracing::info!(
                                    user_pools = pool_count,
                                    "loaded cognito persistence snapshot (migrated from v1)",
                                );
                            }
                        }
                        Err(err) => fatal_exit(format_args!(
                            "failed to parse cognito persistence snapshot: {err}"
                        )),
                    }
                }
                Ok(None) => {
                    tracing::info!("no cognito persistence snapshot found; starting empty");
                }
                Err(err) => fatal_exit(format_args!(
                    "failed to read cognito persistence snapshot: {err}"
                )),
            }
            Some(Arc::new(store) as Arc<dyn fakecloud_persistence::SnapshotStore>)
        } else {
            None
        };
    let mut cognito_service =
        CognitoService::new(cognito_state.clone()).with_delivery(cognito_delivery_ctx);
    if let Some(store) = cognito_snapshot_store {
        cognito_service = cognito_service.with_snapshot_store(store);
    }
    registry.register(Arc::new(cognito_service));
    let kinesis_snapshot_store: Option<Arc<dyn fakecloud_persistence::SnapshotStore>> =
        if persistence_config.mode == fakecloud_persistence::StorageMode::Persistent {
            let data_path = persistence_config
                .data_path
                .as_ref()
                .expect("validated above")
                .clone();
            let path = data_path.join("kinesis").join("snapshot.json");
            let store = fakecloud_persistence::DiskSnapshotStore::new(path);
            match fakecloud_persistence::SnapshotStore::load(&store) {
                Ok(Some(bytes)) => {
                    match serde_json::from_slice::<fakecloud_kinesis::state::KinesisSnapshot>(
                        &bytes,
                    ) {
                        Ok(snapshot) => {
                            if snapshot.schema_version
                                > fakecloud_kinesis::state::KINESIS_SNAPSHOT_SCHEMA_VERSION
                            {
                                fatal_exit(format_args!(
                                    "kinesis persistence schema too new: on-disk={}, max supported={}",
                                    snapshot.schema_version,
                                    fakecloud_kinesis::state::KINESIS_SNAPSHOT_SCHEMA_VERSION,
                                ));
                            }
                            if let Some(accounts) = snapshot.accounts {
                                let account_count = accounts.account_count();
                                *kinesis_state.write() = accounts;
                                tracing::info!(
                                    accounts = account_count,
                                    "loaded kinesis persistence snapshot (multi-account)"
                                );
                            } else if let Some(single_state) = snapshot.state {
                                let stream_count = single_state.streams.len();
                                let account_id = single_state.account_id.clone();
                                let mut mas = kinesis_state.write();
                                *mas.get_or_create(&account_id) = single_state;
                                tracing::info!(
                                    streams = stream_count,
                                    "loaded kinesis persistence snapshot (migrated from v1)"
                                );
                            }
                        }
                        Err(err) => fatal_exit(format_args!(
                            "failed to parse kinesis persistence snapshot: {err}"
                        )),
                    }
                }
                Ok(None) => {
                    tracing::info!("no kinesis persistence snapshot found; starting empty");
                }
                Err(err) => fatal_exit(format_args!(
                    "failed to read kinesis persistence snapshot: {err}"
                )),
            }
            Some(Arc::new(store) as Arc<dyn fakecloud_persistence::SnapshotStore>)
        } else {
            None
        };
    let mut kinesis_service = KinesisService::new(kinesis_state.clone());
    if let Some(store) = kinesis_snapshot_store {
        kinesis_service = kinesis_service.with_snapshot_store(store);
    }
    registry.register(Arc::new(kinesis_service));
    let rds_snapshot_store: Option<Arc<dyn fakecloud_persistence::SnapshotStore>> =
        if persistence_config.mode == fakecloud_persistence::StorageMode::Persistent {
            let data_path = persistence_config
                .data_path
                .as_ref()
                .expect("validated above")
                .clone();
            let path = data_path.join("rds").join("snapshot.json");
            let store = fakecloud_persistence::DiskSnapshotStore::new(path);
            match fakecloud_persistence::SnapshotStore::load(&store) {
                Ok(Some(bytes)) => {
                    match serde_json::from_slice::<fakecloud_rds::state::RdsSnapshot>(&bytes) {
                        Ok(snapshot) => {
                            if snapshot.schema_version
                                > fakecloud_rds::state::RDS_SNAPSHOT_SCHEMA_VERSION
                            {
                                fatal_exit(format_args!(
                                    "rds persistence schema too new: on-disk={}, max supported={}",
                                    snapshot.schema_version,
                                    fakecloud_rds::state::RDS_SNAPSHOT_SCHEMA_VERSION,
                                ));
                            }
                            if let Some(accounts) = snapshot.accounts {
                                let account_count = accounts.account_count();
                                *rds_state.write() = accounts;
                                tracing::info!(
                                    accounts = account_count,
                                    "loaded rds persistence snapshot (multi-account)",
                                );
                            } else if let Some(single_state) = snapshot.state {
                                let instance_count = single_state.instances.len();
                                let account_id = single_state.account_id.clone();
                                let mut mas = rds_state.write();
                                *mas.get_or_create(&account_id) = single_state;
                                tracing::info!(
                                    instances = instance_count,
                                    "loaded rds persistence snapshot (migrated from v1)",
                                );
                            }
                        }
                        Err(err) => fatal_exit(format_args!(
                            "failed to parse rds persistence snapshot: {err}"
                        )),
                    }
                }
                Ok(None) => {
                    tracing::info!("no rds persistence snapshot found; starting empty");
                }
                Err(err) => fatal_exit(format_args!(
                    "failed to read rds persistence snapshot: {err}"
                )),
            }
            Some(Arc::new(store) as Arc<dyn fakecloud_persistence::SnapshotStore>)
        } else {
            None
        };
    let mut rds_service = RdsService::new(rds_state);
    if let Some(ref rt) = rds_runtime {
        rds_service = rds_service.with_runtime(rt.clone());
    }
    if let Some(store) = rds_snapshot_store {
        rds_service = rds_service.with_snapshot_store(store);
    }
    registry.register(Arc::new(rds_service));
    let elasticache_snapshot_store: Option<Arc<dyn fakecloud_persistence::SnapshotStore>> =
        if persistence_config.mode == fakecloud_persistence::StorageMode::Persistent {
            let data_path = persistence_config
                .data_path
                .as_ref()
                .expect("validated above")
                .clone();
            let path = data_path.join("elasticache").join("snapshot.json");
            let store = fakecloud_persistence::DiskSnapshotStore::new(path);
            match fakecloud_persistence::SnapshotStore::load(&store) {
                Ok(Some(bytes)) => {
                    match serde_json::from_slice::<fakecloud_elasticache::state::ElastiCacheSnapshot>(
                        &bytes,
                    ) {
                        Ok(snapshot) => {
                            if snapshot.schema_version
                                > fakecloud_elasticache::state::ELASTICACHE_SNAPSHOT_SCHEMA_VERSION
                            {
                                fatal_exit(format_args!(
                                    "elasticache persistence schema too new: on-disk={}, max supported={}",
                                    snapshot.schema_version,
                                    fakecloud_elasticache::state::ELASTICACHE_SNAPSHOT_SCHEMA_VERSION,
                                ));
                            }
                            if let Some(accounts) = snapshot.accounts {
                                let account_count = accounts.account_count();
                                *elasticache_state.write() = accounts;
                                tracing::info!(
                                    accounts = account_count,
                                    "loaded elasticache persistence snapshot (multi-account)",
                                );
                            } else if let Some(single_state) = snapshot.state {
                                let cluster_count = single_state.cache_clusters.len();
                                let account_id = single_state.account_id.clone();
                                let mut mas = elasticache_state.write();
                                *mas.get_or_create(&account_id) = single_state;
                                tracing::info!(
                                    clusters = cluster_count,
                                    "loaded elasticache persistence snapshot (migrated from v1)",
                                );
                            }
                        }
                        Err(err) => fatal_exit(format_args!(
                            "failed to parse elasticache persistence snapshot: {err}"
                        )),
                    }
                }
                Ok(None) => {
                    tracing::info!("no elasticache persistence snapshot found; starting empty");
                }
                Err(err) => fatal_exit(format_args!(
                    "failed to read elasticache persistence snapshot: {err}"
                )),
            }
            Some(Arc::new(store) as Arc<dyn fakecloud_persistence::SnapshotStore>)
        } else {
            None
        };
    let mut elasticache_service = ElastiCacheService::new(elasticache_state);
    if let Some(ref rt) = elasticache_runtime {
        elasticache_service = elasticache_service.with_runtime(rt.clone());
    }
    if let Some(store) = elasticache_snapshot_store {
        elasticache_service = elasticache_service.with_snapshot_store(store);
    }
    registry.register(Arc::new(elasticache_service));
    let mut sfn_service = StepFunctionsService::new(stepfunctions_state.clone());
    let sfn_delivery_bus = {
        let mut sns_eb_bus = DeliveryBus::new().with_sqs(sqs_delivery.clone());
        if let Some(ref ld) = lambda_delivery {
            sns_eb_bus = sns_eb_bus.with_lambda(ld.clone());
        }
        let sns_delivery_for_sfn_eb = Arc::new(fakecloud_sns::delivery::SnsDeliveryImpl::new(
            sns_state_for_sfn.clone(),
            Arc::new(sns_eb_bus),
        ));
        let mut eb_target_bus = DeliveryBus::new()
            .with_sqs(sqs_delivery.clone())
            .with_sns(sns_delivery_for_sfn_eb);
        if let Some(ref ld) = lambda_delivery {
            eb_target_bus = eb_target_bus.with_lambda(ld.clone());
        }
        let eb_delivery_for_sfn = Arc::new(
            fakecloud_eventbridge::delivery::EventBridgeDeliveryImpl::new(
                eb_state_for_sfn,
                Arc::new(eb_target_bus),
            ),
        );
        let sns_delivery_for_sfn = Arc::new(fakecloud_sns::delivery::SnsDeliveryImpl::new(
            sns_state_for_sfn,
            delivery_for_sns_sfn,
        ));
        let mut bus = DeliveryBus::new()
            .with_sqs(sqs_delivery.clone())
            .with_sns(sns_delivery_for_sfn)
            .with_eventbridge(eb_delivery_for_sfn);
        if let Some(ref ld) = lambda_delivery {
            bus = bus.with_lambda(ld.clone());
        }
        Arc::new(bus)
    };
    sfn_service = sfn_service
        .with_delivery(sfn_delivery_bus.clone())
        .with_dynamodb(dynamodb_state.clone());
    let sfn_snapshot_store: Option<Arc<dyn fakecloud_persistence::SnapshotStore>> =
        if persistence_config.mode == fakecloud_persistence::StorageMode::Persistent {
            let data_path = persistence_config
                .data_path
                .as_ref()
                .expect("validated above")
                .clone();
            let path = data_path.join("stepfunctions").join("snapshot.json");
            let store = fakecloud_persistence::DiskSnapshotStore::new(path);
            match fakecloud_persistence::SnapshotStore::load(&store) {
                Ok(Some(bytes)) => {
                    match serde_json::from_slice::<
                        fakecloud_stepfunctions::state::StepFunctionsSnapshot,
                    >(&bytes)
                    {
                        Ok(snapshot) => {
                            if snapshot.schema_version
                                > fakecloud_stepfunctions::state::STEPFUNCTIONS_SNAPSHOT_SCHEMA_VERSION
                            {
                                fatal_exit(format_args!(
                                    "stepfunctions persistence schema too new: on-disk={}, max supported={}",
                                    snapshot.schema_version,
                                    fakecloud_stepfunctions::state::STEPFUNCTIONS_SNAPSHOT_SCHEMA_VERSION,
                                ));
                            }
                            if let Some(accounts) = snapshot.accounts {
                                let account_count = accounts.account_count();
                                *stepfunctions_state.write() = accounts;
                                tracing::info!(
                                    accounts = account_count,
                                    "loaded stepfunctions persistence snapshot (multi-account)",
                                );
                            } else if let Some(single_state) = snapshot.state {
                                let sm_count = single_state.state_machines.len();
                                let account_id = single_state.account_id.clone();
                                let mut mas = stepfunctions_state.write();
                                *mas.get_or_create(&account_id) = single_state;
                                tracing::info!(
                                    state_machines = sm_count,
                                    "loaded stepfunctions persistence snapshot (migrated from v1)",
                                );
                            }
                        }
                        Err(err) => fatal_exit(format_args!(
                            "failed to parse stepfunctions persistence snapshot: {err}"
                        )),
                    }
                }
                Ok(None) => {
                    tracing::info!("no stepfunctions persistence snapshot found; starting empty");
                }
                Err(err) => fatal_exit(format_args!(
                    "failed to read stepfunctions persistence snapshot: {err}"
                )),
            }
            Some(Arc::new(store) as Arc<dyn fakecloud_persistence::SnapshotStore>)
        } else {
            None
        };
    if let Some(store) = sfn_snapshot_store {
        sfn_service = sfn_service.with_snapshot_store(store);
    }
    registry.register(Arc::new(sfn_service));

    let apigw_snapshot_store: Option<Arc<dyn fakecloud_persistence::SnapshotStore>> =
        if persistence_config.mode == fakecloud_persistence::StorageMode::Persistent {
            let data_path = persistence_config
                .data_path
                .as_ref()
                .expect("validated above")
                .clone();
            let path = data_path.join("apigatewayv2").join("snapshot.json");
            let store = fakecloud_persistence::DiskSnapshotStore::new(path);
            match fakecloud_persistence::SnapshotStore::load(&store) {
                Ok(Some(bytes)) => {
                    match serde_json::from_slice::<
                        fakecloud_apigatewayv2::state::ApiGatewayV2Snapshot,
                    >(&bytes)
                    {
                        Ok(snapshot) => {
                            if snapshot.schema_version
                                > fakecloud_apigatewayv2::state::APIGATEWAYV2_SNAPSHOT_SCHEMA_VERSION
                            {
                                fatal_exit(format_args!(
                                    "apigatewayv2 persistence schema too new: on-disk={}, max supported={}",
                                    snapshot.schema_version,
                                    fakecloud_apigatewayv2::state::APIGATEWAYV2_SNAPSHOT_SCHEMA_VERSION,
                                ));
                            }
                            if let Some(accounts) = snapshot.accounts {
                                let account_count = accounts.account_count();
                                *apigatewayv2_state.write() = accounts;
                                tracing::info!(
                                    accounts = account_count,
                                    "loaded apigatewayv2 persistence snapshot (multi-account)",
                                );
                            } else if let Some(single_state) = snapshot.state {
                                let api_count = single_state.apis.len();
                                let account_id = single_state.account_id.clone();
                                let mut mas = apigatewayv2_state.write();
                                *mas.get_or_create(&account_id) = single_state;
                                tracing::info!(
                                    apis = api_count,
                                    "loaded apigatewayv2 persistence snapshot (migrated from v1)",
                                );
                            }
                        }
                        Err(err) => fatal_exit(format_args!(
                            "failed to parse apigatewayv2 persistence snapshot: {err}"
                        )),
                    }
                }
                Ok(None) => {
                    tracing::info!("no apigatewayv2 persistence snapshot found; starting empty");
                }
                Err(err) => fatal_exit(format_args!(
                    "failed to read apigatewayv2 persistence snapshot: {err}"
                )),
            }
            Some(Arc::new(store) as Arc<dyn fakecloud_persistence::SnapshotStore>)
        } else {
            None
        };
    let mut apigw_service = ApiGatewayV2Service::new(apigatewayv2_state.clone());
    if let Some(ref ld) = lambda_delivery {
        let delivery_for_apigw = Arc::new(DeliveryBus::new().with_lambda(ld.clone()));
        apigw_service = apigw_service.with_delivery(delivery_for_apigw);
    }
    if let Some(store) = apigw_snapshot_store {
        apigw_service = apigw_service.with_snapshot_store(store);
    }
    registry.register(Arc::new(apigw_service));
    let bedrock_snapshot_store: Option<Arc<dyn fakecloud_persistence::SnapshotStore>> =
        if persistence_config.mode == fakecloud_persistence::StorageMode::Persistent {
            let data_path = persistence_config
                .data_path
                .as_ref()
                .expect("validated above")
                .clone();
            let path = data_path.join("bedrock").join("snapshot.json");
            let store = fakecloud_persistence::DiskSnapshotStore::new(path);
            match fakecloud_persistence::SnapshotStore::load(&store) {
                Ok(Some(bytes)) => {
                    match serde_json::from_slice::<fakecloud_bedrock::state::BedrockSnapshot>(
                        &bytes,
                    ) {
                        Ok(snapshot) => {
                            if snapshot.schema_version
                                > fakecloud_bedrock::state::BEDROCK_SNAPSHOT_SCHEMA_VERSION
                            {
                                fatal_exit(format_args!(
                                    "bedrock persistence schema too new: on-disk={}, max supported={}",
                                    snapshot.schema_version,
                                    fakecloud_bedrock::state::BEDROCK_SNAPSHOT_SCHEMA_VERSION,
                                ));
                            }
                            if let Some(accounts) = snapshot.accounts {
                                let account_count = accounts.account_count();
                                *bedrock_state.write() = accounts;
                                tracing::info!(
                                    accounts = account_count,
                                    "loaded bedrock persistence snapshot (multi-account)"
                                );
                            } else if let Some(single_state) = snapshot.state {
                                let guardrail_count = single_state.guardrails.len();
                                let account_id = single_state.account_id.clone();
                                let mut mas = bedrock_state.write();
                                *mas.get_or_create(&account_id) = single_state;
                                tracing::info!(
                                    guardrails = guardrail_count,
                                    "loaded bedrock persistence snapshot (migrated from v1)"
                                );
                            }
                        }
                        Err(err) => fatal_exit(format_args!(
                            "failed to parse bedrock persistence snapshot: {err}"
                        )),
                    }
                }
                Ok(None) => {
                    tracing::info!("no bedrock persistence snapshot found; starting empty");
                }
                Err(err) => fatal_exit(format_args!(
                    "failed to read bedrock persistence snapshot: {err}"
                )),
            }
            Some(Arc::new(store) as Arc<dyn fakecloud_persistence::SnapshotStore>)
        } else {
            None
        };
    let mut bedrock_service = BedrockService::new(bedrock_state.clone());
    if let Some(store) = bedrock_snapshot_store {
        bedrock_service = bedrock_service.with_snapshot_store(store);
    }
    registry.register(Arc::new(bedrock_service));

    let scheduler_snapshot_store: Option<Arc<dyn fakecloud_persistence::SnapshotStore>> =
        if persistence_config.mode == fakecloud_persistence::StorageMode::Persistent {
            let data_path = persistence_config
                .data_path
                .as_ref()
                .expect("validated above")
                .clone();
            let path = data_path.join("scheduler").join("snapshot.json");
            let store = fakecloud_persistence::DiskSnapshotStore::new(path);
            match fakecloud_scheduler::persistence::load_into(&store, &scheduler_state) {
                Ok(fakecloud_scheduler::persistence::LoadOutcome::Loaded(accounts)) => {
                    tracing::info!(accounts, "loaded scheduler persistence snapshot");
                }
                Ok(fakecloud_scheduler::persistence::LoadOutcome::Empty) => {
                    tracing::info!("no scheduler persistence snapshot found; starting empty");
                }
                Err(err) => fatal_exit(format_args!("{err}")),
            }
            Some(Arc::new(store) as Arc<dyn fakecloud_persistence::SnapshotStore>)
        } else {
            None
        };
    let mut scheduler_service = SchedulerService::new(scheduler_state.clone());
    if let Some(store) = scheduler_snapshot_store {
        scheduler_service = scheduler_service.with_snapshot_store(store);
    }
    registry.register(Arc::new(scheduler_service));

    // Spawn the Scheduler firing loop as a background task. Mirrors
    // EventBridge's delivery bus so every target type Scheduler
    // routes (`:sqs:`, `:sns:`, `:lambda:`, `:states:`, `:events:`)
    // resolves to a live sender.
    let sfn_delivery_for_scheduler: Arc<dyn fakecloud_core::delivery::StepFunctionsDelivery> = {
        let mut sns_fanout_for_sfn = DeliveryBus::new().with_sqs(sqs_delivery.clone());
        if let Some(ref ld) = lambda_delivery {
            sns_fanout_for_sfn = sns_fanout_for_sfn.with_lambda(ld.clone());
        }
        let sns_for_sfn = Arc::new(fakecloud_sns::delivery::SnsDeliveryImpl::new(
            sns_state.clone(),
            Arc::new(sns_fanout_for_sfn),
        ));
        // Inner bus for EB rule delivery: matches other call-sites'
        // surface (SQS + SNS + Lambda) so Scheduler-triggered SFN
        // executions that hit EB rules fanning to SNS don't get
        // silently dropped.
        let mut inner_eb_bus = DeliveryBus::new()
            .with_sqs(sqs_delivery.clone())
            .with_sns(sns_delivery_for_scheduler_sfn_eb);
        if let Some(ref ld) = lambda_delivery {
            inner_eb_bus = inner_eb_bus.with_lambda(ld.clone());
        }
        let eb_for_sfn = Arc::new(
            fakecloud_eventbridge::delivery::EventBridgeDeliveryImpl::new(
                eb_state_for_scheduler.clone(),
                Arc::new(inner_eb_bus),
            ),
        );
        let mut sfn_interpreter_bus = DeliveryBus::new()
            .with_sqs(sqs_delivery.clone())
            .with_sns(sns_for_sfn)
            .with_eventbridge(eb_for_sfn);
        if let Some(ref ld) = lambda_delivery {
            sfn_interpreter_bus = sfn_interpreter_bus.with_lambda(ld.clone());
        }
        Arc::new(stepfunctions_delivery::StepFunctionsDeliveryImpl::new(
            stepfunctions_state.clone(),
            Some(Arc::new(sfn_interpreter_bus)),
            Some(dynamodb_state.clone()),
        ))
    };
    let eb_delivery_for_scheduler = {
        let mut inner = DeliveryBus::new()
            .with_sqs(sqs_delivery.clone())
            .with_sns(sns_delivery_for_scheduler_eb);
        if let Some(ref ld) = lambda_delivery {
            inner = inner.with_lambda(ld.clone());
        }
        Arc::new(
            fakecloud_eventbridge::delivery::EventBridgeDeliveryImpl::new(
                eb_state_for_scheduler,
                Arc::new(inner),
            ),
        )
    };
    let delivery_for_scheduler = {
        let mut bus = DeliveryBus::new()
            .with_sqs(sqs_delivery.clone())
            .with_sns(sns_delivery_for_scheduler)
            .with_eventbridge(eb_delivery_for_scheduler)
            .with_stepfunctions(sfn_delivery_for_scheduler);
        if let Some(ref ld) = lambda_delivery {
            bus = bus.with_lambda(ld.clone());
        }
        Arc::new(bus)
    };
    let scheduler_state_for_list = scheduler_state.clone();
    let scheduler_state_for_fire = scheduler_state.clone();
    let delivery_for_scheduler_fire = delivery_for_scheduler.clone();
    let default_account_for_scheduler_fire = cli.account_id.clone();
    let default_region_for_scheduler_fire = cli.region.clone();
    let scheduler_ticker =
        fakecloud_scheduler::ticker::Ticker::new(scheduler_state.clone(), delivery_for_scheduler);
    tokio::spawn(scheduler_ticker.run());

    // Spawn background tasks
    let lifecycle_processor = fakecloud_s3::lifecycle::LifecycleProcessor::new(s3_state.clone());
    tokio::spawn(lifecycle_processor.run());

    let mut sqs_lambda_poller = SqsLambdaPoller::new(sqs_state, lambda_state.clone());
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

    let mut dynamodb_streams_poller =
        DynamoDbStreamsLambdaPoller::new(dynamodb_state.clone(), lambda_invocations_state.clone());
    if let Some(ref ld) = lambda_delivery {
        dynamodb_streams_poller = dynamodb_streams_poller.with_lambda_delivery(ld.clone());
    }
    tokio::spawn(Arc::new(dynamodb_streams_poller).run());

    if let Some(ref rt) = container_runtime {
        let rt = rt.clone();
        tokio::spawn(rt.run_cleanup_loop(std::time::Duration::from_secs(300)));
    }

    let services: Vec<&str> = registry.service_names();
    tracing::info!(services = ?services, "registered services");

    let iam_mode = cli.iam_mode();
    if iam_mode.is_enabled() || cli.verify_sigv4 {
        tracing::warn!(
            verify_sigv4 = cli.verify_sigv4,
            iam_mode = %iam_mode,
            "opt-in security features enabled: access keys with the `test` prefix bypass SigV4 verification and IAM enforcement — see /docs/reference/security"
        );
    }
    if iam_mode.is_enabled() {
        let (enforced, skipped) = registry.iam_enforcement_split();
        tracing::info!(
            enforced = ?enforced,
            skipped = ?skipped,
            "IAM enforcement surface: listed `enforced` services evaluate policies; `skipped` services are not yet wired for enforcement"
        );
    }

    let config = DispatchConfig {
        region: cli.region,
        account_id: cli.account_id,
        verify_sigv4: cli.verify_sigv4,
        iam_mode,
        credential_resolver: Some(
            fakecloud_iam::credential_resolver::IamCredentialResolver::shared(iam_state.clone()),
        ),
        policy_evaluator: Some(
            fakecloud_iam::policy_evaluator::IamPolicyEvaluatorImpl::shared(iam_state.clone()),
        ),
        // Composite resource-policy provider: each concrete provider
        // gates on its own service prefix and returns None for anything
        // it doesn't own, so additional services can be added by
        // appending to this list without touching the core crate.
        resource_policy_provider: Some(fakecloud_core::auth::MultiResourcePolicyProvider::shared(
            vec![
                fakecloud_s3::resource_policy::S3ResourcePolicyProvider::shared(s3_state.clone()),
                fakecloud_sns::resource_policy::SnsResourcePolicyProvider::shared(
                    sns_state.clone(),
                ),
                fakecloud_lambda::resource_policy::LambdaResourcePolicyProvider::shared(
                    lambda_state.clone(),
                ),
                fakecloud_kms::resource_policy::KmsResourcePolicyProvider::shared(
                    kms_state.clone(),
                ),
                fakecloud_iam::resource_policy::StsResourcePolicyProvider::shared(
                    iam_state.clone(),
                ),
            ],
        )),
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
                    let accounts = ls.read();
                    let invocations = accounts
                        .iter()
                        .flat_map(|(_, state)| state.invocations.iter())
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
                    let mas = ss.read();
                    let state = mas.default_ref();
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
                let s3_for_inbound = s3_introspection_state.clone();
                let s3_store_for_inbound = s3_store_for_inbound.clone();
                let delivery_for_inbound = {
                    let mut bus = DeliveryBus::new();
                    let sns_fanout_bus = {
                        let mut b = DeliveryBus::new().with_sqs(sqs_delivery.clone());
                        if let Some(ref ld) = lambda_delivery {
                            b = b.with_lambda(ld.clone());
                        }
                        Arc::new(b)
                    };
                    let sns_for_inbound = Arc::new(
                        fakecloud_sns::delivery::SnsDeliveryImpl::new(
                            sns_introspection_state.clone(),
                            sns_fanout_bus,
                        ),
                    );
                    bus = bus.with_sns(sns_for_inbound);
                    if let Some(ref ld) = lambda_delivery {
                        bus = bus.with_lambda(ld.clone());
                    }
                    Arc::new(bus)
                };
                move |axum::Json(body): axum::Json<types::InboundEmailRequest>| async move {
                    let (message_id, matched_rules, actions) =
                        fakecloud_ses::v1::evaluate_inbound_email(
                            &ss,
                            &body.from,
                            &body.to,
                            &body.subject,
                            &body.body,
                        );

                    // Execute actions for real
                    for (_rule, action) in &actions {
                        match action {
                            fakecloud_ses::state::ReceiptAction::S3 {
                                bucket_name,
                                object_key_prefix,
                                ..
                            } => {
                                let prefix = object_key_prefix.as_deref().unwrap_or("");
                                let key = format!("{prefix}{message_id}");
                                let now = chrono::Utc::now();
                                let data = bytes::Bytes::from(body.body.clone());
                                let size = data.len() as u64;
                                let etag = format!("\"{:x}\"", md5::Md5::digest(&data));
                                let obj = fakecloud_s3::state::S3Object {
                                    key: key.clone(),
                                    body: fakecloud_persistence::BodyRef::Memory(data.clone()),
                                    content_type: "text/plain".to_string(),
                                    etag: etag.clone(),
                                    size,
                                    last_modified: now,
                                    storage_class: "STANDARD".to_string(),
                                    ..Default::default()
                                };
                                let mut mas = s3_for_inbound.write();
                                let state = mas.default_mut();
                                if let Some(bucket) = state.buckets.get_mut(bucket_name) {
                                    tracing::info!(
                                        bucket = %bucket_name,
                                        key = %key,
                                        "SES inbound: stored email in S3"
                                    );
                                    let meta =
                                        fakecloud_s3::persistence::object_meta_snapshot(&obj);
                                    bucket.objects.insert(key.clone(), obj);
                                    drop(mas);
                                    if let Err(err) = s3_store_for_inbound.put_object(
                                        bucket_name,
                                        &key,
                                        None,
                                        fakecloud_persistence::BodySource::Bytes(data),
                                        &meta,
                                    ) {
                                        tracing::error!(
                                            bucket = %bucket_name,
                                            key = %key,
                                            error = %err,
                                            "SES inbound: failed to persist S3 object via store"
                                        );
                                    }
                                } else {
                                    tracing::warn!(
                                        bucket = %bucket_name,
                                        "SES inbound: S3 bucket not found, skipping S3 action"
                                    );
                                }
                            }
                            fakecloud_ses::state::ReceiptAction::Sns { topic_arn, .. } => {
                                let notification = serde_json::json!({
                                    "notificationType": "Received",
                                    "mail": {
                                        "messageId": message_id,
                                        "source": body.from,
                                        "destination": body.to,
                                        "commonHeaders": {
                                            "from": [&body.from],
                                            "to": &body.to,
                                            "subject": &body.subject,
                                        }
                                    },
                                    "content": &body.body,
                                });
                                tracing::info!(
                                    topic_arn = %topic_arn,
                                    "SES inbound: publishing to SNS"
                                );
                                delivery_for_inbound.publish_to_sns(
                                    topic_arn,
                                    &notification.to_string(),
                                    Some(&body.subject),
                                );
                            }
                            fakecloud_ses::state::ReceiptAction::Lambda {
                                function_arn,
                                invocation_type,
                                ..
                            } => {
                                let ses_event = serde_json::json!({
                                    "Records": [{
                                        "eventSource": "aws:ses",
                                        "eventVersion": "1.0",
                                        "ses": {
                                            "mail": {
                                                "messageId": message_id,
                                                "source": body.from,
                                                "destination": body.to,
                                                "commonHeaders": {
                                                    "from": [&body.from],
                                                    "to": &body.to,
                                                    "subject": &body.subject,
                                                }
                                            },
                                            "receipt": {
                                                "recipients": &body.to,
                                                "action": {
                                                    "type": "Lambda",
                                                    "functionArn": function_arn,
                                                    "invocationType": invocation_type.as_deref().unwrap_or("Event"),
                                                }
                                            }
                                        }
                                    }]
                                });
                                let payload = ses_event.to_string();
                                let delivery = delivery_for_inbound.clone();
                                let function_arn = function_arn.clone();
                                tracing::info!(
                                    function_arn = %function_arn,
                                    "SES inbound: invoking Lambda"
                                );
                                tokio::spawn(async move {
                                    match delivery.invoke_lambda(&function_arn, &payload).await {
                                        Some(Ok(_)) => {
                                            tracing::info!(
                                                function_arn = %function_arn,
                                                "SES inbound: Lambda invocation succeeded"
                                            );
                                        }
                                        Some(Err(e)) => {
                                            tracing::error!(
                                                function_arn = %function_arn,
                                                error = %e,
                                                "SES inbound: Lambda invocation failed"
                                            );
                                        }
                                        None => {
                                            tracing::warn!(
                                                "SES inbound: no container runtime available for Lambda invocation"
                                            );
                                        }
                                    }
                                });
                            }
                            // Bounce, AddHeader, Stop are metadata-only — no cross-service delivery
                            _ => {}
                        }
                    }

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
                    let mas = ss.read();
                    let messages = mas
                        .iter()
                        .flat_map(|(_, state)| state.published.iter())
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
                    let mas = ss.read();
                    let queues = mas
                        .iter()
                        .flat_map(|(_, state)| state.queues.values())
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
                    let accounts = es.read();
                    let events = accounts
                        .iter()
                        .flat_map(|(_, state)| state.events.iter())
                        .map(|evt| types::EventBridgeEvent {
                            event_id: evt.event_id.clone(),
                            source: evt.source.clone(),
                            detail_type: evt.detail_type.clone(),
                            detail: evt.detail.clone(),
                            bus_name: evt.event_bus_name.clone(),
                            timestamp: evt.time.to_rfc3339(),
                        })
                        .collect();
                    let lambda = accounts
                        .iter()
                        .flat_map(|(_, state)| state.lambda_invocations.iter())
                        .map(|inv| types::EventBridgeLambdaDelivery {
                            function_arn: inv.function_arn.clone(),
                            payload: inv.payload.clone(),
                            timestamp: inv.timestamp.to_rfc3339(),
                        })
                        .collect();
                    let logs = accounts
                        .iter()
                        .flat_map(|(_, state)| state.log_deliveries.iter())
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

                    let ctx = fakecloud_eventbridge::simulation::FireRuleContext {
                        state: &es,
                        delivery: &delivery,
                        lambda_state: &lambda_state,
                        logs_state: &logs_state,
                        container_runtime: &container_runtime,
                    };
                    match fakecloud_eventbridge::simulation::fire_rule(
                        &ctx,
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
                    let mas = ss.read();
                    let notifications = mas
                        .iter()
                        .flat_map(|(_, state)| state.notification_events.iter())
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
            "/_fakecloud/scheduler/schedules",
            axum::routing::get({
                let state = scheduler_state_for_list;
                move || async move {
                    let rows = fakecloud_scheduler::simulation::list_all_schedules(&state);
                    let schedules = rows
                        .into_iter()
                        .map(|r| types::SchedulerSchedule {
                            account_id: r.account_id,
                            group_name: r.group_name,
                            name: r.name,
                            arn: r.arn,
                            state: r.state,
                            schedule_expression: r.schedule_expression,
                            target_arn: r.target_arn,
                            last_fired: r.last_fired.map(|t| t.to_rfc3339()),
                        })
                        .collect();
                    axum::Json(types::SchedulerSchedulesResponse { schedules })
                }
            }),
        )
        .route(
            "/_fakecloud/scheduler/fire/{group}/{name}",
            axum::routing::post({
                let state = scheduler_state_for_fire;
                let delivery = delivery_for_scheduler_fire;
                let default_account = default_account_for_scheduler_fire;
                let default_region = default_region_for_scheduler_fire;
                move |axum::extract::Path((group, name)): axum::extract::Path<(String, String)>| {
                    let state = state.clone();
                    let delivery = delivery.clone();
                    let default_account = default_account.clone();
                    let default_region = default_region.clone();
                    async move {
                        match fakecloud_scheduler::simulation::fire_schedule_response(
                            &state,
                            &delivery,
                            &default_region,
                            &default_account,
                            &group,
                            &name,
                        ) {
                            Ok(body) => (
                                axum::http::StatusCode::OK,
                                axum::Json(serde_json::json!(body)),
                            ),
                            Err(msg) => (
                                axum::http::StatusCode::NOT_FOUND,
                                axum::Json(serde_json::json!({ "error": msg })),
                            ),
                        }
                    }
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
                        let mas = cs.read();
                        let state = mas.default_ref();
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
                        let mas = cs.read();
                        let state = mas.default_ref();
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
                        let mut mas = cs.write();
                        let state = mas.default_mut();
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
                        let mas = cs.read();
                        let state = mas.default_ref();
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
                        let mut mas = cs.write();
                        let state = mas.default_mut();
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
                        let mas = cs.read();
                        let state = mas.default_ref();
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
                        let accounts = rs.read();
                        let state = accounts.default_ref();
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
            "/_fakecloud/elasticache/clusters",
            axum::routing::get({
                let ec = elasticache_introspection_state.clone();
                move || {
                    let ec = ec.clone();
                    async move {
                        let accounts = ec.read();
                        let state = accounts.default_ref();
                        let mut clusters: Vec<types::ElastiCacheCluster> = state
                            .cache_clusters
                            .values()
                            .map(elasticache_cluster_response)
                            .collect();
                        clusters.sort_by(|a, b| a.cache_cluster_id.cmp(&b.cache_cluster_id));
                        axum::Json(types::ElastiCacheClustersResponse { clusters })
                    }
                }
            }),
        )
        .route(
            "/_fakecloud/elasticache/replication-groups",
            axum::routing::get({
                let ec = elasticache_introspection_state.clone();
                move || {
                    let ec = ec.clone();
                    async move {
                        let accounts = ec.read();
                        let state = accounts.default_ref();
                        let mut replication_groups: Vec<
                            types::ElastiCacheReplicationGroupIntrospection,
                        > = state
                            .replication_groups
                            .values()
                            .map(elasticache_replication_group_response)
                            .collect();
                        replication_groups
                            .sort_by(|a, b| a.replication_group_id.cmp(&b.replication_group_id));
                        axum::Json(types::ElastiCacheReplicationGroupsResponse {
                            replication_groups,
                        })
                    }
                }
            }),
        )
        .route(
            "/_fakecloud/elasticache/serverless-caches",
            axum::routing::get({
                let ec = elasticache_introspection_state;
                move || {
                    let ec = ec.clone();
                    async move {
                        let accounts = ec.read();
                        let state = accounts.default_ref();
                        let mut serverless_caches: Vec<
                            types::ElastiCacheServerlessCacheIntrospection,
                        > = state
                            .serverless_caches
                            .values()
                            .map(elasticache_serverless_cache_response)
                            .collect();
                        serverless_caches
                            .sort_by(|a, b| a.serverless_cache_name.cmp(&b.serverless_cache_name));
                        axum::Json(types::ElastiCacheServerlessCachesResponse { serverless_caches })
                    }
                }
            }),
        )
        .route(
            "/_fakecloud/stepfunctions/executions",
            axum::routing::get({
                let ss = stepfunctions_state.clone();
                move || {
                    let ss = ss.clone();
                    async move {
                        let accounts = ss.read();
                        let state = accounts.default_ref();
                        let mut executions: Vec<types::StepFunctionsExecution> = state
                            .executions
                            .values()
                            .map(|exec| types::StepFunctionsExecution {
                                execution_arn: exec.execution_arn.clone(),
                                state_machine_arn: exec.state_machine_arn.clone(),
                                name: exec.name.clone(),
                                status: exec.status.as_str().to_string(),
                                input: exec.input.clone(),
                                output: exec.output.clone(),
                                start_date: exec.start_date.to_rfc3339(),
                                stop_date: exec.stop_date.map(|d| d.to_rfc3339()),
                            })
                            .collect();
                        executions.sort_by(|a, b| b.start_date.cmp(&a.start_date));
                        axum::Json(types::StepFunctionsExecutionsResponse { executions })
                    }
                }
            }),
        )
        .route(
            "/_fakecloud/apigatewayv2/requests",
            axum::routing::get({
                let apigw_state = apigatewayv2_state.clone();
                move || {
                    let apigw_state = apigw_state.clone();
                    async move {
                        let accounts = apigw_state.read();
                        let state = accounts.default_ref();
                        axum::Json(serde_json::json!({
                            "requests": state.request_history
                        }))
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
        .route(
            "/_fakecloud/reset/{service}/{account_id}",
            axum::routing::post({
                let s = reset_state.clone();
                move |axum::extract::Path((service, account_id)): axum::extract::Path<(String, String)>| async move {
                    match s.reset_service_for_account(&service, &account_id) {
                        Ok(()) => (
                            axum::http::StatusCode::OK,
                            axum::Json(serde_json::json!(types::ResetServiceResponse {
                                reset: format!("{service}/{account_id}")
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
        // Bedrock introspection: list all model invocations
        .route(
            "/_fakecloud/bedrock/invocations",
            axum::routing::get({
                let bs = bedrock_state.clone();
                move || async move {
                    let accounts = bs.read(); let state = accounts.default_ref();
                    let invocations: Vec<serde_json::Value> = state
                        .invocations
                        .iter()
                        .map(|inv| {
                            serde_json::json!({
                                "modelId": inv.model_id,
                                "input": inv.input,
                                "output": inv.output,
                                "timestamp": inv.timestamp.to_rfc3339(),
                                "error": inv.error,
                            })
                        })
                        .collect();
                    axum::Json(serde_json::json!({ "invocations": invocations }))
                }
            }),
        )
        // Bedrock simulation: configure model response
        .route(
            "/_fakecloud/bedrock/models/{model_id}/response",
            axum::routing::post({
                let bs = bedrock_state.clone();
                move |axum::extract::Path(model_id): axum::extract::Path<String>,
                      body: String| async move {
                    let mut accounts = bs.write(); let state = accounts.default_mut();
                    state.custom_responses.insert(model_id.clone(), body);
                    axum::Json(
                        serde_json::json!({ "status": "ok", "modelId": model_id }),
                    )
                }
            }),
        )
        // Bedrock simulation: configure prompt-conditional response rules
        .route(
            "/_fakecloud/bedrock/models/{model_id}/responses",
            axum::routing::post({
                let bs = bedrock_state.clone();
                move |axum::extract::Path(model_id): axum::extract::Path<String>,
                      axum::Json(body): axum::Json<serde_json::Value>| async move {
                    let rules_json = body.get("rules").and_then(|r| r.as_array()).cloned();
                    let Some(rules_json) = rules_json else {
                        return (
                            axum::http::StatusCode::BAD_REQUEST,
                            axum::Json(serde_json::json!({
                                "error": "body must contain a `rules` array"
                            })),
                        );
                    };
                    let mut parsed = Vec::with_capacity(rules_json.len());
                    for rule in rules_json {
                        let prompt_contains = match rule.get("promptContains") {
                            None | Some(serde_json::Value::Null) => None,
                            Some(serde_json::Value::String(s)) => Some(s.clone()),
                            Some(_) => {
                                return (
                                    axum::http::StatusCode::BAD_REQUEST,
                                    axum::Json(serde_json::json!({
                                        "error": "`promptContains` must be a string when provided"
                                    })),
                                );
                            }
                        };
                        let response = match rule.get("response") {
                            Some(serde_json::Value::String(s)) => s.clone(),
                            Some(other) => other.to_string(),
                            None => {
                                return (
                                    axum::http::StatusCode::BAD_REQUEST,
                                    axum::Json(serde_json::json!({
                                        "error": "each rule must include a `response` field"
                                    })),
                                );
                            }
                        };
                        parsed.push(fakecloud_bedrock::state::ResponseRule {
                            prompt_contains,
                            response,
                        });
                    }
                    let mut accounts = bs.write(); let state = accounts.default_mut();
                    state.response_rules.insert(model_id.clone(), parsed);
                    (
                        axum::http::StatusCode::OK,
                        axum::Json(serde_json::json!({
                            "status": "ok",
                            "modelId": model_id
                        })),
                    )
                }
            })
            .delete({
                let bs = bedrock_state.clone();
                move |axum::extract::Path(model_id): axum::extract::Path<String>| async move {
                    let mut accounts = bs.write(); let state = accounts.default_mut();
                    state.response_rules.remove(&model_id);
                    axum::Json(serde_json::json!({ "status": "ok", "modelId": model_id }))
                }
            }),
        )
        // Bedrock fault injection: queue / list / clear fault rules
        .route(
            "/_fakecloud/bedrock/faults",
            axum::routing::post({
                let bs = bedrock_state.clone();
                move |axum::Json(body): axum::Json<serde_json::Value>| async move {
                    let error_type = body
                        .get("errorType")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();
                    let message = body
                        .get("message")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();
                    let http_status_raw =
                        body.get("httpStatus").and_then(|v| v.as_u64()).unwrap_or(500);
                    let Ok(http_status) = u16::try_from(http_status_raw) else {
                        return (
                            axum::http::StatusCode::BAD_REQUEST,
                            axum::Json(serde_json::json!({
                                "error": "`httpStatus` must fit in a u16"
                            })),
                        );
                    };
                    let count_raw = body.get("count").and_then(|v| v.as_u64()).unwrap_or(1);
                    let Ok(count) = u32::try_from(count_raw.max(1)) else {
                        return (
                            axum::http::StatusCode::BAD_REQUEST,
                            axum::Json(serde_json::json!({
                                "error": "`count` must fit in a u32"
                            })),
                        );
                    };
                    let model_id = body
                        .get("modelId")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string());
                    let operation = body
                        .get("operation")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string());
                    if error_type.is_empty() {
                        return (
                            axum::http::StatusCode::BAD_REQUEST,
                            axum::Json(serde_json::json!({
                                "error": "`errorType` is required"
                            })),
                        );
                    }
                    let mut accounts = bs.write(); let state = accounts.default_mut();
                    state
                        .fault_rules
                        .push(fakecloud_bedrock::state::FaultRule {
                            error_type,
                            message,
                            http_status,
                            remaining: count,
                            model_id,
                            operation,
                        });
                    (
                        axum::http::StatusCode::OK,
                        axum::Json(serde_json::json!({ "status": "ok" })),
                    )
                }
            })
            .get({
                let bs = bedrock_state.clone();
                move || async move {
                    let accounts = bs.read(); let state = accounts.default_ref();
                    let faults: Vec<serde_json::Value> = state
                        .fault_rules
                        .iter()
                        .map(|f| {
                            serde_json::json!({
                                "errorType": f.error_type,
                                "message": f.message,
                                "httpStatus": f.http_status,
                                "remaining": f.remaining,
                                "modelId": f.model_id,
                                "operation": f.operation,
                            })
                        })
                        .collect();
                    axum::Json(serde_json::json!({ "faults": faults }))
                }
            })
            .delete({
                let bs = bedrock_state.clone();
                move || async move {
                    let mut accounts = bs.write(); let state = accounts.default_mut();
                    state.fault_rules.clear();
                    axum::Json(serde_json::json!({ "status": "ok" }))
                }
            }),
        )
        .route(
            "/_fakecloud/iam/create-admin",
            axum::routing::post({
                let iam = iam_state.clone();
                move |axum::Json(body): axum::Json<types::CreateAdminRequest>| {
                    let iam = iam.clone();
                    async move {
                        axum::Json(reset::create_admin_in_account(
                            &iam,
                            &body.account_id,
                            &body.user_name,
                        ))
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

    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
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

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    tracing::info!("shutting down");
}

/// Emit a fatal error through the tracing pipeline, flush stderr so the
/// message survives `process::exit`, and terminate with code 1.
fn fatal_exit(args: std::fmt::Arguments<'_>) -> ! {
    use std::io::Write;
    tracing::error!("{args}");
    let _ = std::io::stderr().flush();
    std::process::exit(1);
}

/// Route panics through `tracing::error!` so they show up in CI logs with
/// the same formatting as regular errors. Runs the default hook afterwards
/// so the process keeps its usual backtrace behaviour for developers
/// running locally with `RUST_BACKTRACE=1`.
fn install_panic_hook() {
    let default = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let location = info
            .location()
            .map(|l| format!("{}:{}:{}", l.file(), l.line(), l.column()))
            .unwrap_or_else(|| "<unknown>".to_string());
        let payload = info
            .payload()
            .downcast_ref::<&'static str>()
            .copied()
            .map(|s| s.to_string())
            .or_else(|| info.payload().downcast_ref::<String>().cloned())
            .unwrap_or_else(|| "<non-string panic>".to_string());
        tracing::error!(location = %location, payload = %payload, "panic");
        default(info);
    }));
}
