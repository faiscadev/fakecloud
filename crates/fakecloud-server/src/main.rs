use std::sync::Arc;

use axum::extract::Extension;
use axum::response::IntoResponse;
use axum::Router;
use clap::Parser;
use md5::Digest;
use tower_http::trace::TraceLayer;

use fakecloud_core::delivery::DeliveryBus;
use fakecloud_core::dispatch::{self, DispatchConfig};
use fakecloud_core::registry::ServiceRegistry;
use fakecloud_sdk::types;

mod admin_elasticache_artifacts;
mod admin_lambda_artifacts;
mod appas_hooks;
mod cli;
mod dynamodb_streams_lambda_poller;
mod introspection;
mod kinesis_lambda_poller;
mod lambda_delivery;
mod pipes_runner;
mod reaper;
mod reset;
mod runtime;
mod ses_smtp;
mod sqs_lambda_poller;
mod stepfunctions_delivery;
use cli::Cli;
use dynamodb_streams_lambda_poller::DynamoDbStreamsLambdaPoller;
use introspection::{
    athena_named_query_response, cloudfront_distribution_response, ec2_instance_response,
    ecr_image_response, ecr_pull_through_rule_response, ecr_repository_response,
    ecs_cluster_response, ecs_lifecycle_event, ecs_task_metadata_response, ecs_task_response,
    elasticache_acls_response, elasticache_cluster_response,
    elasticache_replication_group_response, elasticache_serverless_cache_response,
    elbv2_listener_response, elbv2_load_balancer_response, elbv2_rule_response,
    elbv2_target_group_response, organizations_accounts_snapshot, rds_instance_response,
};
use kinesis_lambda_poller::KinesisLambdaPoller;
use reset::ResetState;
use runtime::{
    announce_bound_port, bind_listener, endpoint_url_from_addr, fatal_exit,
    generate_k8s_internal_token, install_panic_hook, parse_basic_auth, run_healthcheck,
    shutdown_signal, wafv2_evaluate_admin,
};
use sqs_lambda_poller::SqsLambdaPoller;

use fakecloud_apigateway::{ApiGatewayFacade, ApiGatewayService};
use fakecloud_apigatewayv2::ApiGatewayV2Service;
use fakecloud_bedrock::BedrockService;
use fakecloud_bedrock_agent::BedrockAgentService;
use fakecloud_bedrock_agent_runtime::BedrockAgentRuntimeService;
use fakecloud_cloudformation::CloudFormationService;
use fakecloud_cloudfront::CloudFrontService;
use fakecloud_cognito::CognitoService;
use fakecloud_dsql::DsqlService;
use fakecloud_dynamodb::DynamoDbService;
use fakecloud_ec2::{Ec2Service, SharedEc2State};
use fakecloud_ecr::EcrService;
use fakecloud_ecs::EcsService;
use fakecloud_elasticache::ElastiCacheService;
use fakecloud_elbv2::Elbv2Service;
use fakecloud_eventbridge::EventBridgeService;
use fakecloud_iam::iam_service::IamService;
use fakecloud_iam::sts_service::StsService;
use fakecloud_kinesis::KinesisService;
use fakecloud_kms::KmsService;
use fakecloud_lambda::LambdaService;
use fakecloud_logs::LogsService;
use fakecloud_organizations::OrganizationsService;
use fakecloud_organizations::SharedOrganizationsState;
use fakecloud_rds::RdsService;
use fakecloud_s3::S3Service;
use fakecloud_scheduler::SchedulerService;
use fakecloud_secretsmanager::SecretsManagerService;
use fakecloud_ses::SesV2Service;
use fakecloud_sns::SnsService;
use fakecloud_sqs::SqsService;
use fakecloud_ssm::SsmService;
use fakecloud_stepfunctions::StepFunctionsService;

mod hooks;
use hooks::*;

/// Outer middleware that serves CloudFront viewer traffic on the main listener.
/// If the request's `Host` matches an enabled distribution, the data plane
/// proxies it to the resolved origin; otherwise the request is handed back for
/// normal AWS dispatch (the common case for all API / introspection traffic).
async fn cloudfront_viewer_middleware(
    axum::extract::State(dp): axum::extract::State<
        std::sync::Arc<fakecloud_cloudfront::dataplane::CloudFrontDataPlane>,
    >,
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    match dp.serve(req).await {
        Ok(resp) => resp,
        Err(req) => next.run(req).await,
    }
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    // `fakecloud healthcheck` probes a running server and exits — used by the
    // container HEALTHCHECK so the slim published image needs no curl/wget.
    if let Some(cli::Command::Healthcheck) = cli.command {
        std::process::exit(run_healthcheck(&cli.addr));
    }
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
        // Persistent mode means "state survives restart", so the data INSIDE
        // container-backed services (RDS databases, EC2 instance data dirs)
        // should be durable too -- not just the control-plane metadata. Those
        // runtimes already know how to back a container with a named volume;
        // they just gate it behind an env var that is off by default (to keep
        // ephemeral/test runs clean). Default those gates ON here, before the
        // runtimes are constructed, so persistent mode is durable end-to-end
        // without extra flags. An explicit user setting always wins.
        // (ElastiCache already persists Redis/Valkey unconditionally.)
        for var in [
            "FAKECLOUD_PERSIST_DB_VOLUMES",
            "FAKECLOUD_PERSIST_EC2_VOLUMES",
        ] {
            if std::env::var_os(var).is_none() {
                std::env::set_var(var, "1");
                tracing::debug!(
                    env = var,
                    "persistent mode: defaulting container data volumes on"
                );
            }
        }
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
    // Bind early so we know the actual port before initialising service state.
    // When the caller passes `--addr 0.0.0.0:0` the OS assigns a free port
    // atomically, eliminating the race between find-a-free-port and bind that
    // previously caused sporadic "Connection refused" in parallel tests.
    let (listener, bound_addr) = bind_listener(&cli.addr)
        .await
        .unwrap_or_else(|e| fatal_exit(format_args!("failed to bind {}: {e}", cli.addr)));
    // Announce the bound port to stdout so test harnesses (fakecloud-testkit)
    // can discover the OS-assigned port when `--addr :0` is used. The prefix
    // makes the line self-identifying: if anything ever prints to stdout
    // before this line, the parser on the other side still finds the port.
    if let Err(e) = announce_bound_port(bound_addr.port(), &mut std::io::stdout().lock()) {
        fatal_exit(format_args!("failed to announce bound port: {e}"));
    }
    tracing::info!(addr = %bound_addr, "fakecloud is ready");
    // Build the endpoint URL from the *actual* bound address so that port 0
    // resolves to the real OS-assigned port in all internal resource URLs
    // (SQS queue URLs, SNS ARNs, etc.).
    let endpoint_url = endpoint_url_from_addr(bound_addr);
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
        let mut mas: fakecloud_core::multi_account::MultiAccountState<fakecloud_sns::SnsState> =
            fakecloud_core::multi_account::MultiAccountState::new(
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
    // Lambda execution backend. FAKECLOUD_LAMBDA_BACKEND=k8s (or the
    // global FAKECLOUD_CONTAINER_BACKEND=k8s) opts into the native
    // Kubernetes Pod backend (issue #1234); anything else (or unset)
    // auto-detects Docker/Podman.
    let lambda_backend = fakecloud_k8s::backend_choice("FAKECLOUD_LAMBDA_BACKEND");
    let k8s_internal_token: Arc<String> = Arc::new(generate_k8s_internal_token());
    let container_runtime = if lambda_backend == fakecloud_k8s::Backend::K8s {
        match fakecloud_lambda::runtime::LambdaRuntime::new_k8s(
            bound_addr.port(),
            (*k8s_internal_token).clone(),
        )
        .await
        {
            Ok(rt) => Some(Arc::new(rt)),
            Err(e) => {
                eprintln!(
                    "Kubernetes Lambda backend selected (FAKECLOUD_LAMBDA_BACKEND/FAKECLOUD_CONTAINER_BACKEND=k8s) but failed to initialize: {e}"
                );
                std::process::exit(1);
            }
        }
    } else {
        fakecloud_lambda::runtime::ContainerRuntime::new(bound_addr.port()).map(Arc::new)
    };
    // Services backed by a container runtime degrade honestly when no
    // Docker/Podman CLI is present. Collect which ones so a single
    // consolidated banner can warn the operator once (below, after the EC2
    // runtime is resolved) instead of scattering five separate log lines.
    let mut degraded_runtimes: Vec<&str> = Vec::new();
    if let Some(ref rt) = container_runtime {
        tracing::info!(backend = rt.cli_name(), "Lambda execution enabled");
    } else {
        degraded_runtimes.push("Lambda (Invoke returns errors for functions with code)");
    }
    let lambda_backend_is_k8s = lambda_backend == fakecloud_k8s::Backend::K8s;
    let secretsmanager_state = Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new(
            &cli.account_id,
            &cli.region,
            &endpoint_url,
        ),
    ));
    // Clones for the CodeBuild service, which resolves PARAMETER_STORE /
    // SECRETS_MANAGER build env vars — captured before these states are moved
    // into their own SSM / Secrets Manager services below.
    let ssm_state_for_codebuild = ssm_state.clone();
    let secretsmanager_state_for_codebuild = secretsmanager_state.clone();
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
    let kms_usage_state: fakecloud_kms::hook::SharedKmsUsageState = Arc::new(
        parking_lot::RwLock::new(fakecloud_kms::hook::KmsUsageState::default()),
    );
    // Hook's snapshot store is set below once kms_snapshot_store is
    // initialized (depends on the persistence config). The OnceLock
    // wiring lets us hand the same Arc to all services up-front and
    // populate the store after persistence is read in.
    let kms_hook_adapter = Arc::new(KmsHookAdapter::new(
        kms_state.clone(),
        kms_usage_state.clone(),
    ));
    let kms_hook_for_services: Arc<dyn fakecloud_core::delivery::KmsHook> =
        kms_hook_adapter.clone();
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
    let docdb_state: fakecloud_docdb::SharedDocDbState = Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new(
            &cli.account_id,
            &cli.region,
            &endpoint_url,
        ),
    ));
    let neptune_state: fakecloud_neptune::SharedNeptuneState = Arc::new(parking_lot::RwLock::new(
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
    // Deferred-fill handle to the central ServiceRegistry. We construct
    // the cell up front, hand it to StepFunctionsService and the
    // EventBridge/Scheduler-side StepFunctionsDelivery impls, then
    // populate it after every service has been registered. The
    // interpreter snapshots the inner `Arc<ServiceRegistry>` only when
    // dispatching `arn:aws:states:::aws-sdk:*` Tasks, so unrelated
    // executions never touch it.
    let sfn_registry_handle: fakecloud_stepfunctions::SharedServiceRegistry =
        Arc::new(std::sync::OnceLock::new());
    let apigatewayv2_state = Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new(
            &cli.account_id,
            &cli.region,
            &endpoint_url,
        ),
    ));
    let apigatewayv2_ws_registry: fakecloud_apigatewayv2::SharedWebSocketRegistry = Arc::new(
        parking_lot::RwLock::new(fakecloud_apigatewayv2::WebSocketRegistry::default()),
    );
    let apigatewayv1_state = Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new(
            &cli.account_id,
            &cli.region,
            &endpoint_url,
        ),
    ));
    let ecr_state = Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new(
            &cli.account_id,
            &cli.region,
            &endpoint_url,
        ),
    ));
    let ecs_state: fakecloud_ecs::SharedEcsState = Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new(
            &cli.account_id,
            &cli.region,
            &endpoint_url,
        ),
    ));
    // CloudFront is a global REST-XML service. Constructed up-front (rather
    // than next to its `registry.register` call further down) so it can
    // join `ResetState` and have its in-memory state cleared by the
    // `/_fakecloud/reset` introspection endpoint alongside every other
    // service.
    let cloudfront_state: fakecloud_cloudfront::SharedCloudFrontState = Arc::new(
        parking_lot::RwLock::new(fakecloud_cloudfront::CloudFrontAccounts::new()),
    );
    // EC2 state, created up-front so it can join `ResetState` and be cleared by
    // `/_fakecloud/reset/ec2` (which also tears down the backing containers).
    let ec2_state: SharedEc2State = Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("000000000000", "us-east-1", ""),
    ));
    let route53_state: fakecloud_route53::SharedRoute53State = Arc::new(parking_lot::RwLock::new(
        fakecloud_route53::Route53Accounts::new(),
    ));
    let acm_state: fakecloud_acm::SharedAcmState =
        Arc::new(parking_lot::RwLock::new(fakecloud_acm::AcmAccounts::new()));
    let acmpca_state: fakecloud_acmpca::SharedAcmPcaState = Arc::new(parking_lot::RwLock::new(
        fakecloud_acmpca::AcmPcaAccounts::new(),
    ));
    let config_state: fakecloud_config::SharedConfigState = Arc::new(parking_lot::RwLock::new(
        fakecloud_config::ConfigAccounts::new(),
    ));
    let route53resolver_state: fakecloud_route53resolver::SharedRoute53ResolverState = Arc::new(
        parking_lot::RwLock::new(fakecloud_route53resolver::Route53ResolverAccounts::new()),
    );
    let app_autoscaling_state: fakecloud_application_autoscaling::SharedApplicationAutoScalingState =
        Arc::new(parking_lot::RwLock::new(
            fakecloud_application_autoscaling::ApplicationAutoScalingAccounts::new(),
        ));
    let autoscaling_state: fakecloud_autoscaling::SharedAutoScalingState = Arc::new(
        parking_lot::RwLock::new(fakecloud_autoscaling::AutoScalingAccounts::new()),
    );
    let batch_state: fakecloud_batch::SharedBatchState = Arc::new(parking_lot::RwLock::new(
        fakecloud_batch::BatchAccounts::new(),
    ));
    let pipes_state: fakecloud_pipes::SharedPipesState = Arc::new(parking_lot::RwLock::new(
        fakecloud_pipes::PipesAccounts::new(),
    ));
    let wafv2_state: fakecloud_wafv2::SharedWafv2State = Arc::new(parking_lot::RwLock::new(
        fakecloud_wafv2::Wafv2Accounts::new(),
    ));
    // Shared in-process rate-limit counter for `RateBasedStatement` rules.
    // Created here so the AwsService instance and the
    // `/_fakecloud/wafv2/evaluate` admin endpoint share their state.
    let wafv2_rate_limiter: Arc<fakecloud_wafv2::RateLimiter> =
        Arc::new(fakecloud_wafv2::RateLimiter::new());
    let athena_state: fakecloud_athena::SharedAthenaState = Arc::new(parking_lot::RwLock::new(
        fakecloud_athena::AthenaAccounts::new(),
    ));
    let redshift_state: fakecloud_redshift::SharedRedshiftState = Arc::new(
        parking_lot::RwLock::new(fakecloud_redshift::RedshiftAccounts::new()),
    );
    let bedrock_state = Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new(
            &cli.account_id,
            &cli.region,
            &endpoint_url,
        ),
    ));
    let bedrock_agent_state: fakecloud_bedrock_agent::SharedBedrockAgentState = Arc::new(
        parking_lot::RwLock::new(fakecloud_bedrock_agent::BedrockAgentAccounts::new()),
    );
    let bedrock_agent_runtime_state: fakecloud_bedrock_agent_runtime::SharedBedrockAgentRuntimeState = Arc::new(
        parking_lot::RwLock::new(fakecloud_bedrock_agent_runtime::BedrockAgentRuntimeAccounts::new()),
    );
    // Organizations state is a global singleton (one org per fakecloud
    // process) — not wrapped in MultiAccountState because an AWS org is
    // a cross-account construct. `None` until CreateOrganization runs.
    let organizations_state: SharedOrganizationsState = Arc::new(parking_lot::RwLock::new(None));
    let scheduler_state: fakecloud_scheduler::SharedSchedulerState = Arc::new(
        parking_lot::RwLock::new(fakecloud_core::multi_account::MultiAccountState::new(
            &cli.account_id,
            &cli.region,
            &endpoint_url,
        )),
    );
    let dsql_state: fakecloud_dsql::SharedDsqlState = Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new(
            &cli.account_id,
            &cli.region,
            &endpoint_url,
        ),
    ));
    let resource_groups_state: fakecloud_resource_groups::SharedResourceGroupsState = Arc::new(
        parking_lot::RwLock::new(fakecloud_core::multi_account::MultiAccountState::new(
            &cli.account_id,
            &cli.region,
            &endpoint_url,
        )),
    );
    let account_state: fakecloud_account::SharedAccountState = Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new(
            &cli.account_id,
            &cli.region,
            &endpoint_url,
        ),
    ));
    let identitystore_state: fakecloud_identitystore::SharedIdentityStoreState = Arc::new(
        parking_lot::RwLock::new(fakecloud_core::multi_account::MultiAccountState::new(
            &cli.account_id,
            &cli.region,
            &endpoint_url,
        )),
    );
    let ssoadmin_state: fakecloud_ssoadmin::SharedSsoAdminState = Arc::new(
        parking_lot::RwLock::new(fakecloud_core::multi_account::MultiAccountState::new(
            &cli.account_id,
            &cli.region,
            &endpoint_url,
        )),
    );
    let dms_state: fakecloud_dms::SharedDmsState = Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new(
            &cli.account_id,
            &cli.region,
            &endpoint_url,
        ),
    ));
    let cloudtrail_state: fakecloud_cloudtrail::SharedCloudTrailState = Arc::new(
        parking_lot::RwLock::new(fakecloud_core::multi_account::MultiAccountState::new(
            &cli.account_id,
            &cli.region,
            &endpoint_url,
        )),
    );
    let ce_state: fakecloud_ce::SharedCeState = Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new(
            &cli.account_id,
            &cli.region,
            &endpoint_url,
        ),
    ));
    let transfer_state: fakecloud_transfer::SharedTransferState = Arc::new(
        parking_lot::RwLock::new(fakecloud_core::multi_account::MultiAccountState::new(
            &cli.account_id,
            &cli.region,
            &endpoint_url,
        )),
    );
    let verifiedpermissions_state: fakecloud_verifiedpermissions::SharedVerifiedPermissionsState =
        Arc::new(parking_lot::RwLock::new(
            fakecloud_core::multi_account::MultiAccountState::new(
                &cli.account_id,
                &cli.region,
                &endpoint_url,
            ),
        ));
    let memorydb_state: fakecloud_memorydb::SharedMemoryDbState = Arc::new(
        parking_lot::RwLock::new(fakecloud_core::multi_account::MultiAccountState::new(
            &cli.account_id,
            &cli.region,
            &endpoint_url,
        )),
    );
    let kinesisanalyticsv2_state: fakecloud_kinesisanalyticsv2::SharedKa2State = Arc::new(
        parking_lot::RwLock::new(fakecloud_core::multi_account::MultiAccountState::new(
            &cli.account_id,
            &cli.region,
            &endpoint_url,
        )),
    );
    let servicediscovery_state: fakecloud_servicediscovery::SharedServiceDiscoveryState = Arc::new(
        parking_lot::RwLock::new(fakecloud_core::multi_account::MultiAccountState::new(
            &cli.account_id,
            &cli.region,
            &endpoint_url,
        )),
    );
    let glacier_state: fakecloud_glacier::SharedGlacierState = Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new(
            &cli.account_id,
            &cli.region,
            &endpoint_url,
        ),
    ));
    let eks_state: fakecloud_eks::SharedEksState = Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new(
            &cli.account_id,
            &cli.region,
            &endpoint_url,
        ),
    ));
    let backup_state: fakecloud_backup::SharedBackupState = Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new(
            &cli.account_id,
            &cli.region,
            &endpoint_url,
        ),
    ));
    let ram_state: fakecloud_ram::SharedRamState = Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new(
            &cli.account_id,
            &cli.region,
            &endpoint_url,
        ),
    ));
    let s3tables_state: fakecloud_s3tables::SharedS3TablesState = Arc::new(
        parking_lot::RwLock::new(fakecloud_core::multi_account::MultiAccountState::new(
            &cli.account_id,
            &cli.region,
            &endpoint_url,
        )),
    );
    let lakeformation_state: fakecloud_lakeformation::SharedLakeFormationState = Arc::new(
        parking_lot::RwLock::new(fakecloud_core::multi_account::MultiAccountState::new(
            &cli.account_id,
            &cli.region,
            &endpoint_url,
        )),
    );
    let codeconnections_state: fakecloud_codeconnections::SharedCodeConnectionsState = Arc::new(
        parking_lot::RwLock::new(fakecloud_core::multi_account::MultiAccountState::new(
            &cli.account_id,
            &cli.region,
            &endpoint_url,
        )),
    );
    let codebuild_state: fakecloud_codebuild::SharedCodeBuildState = Arc::new(
        parking_lot::RwLock::new(fakecloud_core::multi_account::MultiAccountState::new(
            &cli.account_id,
            &cli.region,
            &endpoint_url,
        )),
    );
    let codepipeline_state: fakecloud_codepipeline::SharedCodePipelineState = Arc::new(
        parking_lot::RwLock::new(fakecloud_core::multi_account::MultiAccountState::new(
            &cli.account_id,
            &cli.region,
            &endpoint_url,
        )),
    );
    let codeartifact_state: fakecloud_codeartifact::SharedCodeArtifactState = Arc::new(
        parking_lot::RwLock::new(fakecloud_core::multi_account::MultiAccountState::new(
            &cli.account_id,
            &cli.region,
            &endpoint_url,
        )),
    );
    let emr_state: fakecloud_emr::SharedEmrState = Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new(
            &cli.account_id,
            &cli.region,
            &endpoint_url,
        ),
    ));
    let textract_state: fakecloud_textract::SharedTextractState = Arc::new(
        parking_lot::RwLock::new(fakecloud_core::multi_account::MultiAccountState::new(
            &cli.account_id,
            &cli.region,
            &endpoint_url,
        )),
    );
    let comprehend_state: fakecloud_comprehend::SharedComprehendState = Arc::new(
        parking_lot::RwLock::new(fakecloud_core::multi_account::MultiAccountState::new(
            &cli.account_id,
            &cli.region,
            &endpoint_url,
        )),
    );
    let support_state: fakecloud_support::SharedSupportState = Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new(
            &cli.account_id,
            &cli.region,
            &endpoint_url,
        ),
    ));
    let transcribe_state: fakecloud_transcribe::SharedTranscribeState = Arc::new(
        parking_lot::RwLock::new(fakecloud_core::multi_account::MultiAccountState::new(
            &cli.account_id,
            &cli.region,
            &endpoint_url,
        )),
    );
    let translate_state: fakecloud_translate::SharedTranslateState = Arc::new(
        parking_lot::RwLock::new(fakecloud_core::multi_account::MultiAccountState::new(
            &cli.account_id,
            &cli.region,
            &endpoint_url,
        )),
    );
    let swf_state: fakecloud_swf::SharedSwfState = Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new(
            &cli.account_id,
            &cli.region,
            &endpoint_url,
        ),
    ));
    let timestream_state: fakecloud_timestream::SharedTimestreamState = Arc::new(
        parking_lot::RwLock::new(fakecloud_core::multi_account::MultiAccountState::new(
            &cli.account_id,
            &cli.region,
            &endpoint_url,
        )),
    );
    let shield_state: fakecloud_shield::SharedShieldState = Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new(
            &cli.account_id,
            &cli.region,
            &endpoint_url,
        ),
    ));
    let efs_state: fakecloud_efs::SharedEfsState = Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new(
            &cli.account_id,
            &cli.region,
            &endpoint_url,
        ),
    ));
    let mq_state: fakecloud_mq::SharedMqState = Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new(
            &cli.account_id,
            &cli.region,
            &endpoint_url,
        ),
    ));
    let kafka_state: fakecloud_kafka::SharedKafkaState = Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new(
            &cli.account_id,
            &cli.region,
            &endpoint_url,
        ),
    ));
    let codecommit_state: fakecloud_codecommit::SharedCodeCommitState = Arc::new(
        parking_lot::RwLock::new(fakecloud_core::multi_account::MultiAccountState::new(
            &cli.account_id,
            &cli.region,
            &endpoint_url,
        )),
    );
    // Elastic Beanstalk state is created here (ahead of its full service wiring
    // further below) so the CloudFormation provisioner can share it: an
    // AWS::ElasticBeanstalk::* resource writes through to this state.
    let beanstalk_state: fakecloud_elasticbeanstalk::SharedEbState = Arc::new(
        parking_lot::RwLock::new(fakecloud_elasticbeanstalk::EbAccounts::new()),
    );
    let codedeploy_state: fakecloud_codedeploy::SharedCodeDeployState = Arc::new(
        parking_lot::RwLock::new(fakecloud_core::multi_account::MultiAccountState::new(
            &cli.account_id,
            &cli.region,
            &endpoint_url,
        )),
    );
    let opensearch_state: fakecloud_opensearch::SharedOpenSearchState = Arc::new(
        parking_lot::RwLock::new(fakecloud_core::multi_account::MultiAccountState::new(
            &cli.account_id,
            &cli.region,
            &endpoint_url,
        )),
    );
    let appconfig_state: fakecloud_appconfig::SharedAppConfigState = Arc::new(
        parking_lot::RwLock::new(fakecloud_core::multi_account::MultiAccountState::new(
            &cli.account_id,
            &cli.region,
            &endpoint_url,
        )),
    );
    let mwaa_state: fakecloud_mwaa::SharedMwaaState = Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new(
            &cli.account_id,
            &cli.region,
            &endpoint_url,
        ),
    ));
    let appsync_state: fakecloud_appsync::SharedAppSyncState = Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new(
            &cli.account_id,
            &cli.region,
            &endpoint_url,
        ),
    ));
    let xray_state: fakecloud_xray::SharedXrayState = Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new(
            &cli.account_id,
            &cli.region,
            &endpoint_url,
        ),
    ));
    let amplify_state: fakecloud_amplify::SharedAmplifyState = Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new(
            &cli.account_id,
            &cli.region,
            &endpoint_url,
        ),
    ));
    let mediaconvert_state: fakecloud_mediaconvert::SharedMediaConvertState = Arc::new(
        parking_lot::RwLock::new(fakecloud_core::multi_account::MultiAccountState::new(
            &cli.account_id,
            &cli.region,
            &endpoint_url,
        )),
    );
    let serverlessrepo_state: fakecloud_serverlessrepo::SharedServerlessRepoState = Arc::new(
        parking_lot::RwLock::new(fakecloud_core::multi_account::MultiAccountState::new(
            &cli.account_id,
            &cli.region,
            &endpoint_url,
        )),
    );
    let iotdata_state: fakecloud_iotdata::SharedIotDataState = Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new(
            &cli.account_id,
            &cli.region,
            &endpoint_url,
        ),
    ));
    let pinpoint_state: fakecloud_pinpoint::SharedPinpointState = Arc::new(
        parking_lot::RwLock::new(fakecloud_core::multi_account::MultiAccountState::new(
            &cli.account_id,
            &cli.region,
            &endpoint_url,
        )),
    );
    let iot_state: fakecloud_iot::SharedIotState = Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new(
            &cli.account_id,
            &cli.region,
            &endpoint_url,
        ),
    ));
    let iotwireless_state: fakecloud_iotwireless::SharedIotWirelessState = Arc::new(
        parking_lot::RwLock::new(fakecloud_core::multi_account::MultiAccountState::new(
            &cli.account_id,
            &cli.region,
            &endpoint_url,
        )),
    );
    let sagemaker_state: fakecloud_sagemaker::SharedSageMakerState = Arc::new(
        parking_lot::RwLock::new(fakecloud_core::multi_account::MultiAccountState::new(
            &cli.account_id,
            &cli.region,
            &endpoint_url,
        )),
    );
    let managedblockchain_state: fakecloud_managedblockchain::SharedManagedBlockchainState =
        Arc::new(parking_lot::RwLock::new(
            fakecloud_core::multi_account::MultiAccountState::new(
                &cli.account_id,
                &cli.region,
                &endpoint_url,
            ),
        ));
    let fis_state: fakecloud_fis::SharedFisState = Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new(
            &cli.account_id,
            &cli.region,
            &endpoint_url,
        ),
    ));
    let resource_groups_tagging_state:
        fakecloud_resource_groups_tagging::SharedResourceGroupsTaggingState = Arc::new(
        parking_lot::RwLock::new(fakecloud_core::multi_account::MultiAccountState::new(
            &cli.account_id,
            &cli.region,
            &endpoint_url,
        )),
    );
    let cloudcontrol_state: fakecloud_cloudcontrol::SharedCloudControlState = Arc::new(
        parking_lot::RwLock::new(fakecloud_core::multi_account::MultiAccountState::new(
            &cli.account_id,
            &cli.region,
            &endpoint_url,
        )),
    );
    let rds_runtime = if fakecloud_k8s::backend_choice("FAKECLOUD_RDS_BACKEND")
        == fakecloud_k8s::Backend::K8s
    {
        match fakecloud_rds::runtime::RdsRuntime::new_k8s(bound_addr.port()).await {
            Ok(rt) => Some(Arc::new(rt)),
            Err(e) => {
                eprintln!(
                    "Kubernetes RDS backend selected (FAKECLOUD_RDS_BACKEND/FAKECLOUD_CONTAINER_BACKEND=k8s) but failed to initialize: {e}"
                );
                std::process::exit(1);
            }
        }
    } else {
        fakecloud_rds::runtime::RdsRuntime::new(bound_addr.port()).map(Arc::new)
    };
    if let Some(ref rt) = rds_runtime {
        // Sweep DB Pods left by a previous process (k8s only; no-op on the
        // Docker backend, handled by the shared container reaper).
        rt.reap_stale().await;
        tracing::info!(
            cli = rt.cli_name(),
            "RDS execution enabled via container runtime"
        );
    } else {
        degraded_runtimes.push("RDS (CreateDBInstance and snapshot/replica ops return errors)");
    }
    let elasticache_runtime = if fakecloud_k8s::backend_choice("FAKECLOUD_ELASTICACHE_BACKEND")
        == fakecloud_k8s::Backend::K8s
    {
        match fakecloud_elasticache::runtime::ElastiCacheRuntime::new_k8s(
            bound_addr.port(),
            (*k8s_internal_token).clone(),
        )
        .await
        {
            Ok(rt) => Some(Arc::new(rt)),
            Err(e) => {
                eprintln!(
                    "Kubernetes ElastiCache backend selected (FAKECLOUD_ELASTICACHE_BACKEND/FAKECLOUD_CONTAINER_BACKEND=k8s) but failed to initialize: {e}"
                );
                std::process::exit(1);
            }
        }
    } else {
        fakecloud_elasticache::runtime::ElastiCacheRuntime::new().map(Arc::new)
    };
    if let Some(ref rt) = elasticache_runtime {
        // Sweep cache Pods left by a previous process (k8s only; no-op on
        // the Docker backend, which the shared container reaper handles).
        rt.reap_stale().await;
        tracing::info!(
            cli = rt.cli_name(),
            "ElastiCache execution enabled via container runtime"
        );
    } else {
        degraded_runtimes.push("ElastiCache (metadata-only clusters, no cache data plane)");
    }
    // Amazon MQ backing-broker runtime (Docker/Podman). Constructed here so the
    // degraded-runtimes banner reports it; attached to the MqService further
    // down where the service is built.
    let mq_runtime = fakecloud_mq::MqRuntime::new().map(Arc::new);
    if let Some(ref rt) = mq_runtime {
        tracing::info!(
            cli = rt.cli_name(),
            "MQ broker execution enabled via container runtime"
        );
    } else {
        degraded_runtimes.push("MQ (control-plane-only brokers, no connectable broker data plane)");
    }
    // Amazon MSK (Kafka) backing-broker runtime (Docker/Podman). Constructed
    // here so the degraded-runtimes banner reports it; attached to the
    // KafkaService further down where the service is built.
    let kafka_runtime = fakecloud_kafka::KafkaRuntime::new().map(Arc::new);
    if let Some(ref rt) = kafka_runtime {
        tracing::info!(
            cli = rt.cli_name(),
            "MSK Kafka broker execution enabled via container runtime"
        );
    } else {
        degraded_runtimes
            .push("MSK (control-plane-only clusters, no connectable Kafka broker data plane)");
    }
    // ECS runtime is constructed below, after the EventBridge + CloudWatch
    // Logs wiring is in place. Placeholder kept here so downstream blocks
    // that reference `ecs_runtime` don't need reordering — see the
    // `ecs_runtime = ...` assignment after the delivery bus setup.
    let ecs_runtime: Option<Arc<fakecloud_ecs::runtime::EcsRuntime>>;
    // Cross-service delivery bus
    // Dirty flags shared between the synchronous delivery impls and the
    // background flushers wired after the snapshot stores load. The delivery
    // trait is sync and can't reach the async snapshot writer, so fan-out
    // messages/records are persisted by polling these flags. bug-audit 4.8.
    let sqs_delivery_dirty = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let kinesis_delivery_dirty = Arc::new(std::sync::atomic::AtomicBool::new(false));
    // Step 1: SQS delivery (SNS and EventBridge can push messages into SQS queues)
    let sqs_delivery = Arc::new(
        fakecloud_sqs::delivery::SqsDeliveryImpl::new(sqs_state.clone())
            .with_kms_hook(kms_hook_for_services.clone())
            .with_dirty_flag(sqs_delivery_dirty.clone()),
    );
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
    let kinesis_delivery_for_eb = fakecloud_kinesis::delivery::KinesisDeliveryImpl::with_dirty_flag(
        kinesis_state.clone(),
        kinesis_delivery_dirty.clone(),
    );
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
        Arc::new(
            stepfunctions_delivery::StepFunctionsDeliveryImpl::new(
                stepfunctions_state.clone(),
                Some(Arc::new(sfn_interpreter_bus)),
                Some(dynamodb_state.clone()),
            )
            .with_registry(sfn_registry_handle.clone()),
        )
    };
    let delivery_for_eb = Arc::new(
        DeliveryBus::new()
            .with_sqs(sqs_delivery.clone())
            .with_sns(sns_delivery.clone())
            .with_kinesis(kinesis_delivery_for_eb.clone())
            .with_stepfunctions(sfn_delivery_for_eb),
    );
    // Step 3: S3 delivery (S3 notifications can push to SQS, SNS, Lambda, and EventBridge)
    let sns_delivery_for_ses = sns_delivery.clone();
    let sns_delivery_for_cf = sns_delivery.clone();
    let sns_delivery_for_scheduler = sns_delivery.clone();
    let sns_delivery_for_scheduler_eb = sns_delivery.clone();
    let sns_delivery_for_scheduler_sfn_eb = sns_delivery.clone();
    let sns_delivery_for_rds = sns_delivery.clone();
    let eb_delivery_for_s3 = Arc::new(
        fakecloud_eventbridge::delivery::EventBridgeDeliveryImpl::new(
            eb_state.clone(),
            Arc::new(DeliveryBus::new().with_sqs(sqs_delivery.clone())),
        ),
    );
    let delivery_for_s3 = {
        let mut bus = DeliveryBus::new()
            .with_sqs(sqs_delivery.clone())
            .with_sns(sns_delivery.clone())
            .with_eventbridge(eb_delivery_for_s3);
        if let Some(ref ld) = lambda_delivery {
            bus = bus.with_lambda(ld.clone());
        }
        Arc::new(bus)
    };
    // CloudWatch state must be constructed before logs delivery so
    // CloudWatch Logs metric filters can publish data points into it.
    let cloudwatch_state: fakecloud_cloudwatch::SharedCloudWatchState = Arc::new(
        parking_lot::RwLock::new(fakecloud_cloudwatch::CloudWatchAccounts::new()),
    );
    // Step 4: Logs delivery (subscription filters can push to SQS, Lambda, and Kinesis;
    // metric filters publish CloudWatch metric data points)
    let sqs_delivery_for_ses = sqs_delivery.clone();
    let kinesis_delivery = fakecloud_kinesis::delivery::KinesisDeliveryImpl::with_dirty_flag(
        kinesis_state.clone(),
        kinesis_delivery_dirty.clone(),
    );
    let kinesis_delivery_for_dynamodb =
        fakecloud_kinesis::delivery::KinesisDeliveryImpl::with_dirty_flag(
            kinesis_state.clone(),
            kinesis_delivery_dirty.clone(),
        );
    let s3_delivery_for_logs = Arc::new(fakecloud_s3::delivery::S3DeliveryImpl::new(
        s3_state.clone(),
    ));
    let s3_delivery_for_rds = s3_delivery_for_logs.clone();
    // Firehose state is constructed once and shared between the public
    // FirehoseService (registered later) and the cross-service delivery
    // hook the Logs subscription dispatch uses for `arn:aws:firehose:`
    // destinations. Building it here keeps the wiring in one place.
    let firehose_state: fakecloud_firehose::SharedFirehoseState = Arc::new(
        parking_lot::RwLock::new(fakecloud_firehose::FirehoseAccounts::new()),
    );
    let firehose_delivery_for_logs =
        Arc::new(fakecloud_firehose::delivery::FirehoseDeliveryImpl::new(
            firehose_state.clone(),
            s3_state.clone(),
        ));
    let cloudwatch_delivery_for_logs = Arc::new(fakecloud_cloudwatch::CloudwatchDeliveryImpl::new(
        cloudwatch_state.clone(),
    ));
    let mut delivery_for_logs = DeliveryBus::new()
        .with_sqs(sqs_delivery.clone())
        .with_kinesis(kinesis_delivery)
        .with_s3(s3_delivery_for_logs.clone())
        .with_firehose(firehose_delivery_for_logs.clone())
        .with_cloudwatch_metrics(cloudwatch_delivery_for_logs.clone());
    if let Some(ref ld) = lambda_delivery {
        delivery_for_logs = delivery_for_logs.with_lambda(ld.clone());
    }
    let delivery_for_logs = Arc::new(delivery_for_logs);
    // Step 4b: DynamoDB delivery (Kinesis streaming destinations)
    let delivery_for_dynamodb =
        Arc::new(DeliveryBus::new().with_kinesis(kinesis_delivery_for_dynamodb));
    // Step 4c: ECS runtime, wired with EventBridge + CloudWatch Logs so
    // task state transitions emit `aws.ecs` events and `awslogs`-driver
    // output forwards to CloudWatch Logs. Built here so `sqs_delivery`
    // (the EventBridge SQS target) is available for rule fan-out.
    let eb_delivery_for_ecs = Arc::new(
        fakecloud_eventbridge::delivery::EventBridgeDeliveryImpl::new(
            eb_state.clone(),
            Arc::new(DeliveryBus::new().with_sqs(sqs_delivery.clone())),
        ),
    );
    let elbv2_state: fakecloud_elbv2::SharedElbv2State = Arc::new(parking_lot::RwLock::new(
        fakecloud_elbv2::Elbv2Accounts::new(),
    ));
    let ecs_delivery_bus = Arc::new(
        DeliveryBus::new()
            .with_eventbridge(eb_delivery_for_ecs)
            .with_elbv2_target_registration(Arc::new(Elbv2TargetRegistrationImpl {
                state: elbv2_state.clone(),
            })),
    );
    let ecs_base = if fakecloud_k8s::backend_choice("FAKECLOUD_ECS_BACKEND")
        == fakecloud_k8s::Backend::K8s
    {
        match fakecloud_ecs::runtime::EcsRuntime::new_k8s(bound_addr.port()).await {
            Ok(rt) => Some(rt),
            Err(e) => {
                eprintln!(
                    "Kubernetes ECS backend selected (FAKECLOUD_ECS_BACKEND/FAKECLOUD_CONTAINER_BACKEND=k8s) but failed to initialize: {e}"
                );
                std::process::exit(1);
            }
        }
    } else {
        fakecloud_ecs::runtime::EcsRuntime::new(bound_addr.port())
    };
    ecs_runtime = ecs_base
        .map(|rt| {
            rt.with_delivery_bus(ecs_delivery_bus.clone())
                .with_logs(logs_state.clone())
                .with_secretsmanager(secretsmanager_state.clone())
                .with_ssm(ssm_state.clone())
        })
        .map(Arc::new);
    if let Some(ref rt) = ecs_runtime {
        // Sweep task Pods left by a previous process (k8s only; no-op on
        // the Docker backend, handled by the shared container reaper).
        rt.reap_stale().await;
        tracing::info!(
            cli = rt.cli_name(),
            "ECS task execution enabled via container runtime"
        );
    } else {
        degraded_runtimes.push("ECS (RunTask fails with TaskFailedToStart)");
    }
    // EC2 instance backing runtime. FAKECLOUD_EC2_BACKEND=k8s (or the global
    // FAKECLOUD_CONTAINER_BACKEND=k8s) runs instances as native Kubernetes
    // Pods; anything else (or unset) uses the local Docker/Podman CLI. When no
    // container CLI is present, RunInstances serves metadata-only instances so
    // the control plane still works everywhere.
    let ec2_runtime: Option<Arc<fakecloud_ec2::runtime::Ec2Runtime>> =
        if fakecloud_k8s::backend_choice("FAKECLOUD_EC2_BACKEND") == fakecloud_k8s::Backend::K8s {
            match fakecloud_ec2::runtime::Ec2Runtime::new_k8s(bound_addr.port()).await {
                Ok(rt) => Some(Arc::new(rt)),
                Err(e) => {
                    eprintln!(
                        "Kubernetes EC2 backend selected (FAKECLOUD_EC2_BACKEND/FAKECLOUD_CONTAINER_BACKEND=k8s) but failed to initialize: {e}"
                    );
                    std::process::exit(1);
                }
            }
        } else {
            fakecloud_ec2::runtime::Ec2Runtime::new().map(Arc::new)
        };
    if let Some(ref rt) = ec2_runtime {
        // Sweep instance Pods left by a previous process (k8s only; no-op on
        // the Docker backend, handled by the shared container reaper).
        rt.reap_stale().await;
        tracing::info!(
            cli = rt.cli_name(),
            "EC2 instance execution enabled via container runtime"
        );
    } else {
        degraded_runtimes.push("EC2 (RunInstances serves metadata-only instances)");
    }
    // One consolidated banner instead of five scattered lines: if any
    // container-backed service is degraded, warn once and tell the operator
    // exactly how to enable them.
    if !degraded_runtimes.is_empty() {
        tracing::warn!(
            "No container runtime (Docker/Podman) detected. The following services run in degraded or metadata-only mode and will fail or serve metadata only for operations that need a real container: {}. {}",
            degraded_runtimes.join("; "),
            fakecloud_core::container_net::CONTAINER_RUNTIME_HINT
        );
    }
    // Clone state refs for internal endpoints
    let lambda_invocations_state = lambda_state.clone();
    let ses_emails_state = ses_state.clone();
    let ses_inbound_state = ses_state.clone();
    let sns_introspection_state = sns_state.clone();
    let sns_sms_state = sns_state.clone();
    let sqs_introspection_state = sqs_state.clone();
    let eb_introspection_state = eb_state.clone();
    let s3_introspection_state = s3_state.clone();
    let s3_access_points_introspection_state = s3_state.clone();
    let s3_object_lambda_introspection_state = s3_state.clone();
    let rds_bridge_s3_state = s3_state.clone();
    let rds_introspection_state = rds_state.clone();
    let elasticache_introspection_state = elasticache_state.clone();
    let athena_introspection_state = athena_state.clone();
    let ecr_introspection_state = ecr_state.clone();
    let ecs_introspection_state = ecs_state.clone();
    let dynamodb_ttl_state = dynamodb_state.clone();
    // TTL-expiry REMOVE records flow to a table's Kinesis streaming
    // destinations through the same delivery bus the service uses.
    let dynamodb_ttl_delivery = delivery_for_dynamodb.clone();
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
    let lambda_layer_content_state = lambda_state.clone();
    let sns_sim_pending_state = sns_state.clone();
    let sns_sim_confirm_state = sns_state.clone();
    // Clone state refs for Cognito simulation endpoints
    let cognito_codes_state = cognito_state.clone();
    let cognito_confirm_state = cognito_state.clone();
    let cognito_tokens_state = cognito_state.clone();
    let cognito_expire_state = cognito_state.clone();
    let cognito_events_state = cognito_state.clone();
    let cognito_jwks_state = cognito_state.clone();
    let cognito_oidc_state = cognito_state.clone();
    let cognito_token_state = cognito_state.clone();
    let cognito_userinfo_state = cognito_state.clone();
    let cognito_revoke_state = cognito_state.clone();
    let cognito_authorize_state = cognito_state.clone();
    let glue_state: fakecloud_glue::SharedGlueState =
        Arc::new(parking_lot::RwLock::new(fakecloud_glue::GlueAccounts::new()));
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
        ecr: ecr_state.clone(),
        ecs: ecs_state.clone(),
        cloudfront: cloudfront_state.clone(),
        route53: route53_state.clone(),
        acm: acm_state.clone(),
        acmpca: acmpca_state.clone(),
        config: config_state.clone(),
        route53resolver: route53resolver_state.clone(),
        firehose: firehose_state.clone(),
        glue: glue_state.clone(),
        cloudwatch: cloudwatch_state.clone(),
        application_autoscaling: app_autoscaling_state.clone(),
        wafv2: wafv2_state.clone(),
        athena: athena_state.clone(),
        stepfunctions: stepfunctions_state.clone(),
        scheduler: scheduler_state.clone(),
        apigatewayv1: apigatewayv1_state.clone(),
        apigatewayv2: apigatewayv2_state.clone(),
        bedrock: bedrock_state.clone(),
        bedrock_agent: bedrock_agent_state.clone(),
        bedrock_agent_runtime: bedrock_agent_runtime_state.clone(),
        organizations: organizations_state.clone(),
        container_runtime: container_runtime.clone(),
        rds_runtime: rds_runtime.clone(),
        elasticache_runtime: elasticache_runtime.clone(),
        ecs_runtime: ecs_runtime.clone(),
        ec2: ec2_state.clone(),
        ec2_runtime: ec2_runtime.clone(),
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
                    match serde_json::from_slice::<fakecloud_cloudformation::CloudFormationSnapshot>(
                        &bytes,
                    ) {
                        Ok(snapshot) => {
                            if snapshot.schema_version
                                > fakecloud_cloudformation::CLOUDFORMATION_SNAPSHOT_SCHEMA_VERSION
                            {
                                fatal_exit(format_args!(
                                    "cloudformation persistence schema mismatch: on-disk={}, expected={}",
                                    snapshot.schema_version,
                                    fakecloud_cloudformation::CLOUDFORMATION_SNAPSHOT_SCHEMA_VERSION,
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
        fakecloud_cloudformation::CloudFormationDeps {
            sqs: sqs_state.clone(),
            sns: sns_state.clone(),
            ssm: ssm_state.clone(),
            iam: iam_state.clone(),
            s3: s3_state.clone(),
            eventbridge: eb_state.clone(),
            dynamodb: dynamodb_state.clone(),
            logs: logs_state.clone(),
            lambda: lambda_state.clone(),
            secretsmanager: secretsmanager_state.clone(),
            kinesis: kinesis_state.clone(),
            kms: kms_state.clone(),
            ecr: ecr_state.clone(),
            cloudwatch: cloudwatch_state.clone(),
            elbv2: elbv2_state.clone(),
            organizations: organizations_state.clone(),
            cognito: cognito_state.clone(),
            rds: rds_state.clone(),
            ec2: ec2_state.clone(),
            autoscaling: autoscaling_state.clone(),
            batch: batch_state.clone(),
            pipes: pipes_state.clone(),
            ecs: ecs_state.clone(),
            acm: acm_state.clone(),
            acmpca: acmpca_state.clone(),
            config: config_state.clone(),
            route53resolver: route53resolver_state.clone(),
            elasticache: elasticache_state.clone(),
            route53: route53_state.clone(),
            cloudfront: cloudfront_state.clone(),
            stepfunctions: stepfunctions_state.clone(),
            wafv2: wafv2_state.clone(),
            apigateway: apigatewayv1_state.clone(),
            apigatewayv2: apigatewayv2_state.clone(),
            ses: ses_state.clone(),
            application_autoscaling: app_autoscaling_state.clone(),
            athena: athena_state.clone(),
            firehose: firehose_state.clone(),
            glue: glue_state.clone(),
            eks: eks_state.clone(),
            servicediscovery: servicediscovery_state.clone(),
            codeartifact: codeartifact_state.clone(),
            codecommit: codecommit_state.clone(),
            efs: efs_state.clone(),
            elasticbeanstalk: beanstalk_state.clone(),
            mq: mq_state.clone(),
            kafka: kafka_state.clone(),
            kinesisanalyticsv2: kinesisanalyticsv2_state.clone(),
            delivery: delivery_for_cf,
            lambda_runtime: container_runtime.clone(),
            rds_runtime: rds_runtime.clone(),
            ec2_runtime: ec2_runtime.clone(),
            ecs_runtime: ecs_runtime.clone(),
            elasticache_runtime: elasticache_runtime.clone(),
            mq_runtime: mq_runtime.clone(),
            kafka_runtime: kafka_runtime.clone(),
        },
    );
    if let Some(store) = cloudformation_snapshot_store {
        cloudformation_service = cloudformation_service.with_snapshot_store(store);
    }
    // The CloudFormation provisioner persists every snapshot-backed service a
    // stack op touches by invoking that service's snapshot hook. We collect the
    // hooks as each service is built below, then register CloudFormation last
    // (after the S3 store and all hooks exist) via `with_s3_store` /
    // `with_snapshot_hooks`. Keyed by the service names in
    // `service_key_for_type`.
    let mut cfn_snapshot_hooks: std::collections::BTreeMap<
        &'static str,
        fakecloud_persistence::SnapshotHook,
    > = std::collections::BTreeMap::new();
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
                    match serde_json::from_slice::<fakecloud_sqs::SqsSnapshot>(&bytes) {
                        Ok(snapshot) => {
                            if snapshot.schema_version > fakecloud_sqs::SQS_SNAPSHOT_SCHEMA_VERSION
                            {
                                fatal_exit(format_args!(
                                    "sqs persistence schema too new: on-disk={}, max supported={}",
                                    snapshot.schema_version,
                                    fakecloud_sqs::SQS_SNAPSHOT_SCHEMA_VERSION,
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
    let mut sqs_service = SqsService::new(sqs_state.clone())
        .with_kms_hook(kms_hook_for_services.clone())
        .with_region(cli.region.clone());
    if let Some(store) = sqs_snapshot_store.clone() {
        sqs_service = sqs_service.with_snapshot_store(store);
    }
    // Capture the SQS snapshot hook once: shared by the CFN provisioner and the
    // SQS->Lambda event source poller so poller-driven acks/checkpoints persist.
    let sqs_poller_snapshot_hook = sqs_service.snapshot_hook();
    if let Some(h) = sqs_poller_snapshot_hook.clone() {
        cfn_snapshot_hooks.insert("sqs", h);
    }
    // Resume any in-progress message-move task left RUNNING/CANCELLING by a
    // previous process so it doesn't hang forever and the DLQ drain continues.
    let sqs_service = Arc::new(sqs_service);
    sqs_service.resume_message_move_tasks();
    registry.register(sqs_service);
    // Flush cross-service SQS deliveries (SNS/EventBridge/S3/Scheduler fan-out)
    // that the sync delivery trait cannot persist itself. bug-audit 4.8.
    if let Some(store) = sqs_snapshot_store {
        tokio::spawn(fakecloud_sqs::delivery::run_delivery_flusher(
            sqs_state.clone(),
            store,
            sqs_delivery_dirty.clone(),
            std::time::Duration::from_millis(500),
        ));
    }
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
                    match serde_json::from_slice::<fakecloud_sns::SnsSnapshot>(&bytes) {
                        Ok(snapshot) => {
                            if snapshot.schema_version > fakecloud_sns::SNS_SNAPSHOT_SCHEMA_VERSION
                            {
                                fatal_exit(format_args!(
                                    "sns persistence schema too new: on-disk={}, max supported={}",
                                    snapshot.schema_version,
                                    fakecloud_sns::SNS_SNAPSHOT_SCHEMA_VERSION,
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
    let mut sns_service = SnsService::new(sns_state.clone(), delivery_for_sns)
        .with_kms_hook(kms_hook_for_services.clone())
        .with_region(cli.region.clone());
    if let Some(store) = sns_snapshot_store {
        sns_service = sns_service.with_snapshot_store(store);
    }
    if let Some(h) = sns_service.snapshot_hook() {
        cfn_snapshot_hooks.insert("sns", h);
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
                    match serde_json::from_slice::<fakecloud_eventbridge::EventBridgeSnapshot>(
                        &bytes,
                    ) {
                        Ok(snapshot) => {
                            if snapshot.schema_version
                                > fakecloud_eventbridge::EVENTBRIDGE_SNAPSHOT_SCHEMA_VERSION
                            {
                                fatal_exit(format_args!(
                                    "eventbridge persistence schema too new: on-disk={}, max supported={}",
                                    snapshot.schema_version,
                                    fakecloud_eventbridge::EVENTBRIDGE_SNAPSHOT_SCHEMA_VERSION,
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
    if let Some(h) = eb_service.snapshot_hook() {
        cfn_snapshot_hooks.insert("eventbridge", h);
    }
    registry.register(Arc::new(eb_service));
    // Spawn the EventBridge scheduler as a background task
    let eb_state_for_ses = eb_state.clone();
    let eb_state_for_sfn = eb_state.clone();
    let eb_state_for_scheduler = eb_state.clone();
    let eb_state_for_rds = eb_state.clone();
    let eb_state_for_lambda = eb_state.clone();
    let mut scheduler =
        fakecloud_eventbridge::scheduler::Scheduler::new(eb_state.clone(), delivery_for_eb)
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
                    match serde_json::from_slice::<fakecloud_iam::IamSnapshot>(&bytes) {
                        Ok(snapshot) => {
                            if snapshot.schema_version > fakecloud_iam::IAM_SNAPSHOT_SCHEMA_VERSION
                            {
                                fatal_exit(format_args!(
                                    "iam persistence schema too new: on-disk={}, max supported={}",
                                    snapshot.schema_version,
                                    fakecloud_iam::IAM_SNAPSHOT_SCHEMA_VERSION,
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
    let mut sts_service = StsService::new(iam_state.clone())
        .with_snapshot_lock(iam_snapshot_lock)
        .with_org_membership(
            fakecloud_organizations::resolver::OrganizationsMembershipResolver::shared(
                organizations_state.clone(),
            ),
        );
    if let Some(store) = iam_snapshot_store {
        sts_service = sts_service.with_snapshot_store(store);
    }
    if let Some(h) = iam_service.snapshot_hook() {
        cfn_snapshot_hooks.insert("iam", h);
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
                    match serde_json::from_slice::<fakecloud_ssm::SsmSnapshot>(&bytes) {
                        Ok(snapshot) => {
                            if snapshot.schema_version > fakecloud_ssm::SSM_SNAPSHOT_SCHEMA_VERSION
                            {
                                fatal_exit(format_args!(
                                    "ssm persistence schema mismatch: on-disk={}, expected={}",
                                    snapshot.schema_version,
                                    fakecloud_ssm::SSM_SNAPSHOT_SCHEMA_VERSION,
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
    let ssm_state_for_admin = ssm_state.clone();
    let ssm_state_for_fail = ssm_state.clone();
    let ssm_state_for_policy_events = ssm_state.clone();
    let ssm_state_for_session_inject = ssm_state.clone();
    let mut ssm_service = SsmService::new(ssm_state)
        .with_secretsmanager(secretsmanager_state.clone())
        .with_kms_hook(kms_hook_for_services.clone());
    if let Some(store) = ssm_snapshot_store {
        ssm_service = ssm_service.with_snapshot_store(store);
    }
    if let Some(h) = ssm_service.snapshot_hook() {
        cfn_snapshot_hooks.insert("ssm", h);
    }
    registry.register(Arc::new(ssm_service));
    // DynamoDB is registered later, after s3_store is constructed, so the
    // export path can persist result objects through the S3 store.
    let dynamodb_state_for_register = dynamodb_state.clone();
    let delivery_for_dynamodb_register = delivery_for_dynamodb;
    let mut lambda_service = LambdaService::new(lambda_state.clone());
    lambda_service = lambda_service.with_role_trust_validator(
        fakecloud_iam::pass_role::IamRoleTrustValidator::shared(iam_state.clone()),
    );
    lambda_service = lambda_service.with_s3_delivery(s3_delivery_for_logs.clone());
    if let Some(ref rt) = container_runtime {
        lambda_service = lambda_service.with_runtime(rt.clone());
    }
    // Async-invoke destinations (OnSuccess/OnFailure) route to SQS / SNS /
    // EventBridge / Lambda by ARN scheme.
    let mut lambda_destinations_inner = DeliveryBus::new()
        .with_sqs(sqs_delivery.clone())
        .with_sns(sns_delivery.clone())
        .with_cloudwatch_metrics(cloudwatch_delivery_for_logs.clone());
    if let Some(ref ld) = lambda_delivery {
        lambda_destinations_inner = lambda_destinations_inner.with_lambda(ld.clone());
    }
    let lambda_destinations_bus = Arc::new(
        lambda_destinations_inner.with_eventbridge(Arc::new(
            fakecloud_eventbridge::delivery::EventBridgeDeliveryImpl::new(
                eb_state_for_lambda,
                Arc::new(
                    DeliveryBus::new()
                        .with_sqs(sqs_delivery.clone())
                        .with_sns(sns_delivery.clone()),
                ),
            ),
        )),
    );
    lambda_service = lambda_service.with_delivery_bus(lambda_destinations_bus);
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
                    match serde_json::from_slice::<fakecloud_lambda::LambdaSnapshot>(&bytes) {
                        Ok(snapshot) => {
                            if snapshot.schema_version
                                > fakecloud_lambda::LAMBDA_SNAPSHOT_SCHEMA_VERSION
                            {
                                fatal_exit(format_args!(
                                    "lambda persistence schema too new: on-disk={}, max supported={}",
                                    snapshot.schema_version,
                                    fakecloud_lambda::LAMBDA_SNAPSHOT_SCHEMA_VERSION,
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
    if let Some(h) = lambda_service.snapshot_hook() {
        cfn_snapshot_hooks.insert("lambda", h);
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
                    match serde_json::from_slice::<fakecloud_secretsmanager::SecretsManagerSnapshot>(
                        &bytes,
                    ) {
                        Ok(snapshot) => {
                            if snapshot.schema_version
                                > fakecloud_secretsmanager::SECRETSMANAGER_SNAPSHOT_SCHEMA_VERSION
                            {
                                fatal_exit(format_args!(
                                    "secretsmanager persistence schema too new: on-disk={}, max supported={}",
                                    snapshot.schema_version,
                                    fakecloud_secretsmanager::SECRETSMANAGER_SNAPSHOT_SCHEMA_VERSION,
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
    // Clone the snapshot store for the rotation scheduler's /tick route, which
    // mutates secret state outside the service's action-dispatch path and so
    // must write through itself (bug-audit 2026-06-20, 0.A3).
    let secretsmanager_rotation_snapshot_store = secretsmanager_snapshot_store.clone();
    let mut secretsmanager_service =
        SecretsManagerService::new(secretsmanager_state).with_delivery(delivery_for_secretsmanager);
    secretsmanager_service = secretsmanager_service.with_kms_hook(kms_hook_for_services.clone());
    if let Some(store) = secretsmanager_snapshot_store {
        secretsmanager_service = secretsmanager_service.with_snapshot_store(store);
    }
    if let Some(h) = secretsmanager_service.snapshot_hook() {
        cfn_snapshot_hooks.insert("secretsmanager", h);
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
                    match serde_json::from_slice::<fakecloud_logs::LogsSnapshot>(&bytes) {
                        Ok(snapshot) => {
                            if snapshot.schema_version
                                > fakecloud_logs::LOGS_SNAPSHOT_SCHEMA_VERSION
                            {
                                fatal_exit(format_args!(
                                    "logs persistence schema too new: on-disk={}, max supported={}",
                                    snapshot.schema_version,
                                    fakecloud_logs::LOGS_SNAPSHOT_SCHEMA_VERSION,
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
    let logs_anomalies_state = logs_state.clone();
    let mut logs_service = LogsService::new(logs_state.clone(), delivery_for_logs);
    if let Some(store) = logs_snapshot_store {
        logs_service = logs_service.with_snapshot_store(store);
    }
    if let Some(h) = logs_service.snapshot_hook() {
        cfn_snapshot_hooks.insert("logs", h);
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
                    match serde_json::from_slice::<fakecloud_kms::KmsSnapshot>(&bytes) {
                        Ok(snapshot) => {
                            if snapshot.schema_version > fakecloud_kms::KMS_SNAPSHOT_SCHEMA_VERSION
                            {
                                fatal_exit(format_args!(
                                    "kms persistence schema too new: on-disk={}, max supported={}",
                                    snapshot.schema_version,
                                    fakecloud_kms::KMS_SNAPSHOT_SCHEMA_VERSION,
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
    if let Some(store) = kms_snapshot_store.clone() {
        kms_service = kms_service.with_snapshot_store(store);
    }
    if let Some(h) = kms_service.snapshot_hook() {
        cfn_snapshot_hooks.insert("kms", h);
    }
    registry.register(Arc::new(kms_service));
    // Wire the snapshot store into the hook adapter too, so hook-driven
    // auto-provisioning (`aws/<service>` first-use) persists immediately.
    if let Some(store) = kms_snapshot_store {
        kms_hook_adapter.set_snapshot_store(store);
    }
    let organizations_snapshot_store: Option<Arc<dyn fakecloud_persistence::SnapshotStore>> =
        if persistence_config.mode == fakecloud_persistence::StorageMode::Persistent {
            let data_path = persistence_config
                .data_path
                .as_ref()
                .expect("validated above")
                .clone();
            let path = data_path.join("organizations").join("snapshot.json");
            let store = fakecloud_persistence::DiskSnapshotStore::new(path);
            match fakecloud_persistence::SnapshotStore::load(&store) {
                Ok(Some(bytes)) => {
                    match serde_json::from_slice::<fakecloud_organizations::OrganizationsSnapshot>(
                        &bytes,
                    ) {
                        Ok(snapshot) => {
                            if snapshot.schema_version
                                > fakecloud_organizations::ORGANIZATIONS_SNAPSHOT_SCHEMA_VERSION
                            {
                                fatal_exit(format_args!(
                                    "organizations persistence schema too new: on-disk={}, max supported={}",
                                    snapshot.schema_version,
                                    fakecloud_organizations::ORGANIZATIONS_SNAPSHOT_SCHEMA_VERSION,
                                ));
                            }
                            let present = snapshot.organization.is_some();
                            *organizations_state.write() = snapshot.organization;
                            tracing::info!(
                                organization = present,
                                "loaded organizations persistence snapshot"
                            );
                        }
                        Err(err) => fatal_exit(format_args!(
                            "failed to parse organizations persistence snapshot: {err}"
                        )),
                    }
                }
                Ok(None) => {
                    tracing::info!("no organizations persistence snapshot found; starting empty");
                }
                Err(err) => fatal_exit(format_args!(
                    "failed to read organizations persistence snapshot: {err}"
                )),
            }
            Some(Arc::new(store) as Arc<dyn fakecloud_persistence::SnapshotStore>)
        } else {
            None
        };
    let mut organizations_inner = OrganizationsService::new(organizations_state.clone());
    if let Some(store) = organizations_snapshot_store.clone() {
        organizations_inner = organizations_inner.with_snapshot_store(store);
    }
    if let Some(h) = organizations_inner.snapshot_hook() {
        cfn_snapshot_hooks.insert("organizations", h);
    }
    // Re-arm CreateAccount completion ticks for requests restored as IN_PROGRESS.
    organizations_inner.rearm_in_progress_account_creations();
    // Hook shared with the create-admin admin endpoint, which auto-enrolls an
    // account into the org directly and must persist that through to disk.
    let organizations_persist_hook = organizations_inner.snapshot_hook();
    registry.register(Arc::new(organizations_inner));
    // EC2 (ec2Query protocol). Instances are backed by the optional container
    // runtime (Docker/Podman); persistence is wired in later batches. We keep a
    // clone of the shared state so the introspection router can expose
    // `GET /_fakecloud/ec2/instances`.
    let ec2_snapshot_store: Option<Arc<dyn fakecloud_persistence::SnapshotStore>> =
        if persistence_config.mode == fakecloud_persistence::StorageMode::Persistent {
            let data_path = persistence_config
                .data_path
                .as_ref()
                .expect("validated above")
                .clone();
            let path = data_path.join("ec2").join("snapshot.json");
            let store = fakecloud_persistence::DiskSnapshotStore::new(path);
            match fakecloud_persistence::SnapshotStore::load(&store) {
                Ok(Some(bytes)) => {
                    match serde_json::from_slice::<fakecloud_ec2::Ec2Snapshot>(&bytes) {
                        Ok(snapshot) => {
                            if snapshot.schema_version > fakecloud_ec2::EC2_SNAPSHOT_SCHEMA_VERSION
                            {
                                fatal_exit(format_args!(
                                    "ec2 persistence schema too new: on-disk={}, max supported={}",
                                    snapshot.schema_version,
                                    fakecloud_ec2::EC2_SNAPSHOT_SCHEMA_VERSION,
                                ));
                            }
                            if let Some(accounts) = snapshot.accounts {
                                let account_count = accounts.account_count();
                                *ec2_state.write() = accounts;
                                // Backfill the public AMI catalogue into any
                                // restored account that predates it (legacy
                                // snapshot from before #1964). Idempotent —
                                // seeds have deterministic ids, so already-seeded
                                // accounts are unchanged.
                                {
                                    let mut guard = ec2_state.write();
                                    for (_, st) in guard.iter_mut() {
                                        st.ensure_public_images_seeded();
                                    }
                                }
                                tracing::info!(
                                    accounts = account_count,
                                    "loaded ec2 persistence snapshot"
                                );
                            }
                        }
                        Err(err) => fatal_exit(format_args!(
                            "failed to parse ec2 persistence snapshot: {err}"
                        )),
                    }
                }
                Ok(None) => {
                    tracing::info!("no ec2 persistence snapshot found; starting empty");
                }
                Err(err) => fatal_exit(format_args!(
                    "failed to read ec2 persistence snapshot: {err}"
                )),
            }
            Some(Arc::new(store) as Arc<dyn fakecloud_persistence::SnapshotStore>)
        } else {
            None
        };
    let mut ec2_service =
        Ec2Service::with_state(ec2_state.clone()).with_runtime(ec2_runtime.clone());
    if let Some(store) = ec2_snapshot_store.clone() {
        ec2_service = ec2_service.with_snapshot_store(store);
    }
    if let Some(h) = ec2_service.snapshot_hook() {
        cfn_snapshot_hooks.insert("ec2", h);
    }
    let ec2_introspection_state = ec2_state.clone();
    // Separate clones for the instance-networks introspection endpoint (#1745),
    // which also needs the runtime to report the isolation backend + SG
    // enforcement mode.
    let ec2_networks_state = ec2_state.clone();
    let ec2_networks_runtime = ec2_runtime.clone();
    // Recreate the backing containers for persisted EC2 instances that the
    // snapshot claims should be running. The startup reaper (above) already
    // removed the previous process's containers (their owning PID is dead), so
    // a persisted `running` instance would otherwise point at a removed
    // container with a silently-dead lifecycle. Fire-and-forget: spawns one
    // task per instance and returns immediately (bug-hunt 2026-06-15 0.3).
    ec2_service.recover_persisted_containers().await;
    registry.register(Arc::new(ec2_service));
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
            .with_kms(kms_state.clone())
            .with_kms_hook(kms_hook_for_services.clone()),
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
                    match serde_json::from_slice::<fakecloud_dynamodb::DynamoDbSnapshot>(&bytes) {
                        Ok(snapshot) => {
                            if snapshot.schema_version
                                > fakecloud_dynamodb::DYNAMODB_SNAPSHOT_SCHEMA_VERSION
                            {
                                fatal_exit(format_args!(
                                    "dynamodb persistence schema too new: on-disk={}, max supported={}",
                                    snapshot.schema_version,
                                    fakecloud_dynamodb::DYNAMODB_SNAPSHOT_SCHEMA_VERSION,
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
    // Optional: bulk-load an AWS-format DynamoDB export straight into the
    // internal store before the service is built. Additive — no BatchWriteItem,
    // no effect on the recorded `ImportTable` op. Both inputs required together.
    if let (Some(import_path), Some(describe_path)) = (
        cli.dynamodb_import_path.as_ref(),
        cli.dynamodb_import_describe_table.as_ref(),
    ) {
        let describe_bytes = std::fs::read(describe_path).unwrap_or_else(|e| {
            fatal_exit(format_args!(
                "failed to read describe-table file {}: {e}",
                describe_path.display()
            ))
        });
        let describe: serde_json::Value =
            serde_json::from_slice(&describe_bytes).unwrap_or_else(|e| {
                fatal_exit(format_args!(
                    "failed to parse describe-table JSON {}: {e}",
                    describe_path.display()
                ))
            });
        match fakecloud_dynamodb::import_aws_export(
            &dynamodb_state_for_register,
            &cli.account_id,
            &cli.region,
            import_path,
            &describe,
        ) {
            Ok(fakecloud_dynamodb::ImportOutcome::Imported { table, items }) => {
                tracing::info!(table, items, "bulk-loaded AWS DynamoDB export at startup")
            }
            // Idempotent restart: the table was already present (e.g. from a
            // persisted snapshot). The importer already logged a warning and
            // left the existing data untouched, so booting continues normally.
            Ok(fakecloud_dynamodb::ImportOutcome::SkippedExisting { table }) => tracing::info!(
                table,
                "skipped AWS DynamoDB export import: table already exists in state"
            ),
            Err(e) => fatal_exit(format_args!("dynamodb export import failed: {e}")),
        }
    } else if cli.dynamodb_import_path.is_some() || cli.dynamodb_import_describe_table.is_some() {
        fatal_exit(format_args!(
            "--dynamodb-import-path and --dynamodb-import-describe-table must be provided together"
        ));
    }

    // Keep a clone of the snapshot store (and a dedicated write lock) for the
    // `/_fakecloud/dynamodb/ttl-processor/tick` admin route, which mutates
    // state outside any handler and must persist the result the same way the
    // normal mutating API path does.
    let dynamodb_ttl_snapshot_store = dynamodb_snapshot_store.clone();
    let dynamodb_ttl_snapshot_lock = Arc::new(tokio::sync::Mutex::new(()));
    let mut dynamodb_service = DynamoDbService::new(dynamodb_state_for_register)
        .with_s3(s3_state.clone())
        .with_s3_store(s3_store.clone())
        .with_delivery(delivery_for_dynamodb_register)
        .with_kms_hook(kms_hook_for_services.clone())
        .with_region(cli.region.clone());
    if let Some(store) = dynamodb_snapshot_store {
        dynamodb_service = dynamodb_service.with_snapshot_store(store);
    }
    // Capture the DynamoDB snapshot hook once: shared by the CFN provisioner and
    // the DynamoDB-Streams->Lambda poller so poller-driven checkpoints persist.
    let dynamodb_poller_snapshot_hook = dynamodb_service.snapshot_hook();
    if let Some(h) = dynamodb_poller_snapshot_hook.clone() {
        cfn_snapshot_hooks.insert("dynamodb", h);
    }
    let dynamodb_service = Arc::new(dynamodb_service);
    registry.register(dynamodb_service.clone());
    // Companion data plane: DynamoDB Streams (`streams.dynamodb.<region>.amazonaws.com`)
    // shares the same per-table state populated by mutations on the main
    // service. Lambda event source mappings against
    // `arn:aws:dynamodb:.../stream/...` poll this handler.
    registry.register(Arc::new(fakecloud_dynamodb::DynamoDbStreamsService::new(
        dynamodb_state.clone(),
    )));
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
            .with_eventbridge(eb_delivery_for_ses)
            .with_kinesis(kinesis_delivery_for_eb.clone())
            .with_firehose(firehose_delivery_for_logs.clone())
            .with_cloudwatch_metrics(cloudwatch_delivery_for_logs.clone()),
    );
    let ses_delivery_ctx = fakecloud_ses::fanout::SesDeliveryContext {
        ses_state: ses_state.clone(),
        delivery_bus: delivery_for_ses,
        account_id: cli.account_id.clone(),
        region: cli.region.clone(),
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
                    match serde_json::from_slice::<fakecloud_ses::SesSnapshot>(&bytes) {
                        Ok(snapshot) => {
                            if snapshot.schema_version > fakecloud_ses::SES_SNAPSHOT_SCHEMA_VERSION
                            {
                                fatal_exit(format_args!(
                                    "ses persistence schema too new: on-disk={}, max supported={}",
                                    snapshot.schema_version,
                                    fakecloud_ses::SES_SNAPSHOT_SCHEMA_VERSION,
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
    let mut ses_service = SesV2Service::new(ses_state.clone()).with_delivery(ses_delivery_ctx);
    if let Some(store) = ses_snapshot_store {
        ses_service = ses_service.with_snapshot_store(store);
    }
    if let Some(h) = ses_service.snapshot_hook() {
        cfn_snapshot_hooks.insert("ses", h);
    }
    registry.register(Arc::new(ses_service));
    ses_smtp::maybe_spawn(iam_state.clone(), ses_state.clone());
    let delivery_for_cognito = {
        let mut bus = DeliveryBus::new();
        if let Some(ref ld) = lambda_delivery {
            bus = bus.with_lambda(ld.clone());
        }
        Arc::new(bus)
    };
    let cognito_email_dispatcher: Arc<dyn fakecloud_core::delivery::EmailDispatcher> =
        Arc::new(SesEmailDispatcher {
            state: ses_state.clone(),
        });
    let cognito_sms_dispatcher: Arc<dyn fakecloud_core::delivery::SmsDispatcher> =
        Arc::new(SnsSmsDispatcher {
            state: sns_state.clone(),
        });
    let cognito_delivery_ctx =
        fakecloud_cognito::triggers::CognitoDeliveryContext::new(delivery_for_cognito)
            .with_email(cognito_email_dispatcher)
            .with_sms(cognito_sms_dispatcher);
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
                    match serde_json::from_slice::<fakecloud_cognito::CognitoSnapshot>(&bytes) {
                        Ok(snapshot) => {
                            if snapshot.schema_version
                                > fakecloud_cognito::COGNITO_SNAPSHOT_SCHEMA_VERSION
                            {
                                fatal_exit(format_args!(
                                    "cognito persistence schema too new: on-disk={}, max supported={}",
                                    snapshot.schema_version,
                                    fakecloud_cognito::COGNITO_SNAPSHOT_SCHEMA_VERSION,
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
    // The Cognito Hosted-UI OAuth2 endpoints (/oauth2/token, /oauth2/authorize,
    // /oauth2/revoke, and the code-mint helper) mutate the token/code maps
    // directly from their own axum routes, outside the service's action-dispatch
    // path that snapshots -- so without writing through here, refresh/access
    // tokens, authorization codes, and revocations were lost on restart
    // (bug-audit 2026-06-20, 0.A4). Hand the routes a clone of the store and a
    // shared lock; snapshot files are written atomically.
    let cognito_oauth2_snapshot_store = cognito_snapshot_store.clone();
    let cognito_oauth2_snapshot_lock = Arc::new(tokio::sync::Mutex::new(()));
    let mut cognito_service =
        CognitoService::new(cognito_state.clone()).with_delivery(cognito_delivery_ctx);
    if let Some(store) = cognito_snapshot_store {
        cognito_service = cognito_service.with_snapshot_store(store);
    }
    if let Some(h) = cognito_service.snapshot_hook() {
        cfn_snapshot_hooks.insert("cognito", h);
    }
    registry.register(Arc::new(cognito_service));
    // Cognito Federated Identity Pools (`cognito-identity` service).
    // Shares state with the user-pool service above; lives in the same
    // crate so persistence + reset stay coupled. Holds the IAM state so
    // `GetCredentialsForIdentity` can mint real STS-style temp creds.
    registry.register(Arc::new(fakecloud_cognito::CognitoIdentityService::new(
        cognito_state.clone(),
        iam_state.clone(),
    )));
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
                    match serde_json::from_slice::<fakecloud_kinesis::KinesisSnapshot>(&bytes) {
                        Ok(snapshot) => {
                            if snapshot.schema_version
                                > fakecloud_kinesis::KINESIS_SNAPSHOT_SCHEMA_VERSION
                            {
                                fatal_exit(format_args!(
                                    "kinesis persistence schema too new: on-disk={}, max supported={}",
                                    snapshot.schema_version,
                                    fakecloud_kinesis::KINESIS_SNAPSHOT_SCHEMA_VERSION,
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
    if let Some(store) = kinesis_snapshot_store.clone() {
        kinesis_service = kinesis_service.with_snapshot_store(store);
    }
    // Capture the Kinesis snapshot hook once: shared by the CFN provisioner and
    // the Kinesis->Lambda poller so poller-driven checkpoints persist.
    let kinesis_poller_snapshot_hook = kinesis_service.snapshot_hook();
    if let Some(h) = kinesis_poller_snapshot_hook.clone() {
        cfn_snapshot_hooks.insert("kinesis", h);
    }
    registry.register(Arc::new(kinesis_service));
    // Flush cross-service Kinesis deliveries (DynamoDB streaming / Logs
    // subscription / EventBridge target) that the sync delivery trait cannot
    // persist itself. bug-audit 4.8.
    if let Some(store) = kinesis_snapshot_store {
        tokio::spawn(fakecloud_kinesis::delivery::run_delivery_flusher(
            kinesis_state.clone(),
            store,
            kinesis_delivery_dirty.clone(),
            std::time::Duration::from_millis(500),
        ));
    }
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
                    match serde_json::from_slice::<fakecloud_rds::RdsSnapshot>(&bytes) {
                        Ok(snapshot) => {
                            if snapshot.schema_version > fakecloud_rds::RDS_SNAPSHOT_SCHEMA_VERSION
                            {
                                fatal_exit(format_args!(
                                    "rds persistence schema too new: on-disk={}, max supported={}",
                                    snapshot.schema_version,
                                    fakecloud_rds::RDS_SNAPSHOT_SCHEMA_VERSION,
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
                            // Keep any `creating` placeholder rows the snapshot
                            // captured mid-CreateDBInstance. CreateDBInstance
                            // already acknowledged them to the client, so
                            // DescribeDBInstances must not lose them on restart.
                            // `recover_persisted_containers` (below) re-drives
                            // `creating` rows through `ensure_*` to a live
                            // container — dropping them here would make that
                            // recovery branch dead code and silently vanish an
                            // acknowledged instance.
                            {
                                let mut mas = rds_state.write();
                                for (_, state) in mas.iter_mut() {
                                    // Clear any in-flight identifier reservations a
                                    // restore/replica op left in the snapshot. The
                                    // task that held them is dead, so a leftover
                                    // reservation would otherwise brick that id with
                                    // DBInstanceAlreadyExists forever (bug-audit
                                    // 2026-06-26, 4.2).
                                    state.in_progress_instance_ids.clear();
                                }
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
    let mut rds_service = RdsService::new(rds_state.clone());
    if let Some(ref rt) = rds_runtime {
        rds_service = rds_service.with_runtime(rt.clone());
    }
    if let Some(store) = rds_snapshot_store {
        rds_service = rds_service.with_snapshot_store(store);
    }
    // aws.rds events on lifecycle ops: rule targets see SQS/SNS via the
    // inner bus; more targets mirror what ECS wires.
    let eb_delivery_for_rds = Arc::new(
        fakecloud_eventbridge::delivery::EventBridgeDeliveryImpl::new(
            eb_state_for_rds,
            Arc::new(
                DeliveryBus::new()
                    .with_sqs(sqs_delivery.clone())
                    .with_sns(sns_delivery_for_rds),
            ),
        ),
    );
    let mut rds_bus = DeliveryBus::new()
        .with_eventbridge(eb_delivery_for_rds)
        .with_s3(s3_delivery_for_rds);
    if let Some(ref ld) = lambda_delivery {
        rds_bus = rds_bus.with_lambda(ld.clone());
    }
    let rds_delivery_bus = Arc::new(rds_bus);
    rds_service = rds_service.with_delivery_bus(rds_delivery_bus.clone());
    // Recreate backing containers for persisted DB instances that the
    // snapshot claims should be running. Fire-and-forget: the method
    // spawns one task per instance and returns immediately, so a slow
    // postgres bring-up doesn't block server startup. (Issue #1338.)
    rds_service.recover_persisted_containers().await;
    if let Some(h) = rds_service.snapshot_hook() {
        cfn_snapshot_hooks.insert("rds", h);
    }
    registry.register(Arc::new(rds_service));
    // RDS Data API (rds-data): runs real SQL against the RDS container DBs.
    registry.register(Arc::new(fakecloud_rds_data::RdsDataService::new(
        rds_state.clone(),
    )));
    // Amazon DocumentDB (docdb): RDS-shaped Query API, control-plane only
    // (no backing MongoDB-compatible engine image exists, so clusters and
    // instances are records with well-formed endpoints — see the crate docs).
    let docdb_snapshot_store: Option<Arc<dyn fakecloud_persistence::SnapshotStore>> =
        if persistence_config.mode == fakecloud_persistence::StorageMode::Persistent {
            let data_path = persistence_config
                .data_path
                .as_ref()
                .expect("validated above")
                .clone();
            let path = data_path.join("docdb").join("snapshot.json");
            let store = fakecloud_persistence::DiskSnapshotStore::new(path);
            match fakecloud_persistence::SnapshotStore::load(&store) {
                Ok(Some(bytes)) => {
                    match serde_json::from_slice::<fakecloud_docdb::DocDbSnapshot>(&bytes) {
                        Ok(snapshot) => {
                            if snapshot.schema_version
                                > fakecloud_docdb::DOCDB_SNAPSHOT_SCHEMA_VERSION
                            {
                                fatal_exit(format_args!(
                                    "docdb persistence schema too new: on-disk={}, max supported={}",
                                    snapshot.schema_version,
                                    fakecloud_docdb::DOCDB_SNAPSHOT_SCHEMA_VERSION,
                                ));
                            }
                            if let Some(accounts) = snapshot.accounts {
                                let account_count = accounts.account_count();
                                *docdb_state.write() = accounts;
                                tracing::info!(
                                    accounts = account_count,
                                    "loaded docdb persistence snapshot",
                                );
                            }
                        }
                        Err(err) => fatal_exit(format_args!(
                            "failed to parse docdb persistence snapshot: {err}"
                        )),
                    }
                }
                Ok(None) => {
                    tracing::info!("no docdb persistence snapshot found; starting empty");
                }
                Err(err) => fatal_exit(format_args!(
                    "failed to read docdb persistence snapshot: {err}"
                )),
            }
            Some(Arc::new(store) as Arc<dyn fakecloud_persistence::SnapshotStore>)
        } else {
            None
        };
    let mut docdb_service = fakecloud_docdb::DocDbService::new(docdb_state.clone());
    if let Some(store) = docdb_snapshot_store {
        docdb_service = docdb_service.with_snapshot_store(store);
    }
    if let Some(h) = docdb_service.snapshot_hook() {
        cfn_snapshot_hooks.insert("docdb", h);
    }
    registry.register(Arc::new(docdb_service));
    // Amazon Neptune (neptune): RDS-shaped Query API, control-plane only
    // (no backing Gremlin/SPARQL graph engine image exists, so clusters and
    // instances are records with well-formed endpoints — see the crate docs).
    let neptune_snapshot_store: Option<Arc<dyn fakecloud_persistence::SnapshotStore>> =
        if persistence_config.mode == fakecloud_persistence::StorageMode::Persistent {
            let data_path = persistence_config
                .data_path
                .as_ref()
                .expect("validated above")
                .clone();
            let path = data_path.join("neptune").join("snapshot.json");
            let store = fakecloud_persistence::DiskSnapshotStore::new(path);
            match fakecloud_persistence::SnapshotStore::load(&store) {
                Ok(Some(bytes)) => {
                    match serde_json::from_slice::<fakecloud_neptune::NeptuneSnapshot>(&bytes) {
                        Ok(snapshot) => {
                            if snapshot.schema_version
                                > fakecloud_neptune::NEPTUNE_SNAPSHOT_SCHEMA_VERSION
                            {
                                fatal_exit(format_args!(
                                    "neptune persistence schema too new: on-disk={}, max supported={}",
                                    snapshot.schema_version,
                                    fakecloud_neptune::NEPTUNE_SNAPSHOT_SCHEMA_VERSION,
                                ));
                            }
                            if let Some(accounts) = snapshot.accounts {
                                let account_count = accounts.account_count();
                                *neptune_state.write() = accounts;
                                tracing::info!(
                                    accounts = account_count,
                                    "loaded neptune persistence snapshot",
                                );
                            }
                        }
                        Err(err) => fatal_exit(format_args!(
                            "failed to parse neptune persistence snapshot: {err}"
                        )),
                    }
                }
                Ok(None) => {
                    tracing::info!("no neptune persistence snapshot found; starting empty");
                }
                Err(err) => fatal_exit(format_args!(
                    "failed to read neptune persistence snapshot: {err}"
                )),
            }
            Some(Arc::new(store) as Arc<dyn fakecloud_persistence::SnapshotStore>)
        } else {
            None
        };
    let mut neptune_service = fakecloud_neptune::NeptuneService::new(neptune_state.clone());
    if let Some(store) = neptune_snapshot_store {
        neptune_service = neptune_service.with_snapshot_store(store);
    }
    if let Some(h) = neptune_service.snapshot_hook() {
        cfn_snapshot_hooks.insert("neptune", h);
    }
    registry.register(Arc::new(neptune_service));
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
                    match serde_json::from_slice::<fakecloud_elasticache::ElastiCacheSnapshot>(
                        &bytes,
                    ) {
                        Ok(snapshot) => {
                            if snapshot.schema_version
                                > fakecloud_elasticache::ELASTICACHE_SNAPSHOT_SCHEMA_VERSION
                            {
                                fatal_exit(format_args!(
                                    "elasticache persistence schema too new: on-disk={}, max supported={}",
                                    snapshot.schema_version,
                                    fakecloud_elasticache::ELASTICACHE_SNAPSHOT_SCHEMA_VERSION,
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
    let mut elasticache_service =
        ElastiCacheService::new(elasticache_state).with_s3(s3_state.clone());
    if let Some(ref rt) = elasticache_runtime {
        elasticache_service = elasticache_service.with_runtime(rt.clone());
    }
    if let Some(store) = elasticache_snapshot_store {
        elasticache_service = elasticache_service.with_snapshot_store(store);
    }
    // Same restart-recovery contract as RDS: persisted clusters /
    // replication groups / serverless caches survive a restart but
    // their Docker containers don't, so respawn them on startup. See
    // RDS #1338 for the original bug class.
    elasticache_service.recover_persisted_containers().await;
    if let Some(h) = elasticache_service.snapshot_hook() {
        cfn_snapshot_hooks.insert("elasticache", h);
    }
    registry.register(Arc::new(elasticache_service));
    let ecr_snapshot_store: Option<Arc<dyn fakecloud_persistence::SnapshotStore>> =
        if persistence_config.mode == fakecloud_persistence::StorageMode::Persistent {
            let data_path = persistence_config
                .data_path
                .as_ref()
                .expect("validated above")
                .clone();
            let path = data_path.join("ecr").join("snapshot.json");
            let store = fakecloud_persistence::DiskSnapshotStore::new(path);
            match fakecloud_persistence::SnapshotStore::load(&store) {
                Ok(Some(bytes)) => {
                    match serde_json::from_slice::<fakecloud_ecr::EcrSnapshot>(&bytes) {
                        Ok(snapshot) => {
                            if snapshot.schema_version > fakecloud_ecr::ECR_SNAPSHOT_SCHEMA_VERSION
                            {
                                fatal_exit(format_args!(
                                    "ecr persistence schema too new: on-disk={}, max supported={}",
                                    snapshot.schema_version,
                                    fakecloud_ecr::ECR_SNAPSHOT_SCHEMA_VERSION,
                                ));
                            }
                            if let Some(accounts) = snapshot.accounts {
                                let account_count = accounts.account_count();
                                *ecr_state.write() = accounts;
                                tracing::info!(
                                    accounts = account_count,
                                    "loaded ecr persistence snapshot (multi-account)"
                                );
                            }
                        }
                        Err(err) => fatal_exit(format_args!(
                            "failed to parse ecr persistence snapshot: {err}"
                        )),
                    }
                }
                Ok(None) => {
                    tracing::info!("no ecr persistence snapshot found; starting empty");
                }
                Err(err) => fatal_exit(format_args!(
                    "failed to read ecr persistence snapshot: {err}"
                )),
            }
            Some(Arc::new(store) as Arc<dyn fakecloud_persistence::SnapshotStore>)
        } else {
            None
        };
    let mut ecr_service = EcrService::new(ecr_state.clone()).with_kms(kms_state.clone());
    if let Some(store) = ecr_snapshot_store.clone() {
        ecr_service = ecr_service.with_snapshot_store(store);
    }
    if let Some(h) = ecr_service.snapshot_hook() {
        cfn_snapshot_hooks.insert("ecr", h);
    }
    registry.register(Arc::new(ecr_service));
    // Periodic re-evaluation of ECR lifecycle policies. The ticker
    // re-runs the prune evaluator on every repository with a policy
    // set so time-based selections (e.g. `sinceImagePushed`) take
    // effect even when no new push triggers an evaluation. The tick
    // is a cheap read-only scan when no policies are set. A pruning tick
    // is persisted via the snapshot store so evicted images don't
    // resurrect on restart (bug-audit 4.6). The store is crash-safe
    // (atomic rename), so a fresh lock here is fine even though the
    // request path uses its own.
    let ecr_lifecycle_ticker = fakecloud_ecr::LifecycleTicker::new(ecr_state.clone())
        .with_snapshot(ecr_snapshot_store, Arc::new(tokio::sync::Mutex::new(())));
    tokio::spawn(ecr_lifecycle_ticker.run());
    let ecs_snapshot_store: Option<Arc<dyn fakecloud_persistence::SnapshotStore>> =
        if persistence_config.mode == fakecloud_persistence::StorageMode::Persistent {
            let data_path = persistence_config
                .data_path
                .as_ref()
                .expect("validated above")
                .clone();
            let path = data_path.join("ecs").join("snapshot.json");
            let store = fakecloud_persistence::DiskSnapshotStore::new(path);
            match fakecloud_persistence::SnapshotStore::load(&store) {
                Ok(Some(bytes)) => {
                    match serde_json::from_slice::<fakecloud_ecs::EcsSnapshot>(&bytes) {
                        Ok(snapshot) => {
                            if snapshot.schema_version > fakecloud_ecs::ECS_SNAPSHOT_SCHEMA_VERSION
                            {
                                fatal_exit(format_args!(
                                    "ecs persistence schema too new: on-disk={}, max supported={}",
                                    snapshot.schema_version,
                                    fakecloud_ecs::ECS_SNAPSHOT_SCHEMA_VERSION,
                                ));
                            }
                            if let Some(accounts) = snapshot.accounts {
                                let account_count = accounts.account_count();
                                *ecs_state.write() = accounts;
                                tracing::info!(
                                    accounts = account_count,
                                    "loaded ecs persistence snapshot (multi-account)"
                                );
                            }
                        }
                        Err(err) => fatal_exit(format_args!(
                            "failed to parse ecs persistence snapshot: {err}"
                        )),
                    }
                }
                Ok(None) => {
                    tracing::info!("no ecs persistence snapshot found; starting empty");
                }
                Err(err) => fatal_exit(format_args!(
                    "failed to read ecs persistence snapshot: {err}"
                )),
            }
            Some(Arc::new(store) as Arc<dyn fakecloud_persistence::SnapshotStore>)
        } else {
            None
        };
    let mut ecs_service = EcsService::new(ecs_state.clone());
    ecs_service = ecs_service.with_role_trust_validator(
        fakecloud_iam::pass_role::IamRoleTrustValidator::shared(iam_state.clone()),
    );
    if let Some(store) = ecs_snapshot_store {
        ecs_service = ecs_service.with_snapshot_store(store);
    }
    if let Some(ref rt) = ecs_runtime {
        ecs_service = ecs_service.with_runtime(rt.clone());
    }
    // Reconcile persisted task state with reality: persisted tasks
    // marked RUNNING survive a restart but the docker container does
    // not, so flip them to STOPPED and zero service counts. The
    // scheduler ticker brings services back to desiredCount. Same
    // restart-bug class as RDS #1338.
    ecs_service.reconcile_persisted_tasks().await;
    let ecs_service = Arc::new(ecs_service);
    let ecs_service_for_scheduler = ecs_service.clone();
    // ECS desiredCount scheduler ticker. reconcile_persisted_tasks STOPs all
    // tasks and zeroes counts on restart; this ticker is what brings each
    // service back up to its desiredCount (and re-launches tasks lost to
    // crashed containers during normal operation). bug-audit 4.7.
    let ecs_service_for_desired_count = ecs_service.clone();
    tokio::spawn(fakecloud_ecs::run_scheduler_ticker(
        ecs_service_for_desired_count,
        std::time::Duration::from_secs(3),
    ));
    if let Some(h) = ecs_service.snapshot_hook() {
        cfn_snapshot_hooks.insert("ecs", h);
    }
    registry.register(ecs_service);
    let elbv2_introspection_state = elbv2_state.clone();
    // Wire an S3-only delivery bus so the ALB dataplane can flush
    // gzipped access-log + connection-log batches to the bucket
    // referenced by the LB's `access_logs.s3.bucket` /
    // `connection_logs.s3.bucket` attributes.
    let s3_delivery_for_elbv2 = Arc::new(fakecloud_s3::delivery::S3DeliveryImpl::new(
        s3_state.clone(),
    ));
    let elbv2_delivery_bus = Arc::new(DeliveryBus::new().with_s3(s3_delivery_for_elbv2));
    let elbv2_snapshot_store: Option<Arc<dyn fakecloud_persistence::SnapshotStore>> =
        if persistence_config.mode == fakecloud_persistence::StorageMode::Persistent {
            let data_path = persistence_config
                .data_path
                .as_ref()
                .expect("validated above")
                .clone();
            let path = data_path.join("elbv2").join("snapshot.json");
            let store = fakecloud_persistence::DiskSnapshotStore::new(path);
            match fakecloud_persistence::SnapshotStore::load(&store) {
                Ok(Some(bytes)) => {
                    match serde_json::from_slice::<fakecloud_elbv2::Elbv2Snapshot>(&bytes) {
                        Ok(snapshot) => {
                            if snapshot.schema_version
                                > fakecloud_elbv2::ELBV2_SNAPSHOT_SCHEMA_VERSION
                            {
                                fatal_exit(format_args!(
                                    "elbv2 persistence schema too new: on-disk={}, max supported={}",
                                    snapshot.schema_version,
                                    fakecloud_elbv2::ELBV2_SNAPSHOT_SCHEMA_VERSION,
                                ));
                            }
                            if let Some(accounts) = snapshot.accounts {
                                *elbv2_state.write() = accounts;
                                tracing::info!("loaded elbv2 persistence snapshot");
                            }
                        }
                        Err(err) => fatal_exit(format_args!(
                            "failed to parse elbv2 persistence snapshot: {err}"
                        )),
                    }
                }
                Ok(None) => {
                    tracing::info!("no elbv2 persistence snapshot found; starting empty");
                }
                Err(err) => fatal_exit(format_args!(
                    "failed to read elbv2 persistence snapshot: {err}"
                )),
            }
            Some(Arc::new(store) as Arc<dyn fakecloud_persistence::SnapshotStore>)
        } else {
            None
        };
    let mut elbv2_inner = Elbv2Service::new_without_dataplane(elbv2_state.clone())
        .with_waf_state(wafv2_state.clone())
        .with_delivery_bus(elbv2_delivery_bus);
    if let Some(store) = elbv2_snapshot_store.clone() {
        elbv2_inner = elbv2_inner.with_snapshot_store(store);
    }
    if let Some(h) = elbv2_inner.snapshot_hook() {
        cfn_snapshot_hooks.insert("elbv2", h);
    }
    let elbv2_service = Arc::new(elbv2_inner);
    elbv2_service.start_dataplane();
    let elbv2_service_for_admin = elbv2_service.clone();
    registry.register(elbv2_service);
    let cloudfront_snapshot_store: Option<Arc<dyn fakecloud_persistence::SnapshotStore>> =
        if persistence_config.mode == fakecloud_persistence::StorageMode::Persistent {
            let data_path = persistence_config
                .data_path
                .as_ref()
                .expect("validated above")
                .clone();
            let path = data_path.join("cloudfront").join("snapshot.json");
            let store = fakecloud_persistence::DiskSnapshotStore::new(path);
            match fakecloud_persistence::SnapshotStore::load(&store) {
                Ok(Some(bytes)) => {
                    match serde_json::from_slice::<fakecloud_cloudfront::CloudFrontSnapshot>(&bytes)
                    {
                        Ok(snapshot) => {
                            if snapshot.schema_version
                                > fakecloud_cloudfront::CLOUDFRONT_SNAPSHOT_SCHEMA_VERSION
                            {
                                fatal_exit(format_args!(
                                    "cloudfront persistence schema too new: on-disk={}, max supported={}",
                                    snapshot.schema_version,
                                    fakecloud_cloudfront::CLOUDFRONT_SNAPSHOT_SCHEMA_VERSION,
                                ));
                            }
                            if let Some(accounts) = snapshot.accounts {
                                let account_count = accounts.account_count();
                                *cloudfront_state.write() = accounts;
                                tracing::info!(
                                    accounts = account_count,
                                    "loaded cloudfront persistence snapshot"
                                );
                            }
                        }
                        Err(err) => fatal_exit(format_args!(
                            "failed to parse cloudfront persistence snapshot: {err}"
                        )),
                    }
                }
                Ok(None) => {
                    tracing::info!("no cloudfront persistence snapshot found; starting empty");
                }
                Err(err) => fatal_exit(format_args!(
                    "failed to read cloudfront persistence snapshot: {err}"
                )),
            }
            Some(Arc::new(store) as Arc<dyn fakecloud_persistence::SnapshotStore>)
        } else {
            None
        };
    let mut cloudfront_inner = CloudFrontService::new(cloudfront_state.clone());
    if let Some(store) = cloudfront_snapshot_store.clone() {
        cloudfront_inner = cloudfront_inner.with_snapshot_store(store);
    }
    if let Some(h) = cloudfront_inner.snapshot_hook() {
        cfn_snapshot_hooks.insert("cloudfront", h);
    }
    // Re-arm propagation ticks for resources restored as InProgress so they
    // still transition to Deployed after a restart.
    cloudfront_inner.rearm_in_progress();
    let cloudfront_service = Arc::new(cloudfront_inner);
    registry.register(cloudfront_service.clone());
    // Build the in-process CloudFront data plane. Enabled distributions are served
    // on THIS main listener, routed by the `Host` header (their `<id>.cloudfront.net`
    // domain or an alias CNAME), via an outer middleware installed below. No second
    // port is opened, so a distribution is reachable from outside a container
    // whenever the main port is published. `bound_addr.port()` is passed so
    // S3-website origins (served by this same process) can be reached here.
    let cloudfront_dataplane = fakecloud_cloudfront::dataplane::CloudFrontDataPlane::new(
        cloudfront_service.shared_state(),
        bound_addr.port(),
    );
    let cloudfront_introspection_state = cloudfront_state.clone();
    let route53_snapshot_store: Option<Arc<dyn fakecloud_persistence::SnapshotStore>> =
        if persistence_config.mode == fakecloud_persistence::StorageMode::Persistent {
            let data_path = persistence_config
                .data_path
                .as_ref()
                .expect("validated above")
                .clone();
            let path = data_path.join("route53").join("snapshot.json");
            let store = fakecloud_persistence::DiskSnapshotStore::new(path);
            match fakecloud_persistence::SnapshotStore::load(&store) {
                Ok(Some(bytes)) => {
                    match serde_json::from_slice::<fakecloud_route53::Route53Snapshot>(&bytes) {
                        Ok(snapshot) => {
                            if snapshot.schema_version
                                > fakecloud_route53::ROUTE53_SNAPSHOT_SCHEMA_VERSION
                            {
                                fatal_exit(format_args!(
                                    "route53 persistence schema too new: on-disk={}, max supported={}",
                                    snapshot.schema_version,
                                    fakecloud_route53::ROUTE53_SNAPSHOT_SCHEMA_VERSION,
                                ));
                            }
                            if let Some(accounts) = snapshot.accounts {
                                let account_count = accounts.account_count();
                                *route53_state.write() = accounts;
                                tracing::info!(
                                    accounts = account_count,
                                    "loaded route53 persistence snapshot"
                                );
                            }
                        }
                        Err(err) => fatal_exit(format_args!(
                            "failed to parse route53 persistence snapshot: {err}"
                        )),
                    }
                }
                Ok(None) => {
                    tracing::info!("no route53 persistence snapshot found; starting empty");
                }
                Err(err) => fatal_exit(format_args!(
                    "failed to read route53 persistence snapshot: {err}"
                )),
            }
            Some(Arc::new(store) as Arc<dyn fakecloud_persistence::SnapshotStore>)
        } else {
            None
        };
    let mut route53_inner = fakecloud_route53::Route53Service::new(route53_state.clone())
        .with_logs(logs_state.clone())
        .with_elbv2(elbv2_state.clone())
        .with_cloudfront(cloudfront_state.clone())
        .with_s3(s3_state.clone());
    if let Some(store) = route53_snapshot_store.clone() {
        route53_inner = route53_inner.with_snapshot_store(store);
    }
    if let Some(h) = route53_inner.snapshot_hook() {
        cfn_snapshot_hooks.insert("route53", h);
    }
    let route53_service = Arc::new(route53_inner);
    registry.register(route53_service.clone());
    let acm_snapshot_store: Option<Arc<dyn fakecloud_persistence::SnapshotStore>> =
        if persistence_config.mode == fakecloud_persistence::StorageMode::Persistent {
            let data_path = persistence_config
                .data_path
                .as_ref()
                .expect("validated above")
                .clone();
            let path = data_path.join("acm").join("snapshot.json");
            let store = fakecloud_persistence::DiskSnapshotStore::new(path);
            match fakecloud_persistence::SnapshotStore::load(&store) {
                Ok(Some(bytes)) => {
                    match serde_json::from_slice::<fakecloud_acm::AcmSnapshot>(&bytes) {
                        Ok(snapshot) => {
                            if snapshot.schema_version > fakecloud_acm::ACM_SNAPSHOT_SCHEMA_VERSION
                            {
                                fatal_exit(format_args!(
                                    "acm persistence schema too new: on-disk={}, max supported={}",
                                    snapshot.schema_version,
                                    fakecloud_acm::ACM_SNAPSHOT_SCHEMA_VERSION,
                                ));
                            }
                            if let Some(accounts) = snapshot.accounts {
                                let account_count = accounts.accounts.len();
                                *acm_state.write() = accounts;
                                tracing::info!(
                                    accounts = account_count,
                                    "loaded acm persistence snapshot"
                                );
                            }
                        }
                        Err(err) => fatal_exit(format_args!(
                            "failed to parse acm persistence snapshot: {err}"
                        )),
                    }
                }
                Ok(None) => {
                    tracing::info!("no acm persistence snapshot found; starting empty");
                }
                Err(err) => fatal_exit(format_args!(
                    "failed to read acm persistence snapshot: {err}"
                )),
            }
            Some(Arc::new(store) as Arc<dyn fakecloud_persistence::SnapshotStore>)
        } else {
            None
        };
    let mut acm_inner = fakecloud_acm::AcmService::new(acm_state.clone());
    if let Some(store) = acm_snapshot_store.clone() {
        acm_inner = acm_inner.with_snapshot_store(store);
    }
    if let Some(h) = acm_inner.snapshot_hook() {
        cfn_snapshot_hooks.insert("acm", h);
    }
    // Re-arm auto-issue ticks for any DNS certs restored as PENDING_VALIDATION
    // so they still transition to ISSUED after a restart.
    acm_inner.rearm_pending_validations();
    let acm_service = Arc::new(acm_inner);
    registry.register(acm_service.clone());
    let acmpca_snapshot_store: Option<Arc<dyn fakecloud_persistence::SnapshotStore>> =
        if persistence_config.mode == fakecloud_persistence::StorageMode::Persistent {
            let data_path = persistence_config
                .data_path
                .as_ref()
                .expect("validated above")
                .clone();
            let path = data_path.join("acm-pca").join("snapshot.json");
            let store = fakecloud_persistence::DiskSnapshotStore::new(path);
            match fakecloud_persistence::SnapshotStore::load(&store) {
                Ok(Some(bytes)) => {
                    match serde_json::from_slice::<fakecloud_acmpca::AcmPcaSnapshot>(&bytes) {
                        Ok(snapshot) => {
                            if snapshot.schema_version
                                > fakecloud_acmpca::ACM_PCA_SNAPSHOT_SCHEMA_VERSION
                            {
                                fatal_exit(format_args!(
                                    "acm-pca persistence schema too new: on-disk={}, max supported={}",
                                    snapshot.schema_version,
                                    fakecloud_acmpca::ACM_PCA_SNAPSHOT_SCHEMA_VERSION,
                                ));
                            }
                            if let Some(accounts) = snapshot.accounts {
                                let account_count = accounts.accounts.len();
                                *acmpca_state.write() = accounts;
                                tracing::info!(
                                    accounts = account_count,
                                    "loaded acm-pca persistence snapshot"
                                );
                            }
                        }
                        Err(err) => fatal_exit(format_args!(
                            "failed to parse acm-pca persistence snapshot: {err}"
                        )),
                    }
                }
                Ok(None) => {
                    tracing::info!("no acm-pca persistence snapshot found; starting empty");
                }
                Err(err) => fatal_exit(format_args!(
                    "failed to read acm-pca persistence snapshot: {err}"
                )),
            }
            Some(Arc::new(store) as Arc<dyn fakecloud_persistence::SnapshotStore>)
        } else {
            None
        };
    let mut acmpca_inner =
        fakecloud_acmpca::AcmPcaService::new(acmpca_state.clone()).with_s3(s3_state.clone());
    if let Some(store) = acmpca_snapshot_store.clone() {
        acmpca_inner = acmpca_inner.with_snapshot_store(store);
    }
    if let Some(h) = acmpca_inner.snapshot_hook() {
        cfn_snapshot_hooks.insert("acm-pca", h);
    }
    // Re-arm key generation for any CA restored in CREATING (its key never
    // persisted because the previous process exited mid-keygen).
    acmpca_inner.rearm_pending_creations();
    let acmpca_service = Arc::new(acmpca_inner);
    registry.register(acmpca_service.clone());
    let route53resolver_snapshot_store: Option<Arc<dyn fakecloud_persistence::SnapshotStore>> =
        if persistence_config.mode == fakecloud_persistence::StorageMode::Persistent {
            let data_path = persistence_config
                .data_path
                .as_ref()
                .expect("validated above")
                .clone();
            let path = data_path.join("route53resolver").join("snapshot.json");
            let store = fakecloud_persistence::DiskSnapshotStore::new(path);
            match fakecloud_persistence::SnapshotStore::load(&store) {
                Ok(Some(bytes)) => {
                    match serde_json::from_slice::<fakecloud_route53resolver::Route53ResolverSnapshot>(
                        &bytes,
                    ) {
                        Ok(snapshot) => {
                            if snapshot.schema_version
                                > fakecloud_route53resolver::R53R_SNAPSHOT_SCHEMA_VERSION
                            {
                                fatal_exit(format_args!(
                                    "route53resolver persistence schema too new: on-disk={}, max supported={}",
                                    snapshot.schema_version,
                                    fakecloud_route53resolver::R53R_SNAPSHOT_SCHEMA_VERSION,
                                ));
                            }
                            if let Some(accounts) = snapshot.accounts {
                                let account_count = accounts.accounts.len();
                                *route53resolver_state.write() = accounts;
                                tracing::info!(
                                    accounts = account_count,
                                    "loaded route53resolver persistence snapshot"
                                );
                            }
                        }
                        Err(err) => fatal_exit(format_args!(
                            "failed to parse route53resolver persistence snapshot: {err}"
                        )),
                    }
                }
                Ok(None) => {
                    tracing::info!("no route53resolver persistence snapshot found; starting empty");
                }
                Err(err) => fatal_exit(format_args!(
                    "failed to read route53resolver persistence snapshot: {err}"
                )),
            }
            Some(Arc::new(store) as Arc<dyn fakecloud_persistence::SnapshotStore>)
        } else {
            None
        };
    let mut route53resolver_inner =
        fakecloud_route53resolver::Route53ResolverService::new(route53resolver_state.clone())
            .with_ec2_state(ec2_state.clone())
            .with_s3_state(s3_state.clone());
    if let Some(store) = route53resolver_snapshot_store.clone() {
        route53resolver_inner = route53resolver_inner.with_snapshot_store(store);
    }
    if let Some(h) = route53resolver_inner.snapshot_hook() {
        cfn_snapshot_hooks.insert("route53resolver", h);
    }
    // Re-arm the background settle for any resource restored mid-transition.
    route53resolver_inner.rearm_pending();
    registry.register(Arc::new(route53resolver_inner));
    let config_snapshot_store: Option<Arc<dyn fakecloud_persistence::SnapshotStore>> =
        if persistence_config.mode == fakecloud_persistence::StorageMode::Persistent {
            let data_path = persistence_config
                .data_path
                .as_ref()
                .expect("validated above")
                .clone();
            let path = data_path.join("config").join("snapshot.json");
            let store = fakecloud_persistence::DiskSnapshotStore::new(path);
            match fakecloud_persistence::SnapshotStore::load(&store) {
                Ok(Some(bytes)) => {
                    match serde_json::from_slice::<fakecloud_config::ConfigSnapshot>(&bytes) {
                        Ok(snapshot) => {
                            if snapshot.schema_version
                                > fakecloud_config::CONFIG_SNAPSHOT_SCHEMA_VERSION
                            {
                                fatal_exit(format_args!(
                                    "config persistence schema too new: on-disk={}, max supported={}",
                                    snapshot.schema_version,
                                    fakecloud_config::CONFIG_SNAPSHOT_SCHEMA_VERSION,
                                ));
                            }
                            if let Some(accounts) = snapshot.accounts {
                                let account_count = accounts.accounts.len();
                                *config_state.write() = accounts;
                                tracing::info!(
                                    accounts = account_count,
                                    "loaded config persistence snapshot"
                                );
                            }
                        }
                        Err(err) => fatal_exit(format_args!(
                            "failed to parse config persistence snapshot: {err}"
                        )),
                    }
                }
                Ok(None) => {
                    tracing::info!("no config persistence snapshot found; starting empty");
                }
                Err(err) => fatal_exit(format_args!(
                    "failed to read config persistence snapshot: {err}"
                )),
            }
            Some(Arc::new(store) as Arc<dyn fakecloud_persistence::SnapshotStore>)
        } else {
            None
        };
    let mut config_inner = fakecloud_config::ConfigService::new(config_state.clone())
        .with_cross_service(fakecloud_config::CrossServiceStates {
            s3: Some(s3_state.clone()),
            iam: Some(iam_state.clone()),
            ec2: Some(ec2_state.clone()),
        })
        .with_lambda(lambda_state.clone(), container_runtime.clone());
    if let Some(store) = config_snapshot_store.clone() {
        config_inner = config_inner.with_snapshot_store(store);
    }
    if let Some(h) = config_inner.snapshot_hook() {
        cfn_snapshot_hooks.insert("config", h);
    }
    let config_service = Arc::new(config_inner);
    registry.register(config_service.clone());
    let firehose_snapshot_store: Option<Arc<dyn fakecloud_persistence::SnapshotStore>> =
        if persistence_config.mode == fakecloud_persistence::StorageMode::Persistent {
            let data_path = persistence_config
                .data_path
                .as_ref()
                .expect("validated above")
                .clone();
            let path = data_path.join("firehose").join("snapshot.json");
            let store = fakecloud_persistence::DiskSnapshotStore::new(path);
            match fakecloud_persistence::SnapshotStore::load(&store) {
                Ok(Some(bytes)) => {
                    match serde_json::from_slice::<fakecloud_firehose::FirehoseSnapshot>(&bytes) {
                        Ok(snapshot) => {
                            if snapshot.schema_version
                                > fakecloud_firehose::FIREHOSE_SNAPSHOT_SCHEMA_VERSION
                            {
                                fatal_exit(format_args!(
                                    "firehose persistence schema too new: on-disk={}, max supported={}",
                                    snapshot.schema_version,
                                    fakecloud_firehose::FIREHOSE_SNAPSHOT_SCHEMA_VERSION,
                                ));
                            }
                            if let Some(accounts) = snapshot.accounts {
                                let account_count = accounts.accounts.len();
                                *firehose_state.write() = accounts;
                                tracing::info!(
                                    accounts = account_count,
                                    "loaded firehose persistence snapshot"
                                );
                            }
                        }
                        Err(err) => fatal_exit(format_args!(
                            "failed to parse firehose persistence snapshot: {err}"
                        )),
                    }
                }
                Ok(None) => {
                    tracing::info!("no firehose persistence snapshot found; starting empty");
                }
                Err(err) => fatal_exit(format_args!(
                    "failed to read firehose persistence snapshot: {err}"
                )),
            }
            Some(Arc::new(store) as Arc<dyn fakecloud_persistence::SnapshotStore>)
        } else {
            None
        };
    let mut firehose_service =
        fakecloud_firehose::FirehoseService::new(firehose_state.clone()).with_s3(s3_state.clone());
    if let Some(store) = firehose_snapshot_store.clone() {
        firehose_service = firehose_service.with_snapshot_store(store);
    }
    if let Some(h) = firehose_service.snapshot_hook() {
        cfn_snapshot_hooks.insert("firehose", h);
    }
    registry.register(Arc::new(firehose_service));
    let glue_snapshot_store: Option<Arc<dyn fakecloud_persistence::SnapshotStore>> =
        if persistence_config.mode == fakecloud_persistence::StorageMode::Persistent {
            let data_path = persistence_config
                .data_path
                .as_ref()
                .expect("validated above")
                .clone();
            let path = data_path.join("glue").join("snapshot.json");
            let store = fakecloud_persistence::DiskSnapshotStore::new(path);
            match fakecloud_persistence::SnapshotStore::load(&store) {
                Ok(Some(bytes)) => {
                    match serde_json::from_slice::<fakecloud_glue::GlueSnapshot>(&bytes) {
                        Ok(snapshot) => {
                            if snapshot.schema_version
                                > fakecloud_glue::GLUE_SNAPSHOT_SCHEMA_VERSION
                            {
                                fatal_exit(format_args!(
                                    "glue persistence schema too new: on-disk={}, max supported={}",
                                    snapshot.schema_version,
                                    fakecloud_glue::GLUE_SNAPSHOT_SCHEMA_VERSION,
                                ));
                            }
                            if let Some(accounts) = snapshot.accounts {
                                let account_count = accounts.accounts.len();
                                *glue_state.write() = accounts;
                                tracing::info!(
                                    accounts = account_count,
                                    "loaded glue persistence snapshot"
                                );
                            }
                        }
                        Err(err) => fatal_exit(format_args!(
                            "failed to parse glue persistence snapshot: {err}"
                        )),
                    }
                }
                Ok(None) => {
                    tracing::info!("no glue persistence snapshot found; starting empty");
                }
                Err(err) => fatal_exit(format_args!(
                    "failed to read glue persistence snapshot: {err}"
                )),
            }
            Some(Arc::new(store) as Arc<dyn fakecloud_persistence::SnapshotStore>)
        } else {
            None
        };
    let mut glue_service = fakecloud_glue::GlueService::new(glue_state.clone());
    if let Some(store) = glue_snapshot_store.clone() {
        glue_service = glue_service.with_snapshot_store(store);
    }
    if let Some(h) = glue_service.snapshot_hook() {
        cfn_snapshot_hooks.insert("glue", h);
    }
    registry.register(Arc::new(glue_service));

    // Amazon EMR (elasticmapreduce): awsJson1.1 control plane (clusters/job
    // flows, steps, instance groups/fleets, instances, bootstrap actions,
    // security configurations, Studios + session mappings, notebook executions,
    // persistent app UIs, interactive sessions, block-public-access, scaling /
    // auto-termination policies, release labels, tags).
    let emr_snapshot_store: Option<Arc<dyn fakecloud_persistence::SnapshotStore>> =
        if persistence_config.mode == fakecloud_persistence::StorageMode::Persistent {
            let data_path = persistence_config
                .data_path
                .as_ref()
                .expect("validated above")
                .clone();
            let path = data_path.join("emr").join("snapshot.json");
            let store = fakecloud_persistence::DiskSnapshotStore::new(path);
            match fakecloud_emr::persistence::load_into(&store, &emr_state) {
                Ok(fakecloud_emr::persistence::LoadOutcome::Loaded(accounts)) => {
                    tracing::info!(accounts, "loaded emr persistence snapshot");
                }
                Ok(fakecloud_emr::persistence::LoadOutcome::Empty) => {
                    tracing::info!("no emr persistence snapshot found; starting empty");
                }
                Err(err) => fatal_exit(format_args!("{err}")),
            }
            Some(Arc::new(store) as Arc<dyn fakecloud_persistence::SnapshotStore>)
        } else {
            None
        };
    let mut emr_service = fakecloud_emr::EmrService::new(emr_state.clone());
    if let Some(store) = emr_snapshot_store {
        emr_service = emr_service.with_snapshot_store(store);
    }
    if let Some(h) = emr_service.snapshot_hook() {
        cfn_snapshot_hooks.insert("emr", h);
    }
    registry.register(Arc::new(emr_service));
    // Amazon Textract: awsJson1_1 document text/analysis extraction (sync
    // Detect/Analyze ops, async Start*/Get* jobs, custom adapters + versions,
    // tagging). OCR/ML inference is an honest gap; the API surface, validation,
    // job lifecycle, adapters and persistence are real.
    let textract_snapshot_store: Option<Arc<dyn fakecloud_persistence::SnapshotStore>> =
        if persistence_config.mode == fakecloud_persistence::StorageMode::Persistent {
            let data_path = persistence_config
                .data_path
                .as_ref()
                .expect("validated above")
                .clone();
            let path = data_path.join("textract").join("snapshot.json");
            let store = fakecloud_persistence::DiskSnapshotStore::new(path);
            match fakecloud_textract::persistence::load_into(&store, &textract_state) {
                Ok(fakecloud_textract::persistence::LoadOutcome::Loaded(accounts)) => {
                    tracing::info!(accounts, "loaded textract persistence snapshot");
                }
                Ok(fakecloud_textract::persistence::LoadOutcome::Empty) => {
                    tracing::info!("no textract persistence snapshot found; starting empty");
                }
                Err(err) => fatal_exit(format_args!("{err}")),
            }
            Some(Arc::new(store) as Arc<dyn fakecloud_persistence::SnapshotStore>)
        } else {
            None
        };
    let mut textract_service = fakecloud_textract::TextractService::new(textract_state.clone());
    if let Some(store) = textract_snapshot_store {
        textract_service = textract_service.with_snapshot_store(store);
    }
    if let Some(h) = textract_service.snapshot_hook() {
        cfn_snapshot_hooks.insert("textract", h);
    }
    registry.register(Arc::new(textract_service));
    // Amazon Transcribe (transcribe): awsJson1.1 speech-to-text control plane
    // (transcription / medical-transcription / call-analytics / medical-scribe
    // jobs, custom + medical vocabularies, vocabulary filters, custom language
    // models, call-analytics categories, tags).
    let transcribe_snapshot_store: Option<Arc<dyn fakecloud_persistence::SnapshotStore>> =
        if persistence_config.mode == fakecloud_persistence::StorageMode::Persistent {
            let data_path = persistence_config
                .data_path
                .as_ref()
                .expect("validated above")
                .clone();
            let path = data_path.join("transcribe").join("snapshot.json");
            let store = fakecloud_persistence::DiskSnapshotStore::new(path);
            match fakecloud_transcribe::persistence::load_into(&store, &transcribe_state) {
                Ok(fakecloud_transcribe::persistence::LoadOutcome::Loaded(accounts)) => {
                    tracing::info!(accounts, "loaded transcribe persistence snapshot");
                }
                Ok(fakecloud_transcribe::persistence::LoadOutcome::Empty) => {
                    tracing::info!("no transcribe persistence snapshot found; starting empty");
                }
                Err(err) => fatal_exit(format_args!("{err}")),
            }
            Some(Arc::new(store) as Arc<dyn fakecloud_persistence::SnapshotStore>)
        } else {
            None
        };
    let mut transcribe_service =
        fakecloud_transcribe::TranscribeService::new(transcribe_state.clone());
    if let Some(store) = transcribe_snapshot_store {
        transcribe_service = transcribe_service.with_snapshot_store(store);
    }
    if let Some(h) = transcribe_service.snapshot_hook() {
        cfn_snapshot_hooks.insert("transcribe", h);
    }
    registry.register(Arc::new(transcribe_service));

    // Amazon Translate (translate): awsJson1.1 text/document translation control
    // plane (synchronous TranslateText / TranslateDocument passthrough, async
    // batch translation jobs, parallel data, custom terminologies, supported
    // languages, tags).
    let translate_snapshot_store: Option<Arc<dyn fakecloud_persistence::SnapshotStore>> =
        if persistence_config.mode == fakecloud_persistence::StorageMode::Persistent {
            let data_path = persistence_config
                .data_path
                .as_ref()
                .expect("validated above")
                .clone();
            let path = data_path.join("translate").join("snapshot.json");
            let store = fakecloud_persistence::DiskSnapshotStore::new(path);
            match fakecloud_translate::persistence::load_into(&store, &translate_state) {
                Ok(fakecloud_translate::persistence::LoadOutcome::Loaded(accounts)) => {
                    tracing::info!(accounts, "loaded translate persistence snapshot");
                }
                Ok(fakecloud_translate::persistence::LoadOutcome::Empty) => {
                    tracing::info!("no translate persistence snapshot found; starting empty");
                }
                Err(err) => fatal_exit(format_args!("{err}")),
            }
            Some(Arc::new(store) as Arc<dyn fakecloud_persistence::SnapshotStore>)
        } else {
            None
        };
    let mut translate_service = fakecloud_translate::TranslateService::new(translate_state.clone());
    if let Some(store) = translate_snapshot_store {
        translate_service = translate_service.with_snapshot_store(store);
    }
    if let Some(h) = translate_service.snapshot_hook() {
        cfn_snapshot_hooks.insert("translate", h);
    }
    registry.register(Arc::new(translate_service));

    // Amazon SWF (Simple Workflow Service): awsJson1_0 control plane (domains,
    // versioned activity/workflow types, workflow executions with a real
    // decider/worker state machine -- decision tasks, activity tasks, and the
    // event history that ties them together -- plus pending-task counts and
    // ARN-keyed domain tagging).
    let swf_snapshot_store: Option<Arc<dyn fakecloud_persistence::SnapshotStore>> =
        if persistence_config.mode == fakecloud_persistence::StorageMode::Persistent {
            let data_path = persistence_config
                .data_path
                .as_ref()
                .expect("validated above")
                .clone();
            let path = data_path.join("swf").join("snapshot.json");
            let store = fakecloud_persistence::DiskSnapshotStore::new(path);
            match fakecloud_swf::persistence::load_into(&store, &swf_state) {
                Ok(fakecloud_swf::persistence::LoadOutcome::Loaded(accounts)) => {
                    tracing::info!(accounts, "loaded swf persistence snapshot");
                }
                Ok(fakecloud_swf::persistence::LoadOutcome::Empty) => {
                    tracing::info!("no swf persistence snapshot found; starting empty");
                }
                Err(err) => fatal_exit(format_args!("{err}")),
            }
            Some(Arc::new(store) as Arc<dyn fakecloud_persistence::SnapshotStore>)
        } else {
            None
        };
    let mut swf_service = fakecloud_swf::SwfService::new(swf_state.clone());
    if let Some(store) = swf_snapshot_store {
        swf_service = swf_service.with_snapshot_store(store);
    }
    if let Some(h) = swf_service.snapshot_hook() {
        cfn_snapshot_hooks.insert("swf", h);
    }
    registry.register(Arc::new(swf_service));

    // Amazon Timestream (Write + Query): awsJson1_0 control plane over one
    // shared store -- databases, tables, ingested points (queryable via a
    // bounded SQL handler), scheduled queries, batch-load tasks, account
    // settings, endpoint discovery, and ARN-keyed tagging. Both SDK clients
    // share the `Timestream_20181101` target prefix.
    let timestream_snapshot_store: Option<Arc<dyn fakecloud_persistence::SnapshotStore>> =
        if persistence_config.mode == fakecloud_persistence::StorageMode::Persistent {
            let data_path = persistence_config
                .data_path
                .as_ref()
                .expect("validated above")
                .clone();
            let path = data_path.join("timestream").join("snapshot.json");
            let store = fakecloud_persistence::DiskSnapshotStore::new(path);
            match fakecloud_timestream::persistence::load_into(&store, &timestream_state) {
                Ok(fakecloud_timestream::persistence::LoadOutcome::Loaded(accounts)) => {
                    tracing::info!(accounts, "loaded timestream persistence snapshot");
                }
                Ok(fakecloud_timestream::persistence::LoadOutcome::Empty) => {
                    tracing::info!("no timestream persistence snapshot found; starting empty");
                }
                Err(err) => fatal_exit(format_args!("{err}")),
            }
            Some(Arc::new(store) as Arc<dyn fakecloud_persistence::SnapshotStore>)
        } else {
            None
        };
    let mut timestream_service =
        fakecloud_timestream::TimestreamService::new(timestream_state.clone());
    if let Some(store) = timestream_snapshot_store {
        timestream_service = timestream_service.with_snapshot_store(store);
    }
    if let Some(h) = timestream_service.snapshot_hook() {
        cfn_snapshot_hooks.insert("timestream", h);
    }
    registry.register(Arc::new(timestream_service));

    // AWS Shield / Shield Advanced: awsJson1.1 control plane (protections,
    // protection groups, the annual auto-renewing subscription, emergency
    // contacts, DRT access, proactive engagement, application-layer automatic
    // response, health-check association, tags). Attack surfacing is honest
    // (empty list / zeroed statistics; no synthetic DDoS records).
    let shield_snapshot_store: Option<Arc<dyn fakecloud_persistence::SnapshotStore>> =
        if persistence_config.mode == fakecloud_persistence::StorageMode::Persistent {
            let data_path = persistence_config
                .data_path
                .as_ref()
                .expect("validated above")
                .clone();
            let path = data_path.join("shield").join("snapshot.json");
            let store = fakecloud_persistence::DiskSnapshotStore::new(path);
            match fakecloud_shield::persistence::load_into(&store, &shield_state) {
                Ok(fakecloud_shield::persistence::LoadOutcome::Loaded(accounts)) => {
                    tracing::info!(accounts, "loaded shield persistence snapshot");
                }
                Ok(fakecloud_shield::persistence::LoadOutcome::Empty) => {
                    tracing::info!("no shield persistence snapshot found; starting empty");
                }
                Err(err) => fatal_exit(format_args!("{err}")),
            }
            Some(Arc::new(store) as Arc<dyn fakecloud_persistence::SnapshotStore>)
        } else {
            None
        };
    let mut shield_service = fakecloud_shield::ShieldService::new(shield_state.clone());
    if let Some(store) = shield_snapshot_store {
        shield_service = shield_service.with_snapshot_store(store);
    }
    if let Some(h) = shield_service.snapshot_hook() {
        cfn_snapshot_hooks.insert("shield", h);
    }
    registry.register(Arc::new(shield_service));
    // Amazon Comprehend (comprehend): awsJson1.1 NLP control + inference plane
    // (synchronous + batch detection, nine async analysis-job families, custom
    // document classifiers + entity recognizers, endpoints, flywheels +
    // iterations, datasets, resource policies, model import, tags).
    let comprehend_snapshot_store: Option<Arc<dyn fakecloud_persistence::SnapshotStore>> =
        if persistence_config.mode == fakecloud_persistence::StorageMode::Persistent {
            let data_path = persistence_config
                .data_path
                .as_ref()
                .expect("validated above")
                .clone();
            let path = data_path.join("comprehend").join("snapshot.json");
            let store = fakecloud_persistence::DiskSnapshotStore::new(path);
            match fakecloud_comprehend::persistence::load_into(&store, &comprehend_state) {
                Ok(fakecloud_comprehend::persistence::LoadOutcome::Loaded(accounts)) => {
                    tracing::info!(accounts, "loaded comprehend persistence snapshot");
                }
                Ok(fakecloud_comprehend::persistence::LoadOutcome::Empty) => {
                    tracing::info!("no comprehend persistence snapshot found; starting empty");
                }
                Err(err) => fatal_exit(format_args!("{err}")),
            }
            Some(Arc::new(store) as Arc<dyn fakecloud_persistence::SnapshotStore>)
        } else {
            None
        };
    let mut comprehend_service =
        fakecloud_comprehend::ComprehendService::new(comprehend_state.clone());
    if let Some(store) = comprehend_snapshot_store {
        comprehend_service = comprehend_service.with_snapshot_store(store);
    }
    if let Some(h) = comprehend_service.snapshot_hook() {
        cfn_snapshot_hooks.insert("comprehend", h);
    }
    registry.register(Arc::new(comprehend_service));

    // AWS Support (support): awsJson1.1 support-cases + Trusted Advisor control
    // plane (cases, communications, attachment sets, severity levels, the
    // Trusted Advisor check catalogue + refresh state machine).
    let support_snapshot_store: Option<Arc<dyn fakecloud_persistence::SnapshotStore>> =
        if persistence_config.mode == fakecloud_persistence::StorageMode::Persistent {
            let data_path = persistence_config
                .data_path
                .as_ref()
                .expect("validated above")
                .clone();
            let path = data_path.join("support").join("snapshot.json");
            let store = fakecloud_persistence::DiskSnapshotStore::new(path);
            match fakecloud_support::persistence::load_into(&store, &support_state) {
                Ok(fakecloud_support::persistence::LoadOutcome::Loaded(accounts)) => {
                    tracing::info!(accounts, "loaded support persistence snapshot");
                }
                Ok(fakecloud_support::persistence::LoadOutcome::Empty) => {
                    tracing::info!("no support persistence snapshot found; starting empty");
                }
                Err(err) => fatal_exit(format_args!("{err}")),
            }
            Some(Arc::new(store) as Arc<dyn fakecloud_persistence::SnapshotStore>)
        } else {
            None
        };
    let mut support_service = fakecloud_support::SupportService::new(support_state.clone());
    if let Some(store) = support_snapshot_store {
        support_service = support_service.with_snapshot_store(store);
    }
    if let Some(h) = support_service.snapshot_hook() {
        cfn_snapshot_hooks.insert("support", h);
    }
    registry.register(Arc::new(support_service));
    let cloudwatch_snapshot_store: Option<Arc<dyn fakecloud_persistence::SnapshotStore>> =
        if persistence_config.mode == fakecloud_persistence::StorageMode::Persistent {
            let data_path = persistence_config
                .data_path
                .as_ref()
                .expect("validated above")
                .clone();
            let path = data_path.join("cloudwatch").join("snapshot.json");
            let store = fakecloud_persistence::DiskSnapshotStore::new(path);
            match fakecloud_persistence::SnapshotStore::load(&store) {
                Ok(Some(bytes)) => {
                    match serde_json::from_slice::<fakecloud_cloudwatch::CloudWatchSnapshot>(&bytes)
                    {
                        Ok(snapshot) => {
                            if snapshot.schema_version
                                > fakecloud_cloudwatch::CLOUDWATCH_SNAPSHOT_SCHEMA_VERSION
                            {
                                fatal_exit(format_args!(
                                    "cloudwatch persistence schema too new: on-disk={}, max supported={}",
                                    snapshot.schema_version,
                                    fakecloud_cloudwatch::CLOUDWATCH_SNAPSHOT_SCHEMA_VERSION,
                                ));
                            }
                            let account_count = snapshot.accounts.accounts.len();
                            *cloudwatch_state.write() = snapshot.accounts;
                            tracing::info!(
                                accounts = account_count,
                                "loaded cloudwatch persistence snapshot"
                            );
                        }
                        Err(err) => fatal_exit(format_args!(
                            "failed to parse cloudwatch persistence snapshot: {err}"
                        )),
                    }
                }
                Ok(None) => {
                    tracing::info!("no cloudwatch persistence snapshot found; starting empty");
                }
                Err(err) => fatal_exit(format_args!(
                    "failed to read cloudwatch persistence snapshot: {err}"
                )),
            }
            Some(Arc::new(store) as Arc<dyn fakecloud_persistence::SnapshotStore>)
        } else {
            None
        };
    let mut cloudwatch_service =
        fakecloud_cloudwatch::CloudWatchService::new(cloudwatch_state.clone());
    if let Some(store) = cloudwatch_snapshot_store {
        cloudwatch_service = cloudwatch_service.with_snapshot_store(store);
    }
    if let Some(h) = cloudwatch_service.snapshot_hook() {
        cfn_snapshot_hooks.insert("cloudwatch", h);
    }
    registry.register(Arc::new(cloudwatch_service));
    let app_autoscaling_snapshot_store: Option<Arc<dyn fakecloud_persistence::SnapshotStore>> =
        if persistence_config.mode == fakecloud_persistence::StorageMode::Persistent {
            let data_path = persistence_config
                .data_path
                .as_ref()
                .expect("validated above")
                .clone();
            let path = data_path
                .join("application-autoscaling")
                .join("snapshot.json");
            let store = fakecloud_persistence::DiskSnapshotStore::new(path);
            match fakecloud_persistence::SnapshotStore::load(&store) {
                Ok(Some(bytes)) => {
                    match serde_json::from_slice::<
                        fakecloud_application_autoscaling::ApplicationAutoScalingSnapshot,
                    >(&bytes)
                    {
                        Ok(snapshot) => {
                            if snapshot.schema_version
                                > fakecloud_application_autoscaling::APPLICATION_AUTOSCALING_SNAPSHOT_SCHEMA_VERSION
                            {
                                fatal_exit(format_args!(
                                    "application-autoscaling persistence schema too new: on-disk={}, max supported={}",
                                    snapshot.schema_version,
                                    fakecloud_application_autoscaling::APPLICATION_AUTOSCALING_SNAPSHOT_SCHEMA_VERSION,
                                ));
                            }
                            if let Some(accounts) = snapshot.accounts {
                                let account_count = accounts.accounts.len();
                                *app_autoscaling_state.write() = accounts;
                                tracing::info!(
                                    accounts = account_count,
                                    "loaded application-autoscaling persistence snapshot"
                                );
                            }
                        }
                        Err(err) => fatal_exit(format_args!(
                            "failed to parse application-autoscaling persistence snapshot: {err}"
                        )),
                    }
                }
                Ok(None) => {
                    tracing::info!(
                        "no application-autoscaling persistence snapshot found; starting empty"
                    );
                }
                Err(err) => fatal_exit(format_args!(
                    "failed to read application-autoscaling persistence snapshot: {err}"
                )),
            }
            Some(Arc::new(store) as Arc<dyn fakecloud_persistence::SnapshotStore>)
        } else {
            None
        };
    let mut app_autoscaling_service =
        fakecloud_application_autoscaling::ApplicationAutoScalingService::new(
            app_autoscaling_state.clone(),
        );
    if let Some(store) = app_autoscaling_snapshot_store.clone() {
        app_autoscaling_service = app_autoscaling_service.with_snapshot_store(store);
    }
    if let Some(h) = app_autoscaling_service.snapshot_hook() {
        cfn_snapshot_hooks.insert("application-autoscaling", h);
    }
    registry.register(Arc::new(app_autoscaling_service));

    // EC2 Auto Scaling (the `autoscaling` service — Auto Scaling Groups +
    // Launch Configurations), distinct from Application Auto Scaling above.
    let autoscaling_snapshot_store: Option<Arc<dyn fakecloud_persistence::SnapshotStore>> =
        if persistence_config.mode == fakecloud_persistence::StorageMode::Persistent {
            let data_path = persistence_config
                .data_path
                .as_ref()
                .expect("validated above")
                .clone();
            let path = data_path.join("autoscaling").join("snapshot.json");
            let store = fakecloud_persistence::DiskSnapshotStore::new(path);
            match fakecloud_persistence::SnapshotStore::load(&store) {
                Ok(Some(bytes)) => {
                    match serde_json::from_slice::<fakecloud_autoscaling::AutoScalingSnapshot>(
                        &bytes,
                    ) {
                        Ok(snapshot) => {
                            if snapshot.schema_version
                                > fakecloud_autoscaling::AUTOSCALING_SNAPSHOT_SCHEMA_VERSION
                            {
                                fatal_exit(format_args!(
                                    "autoscaling persistence schema too new: on-disk={}, max supported={}",
                                    snapshot.schema_version,
                                    fakecloud_autoscaling::AUTOSCALING_SNAPSHOT_SCHEMA_VERSION,
                                ));
                            }
                            if let Some(accounts) = snapshot.accounts {
                                let account_count = accounts.accounts.len();
                                *autoscaling_state.write() = accounts;
                                tracing::info!(
                                    accounts = account_count,
                                    "loaded autoscaling persistence snapshot"
                                );
                            }
                        }
                        Err(err) => fatal_exit(format_args!(
                            "failed to parse autoscaling persistence snapshot: {err}"
                        )),
                    }
                }
                Ok(None) => {
                    tracing::info!("no autoscaling persistence snapshot found; starting empty");
                }
                Err(err) => fatal_exit(format_args!(
                    "failed to read autoscaling persistence snapshot: {err}"
                )),
            }
            Some(Arc::new(store) as Arc<dyn fakecloud_persistence::SnapshotStore>)
        } else {
            None
        };
    let mut autoscaling_service =
        fakecloud_autoscaling::AutoScalingService::new(autoscaling_state.clone())
            .with_ec2(ec2_state.clone(), ec2_runtime.clone())
            // ASG capacity reconciliation launches REAL EC2 instances through a
            // bare Ec2Service; without the EC2 snapshot hook those records live
            // only in memory and leak their containers on restart (the EC2
            // boot-recovery has no persisted row to re-drive). The "ec2" hook was
            // registered above when the EC2 service was wired.
            .with_ec2_snapshot_hook(cfn_snapshot_hooks.get("ec2").cloned());
    if let Some(store) = autoscaling_snapshot_store.clone() {
        autoscaling_service = autoscaling_service.with_snapshot_store(store);
    }
    if let Some(h) = autoscaling_service.snapshot_hook() {
        cfn_snapshot_hooks.insert("autoscaling", h);
    }
    registry.register(Arc::new(autoscaling_service));

    // AWS Batch — control plane (compute environments, job queues, job
    // definitions). Real container-backed job execution lands in a later batch.
    let batch_snapshot_store: Option<Arc<dyn fakecloud_persistence::SnapshotStore>> =
        if persistence_config.mode == fakecloud_persistence::StorageMode::Persistent {
            let data_path = persistence_config
                .data_path
                .as_ref()
                .expect("validated above")
                .clone();
            let path = data_path.join("batch").join("snapshot.json");
            let store = fakecloud_persistence::DiskSnapshotStore::new(path);
            match fakecloud_persistence::SnapshotStore::load(&store) {
                Ok(Some(bytes)) => {
                    match serde_json::from_slice::<fakecloud_batch::BatchSnapshot>(&bytes) {
                        Ok(snapshot) => {
                            if snapshot.schema_version
                                > fakecloud_batch::BATCH_SNAPSHOT_SCHEMA_VERSION
                            {
                                fatal_exit(format_args!(
                                    "batch persistence schema too new: on-disk={}, max supported={}",
                                    snapshot.schema_version,
                                    fakecloud_batch::BATCH_SNAPSHOT_SCHEMA_VERSION,
                                ));
                            }
                            if let Some(accounts) = snapshot.accounts {
                                *batch_state.write() = accounts;
                                tracing::info!("loaded batch persistence snapshot");
                            }
                        }
                        Err(err) => fatal_exit(format_args!(
                            "failed to parse batch persistence snapshot: {err}"
                        )),
                    }
                }
                Ok(None) => {
                    tracing::info!("no batch persistence snapshot found; starting empty");
                }
                Err(err) => fatal_exit(format_args!(
                    "failed to read batch persistence snapshot: {err}"
                )),
            }
            Some(Arc::new(store) as Arc<dyn fakecloud_persistence::SnapshotStore>)
        } else {
            None
        };
    let mut batch_service = fakecloud_batch::BatchService::new(batch_state.clone())
        .with_ecs(ecs_state.clone(), ecs_runtime.clone());
    if let Some(store) = batch_snapshot_store.clone() {
        batch_service = batch_service.with_snapshot_store(store);
    }
    // Fail any jobs left mid-flight by a restart (their drivers + ECS tasks are
    // gone) so they don't hang forever.
    batch_service.reconcile_persisted_jobs().await;
    if let Some(h) = batch_service.snapshot_hook() {
        cfn_snapshot_hooks.insert("batch", h);
    }
    registry.register(Arc::new(batch_service));

    // EventBridge Pipes — control plane (CreatePipe/Describe/List/Update/
    // Delete/Start/Stop + tags) with a faithful lifecycle state machine. Real
    // source->enrichment->target execution lands in a later batch.
    let pipes_snapshot_store: Option<Arc<dyn fakecloud_persistence::SnapshotStore>> =
        if persistence_config.mode == fakecloud_persistence::StorageMode::Persistent {
            let data_path = persistence_config
                .data_path
                .as_ref()
                .expect("validated above")
                .clone();
            let path = data_path.join("pipes").join("snapshot.json");
            let store = fakecloud_persistence::DiskSnapshotStore::new(path);
            match fakecloud_persistence::SnapshotStore::load(&store) {
                Ok(Some(bytes)) => {
                    match serde_json::from_slice::<fakecloud_pipes::PipesSnapshot>(&bytes) {
                        Ok(snapshot) => {
                            if snapshot.schema_version
                                > fakecloud_pipes::PIPES_SNAPSHOT_SCHEMA_VERSION
                            {
                                fatal_exit(format_args!(
                                    "pipes persistence schema too new: on-disk={}, max supported={}",
                                    snapshot.schema_version,
                                    fakecloud_pipes::PIPES_SNAPSHOT_SCHEMA_VERSION,
                                ));
                            }
                            if let Some(accounts) = snapshot.accounts {
                                *pipes_state.write() = accounts;
                                tracing::info!("loaded pipes persistence snapshot");
                            }
                        }
                        Err(err) => fatal_exit(format_args!(
                            "failed to parse pipes persistence snapshot: {err}"
                        )),
                    }
                }
                Ok(None) => {
                    tracing::info!("no pipes persistence snapshot found; starting empty");
                }
                Err(err) => fatal_exit(format_args!(
                    "failed to read pipes persistence snapshot: {err}"
                )),
            }
            Some(Arc::new(store) as Arc<dyn fakecloud_persistence::SnapshotStore>)
        } else {
            None
        };
    let mut pipes_service = fakecloud_pipes::PipesService::new(pipes_state.clone());
    if let Some(store) = pipes_snapshot_store.clone() {
        pipes_service = pipes_service.with_snapshot_store(store);
    }
    // Re-drive any pipe left mid-transition by a restart so it doesn't stay
    // stuck in CREATING/UPDATING/etc forever.
    pipes_service.recover_persisted_pipes().await;
    // Capture the pipes snapshot hook for the runner so its checkpoint advances
    // are flushed to disk (M1), in addition to the CFN provisioner path.
    let pipes_persist_hook = pipes_service.snapshot_hook();
    if let Some(h) = pipes_service.snapshot_hook() {
        cfn_snapshot_hooks.insert("pipes", h);
    }
    registry.register(Arc::new(pipes_service));

    let wafv2_snapshot_store: Option<Arc<dyn fakecloud_persistence::SnapshotStore>> =
        if persistence_config.mode == fakecloud_persistence::StorageMode::Persistent {
            let data_path = persistence_config
                .data_path
                .as_ref()
                .expect("validated above")
                .clone();
            let path = data_path.join("wafv2").join("snapshot.json");
            let store = fakecloud_persistence::DiskSnapshotStore::new(path);
            match fakecloud_persistence::SnapshotStore::load(&store) {
                Ok(Some(bytes)) => {
                    match serde_json::from_slice::<fakecloud_wafv2::Wafv2Snapshot>(&bytes) {
                        Ok(snapshot) => {
                            if snapshot.schema_version
                                > fakecloud_wafv2::WAFV2_SNAPSHOT_SCHEMA_VERSION
                            {
                                fatal_exit(format_args!(
                                    "wafv2 persistence schema too new: on-disk={}, max supported={}",
                                    snapshot.schema_version,
                                    fakecloud_wafv2::WAFV2_SNAPSHOT_SCHEMA_VERSION,
                                ));
                            }
                            if let Some(accounts) = snapshot.accounts {
                                let account_count = accounts.accounts.len();
                                *wafv2_state.write() = accounts;
                                tracing::info!(
                                    accounts = account_count,
                                    "loaded wafv2 persistence snapshot"
                                );
                            }
                        }
                        Err(err) => fatal_exit(format_args!(
                            "failed to parse wafv2 persistence snapshot: {err}"
                        )),
                    }
                }
                Ok(None) => {
                    tracing::info!("no wafv2 persistence snapshot found; starting empty");
                }
                Err(err) => fatal_exit(format_args!(
                    "failed to read wafv2 persistence snapshot: {err}"
                )),
            }
            Some(Arc::new(store) as Arc<dyn fakecloud_persistence::SnapshotStore>)
        } else {
            None
        };
    let mut wafv2_service = fakecloud_wafv2::Wafv2Service::with_rate_limiter(
        wafv2_state.clone(),
        wafv2_rate_limiter.clone(),
    );
    if let Some(store) = wafv2_snapshot_store.clone() {
        wafv2_service = wafv2_service.with_snapshot_store(store);
    }
    if let Some(h) = wafv2_service.snapshot_hook() {
        cfn_snapshot_hooks.insert("wafv2", h);
    }
    registry.register(Arc::new(wafv2_service));
    let athena_snapshot_store: Option<Arc<dyn fakecloud_persistence::SnapshotStore>> =
        if persistence_config.mode == fakecloud_persistence::StorageMode::Persistent {
            let data_path = persistence_config
                .data_path
                .as_ref()
                .expect("validated above")
                .clone();
            let path = data_path.join("athena").join("snapshot.json");
            let store = fakecloud_persistence::DiskSnapshotStore::new(path);
            match fakecloud_persistence::SnapshotStore::load(&store) {
                Ok(Some(bytes)) => {
                    match serde_json::from_slice::<fakecloud_athena::AthenaSnapshot>(&bytes) {
                        Ok(snapshot) => {
                            if snapshot.schema_version
                                > fakecloud_athena::ATHENA_SNAPSHOT_SCHEMA_VERSION
                            {
                                fatal_exit(format_args!(
                                    "athena persistence schema too new: on-disk={}, max supported={}",
                                    snapshot.schema_version,
                                    fakecloud_athena::ATHENA_SNAPSHOT_SCHEMA_VERSION,
                                ));
                            }
                            if let Some(accounts) = snapshot.accounts {
                                let account_count = accounts.accounts.len();
                                *athena_state.write() = accounts;
                                tracing::info!(
                                    accounts = account_count,
                                    "loaded athena persistence snapshot"
                                );
                            }
                        }
                        Err(err) => fatal_exit(format_args!(
                            "failed to parse athena persistence snapshot: {err}"
                        )),
                    }
                }
                Ok(None) => {
                    tracing::info!("no athena persistence snapshot found; starting empty");
                }
                Err(err) => fatal_exit(format_args!(
                    "failed to read athena persistence snapshot: {err}"
                )),
            }
            Some(Arc::new(store) as Arc<dyn fakecloud_persistence::SnapshotStore>)
        } else {
            None
        };
    let mut athena_service = fakecloud_athena::AthenaService::new(athena_state.clone())
        .with_glue(glue_state.clone())
        .with_s3(s3_state.clone());
    if let Some(store) = athena_snapshot_store.clone() {
        athena_service = athena_service.with_snapshot_store(store);
    }
    if let Some(h) = athena_service.snapshot_hook() {
        cfn_snapshot_hooks.insert("athena", h);
    }
    registry.register(Arc::new(athena_service));
    let redshift_snapshot_store: Option<Arc<dyn fakecloud_persistence::SnapshotStore>> =
        if persistence_config.mode == fakecloud_persistence::StorageMode::Persistent {
            let data_path = persistence_config
                .data_path
                .as_ref()
                .expect("validated above")
                .clone();
            let path = data_path.join("redshift").join("snapshot.json");
            let store = fakecloud_persistence::DiskSnapshotStore::new(path);
            match fakecloud_persistence::SnapshotStore::load(&store) {
                Ok(Some(bytes)) => {
                    match serde_json::from_slice::<fakecloud_redshift::RedshiftSnapshot>(&bytes) {
                        Ok(snapshot) => {
                            if snapshot.schema_version
                                > fakecloud_redshift::REDSHIFT_SNAPSHOT_SCHEMA_VERSION
                            {
                                fatal_exit(format_args!(
                                    "redshift persistence schema too new: on-disk={}, max supported={}",
                                    snapshot.schema_version,
                                    fakecloud_redshift::REDSHIFT_SNAPSHOT_SCHEMA_VERSION,
                                ));
                            }
                            if let Some(accounts) = snapshot.accounts {
                                let account_count = accounts.accounts.len();
                                *redshift_state.write() = accounts;
                                tracing::info!(
                                    accounts = account_count,
                                    "loaded redshift persistence snapshot"
                                );
                            }
                        }
                        Err(err) => fatal_exit(format_args!(
                            "failed to parse redshift persistence snapshot: {err}"
                        )),
                    }
                }
                Ok(None) => {
                    tracing::info!("no redshift persistence snapshot found; starting empty");
                }
                Err(err) => fatal_exit(format_args!(
                    "failed to read redshift persistence snapshot: {err}"
                )),
            }
            Some(Arc::new(store) as Arc<dyn fakecloud_persistence::SnapshotStore>)
        } else {
            None
        };
    let mut redshift_service = fakecloud_redshift::RedshiftService::new(redshift_state.clone());
    if let Some(store) = redshift_snapshot_store.clone() {
        redshift_service = redshift_service.with_snapshot_store(store);
    }
    if let Some(h) = redshift_service.snapshot_hook() {
        cfn_snapshot_hooks.insert("redshift", h);
    }
    registry.register(Arc::new(redshift_service));
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
        .with_dynamodb(dynamodb_state.clone())
        .with_registry(sfn_registry_handle.clone());
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
                    match serde_json::from_slice::<fakecloud_stepfunctions::StepFunctionsSnapshot>(
                        &bytes,
                    ) {
                        Ok(snapshot) => {
                            if snapshot.schema_version
                                > fakecloud_stepfunctions::STEPFUNCTIONS_SNAPSHOT_SCHEMA_VERSION
                            {
                                fatal_exit(format_args!(
                                    "stepfunctions persistence schema too new: on-disk={}, max supported={}",
                                    snapshot.schema_version,
                                    fakecloud_stepfunctions::STEPFUNCTIONS_SNAPSHOT_SCHEMA_VERSION,
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
            // Executions that were RUNNING when the server stopped have no
            // interpreter driving them anymore; abort them so they don't report
            // RUNNING forever (bug-audit 2026-06-20, 0.A2).
            let aborted =
                fakecloud_stepfunctions::reconcile_interrupted_executions(&stepfunctions_state);
            if aborted > 0 {
                tracing::info!(
                    aborted,
                    "aborted stepfunctions executions interrupted by restart"
                );
            }
            Some(Arc::new(store) as Arc<dyn fakecloud_persistence::SnapshotStore>)
        } else {
            None
        };
    if let Some(store) = sfn_snapshot_store {
        sfn_service = sfn_service.with_snapshot_store(store);
    }
    if let Some(h) = sfn_service.snapshot_hook() {
        cfn_snapshot_hooks.insert("stepfunctions", h);
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
                    match serde_json::from_slice::<fakecloud_apigatewayv2::ApiGatewayV2Snapshot>(
                        &bytes,
                    ) {
                        Ok(snapshot) => {
                            if snapshot.schema_version
                                > fakecloud_apigatewayv2::APIGATEWAYV2_SNAPSHOT_SCHEMA_VERSION
                            {
                                fatal_exit(format_args!(
                                    "apigatewayv2 persistence schema too new: on-disk={}, max supported={}",
                                    snapshot.schema_version,
                                    fakecloud_apigatewayv2::APIGATEWAYV2_SNAPSHOT_SCHEMA_VERSION,
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
    let mut apigw_service = ApiGatewayV2Service::new(apigatewayv2_state.clone())
        .with_waf(wafv2_state.clone(), wafv2_rate_limiter.clone());
    if let Some(ref ld) = lambda_delivery {
        let cognito_jwt_verifier: Arc<dyn fakecloud_core::delivery::CognitoJwtVerifier> = Arc::new(
            fakecloud_cognito::StateBackedJwtVerifier::new(cognito_state.clone()),
        );
        let delivery_for_apigw = Arc::new(
            DeliveryBus::new()
                .with_lambda(ld.clone())
                .with_cognito_jwt_verifier(cognito_jwt_verifier),
        );
        apigw_service = apigw_service.with_delivery(delivery_for_apigw);
    }
    if let Some(store) = apigw_snapshot_store {
        apigw_service = apigw_service.with_snapshot_store(store);
    }
    let apigatewayv2_service = Arc::new(apigw_service);
    if let Some(h) = apigatewayv2_service.snapshot_hook() {
        cfn_snapshot_hooks.insert("apigatewayv2", h);
    }
    let v2_arc: Arc<dyn fakecloud_core::service::AwsService> = apigatewayv2_service.clone();
    // v1 (REST APIs) shares the SigV4 service identifier `apigateway`
    // with v2; the registry is keyed by that identifier so we wrap
    // both behind a facade that routes by URL prefix.
    let apigw_v1_snapshot_store: Option<Arc<dyn fakecloud_persistence::SnapshotStore>> =
        if persistence_config.mode == fakecloud_persistence::StorageMode::Persistent {
            let data_path = persistence_config
                .data_path
                .as_ref()
                .expect("validated above")
                .clone();
            let path = data_path.join("apigatewayv1").join("snapshot.json");
            let store = fakecloud_persistence::DiskSnapshotStore::new(path);
            match fakecloud_persistence::SnapshotStore::load(&store) {
                Ok(Some(bytes)) => {
                    match serde_json::from_slice::<fakecloud_apigateway::ApiGatewaySnapshot>(&bytes)
                    {
                        Ok(snapshot) => {
                            if snapshot.schema_version
                                > fakecloud_apigateway::APIGATEWAY_SNAPSHOT_SCHEMA_VERSION
                            {
                                fatal_exit(format_args!(
                                    "apigatewayv1 persistence schema too new: on-disk={}, max supported={}",
                                    snapshot.schema_version,
                                    fakecloud_apigateway::APIGATEWAY_SNAPSHOT_SCHEMA_VERSION,
                                ));
                            }
                            if let Some(accounts) = snapshot.accounts {
                                let account_count = accounts.account_count();
                                *apigatewayv1_state.write() = accounts;
                                tracing::info!(
                                    accounts = account_count,
                                    "loaded apigatewayv1 persistence snapshot",
                                );
                            }
                        }
                        Err(err) => fatal_exit(format_args!(
                            "failed to parse apigatewayv1 persistence snapshot: {err}"
                        )),
                    }
                }
                Ok(None) => {
                    tracing::info!("no apigatewayv1 persistence snapshot found; starting empty");
                }
                Err(err) => fatal_exit(format_args!(
                    "failed to read apigatewayv1 persistence snapshot: {err}"
                )),
            }
            Some(Arc::new(store) as Arc<dyn fakecloud_persistence::SnapshotStore>)
        } else {
            None
        };
    let apigw_v1_registry_handle = Arc::new(std::sync::OnceLock::new());
    let mut apigw_v1_service = ApiGatewayService::new(apigatewayv1_state.clone())
        .with_waf(wafv2_state.clone(), wafv2_rate_limiter.clone())
        .with_elbv2(elbv2_state.clone())
        .with_registry(apigw_v1_registry_handle.clone());
    {
        let cognito_jwt_verifier: Arc<dyn fakecloud_core::delivery::CognitoJwtVerifier> = Arc::new(
            fakecloud_cognito::StateBackedJwtVerifier::new(cognito_state.clone()),
        );
        let mut bus = DeliveryBus::new().with_cognito_jwt_verifier(cognito_jwt_verifier);
        if let Some(ref ld) = lambda_delivery {
            bus = bus.with_lambda(ld.clone());
        }
        apigw_v1_service = apigw_v1_service.with_delivery(Arc::new(bus));
    }
    if let Some(store) = apigw_v1_snapshot_store {
        apigw_v1_service = apigw_v1_service.with_snapshot_store(store);
    }
    let v1_arc = Arc::new(apigw_v1_service);
    if let Some(h) = v1_arc.snapshot_hook() {
        cfn_snapshot_hooks.insert("apigateway", h);
    }
    registry.register(Arc::new(ApiGatewayFacade::new(v1_arc, v2_arc)));
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
                    match serde_json::from_slice::<fakecloud_bedrock::BedrockSnapshot>(&bytes) {
                        Ok(snapshot) => {
                            if snapshot.schema_version
                                > fakecloud_bedrock::BEDROCK_SNAPSHOT_SCHEMA_VERSION
                            {
                                fatal_exit(format_args!(
                                    "bedrock persistence schema too new: on-disk={}, max supported={}",
                                    snapshot.schema_version,
                                    fakecloud_bedrock::BEDROCK_SNAPSHOT_SCHEMA_VERSION,
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
    if let Some(h) = bedrock_service.snapshot_hook() {
        cfn_snapshot_hooks.insert("bedrock", h);
    }
    registry.register(Arc::new(bedrock_service));
    let bedrock_agent_snapshot_store: Option<Arc<dyn fakecloud_persistence::SnapshotStore>> =
        if persistence_config.mode == fakecloud_persistence::StorageMode::Persistent {
            let data_path = persistence_config
                .data_path
                .as_ref()
                .expect("validated above")
                .clone();
            let path = data_path.join("bedrock-agent").join("snapshot.json");
            let store = fakecloud_persistence::DiskSnapshotStore::new(path);
            match fakecloud_persistence::SnapshotStore::load(&store) {
                Ok(Some(bytes)) => {
                    match serde_json::from_slice::<fakecloud_bedrock_agent::BedrockAgentSnapshot>(
                        &bytes,
                    ) {
                        Ok(snapshot) => {
                            if snapshot.schema_version
                                > fakecloud_bedrock_agent::BEDROCK_AGENT_SNAPSHOT_SCHEMA_VERSION
                            {
                                fatal_exit(format_args!(
                                    "bedrock-agent persistence schema too new: on-disk={}, max supported={}",
                                    snapshot.schema_version,
                                    fakecloud_bedrock_agent::BEDROCK_AGENT_SNAPSHOT_SCHEMA_VERSION,
                                ));
                            }
                            if let Some(accounts) = snapshot.accounts {
                                let account_count = accounts.accounts.len();
                                *bedrock_agent_state.write() = accounts;
                                tracing::info!(
                                    accounts = account_count,
                                    "loaded bedrock-agent persistence snapshot"
                                );
                            }
                        }
                        Err(err) => fatal_exit(format_args!(
                            "failed to parse bedrock-agent persistence snapshot: {err}"
                        )),
                    }
                }
                Ok(None) => {
                    tracing::info!("no bedrock-agent persistence snapshot found; starting empty");
                }
                Err(err) => fatal_exit(format_args!(
                    "failed to read bedrock-agent persistence snapshot: {err}"
                )),
            }
            Some(Arc::new(store) as Arc<dyn fakecloud_persistence::SnapshotStore>)
        } else {
            None
        };
    let mut bedrock_agent_service = BedrockAgentService::new(bedrock_agent_state.clone());
    if let Some(store) = bedrock_agent_snapshot_store.clone() {
        bedrock_agent_service = bedrock_agent_service.with_snapshot_store(store);
    }
    if let Some(h) = bedrock_agent_service.snapshot_hook() {
        cfn_snapshot_hooks.insert("bedrock-agent", h);
    }
    registry.register(Arc::new(bedrock_agent_service));
    registry.register(Arc::new(
        BedrockAgentRuntimeService::new(bedrock_agent_runtime_state.clone())
            .with_agent_state(bedrock_agent_state.clone()),
    ));
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
    // Clone the snapshot store for the background ticker, which mutates
    // schedule state (last_fired, one-shot deletion) outside the service's
    // action-dispatch path and so must write through itself (bug-audit
    // 2026-06-20, 0.A5).
    let scheduler_ticker_snapshot_store = scheduler_snapshot_store.clone();
    let mut scheduler_service = SchedulerService::new(scheduler_state.clone());
    if let Some(store) = scheduler_snapshot_store {
        scheduler_service = scheduler_service.with_snapshot_store(store);
    }
    if let Some(h) = scheduler_service.snapshot_hook() {
        cfn_snapshot_hooks.insert("scheduler", h);
    }
    registry.register(Arc::new(scheduler_service));

    // Aurora DSQL control plane.
    let dsql_snapshot_store: Option<Arc<dyn fakecloud_persistence::SnapshotStore>> =
        if persistence_config.mode == fakecloud_persistence::StorageMode::Persistent {
            let data_path = persistence_config
                .data_path
                .as_ref()
                .expect("validated above")
                .clone();
            let path = data_path.join("dsql").join("snapshot.json");
            let store = fakecloud_persistence::DiskSnapshotStore::new(path);
            match fakecloud_dsql::persistence::load_into(&store, &dsql_state) {
                Ok(fakecloud_dsql::persistence::LoadOutcome::Loaded(accounts)) => {
                    tracing::info!(accounts, "loaded dsql persistence snapshot");
                }
                Ok(fakecloud_dsql::persistence::LoadOutcome::Empty) => {
                    tracing::info!("no dsql persistence snapshot found; starting empty");
                }
                Err(err) => fatal_exit(format_args!("{err}")),
            }
            Some(Arc::new(store) as Arc<dyn fakecloud_persistence::SnapshotStore>)
        } else {
            None
        };
    let mut dsql_service = DsqlService::new(dsql_state.clone());
    if let Some(store) = dsql_snapshot_store {
        dsql_service = dsql_service.with_snapshot_store(store);
    }
    if let Some(h) = dsql_service.snapshot_hook() {
        cfn_snapshot_hooks.insert("dsql", h);
    }
    // Advance CREATING -> ACTIVE / DELETING -> DELETED out of band so waiters
    // converge; the ticker owns its own snapshot write-through.
    dsql_service.start_ticker();
    registry.register(Arc::new(dsql_service));

    // Resource Groups control plane.
    let resource_groups_snapshot_store: Option<Arc<dyn fakecloud_persistence::SnapshotStore>> =
        if persistence_config.mode == fakecloud_persistence::StorageMode::Persistent {
            let data_path = persistence_config
                .data_path
                .as_ref()
                .expect("validated above")
                .clone();
            let path = data_path.join("resource-groups").join("snapshot.json");
            let store = fakecloud_persistence::DiskSnapshotStore::new(path);
            match fakecloud_resource_groups::persistence::load_into(&store, &resource_groups_state)
            {
                Ok(fakecloud_resource_groups::persistence::LoadOutcome::Loaded(accounts)) => {
                    tracing::info!(accounts, "loaded resource-groups persistence snapshot");
                }
                Ok(fakecloud_resource_groups::persistence::LoadOutcome::Empty) => {
                    tracing::info!("no resource-groups persistence snapshot found; starting empty");
                }
                Err(err) => fatal_exit(format_args!("{err}")),
            }
            Some(Arc::new(store) as Arc<dyn fakecloud_persistence::SnapshotStore>)
        } else {
            None
        };
    let mut resource_groups_service =
        fakecloud_resource_groups::ResourceGroupsService::new(resource_groups_state.clone());
    if let Some(store) = resource_groups_snapshot_store {
        resource_groups_service = resource_groups_service.with_snapshot_store(store);
    }
    registry.register(Arc::new(resource_groups_service));

    // Account Management control plane.
    let account_snapshot_store: Option<Arc<dyn fakecloud_persistence::SnapshotStore>> =
        if persistence_config.mode == fakecloud_persistence::StorageMode::Persistent {
            let data_path = persistence_config
                .data_path
                .as_ref()
                .expect("validated above")
                .clone();
            let path = data_path.join("account").join("snapshot.json");
            let store = fakecloud_persistence::DiskSnapshotStore::new(path);
            match fakecloud_account::persistence::load_into(&store, &account_state) {
                Ok(fakecloud_account::persistence::LoadOutcome::Loaded(accounts)) => {
                    tracing::info!(accounts, "loaded account persistence snapshot");
                }
                Ok(fakecloud_account::persistence::LoadOutcome::Empty) => {
                    tracing::info!("no account persistence snapshot found; starting empty");
                }
                Err(err) => fatal_exit(format_args!("{err}")),
            }
            Some(Arc::new(store) as Arc<dyn fakecloud_persistence::SnapshotStore>)
        } else {
            None
        };
    let mut account_service = fakecloud_account::AccountService::new(account_state.clone());
    if let Some(store) = account_snapshot_store {
        account_service = account_service.with_snapshot_store(store);
    }
    registry.register(Arc::new(account_service));

    // IAM Identity Center Identity Store control plane.
    let identitystore_snapshot_store: Option<Arc<dyn fakecloud_persistence::SnapshotStore>> =
        if persistence_config.mode == fakecloud_persistence::StorageMode::Persistent {
            let data_path = persistence_config
                .data_path
                .as_ref()
                .expect("validated above")
                .clone();
            let path = data_path.join("identitystore").join("snapshot.json");
            let store = fakecloud_persistence::DiskSnapshotStore::new(path);
            match fakecloud_identitystore::persistence::load_into(&store, &identitystore_state) {
                Ok(fakecloud_identitystore::persistence::LoadOutcome::Loaded(accounts)) => {
                    tracing::info!(accounts, "loaded identitystore persistence snapshot");
                }
                Ok(fakecloud_identitystore::persistence::LoadOutcome::Empty) => {
                    tracing::info!("no identitystore persistence snapshot found; starting empty");
                }
                Err(err) => fatal_exit(format_args!("{err}")),
            }
            Some(Arc::new(store) as Arc<dyn fakecloud_persistence::SnapshotStore>)
        } else {
            None
        };
    let mut identitystore_service =
        fakecloud_identitystore::IdentityStoreService::new(identitystore_state.clone());
    if let Some(store) = identitystore_snapshot_store {
        identitystore_service = identitystore_service.with_snapshot_store(store);
    }
    registry.register(Arc::new(identitystore_service));

    // IAM Identity Center SSO Admin control plane.
    let ssoadmin_snapshot_store: Option<Arc<dyn fakecloud_persistence::SnapshotStore>> =
        if persistence_config.mode == fakecloud_persistence::StorageMode::Persistent {
            let data_path = persistence_config
                .data_path
                .as_ref()
                .expect("validated above")
                .clone();
            let path = data_path.join("ssoadmin").join("snapshot.json");
            let store = fakecloud_persistence::DiskSnapshotStore::new(path);
            match fakecloud_ssoadmin::persistence::load_into(&store, &ssoadmin_state) {
                Ok(fakecloud_ssoadmin::persistence::LoadOutcome::Loaded(accounts)) => {
                    tracing::info!(accounts, "loaded ssoadmin persistence snapshot");
                }
                Ok(fakecloud_ssoadmin::persistence::LoadOutcome::Empty) => {
                    tracing::info!("no ssoadmin persistence snapshot found; starting empty");
                }
                Err(err) => fatal_exit(format_args!("{err}")),
            }
            Some(Arc::new(store) as Arc<dyn fakecloud_persistence::SnapshotStore>)
        } else {
            None
        };
    fakecloud_ssoadmin::state::ensure_default_instance(
        &ssoadmin_state,
        &cli.account_id,
        &cli.region,
    );
    let mut ssoadmin_service = fakecloud_ssoadmin::SsoAdminService::new(ssoadmin_state.clone());
    if let Some(store) = ssoadmin_snapshot_store {
        ssoadmin_service = ssoadmin_service.with_snapshot_store(store);
    }
    registry.register(Arc::new(ssoadmin_service));

    // Database Migration Service control plane.
    let dms_snapshot_store: Option<Arc<dyn fakecloud_persistence::SnapshotStore>> =
        if persistence_config.mode == fakecloud_persistence::StorageMode::Persistent {
            let data_path = persistence_config
                .data_path
                .as_ref()
                .expect("validated above")
                .clone();
            let path = data_path.join("dms").join("snapshot.json");
            let store = fakecloud_persistence::DiskSnapshotStore::new(path);
            match fakecloud_dms::persistence::load_into(&store, &dms_state) {
                Ok(fakecloud_dms::persistence::LoadOutcome::Loaded(accounts)) => {
                    tracing::info!(accounts, "loaded dms persistence snapshot");
                }
                Ok(fakecloud_dms::persistence::LoadOutcome::Empty) => {
                    tracing::info!("no dms persistence snapshot found; starting empty");
                }
                Err(err) => fatal_exit(format_args!("{err}")),
            }
            Some(Arc::new(store) as Arc<dyn fakecloud_persistence::SnapshotStore>)
        } else {
            None
        };
    let mut dms_service = fakecloud_dms::DmsService::new(dms_state.clone());
    if let Some(store) = dms_snapshot_store {
        dms_service = dms_service.with_snapshot_store(store);
    }
    registry.register(Arc::new(dms_service));

    // CloudTrail control plane.
    let cloudtrail_snapshot_store: Option<Arc<dyn fakecloud_persistence::SnapshotStore>> =
        if persistence_config.mode == fakecloud_persistence::StorageMode::Persistent {
            let data_path = persistence_config
                .data_path
                .as_ref()
                .expect("validated above")
                .clone();
            let path = data_path.join("cloudtrail").join("snapshot.json");
            let store = fakecloud_persistence::DiskSnapshotStore::new(path);
            match fakecloud_cloudtrail::persistence::load_into(&store, &cloudtrail_state) {
                Ok(fakecloud_cloudtrail::persistence::LoadOutcome::Loaded(accounts)) => {
                    tracing::info!(accounts, "loaded cloudtrail persistence snapshot");
                }
                Ok(fakecloud_cloudtrail::persistence::LoadOutcome::Empty) => {
                    tracing::info!("no cloudtrail persistence snapshot found; starting empty");
                }
                Err(err) => fatal_exit(format_args!("{err}")),
            }
            Some(Arc::new(store) as Arc<dyn fakecloud_persistence::SnapshotStore>)
        } else {
            None
        };
    let mut cloudtrail_service =
        fakecloud_cloudtrail::CloudTrailService::new(cloudtrail_state.clone());
    if let Some(store) = cloudtrail_snapshot_store {
        cloudtrail_service = cloudtrail_service.with_snapshot_store(store);
    }
    registry.register(Arc::new(cloudtrail_service));

    // Cost Explorer control plane.
    let ce_snapshot_store: Option<Arc<dyn fakecloud_persistence::SnapshotStore>> =
        if persistence_config.mode == fakecloud_persistence::StorageMode::Persistent {
            let data_path = persistence_config
                .data_path
                .as_ref()
                .expect("validated above")
                .clone();
            let path = data_path.join("ce").join("snapshot.json");
            let store = fakecloud_persistence::DiskSnapshotStore::new(path);
            match fakecloud_ce::persistence::load_into(&store, &ce_state) {
                Ok(fakecloud_ce::persistence::LoadOutcome::Loaded(accounts)) => {
                    tracing::info!(accounts, "loaded ce persistence snapshot");
                }
                Ok(fakecloud_ce::persistence::LoadOutcome::Empty) => {
                    tracing::info!("no ce persistence snapshot found; starting empty");
                }
                Err(err) => fatal_exit(format_args!("{err}")),
            }
            Some(Arc::new(store) as Arc<dyn fakecloud_persistence::SnapshotStore>)
        } else {
            None
        };
    let mut ce_service = fakecloud_ce::CeService::new(ce_state.clone());
    if let Some(store) = ce_snapshot_store {
        ce_service = ce_service.with_snapshot_store(store);
    }
    registry.register(Arc::new(ce_service));

    // Transfer Family control plane.
    let transfer_snapshot_store: Option<Arc<dyn fakecloud_persistence::SnapshotStore>> =
        if persistence_config.mode == fakecloud_persistence::StorageMode::Persistent {
            let data_path = persistence_config
                .data_path
                .as_ref()
                .expect("validated above")
                .clone();
            let path = data_path.join("transfer").join("snapshot.json");
            let store = fakecloud_persistence::DiskSnapshotStore::new(path);
            match fakecloud_transfer::persistence::load_into(&store, &transfer_state) {
                Ok(fakecloud_transfer::persistence::LoadOutcome::Loaded(accounts)) => {
                    tracing::info!(accounts, "loaded transfer persistence snapshot");
                }
                Ok(fakecloud_transfer::persistence::LoadOutcome::Empty) => {
                    tracing::info!("no transfer persistence snapshot found; starting empty");
                }
                Err(err) => fatal_exit(format_args!("{err}")),
            }
            Some(Arc::new(store) as Arc<dyn fakecloud_persistence::SnapshotStore>)
        } else {
            None
        };
    let mut transfer_service = fakecloud_transfer::TransferService::new(transfer_state.clone());
    if let Some(store) = transfer_snapshot_store {
        transfer_service = transfer_service.with_snapshot_store(store);
    }
    registry.register(Arc::new(transfer_service));

    // Verified Permissions control plane + Cedar authorization.
    let verifiedpermissions_snapshot_store: Option<Arc<dyn fakecloud_persistence::SnapshotStore>> =
        if persistence_config.mode == fakecloud_persistence::StorageMode::Persistent {
            let data_path = persistence_config
                .data_path
                .as_ref()
                .expect("validated above")
                .clone();
            let path = data_path.join("verifiedpermissions").join("snapshot.json");
            let store = fakecloud_persistence::DiskSnapshotStore::new(path);
            match fakecloud_verifiedpermissions::persistence::load_into(
                &store,
                &verifiedpermissions_state,
            ) {
                Ok(fakecloud_verifiedpermissions::persistence::LoadOutcome::Loaded(accounts)) => {
                    tracing::info!(accounts, "loaded verifiedpermissions persistence snapshot");
                }
                Ok(fakecloud_verifiedpermissions::persistence::LoadOutcome::Empty) => {
                    tracing::info!(
                        "no verifiedpermissions persistence snapshot found; starting empty"
                    );
                }
                Err(err) => fatal_exit(format_args!("{err}")),
            }
            Some(Arc::new(store) as Arc<dyn fakecloud_persistence::SnapshotStore>)
        } else {
            None
        };
    let mut verifiedpermissions_service =
        fakecloud_verifiedpermissions::VerifiedPermissionsService::new(
            verifiedpermissions_state.clone(),
        );
    if let Some(store) = verifiedpermissions_snapshot_store {
        verifiedpermissions_service = verifiedpermissions_service.with_snapshot_store(store);
    }
    registry.register(Arc::new(verifiedpermissions_service));

    // MemoryDB control plane.
    let memorydb_snapshot_store: Option<Arc<dyn fakecloud_persistence::SnapshotStore>> =
        if persistence_config.mode == fakecloud_persistence::StorageMode::Persistent {
            let data_path = persistence_config
                .data_path
                .as_ref()
                .expect("validated above")
                .clone();
            let path = data_path.join("memorydb").join("snapshot.json");
            let store = fakecloud_persistence::DiskSnapshotStore::new(path);
            match fakecloud_memorydb::persistence::load_into(&store, &memorydb_state) {
                Ok(fakecloud_memorydb::persistence::LoadOutcome::Loaded(accounts)) => {
                    tracing::info!(accounts, "loaded memorydb persistence snapshot");
                }
                Ok(fakecloud_memorydb::persistence::LoadOutcome::Empty) => {
                    tracing::info!("no memorydb persistence snapshot found; starting empty");
                }
                Err(err) => fatal_exit(format_args!("{err}")),
            }
            Some(Arc::new(store) as Arc<dyn fakecloud_persistence::SnapshotStore>)
        } else {
            None
        };
    let mut memorydb_service = fakecloud_memorydb::MemoryDbService::new(memorydb_state.clone());
    if let Some(store) = memorydb_snapshot_store {
        memorydb_service = memorydb_service.with_snapshot_store(store);
    }
    registry.register(Arc::new(memorydb_service));

    // Amazon Managed Service for Apache Flink (kinesisanalyticsv2) control plane.
    let kinesisanalyticsv2_snapshot_store: Option<Arc<dyn fakecloud_persistence::SnapshotStore>> =
        if persistence_config.mode == fakecloud_persistence::StorageMode::Persistent {
            let data_path = persistence_config
                .data_path
                .as_ref()
                .expect("validated above")
                .clone();
            let path = data_path.join("kinesisanalyticsv2").join("snapshot.json");
            let store = fakecloud_persistence::DiskSnapshotStore::new(path);
            match fakecloud_kinesisanalyticsv2::persistence::load_into(
                &store,
                &kinesisanalyticsv2_state,
            ) {
                Ok(fakecloud_kinesisanalyticsv2::persistence::LoadOutcome::Loaded(accounts)) => {
                    tracing::info!(accounts, "loaded kinesisanalyticsv2 persistence snapshot");
                }
                Ok(fakecloud_kinesisanalyticsv2::persistence::LoadOutcome::Empty) => {
                    tracing::info!(
                        "no kinesisanalyticsv2 persistence snapshot found; starting empty"
                    );
                }
                Err(err) => fatal_exit(format_args!("{err}")),
            }
            Some(Arc::new(store) as Arc<dyn fakecloud_persistence::SnapshotStore>)
        } else {
            None
        };
    let mut kinesisanalyticsv2_service =
        fakecloud_kinesisanalyticsv2::Ka2Service::new(kinesisanalyticsv2_state.clone())
            .with_ec2_state(ec2_state.clone());
    if let Some(store) = kinesisanalyticsv2_snapshot_store {
        kinesisanalyticsv2_service = kinesisanalyticsv2_service.with_snapshot_store(store);
    }
    // Amazon Managed Service for Apache Flink: a Flink-flavor application with a
    // JAR in S3 runs as a REAL Apache Flink job in a Docker container (the
    // MQ/MSK data-plane bar). Attach the backing-container runtime + the
    // in-process S3 reader used to fetch the application's code JAR. `None` when
    // no container CLI is available or the backend is disabled -> the app stays
    // on the control-plane state machine.
    if let Some(flink_runtime) = fakecloud_kinesisanalyticsv2::FlinkRuntime::new().map(Arc::new) {
        kinesisanalyticsv2_service = kinesisanalyticsv2_service
            .with_runtime(flink_runtime)
            .with_s3(s3_delivery_for_logs.clone());
    }
    // Re-attach backing Flink containers for persisted RUNNING apps (same
    // restart-recovery contract as MSK / MQ / RDS). Fire-and-forget per app.
    kinesisanalyticsv2_service
        .recover_persisted_containers()
        .await;
    // CloudFormation `AWS::KinesisAnalyticsV2::*` resources mutate the ka2 state
    // directly; register its snapshot hook so a CFN-provisioned application is
    // written through to disk after a stack op, matching the direct API.
    if let Some(h) = kinesisanalyticsv2_service.snapshot_hook() {
        cfn_snapshot_hooks.insert("kinesisanalyticsv2", h);
    }
    registry.register(Arc::new(kinesisanalyticsv2_service));

    // Cloud Map (servicediscovery) namespace control plane.
    let servicediscovery_snapshot_store: Option<Arc<dyn fakecloud_persistence::SnapshotStore>> =
        if persistence_config.mode == fakecloud_persistence::StorageMode::Persistent {
            let data_path = persistence_config
                .data_path
                .as_ref()
                .expect("validated above")
                .clone();
            let path = data_path.join("servicediscovery").join("snapshot.json");
            let store = fakecloud_persistence::DiskSnapshotStore::new(path);
            match fakecloud_servicediscovery::persistence::load_into(
                &store,
                &servicediscovery_state,
            ) {
                Ok(fakecloud_servicediscovery::persistence::LoadOutcome::Loaded(accounts)) => {
                    tracing::info!(accounts, "loaded servicediscovery persistence snapshot");
                }
                Ok(fakecloud_servicediscovery::persistence::LoadOutcome::Empty) => {
                    tracing::info!(
                        "no servicediscovery persistence snapshot found; starting empty"
                    );
                }
                Err(err) => fatal_exit(format_args!("{err}")),
            }
            Some(Arc::new(store) as Arc<dyn fakecloud_persistence::SnapshotStore>)
        } else {
            None
        };
    let mut servicediscovery_service =
        fakecloud_servicediscovery::ServiceDiscoveryService::new(servicediscovery_state.clone());
    if let Some(store) = servicediscovery_snapshot_store {
        servicediscovery_service = servicediscovery_service.with_snapshot_store(store);
    }
    if let Some(h) = servicediscovery_service.snapshot_hook() {
        cfn_snapshot_hooks.insert("servicediscovery", h);
    }
    registry.register(Arc::new(servicediscovery_service));

    // EKS cluster control plane.
    let eks_snapshot_store: Option<Arc<dyn fakecloud_persistence::SnapshotStore>> =
        if persistence_config.mode == fakecloud_persistence::StorageMode::Persistent {
            let data_path = persistence_config
                .data_path
                .as_ref()
                .expect("validated above")
                .clone();
            let path = data_path.join("eks").join("snapshot.json");
            let store = fakecloud_persistence::DiskSnapshotStore::new(path);
            match fakecloud_eks::persistence::load_into(&store, &eks_state) {
                Ok(fakecloud_eks::persistence::LoadOutcome::Loaded(accounts)) => {
                    tracing::info!(accounts, "loaded eks persistence snapshot");
                }
                Ok(fakecloud_eks::persistence::LoadOutcome::Empty) => {
                    tracing::info!("no eks persistence snapshot found; starting empty");
                }
                Err(err) => fatal_exit(format_args!("{err}")),
            }
            Some(Arc::new(store) as Arc<dyn fakecloud_persistence::SnapshotStore>)
        } else {
            None
        };
    let mut eks_service = fakecloud_eks::EksService::new(eks_state.clone());
    if let Some(store) = eks_snapshot_store {
        eks_service = eks_service.with_snapshot_store(store);
    }
    if let Some(h) = eks_service.snapshot_hook() {
        cfn_snapshot_hooks.insert("eks", h);
    }
    registry.register(Arc::new(eks_service));

    // Amazon S3 Glacier: vaults, archives (real bytes + tree hash), multipart
    // uploads, retrieval/inventory jobs, vault lock, tags, and policies.
    let glacier_snapshot_store: Option<Arc<dyn fakecloud_persistence::SnapshotStore>> =
        if persistence_config.mode == fakecloud_persistence::StorageMode::Persistent {
            let data_path = persistence_config
                .data_path
                .as_ref()
                .expect("validated above")
                .clone();
            let path = data_path.join("glacier").join("snapshot.json");
            let store = fakecloud_persistence::DiskSnapshotStore::new(path);
            match fakecloud_glacier::persistence::load_into(&store, &glacier_state) {
                Ok(fakecloud_glacier::persistence::LoadOutcome::Loaded(accounts)) => {
                    tracing::info!(accounts, "loaded glacier persistence snapshot");
                }
                Ok(fakecloud_glacier::persistence::LoadOutcome::Empty) => {
                    tracing::info!("no glacier persistence snapshot found; starting empty");
                }
                Err(err) => fatal_exit(format_args!("{err}")),
            }
            Some(Arc::new(store) as Arc<dyn fakecloud_persistence::SnapshotStore>)
        } else {
            None
        };
    let mut glacier_service = fakecloud_glacier::GlacierService::new(glacier_state.clone());
    if let Some(store) = glacier_snapshot_store {
        glacier_service = glacier_service.with_snapshot_store(store);
    }
    if let Some(h) = glacier_service.snapshot_hook() {
        cfn_snapshot_hooks.insert("glacier", h);
    }
    registry.register(Arc::new(glacier_service));

    // AWS Elastic Beanstalk: applications, application versions, environments
    // (with an async Launching->Ready lifecycle), configuration templates,
    // configuration option settings, events, platforms. Orchestration facade;
    // control-plane-complete with the app-execution data-plane deferred.
    // `beanstalk_state` is created earlier (near the CodeCommit state) so the
    // CloudFormation provisioner can share it; this block loads persistence and
    // wires the full service on top of that same state.
    let beanstalk_snapshot_store: Option<Arc<dyn fakecloud_persistence::SnapshotStore>> =
        if persistence_config.mode == fakecloud_persistence::StorageMode::Persistent {
            let data_path = persistence_config
                .data_path
                .as_ref()
                .expect("validated above")
                .clone();
            let path = data_path.join("elasticbeanstalk").join("snapshot.json");
            let store = fakecloud_persistence::DiskSnapshotStore::new(path);
            match fakecloud_persistence::SnapshotStore::load(&store) {
                Ok(Some(bytes)) => {
                    match serde_json::from_slice::<
                        fakecloud_elasticbeanstalk::ElasticBeanstalkSnapshot,
                    >(&bytes)
                    {
                        Ok(snapshot) => {
                            if snapshot.schema_version
                                > fakecloud_elasticbeanstalk::ELASTICBEANSTALK_SNAPSHOT_SCHEMA_VERSION
                            {
                                fatal_exit(format_args!(
                                    "elasticbeanstalk persistence schema too new: on-disk={}, max supported={}",
                                    snapshot.schema_version,
                                    fakecloud_elasticbeanstalk::ELASTICBEANSTALK_SNAPSHOT_SCHEMA_VERSION,
                                ));
                            }
                            if let Some(accounts) = snapshot.accounts {
                                let account_count = accounts.account_count();
                                *beanstalk_state.write() = accounts;
                                tracing::info!(
                                    accounts = account_count,
                                    "loaded elasticbeanstalk persistence snapshot (multi-account)"
                                );
                            }
                        }
                        Err(err) => fatal_exit(format_args!(
                            "failed to parse elasticbeanstalk persistence snapshot: {err}"
                        )),
                    }
                }
                Ok(None) => {
                    tracing::info!(
                        "no elasticbeanstalk persistence snapshot found; starting empty"
                    );
                }
                Err(err) => fatal_exit(format_args!(
                    "failed to read elasticbeanstalk persistence snapshot: {err}"
                )),
            }
            Some(Arc::new(store) as Arc<dyn fakecloud_persistence::SnapshotStore>)
        } else {
            None
        };
    let mut beanstalk_service =
        fakecloud_elasticbeanstalk::ElasticBeanstalkService::new(beanstalk_state.clone());
    if let Some(store) = beanstalk_snapshot_store {
        beanstalk_service = beanstalk_service.with_snapshot_store(store);
    }
    // Re-drive any environment left mid-transition by a restart, mirroring the
    // RDS / ElastiCache container recovery contract.
    beanstalk_service.recover_pending_environments();
    if let Some(h) = beanstalk_service.snapshot_hook() {
        cfn_snapshot_hooks.insert("elasticbeanstalk", h);
    }
    registry.register(Arc::new(beanstalk_service));

    // AWS Backup control plane.
    let backup_snapshot_store: Option<Arc<dyn fakecloud_persistence::SnapshotStore>> =
        if persistence_config.mode == fakecloud_persistence::StorageMode::Persistent {
            let data_path = persistence_config
                .data_path
                .as_ref()
                .expect("validated above")
                .clone();
            let path = data_path.join("backup").join("snapshot.json");
            let store = fakecloud_persistence::DiskSnapshotStore::new(path);
            match fakecloud_backup::persistence::load_into(&store, &backup_state) {
                Ok(fakecloud_backup::persistence::LoadOutcome::Loaded(accounts)) => {
                    tracing::info!(accounts, "loaded backup persistence snapshot");
                }
                Ok(fakecloud_backup::persistence::LoadOutcome::Empty) => {
                    tracing::info!("no backup persistence snapshot found; starting empty");
                }
                Err(err) => fatal_exit(format_args!("{err}")),
            }
            Some(Arc::new(store) as Arc<dyn fakecloud_persistence::SnapshotStore>)
        } else {
            None
        };
    let mut backup_service = fakecloud_backup::BackupService::new(backup_state.clone());
    if let Some(store) = backup_snapshot_store {
        backup_service = backup_service.with_snapshot_store(store);
    }
    if let Some(h) = backup_service.snapshot_hook() {
        cfn_snapshot_hooks.insert("backup", h);
    }
    registry.register(Arc::new(backup_service));

    // AWS Resource Access Manager control plane.
    let ram_snapshot_store: Option<Arc<dyn fakecloud_persistence::SnapshotStore>> =
        if persistence_config.mode == fakecloud_persistence::StorageMode::Persistent {
            let data_path = persistence_config
                .data_path
                .as_ref()
                .expect("validated above")
                .clone();
            let path = data_path.join("ram").join("snapshot.json");
            let store = fakecloud_persistence::DiskSnapshotStore::new(path);
            match fakecloud_ram::persistence::load_into(&store, &ram_state) {
                Ok(fakecloud_ram::persistence::LoadOutcome::Loaded(accounts)) => {
                    tracing::info!(accounts, "loaded ram persistence snapshot");
                }
                Ok(fakecloud_ram::persistence::LoadOutcome::Empty) => {
                    tracing::info!("no ram persistence snapshot found; starting empty");
                }
                Err(err) => fatal_exit(format_args!("{err}")),
            }
            Some(Arc::new(store) as Arc<dyn fakecloud_persistence::SnapshotStore>)
        } else {
            None
        };
    let mut ram_service = fakecloud_ram::RamService::new(ram_state.clone());
    if let Some(store) = ram_snapshot_store {
        ram_service = ram_service.with_snapshot_store(store);
    }
    if let Some(h) = ram_service.snapshot_hook() {
        cfn_snapshot_hooks.insert("ram", h);
    }
    registry.register(Arc::new(ram_service));

    // Amazon S3 Tables control plane.
    let s3tables_snapshot_store: Option<Arc<dyn fakecloud_persistence::SnapshotStore>> =
        if persistence_config.mode == fakecloud_persistence::StorageMode::Persistent {
            let data_path = persistence_config
                .data_path
                .as_ref()
                .expect("validated above")
                .clone();
            let path = data_path.join("s3tables").join("snapshot.json");
            let store = fakecloud_persistence::DiskSnapshotStore::new(path);
            match fakecloud_s3tables::persistence::load_into(&store, &s3tables_state) {
                Ok(fakecloud_s3tables::persistence::LoadOutcome::Loaded(accounts)) => {
                    tracing::info!(accounts, "loaded s3tables persistence snapshot");
                }
                Ok(fakecloud_s3tables::persistence::LoadOutcome::Empty) => {
                    tracing::info!("no s3tables persistence snapshot found; starting empty");
                }
                Err(err) => fatal_exit(format_args!("{err}")),
            }
            Some(Arc::new(store) as Arc<dyn fakecloud_persistence::SnapshotStore>)
        } else {
            None
        };
    let mut s3tables_service = fakecloud_s3tables::S3TablesService::new(s3tables_state.clone());
    if let Some(store) = s3tables_snapshot_store {
        s3tables_service = s3tables_service.with_snapshot_store(store);
    }
    if let Some(h) = s3tables_service.snapshot_hook() {
        cfn_snapshot_hooks.insert("s3tables", h);
    }
    registry.register(Arc::new(s3tables_service));

    // AWS Lake Formation governance control plane.
    let lakeformation_snapshot_store: Option<Arc<dyn fakecloud_persistence::SnapshotStore>> =
        if persistence_config.mode == fakecloud_persistence::StorageMode::Persistent {
            let data_path = persistence_config
                .data_path
                .as_ref()
                .expect("validated above")
                .clone();
            let path = data_path.join("lakeformation").join("snapshot.json");
            let store = fakecloud_persistence::DiskSnapshotStore::new(path);
            match fakecloud_lakeformation::persistence::load_into(&store, &lakeformation_state) {
                Ok(fakecloud_lakeformation::persistence::LoadOutcome::Loaded(accounts)) => {
                    tracing::info!(accounts, "loaded lakeformation persistence snapshot");
                }
                Ok(fakecloud_lakeformation::persistence::LoadOutcome::Empty) => {
                    tracing::info!("no lakeformation persistence snapshot found; starting empty");
                }
                Err(err) => fatal_exit(format_args!("{err}")),
            }
            Some(Arc::new(store) as Arc<dyn fakecloud_persistence::SnapshotStore>)
        } else {
            None
        };
    let mut lakeformation_service =
        fakecloud_lakeformation::LakeFormationService::new(lakeformation_state.clone());
    if let Some(store) = lakeformation_snapshot_store {
        lakeformation_service = lakeformation_service.with_snapshot_store(store);
    }
    if let Some(h) = lakeformation_service.snapshot_hook() {
        cfn_snapshot_hooks.insert("lakeformation", h);
    }
    registry.register(Arc::new(lakeformation_service));

    // AWS CodeBuild control plane.
    let codebuild_snapshot_store: Option<Arc<dyn fakecloud_persistence::SnapshotStore>> =
        if persistence_config.mode == fakecloud_persistence::StorageMode::Persistent {
            let data_path = persistence_config
                .data_path
                .as_ref()
                .expect("validated above")
                .clone();
            let path = data_path.join("codebuild").join("snapshot.json");
            let store = fakecloud_persistence::DiskSnapshotStore::new(path);
            match fakecloud_codebuild::persistence::load_into(&store, &codebuild_state) {
                Ok(fakecloud_codebuild::persistence::LoadOutcome::Loaded(accounts)) => {
                    tracing::info!(accounts, "loaded codebuild persistence snapshot");
                }
                Ok(fakecloud_codebuild::persistence::LoadOutcome::Empty) => {
                    tracing::info!("no codebuild persistence snapshot found; starting empty");
                }
                Err(err) => fatal_exit(format_args!("{err}")),
            }
            Some(Arc::new(store) as Arc<dyn fakecloud_persistence::SnapshotStore>)
        } else {
            None
        };
    let mut codebuild_service = fakecloud_codebuild::CodeBuildService::new(codebuild_state.clone())
        .with_logs(logs_state.clone())
        .with_s3(Arc::new(fakecloud_s3::delivery::S3DeliveryImpl::new(
            s3_state.clone(),
        )))
        .with_secret_stores(ssm_state_for_codebuild, secretsmanager_state_for_codebuild)
        .with_backend_autodetect();
    if let Some(store) = codebuild_snapshot_store {
        codebuild_service = codebuild_service.with_snapshot_store(store);
    }
    if let Some(h) = codebuild_service.snapshot_hook() {
        cfn_snapshot_hooks.insert("codebuild", h);
    }
    registry.register(Arc::new(codebuild_service));

    // AWS CodeConnections: awsJson1.0 control plane (connections, hosts,
    // repository links, sync configurations).
    let codeconnections_snapshot_store: Option<Arc<dyn fakecloud_persistence::SnapshotStore>> =
        if persistence_config.mode == fakecloud_persistence::StorageMode::Persistent {
            let data_path = persistence_config
                .data_path
                .as_ref()
                .expect("validated above")
                .clone();
            let path = data_path.join("codeconnections").join("snapshot.json");
            let store = fakecloud_persistence::DiskSnapshotStore::new(path);
            match fakecloud_codeconnections::persistence::load_into(&store, &codeconnections_state)
            {
                Ok(fakecloud_codeconnections::persistence::LoadOutcome::Loaded(accounts)) => {
                    tracing::info!(accounts, "loaded codeconnections persistence snapshot");
                }
                Ok(fakecloud_codeconnections::persistence::LoadOutcome::Empty) => {
                    tracing::info!("no codeconnections persistence snapshot found; starting empty");
                }
                Err(err) => fatal_exit(format_args!("{err}")),
            }
            Some(Arc::new(store) as Arc<dyn fakecloud_persistence::SnapshotStore>)
        } else {
            None
        };
    let mut codeconnections_service =
        fakecloud_codeconnections::CodeConnectionsService::new(codeconnections_state.clone());
    if let Some(store) = codeconnections_snapshot_store {
        codeconnections_service = codeconnections_service.with_snapshot_store(store);
    }
    if let Some(h) = codeconnections_service.snapshot_hook() {
        cfn_snapshot_hooks.insert("codeconnections", h);
    }
    registry.register(Arc::new(codeconnections_service));

    // AWS CodeDeploy: awsJson1.1 control plane (applications, revisions,
    // deployment groups, deployment configurations, deployments).
    let codedeploy_snapshot_store: Option<Arc<dyn fakecloud_persistence::SnapshotStore>> =
        if persistence_config.mode == fakecloud_persistence::StorageMode::Persistent {
            let data_path = persistence_config
                .data_path
                .as_ref()
                .expect("validated above")
                .clone();
            let path = data_path.join("codedeploy").join("snapshot.json");
            let store = fakecloud_persistence::DiskSnapshotStore::new(path);
            match fakecloud_codedeploy::persistence::load_into(&store, &codedeploy_state) {
                Ok(fakecloud_codedeploy::persistence::LoadOutcome::Loaded(accounts)) => {
                    tracing::info!(accounts, "loaded codedeploy persistence snapshot");
                }
                Ok(fakecloud_codedeploy::persistence::LoadOutcome::Empty) => {
                    tracing::info!("no codedeploy persistence snapshot found; starting empty");
                }
                Err(err) => fatal_exit(format_args!("{err}")),
            }
            Some(Arc::new(store) as Arc<dyn fakecloud_persistence::SnapshotStore>)
        } else {
            None
        };
    let mut codedeploy_service =
        fakecloud_codedeploy::CodeDeployService::new(codedeploy_state.clone());
    if let Some(store) = codedeploy_snapshot_store {
        codedeploy_service = codedeploy_service.with_snapshot_store(store);
    }
    if let Some(h) = codedeploy_service.snapshot_hook() {
        cfn_snapshot_hooks.insert("codedeploy", h);
    }
    registry.register(Arc::new(codedeploy_service));

    // AWS CodePipeline: awsJson1.1 control plane (pipelines, executions,
    // custom action types, webhooks, jobs, tagging).
    let codepipeline_snapshot_store: Option<Arc<dyn fakecloud_persistence::SnapshotStore>> =
        if persistence_config.mode == fakecloud_persistence::StorageMode::Persistent {
            let data_path = persistence_config
                .data_path
                .as_ref()
                .expect("validated above")
                .clone();
            let path = data_path.join("codepipeline").join("snapshot.json");
            let store = fakecloud_persistence::DiskSnapshotStore::new(path);
            match fakecloud_codepipeline::persistence::load_into(&store, &codepipeline_state) {
                Ok(fakecloud_codepipeline::persistence::LoadOutcome::Loaded(accounts)) => {
                    tracing::info!(accounts, "loaded codepipeline persistence snapshot");
                }
                Ok(fakecloud_codepipeline::persistence::LoadOutcome::Empty) => {
                    tracing::info!("no codepipeline persistence snapshot found; starting empty");
                }
                Err(err) => fatal_exit(format_args!("{err}")),
            }
            Some(Arc::new(store) as Arc<dyn fakecloud_persistence::SnapshotStore>)
        } else {
            None
        };
    let mut codepipeline_service =
        fakecloud_codepipeline::CodePipelineService::new(codepipeline_state.clone());
    if let Some(store) = codepipeline_snapshot_store {
        codepipeline_service = codepipeline_service.with_snapshot_store(store);
    }
    if let Some(h) = codepipeline_service.snapshot_hook() {
        cfn_snapshot_hooks.insert("codepipeline", h);
    }
    registry.register(Arc::new(codepipeline_service));

    // AWS CodeArtifact: restJson1 control plane (domains, repositories,
    // package groups, packages/versions/assets, policies, auth, tagging).
    let codeartifact_snapshot_store: Option<Arc<dyn fakecloud_persistence::SnapshotStore>> =
        if persistence_config.mode == fakecloud_persistence::StorageMode::Persistent {
            let data_path = persistence_config
                .data_path
                .as_ref()
                .expect("validated above")
                .clone();
            let path = data_path.join("codeartifact").join("snapshot.json");
            let store = fakecloud_persistence::DiskSnapshotStore::new(path);
            match fakecloud_codeartifact::persistence::load_into(&store, &codeartifact_state) {
                Ok(fakecloud_codeartifact::persistence::LoadOutcome::Loaded(accounts)) => {
                    tracing::info!(accounts, "loaded codeartifact persistence snapshot");
                }
                Ok(fakecloud_codeartifact::persistence::LoadOutcome::Empty) => {
                    tracing::info!("no codeartifact persistence snapshot found; starting empty");
                }
                Err(err) => fatal_exit(format_args!("{err}")),
            }
            Some(Arc::new(store) as Arc<dyn fakecloud_persistence::SnapshotStore>)
        } else {
            None
        };
    let mut codeartifact_service =
        fakecloud_codeartifact::CodeArtifactService::new(codeartifact_state.clone());
    if let Some(store) = codeartifact_snapshot_store {
        codeartifact_service = codeartifact_service.with_snapshot_store(store);
    }
    if let Some(h) = codeartifact_service.snapshot_hook() {
        cfn_snapshot_hooks.insert("codeartifact", h);
    }
    registry.register(Arc::new(codeartifact_service));

    // Amazon EFS: restJson1 control plane (file systems, mount targets, access
    // points, lifecycle/backup/policy configuration, replication, tagging).
    let efs_snapshot_store: Option<Arc<dyn fakecloud_persistence::SnapshotStore>> =
        if persistence_config.mode == fakecloud_persistence::StorageMode::Persistent {
            let data_path = persistence_config
                .data_path
                .as_ref()
                .expect("validated above")
                .clone();
            let path = data_path.join("efs").join("snapshot.json");
            let store = fakecloud_persistence::DiskSnapshotStore::new(path);
            match fakecloud_efs::persistence::load_into(&store, &efs_state) {
                Ok(fakecloud_efs::persistence::LoadOutcome::Loaded(accounts)) => {
                    tracing::info!(accounts, "loaded efs persistence snapshot");
                }
                Ok(fakecloud_efs::persistence::LoadOutcome::Empty) => {
                    tracing::info!("no efs persistence snapshot found; starting empty");
                }
                Err(err) => fatal_exit(format_args!("{err}")),
            }
            Some(Arc::new(store) as Arc<dyn fakecloud_persistence::SnapshotStore>)
        } else {
            None
        };
    let mut efs_service =
        fakecloud_efs::EfsService::new(efs_state.clone()).with_ec2_state(ec2_state.clone());
    if let Some(store) = efs_snapshot_store {
        efs_service = efs_service.with_snapshot_store(store);
    }
    if let Some(h) = efs_service.snapshot_hook() {
        cfn_snapshot_hooks.insert("elasticfilesystem", h);
    }
    registry.register(Arc::new(efs_service));

    // Amazon MQ: restJson1 control plane (brokers, configurations, users, tags).
    let mq_snapshot_store: Option<Arc<dyn fakecloud_persistence::SnapshotStore>> =
        if persistence_config.mode == fakecloud_persistence::StorageMode::Persistent {
            let data_path = persistence_config
                .data_path
                .as_ref()
                .expect("validated above")
                .clone();
            let path = data_path.join("mq").join("snapshot.json");
            let store = fakecloud_persistence::DiskSnapshotStore::new(path);
            match fakecloud_mq::persistence::load_into(&store, &mq_state) {
                Ok(fakecloud_mq::persistence::LoadOutcome::Loaded(accounts)) => {
                    tracing::info!(accounts, "loaded mq persistence snapshot");
                }
                Ok(fakecloud_mq::persistence::LoadOutcome::Empty) => {
                    tracing::info!("no mq persistence snapshot found; starting empty");
                }
                Err(err) => fatal_exit(format_args!("{err}")),
            }
            Some(Arc::new(store) as Arc<dyn fakecloud_persistence::SnapshotStore>)
        } else {
            None
        };
    let mut mq_service = fakecloud_mq::MqService::new(mq_state.clone());
    if let Some(store) = mq_snapshot_store {
        mq_service = mq_service.with_snapshot_store(store);
    }
    // Amazon MQ brokers are backed by REAL ActiveMQ/RabbitMQ containers so a
    // client actually connects and exchanges messages (the RDS/ElastiCache
    // bar). The runtime is constructed earlier alongside the other backing
    // runtimes (so the degraded-runtimes banner reports it); attach it here.
    if let Some(ref rt) = mq_runtime {
        mq_service = mq_service.with_runtime(rt.clone());
    }
    // Recreate backing containers for persisted brokers the snapshot claims
    // should be running (same restart-recovery contract as RDS #1338).
    // Fire-and-forget: one task per broker, so a slow broker bring-up doesn't
    // block startup.
    mq_service.recover_persisted_containers().await;
    if let Some(h) = mq_service.snapshot_hook() {
        cfn_snapshot_hooks.insert("mq", h);
    }
    registry.register(Arc::new(mq_service));

    // Amazon MSK (Managed Streaming for Apache Kafka): restJson1 control plane.
    // Full 59-op control plane with persistence; the real Kafka-broker data
    // plane is a later batch.
    let kafka_snapshot_store: Option<Arc<dyn fakecloud_persistence::SnapshotStore>> =
        if persistence_config.mode == fakecloud_persistence::StorageMode::Persistent {
            let data_path = persistence_config
                .data_path
                .as_ref()
                .expect("validated above")
                .clone();
            let path = data_path.join("kafka").join("snapshot.json");
            let store = fakecloud_persistence::DiskSnapshotStore::new(path);
            match fakecloud_kafka::persistence::load_into(&store, &kafka_state) {
                Ok(fakecloud_kafka::persistence::LoadOutcome::Loaded(accounts)) => {
                    tracing::info!(accounts, "loaded kafka persistence snapshot");
                }
                Ok(fakecloud_kafka::persistence::LoadOutcome::Empty) => {
                    tracing::info!("no kafka persistence snapshot found; starting empty");
                }
                Err(err) => fatal_exit(format_args!("{err}")),
            }
            Some(Arc::new(store) as Arc<dyn fakecloud_persistence::SnapshotStore>)
        } else {
            None
        };
    let mut kafka_service = fakecloud_kafka::KafkaService::new(kafka_state.clone());
    if let Some(store) = kafka_snapshot_store {
        kafka_service = kafka_service.with_snapshot_store(store);
    }
    // MSK clusters are backed by REAL single-node Kafka broker containers so a
    // client genuinely produces/consumes through them (the RDS/ElastiCache/MQ
    // bar). Attach the runtime constructed earlier alongside the other backing
    // runtimes.
    if let Some(ref rt) = kafka_runtime {
        kafka_service = kafka_service.with_runtime(rt.clone());
    }
    // Recreate backing containers for persisted clusters the snapshot claims
    // should be running (same restart-recovery contract as RDS #1338 / MQ).
    // Fire-and-forget: one task per cluster, so a slow broker bring-up doesn't
    // block startup.
    kafka_service.recover_persisted_containers().await;
    if let Some(h) = kafka_service.snapshot_hook() {
        cfn_snapshot_hooks.insert("kafka", h);
    }
    registry.register(Arc::new(kafka_service));

    // AWS CodeCommit: awsJson1.1 git-repository control plane (repositories,
    // branches, commits/files/blobs, pull requests, approval-rule templates,
    // comments, triggers, tagging) over a real content-addressed object store.
    let codecommit_snapshot_store: Option<Arc<dyn fakecloud_persistence::SnapshotStore>> =
        if persistence_config.mode == fakecloud_persistence::StorageMode::Persistent {
            let data_path = persistence_config
                .data_path
                .as_ref()
                .expect("validated above")
                .clone();
            let path = data_path.join("codecommit").join("snapshot.json");
            let store = fakecloud_persistence::DiskSnapshotStore::new(path);
            match fakecloud_codecommit::persistence::load_into(&store, &codecommit_state) {
                Ok(fakecloud_codecommit::persistence::LoadOutcome::Loaded(accounts)) => {
                    tracing::info!(accounts, "loaded codecommit persistence snapshot");
                }
                Ok(fakecloud_codecommit::persistence::LoadOutcome::Empty) => {
                    tracing::info!("no codecommit persistence snapshot found; starting empty");
                }
                Err(err) => fatal_exit(format_args!("{err}")),
            }
            Some(Arc::new(store) as Arc<dyn fakecloud_persistence::SnapshotStore>)
        } else {
            None
        };
    let mut codecommit_service =
        fakecloud_codecommit::CodeCommitService::new(codecommit_state.clone());
    if let Some(store) = codecommit_snapshot_store {
        codecommit_service = codecommit_service.with_snapshot_store(store);
    }
    if let Some(h) = codecommit_service.snapshot_hook() {
        cfn_snapshot_hooks.insert("codecommit", h);
    }
    registry.register(Arc::new(codecommit_service));

    // Amazon OpenSearch Service + Amazon Elasticsearch Service (both `es`).
    let opensearch_snapshot_store: Option<Arc<dyn fakecloud_persistence::SnapshotStore>> =
        if persistence_config.mode == fakecloud_persistence::StorageMode::Persistent {
            let data_path = persistence_config
                .data_path
                .as_ref()
                .expect("validated above")
                .clone();
            let path = data_path.join("opensearch").join("snapshot.json");
            let store = fakecloud_persistence::DiskSnapshotStore::new(path);
            match fakecloud_opensearch::persistence::load_into(&store, &opensearch_state) {
                Ok(fakecloud_opensearch::persistence::LoadOutcome::Loaded(accounts)) => {
                    tracing::info!(accounts, "loaded opensearch persistence snapshot");
                }
                Ok(fakecloud_opensearch::persistence::LoadOutcome::Empty) => {
                    tracing::info!("no opensearch persistence snapshot found; starting empty");
                }
                Err(err) => fatal_exit(format_args!("{err}")),
            }
            Some(Arc::new(store) as Arc<dyn fakecloud_persistence::SnapshotStore>)
        } else {
            None
        };
    let mut opensearch_service =
        fakecloud_opensearch::OpenSearchService::new(opensearch_state.clone());
    if let Some(store) = opensearch_snapshot_store {
        opensearch_service = opensearch_service.with_snapshot_store(store);
    }
    if let Some(h) = opensearch_service.snapshot_hook() {
        cfn_snapshot_hooks.insert("es", h);
    }
    registry.register(Arc::new(opensearch_service));

    // AWS AppConfig control plane + AppConfig Data plane (both `appconfig`).
    let appconfig_snapshot_store: Option<Arc<dyn fakecloud_persistence::SnapshotStore>> =
        if persistence_config.mode == fakecloud_persistence::StorageMode::Persistent {
            let data_path = persistence_config
                .data_path
                .as_ref()
                .expect("validated above")
                .clone();
            let path = data_path.join("appconfig").join("snapshot.json");
            let store = fakecloud_persistence::DiskSnapshotStore::new(path);
            match fakecloud_appconfig::persistence::load_into(&store, &appconfig_state) {
                Ok(fakecloud_appconfig::persistence::LoadOutcome::Loaded(accounts)) => {
                    tracing::info!(accounts, "loaded appconfig persistence snapshot");
                }
                Ok(fakecloud_appconfig::persistence::LoadOutcome::Empty) => {
                    tracing::info!("no appconfig persistence snapshot found; starting empty");
                }
                Err(err) => fatal_exit(format_args!("{err}")),
            }
            Some(Arc::new(store) as Arc<dyn fakecloud_persistence::SnapshotStore>)
        } else {
            None
        };
    let mut appconfig_service = fakecloud_appconfig::AppConfigService::new(appconfig_state.clone());
    if let Some(store) = appconfig_snapshot_store {
        appconfig_service = appconfig_service.with_snapshot_store(store);
    }
    if let Some(h) = appconfig_service.snapshot_hook() {
        cfn_snapshot_hooks.insert("appconfig", h);
    }
    registry.register(Arc::new(appconfig_service));

    let mwaa_snapshot_store: Option<Arc<dyn fakecloud_persistence::SnapshotStore>> =
        if persistence_config.mode == fakecloud_persistence::StorageMode::Persistent {
            let data_path = persistence_config
                .data_path
                .as_ref()
                .expect("validated above")
                .clone();
            let path = data_path.join("mwaa").join("snapshot.json");
            let store = fakecloud_persistence::DiskSnapshotStore::new(path);
            match fakecloud_mwaa::persistence::load_into(&store, &mwaa_state) {
                Ok(fakecloud_mwaa::persistence::LoadOutcome::Loaded(accounts)) => {
                    tracing::info!(accounts, "loaded mwaa persistence snapshot");
                }
                Ok(fakecloud_mwaa::persistence::LoadOutcome::Empty) => {
                    tracing::info!("no mwaa persistence snapshot found; starting empty");
                }
                Err(err) => fatal_exit(format_args!("{err}")),
            }
            Some(Arc::new(store) as Arc<dyn fakecloud_persistence::SnapshotStore>)
        } else {
            None
        };
    let mut mwaa_service = fakecloud_mwaa::MwaaService::new(mwaa_state.clone());
    if let Some(store) = mwaa_snapshot_store {
        mwaa_service = mwaa_service.with_snapshot_store(store);
    }
    if let Some(h) = mwaa_service.snapshot_hook() {
        cfn_snapshot_hooks.insert("mwaa", h);
    }
    registry.register(Arc::new(mwaa_service));

    let xray_snapshot_store: Option<Arc<dyn fakecloud_persistence::SnapshotStore>> =
        if persistence_config.mode == fakecloud_persistence::StorageMode::Persistent {
            let data_path = persistence_config
                .data_path
                .as_ref()
                .expect("validated above")
                .clone();
            let path = data_path.join("xray").join("snapshot.json");
            let store = fakecloud_persistence::DiskSnapshotStore::new(path);
            match fakecloud_xray::persistence::load_into(&store, &xray_state) {
                Ok(fakecloud_xray::persistence::LoadOutcome::Loaded(accounts)) => {
                    tracing::info!(accounts, "loaded xray persistence snapshot");
                }
                Ok(fakecloud_xray::persistence::LoadOutcome::Empty) => {
                    tracing::info!("no xray persistence snapshot found; starting empty");
                }
                Err(err) => fatal_exit(format_args!("{err}")),
            }
            Some(Arc::new(store) as Arc<dyn fakecloud_persistence::SnapshotStore>)
        } else {
            None
        };
    let mut xray_service = fakecloud_xray::XrayService::new(xray_state.clone());
    if let Some(store) = xray_snapshot_store {
        xray_service = xray_service.with_snapshot_store(store);
    }
    if let Some(h) = xray_service.snapshot_hook() {
        cfn_snapshot_hooks.insert("xray", h);
    }
    registry.register(Arc::new(xray_service));

    let appsync_snapshot_store: Option<Arc<dyn fakecloud_persistence::SnapshotStore>> =
        if persistence_config.mode == fakecloud_persistence::StorageMode::Persistent {
            let data_path = persistence_config
                .data_path
                .as_ref()
                .expect("validated above")
                .clone();
            let path = data_path.join("appsync").join("snapshot.json");
            let store = fakecloud_persistence::DiskSnapshotStore::new(path);
            match fakecloud_appsync::persistence::load_into(&store, &appsync_state) {
                Ok(fakecloud_appsync::persistence::LoadOutcome::Loaded(accounts)) => {
                    tracing::info!(accounts, "loaded appsync persistence snapshot");
                }
                Ok(fakecloud_appsync::persistence::LoadOutcome::Empty) => {
                    tracing::info!("no appsync persistence snapshot found; starting empty");
                }
                Err(err) => fatal_exit(format_args!("{err}")),
            }
            Some(Arc::new(store) as Arc<dyn fakecloud_persistence::SnapshotStore>)
        } else {
            None
        };
    let mut appsync_service = fakecloud_appsync::AppSyncService::new(appsync_state.clone());
    if let Some(store) = appsync_snapshot_store {
        appsync_service = appsync_service.with_snapshot_store(store);
    }
    if let Some(h) = appsync_service.snapshot_hook() {
        cfn_snapshot_hooks.insert("appsync", h);
    }
    registry.register(Arc::new(appsync_service));

    let amplify_snapshot_store: Option<Arc<dyn fakecloud_persistence::SnapshotStore>> =
        if persistence_config.mode == fakecloud_persistence::StorageMode::Persistent {
            let data_path = persistence_config
                .data_path
                .as_ref()
                .expect("validated above")
                .clone();
            let path = data_path.join("amplify").join("snapshot.json");
            let store = fakecloud_persistence::DiskSnapshotStore::new(path);
            match fakecloud_amplify::persistence::load_into(&store, &amplify_state) {
                Ok(fakecloud_amplify::persistence::LoadOutcome::Loaded(accounts)) => {
                    tracing::info!(accounts, "loaded amplify persistence snapshot");
                }
                Ok(fakecloud_amplify::persistence::LoadOutcome::Empty) => {
                    tracing::info!("no amplify persistence snapshot found; starting empty");
                }
                Err(err) => fatal_exit(format_args!("{err}")),
            }
            Some(Arc::new(store) as Arc<dyn fakecloud_persistence::SnapshotStore>)
        } else {
            None
        };
    let mut amplify_service = fakecloud_amplify::AmplifyService::new(amplify_state.clone());
    if let Some(store) = amplify_snapshot_store {
        amplify_service = amplify_service.with_snapshot_store(store);
    }
    if let Some(h) = amplify_service.snapshot_hook() {
        cfn_snapshot_hooks.insert("amplify", h);
    }
    registry.register(Arc::new(amplify_service));

    let mediaconvert_snapshot_store: Option<Arc<dyn fakecloud_persistence::SnapshotStore>> =
        if persistence_config.mode == fakecloud_persistence::StorageMode::Persistent {
            let data_path = persistence_config
                .data_path
                .as_ref()
                .expect("validated above")
                .clone();
            let path = data_path.join("mediaconvert").join("snapshot.json");
            let store = fakecloud_persistence::DiskSnapshotStore::new(path);
            match fakecloud_mediaconvert::persistence::load_into(&store, &mediaconvert_state) {
                Ok(fakecloud_mediaconvert::persistence::LoadOutcome::Loaded(accounts)) => {
                    tracing::info!(accounts, "loaded mediaconvert persistence snapshot");
                }
                Ok(fakecloud_mediaconvert::persistence::LoadOutcome::Empty) => {
                    tracing::info!("no mediaconvert persistence snapshot found; starting empty");
                }
                Err(err) => fatal_exit(format_args!("{err}")),
            }
            Some(Arc::new(store) as Arc<dyn fakecloud_persistence::SnapshotStore>)
        } else {
            None
        };
    let mut mediaconvert_service =
        fakecloud_mediaconvert::MediaConvertService::new(mediaconvert_state.clone());
    if let Some(store) = mediaconvert_snapshot_store {
        mediaconvert_service = mediaconvert_service.with_snapshot_store(store);
    }
    if let Some(h) = mediaconvert_service.snapshot_hook() {
        cfn_snapshot_hooks.insert("mediaconvert", h);
    }
    registry.register(Arc::new(mediaconvert_service));

    let serverlessrepo_snapshot_store: Option<Arc<dyn fakecloud_persistence::SnapshotStore>> =
        if persistence_config.mode == fakecloud_persistence::StorageMode::Persistent {
            let data_path = persistence_config
                .data_path
                .as_ref()
                .expect("validated above")
                .clone();
            let path = data_path.join("serverlessrepo").join("snapshot.json");
            let store = fakecloud_persistence::DiskSnapshotStore::new(path);
            match fakecloud_serverlessrepo::persistence::load_into(&store, &serverlessrepo_state) {
                Ok(fakecloud_serverlessrepo::persistence::LoadOutcome::Loaded(accounts)) => {
                    tracing::info!(accounts, "loaded serverlessrepo persistence snapshot");
                }
                Ok(fakecloud_serverlessrepo::persistence::LoadOutcome::Empty) => {
                    tracing::info!("no serverlessrepo persistence snapshot found; starting empty");
                }
                Err(err) => fatal_exit(format_args!("{err}")),
            }
            Some(Arc::new(store) as Arc<dyn fakecloud_persistence::SnapshotStore>)
        } else {
            None
        };
    let mut serverlessrepo_service =
        fakecloud_serverlessrepo::ServerlessRepoService::new(serverlessrepo_state.clone());
    if let Some(store) = serverlessrepo_snapshot_store {
        serverlessrepo_service = serverlessrepo_service.with_snapshot_store(store);
    }
    if let Some(h) = serverlessrepo_service.snapshot_hook() {
        cfn_snapshot_hooks.insert("serverlessrepo", h);
    }
    registry.register(Arc::new(serverlessrepo_service));

    let iotdata_snapshot_store: Option<Arc<dyn fakecloud_persistence::SnapshotStore>> =
        if persistence_config.mode == fakecloud_persistence::StorageMode::Persistent {
            let data_path = persistence_config
                .data_path
                .as_ref()
                .expect("validated above")
                .clone();
            let path = data_path.join("iotdata").join("snapshot.json");
            let store = fakecloud_persistence::DiskSnapshotStore::new(path);
            match fakecloud_iotdata::persistence::load_into(&store, &iotdata_state) {
                Ok(fakecloud_iotdata::persistence::LoadOutcome::Loaded(accounts)) => {
                    tracing::info!(accounts, "loaded iotdata persistence snapshot");
                }
                Ok(fakecloud_iotdata::persistence::LoadOutcome::Empty) => {
                    tracing::info!("no iotdata persistence snapshot found; starting empty");
                }
                Err(err) => fatal_exit(format_args!("{err}")),
            }
            Some(Arc::new(store) as Arc<dyn fakecloud_persistence::SnapshotStore>)
        } else {
            None
        };
    let mut iotdata_service = fakecloud_iotdata::IotDataService::new(iotdata_state.clone());
    if let Some(store) = iotdata_snapshot_store {
        iotdata_service = iotdata_service.with_snapshot_store(store);
    }
    if let Some(h) = iotdata_service.snapshot_hook() {
        cfn_snapshot_hooks.insert("iotdata", h);
    }
    registry.register(Arc::new(iotdata_service));

    // Amazon Pinpoint: restJson1 control plane over apps, campaigns, segments,
    // endpoints, channels, journeys, templates, jobs, event streams,
    // recommenders, and tags. Signs as `mobiletargeting`, aliased to `pinpoint`.
    let pinpoint_snapshot_store: Option<Arc<dyn fakecloud_persistence::SnapshotStore>> =
        if persistence_config.mode == fakecloud_persistence::StorageMode::Persistent {
            let data_path = persistence_config
                .data_path
                .as_ref()
                .expect("validated above")
                .clone();
            let path = data_path.join("pinpoint").join("snapshot.json");
            let store = fakecloud_persistence::DiskSnapshotStore::new(path);
            match fakecloud_pinpoint::persistence::load_into(&store, &pinpoint_state) {
                Ok(fakecloud_pinpoint::persistence::LoadOutcome::Loaded(accounts)) => {
                    tracing::info!(accounts, "loaded pinpoint persistence snapshot");
                }
                Ok(fakecloud_pinpoint::persistence::LoadOutcome::Empty) => {
                    tracing::info!("no pinpoint persistence snapshot found; starting empty");
                }
                Err(err) => fatal_exit(format_args!("{err}")),
            }
            Some(Arc::new(store) as Arc<dyn fakecloud_persistence::SnapshotStore>)
        } else {
            None
        };
    let mut pinpoint_service = fakecloud_pinpoint::PinpointService::new(pinpoint_state.clone());
    if let Some(store) = pinpoint_snapshot_store {
        pinpoint_service = pinpoint_service.with_snapshot_store(store);
    }
    if let Some(h) = pinpoint_service.snapshot_hook() {
        cfn_snapshot_hooks.insert("pinpoint", h);
    }
    registry.register(Arc::new(pinpoint_service));

    let iot_snapshot_store: Option<Arc<dyn fakecloud_persistence::SnapshotStore>> =
        if persistence_config.mode == fakecloud_persistence::StorageMode::Persistent {
            let data_path = persistence_config
                .data_path
                .as_ref()
                .expect("validated above")
                .clone();
            let path = data_path.join("iot").join("snapshot.json");
            let store = fakecloud_persistence::DiskSnapshotStore::new(path);
            match fakecloud_iot::persistence::load_into(&store, &iot_state) {
                Ok(fakecloud_iot::persistence::LoadOutcome::Loaded(accounts)) => {
                    tracing::info!(accounts, "loaded iot persistence snapshot");
                }
                Ok(fakecloud_iot::persistence::LoadOutcome::Empty) => {
                    tracing::info!("no iot persistence snapshot found; starting empty");
                }
                Err(err) => fatal_exit(format_args!("{err}")),
            }
            Some(Arc::new(store) as Arc<dyn fakecloud_persistence::SnapshotStore>)
        } else {
            None
        };
    let mut iot_service = fakecloud_iot::IotService::new(iot_state.clone());
    if let Some(store) = iot_snapshot_store {
        iot_service = iot_service.with_snapshot_store(store);
    }
    if let Some(h) = iot_service.snapshot_hook() {
        cfn_snapshot_hooks.insert("iot", h);
    }
    registry.register(Arc::new(iot_service));

    let iotwireless_snapshot_store: Option<Arc<dyn fakecloud_persistence::SnapshotStore>> =
        if persistence_config.mode == fakecloud_persistence::StorageMode::Persistent {
            let data_path = persistence_config
                .data_path
                .as_ref()
                .expect("validated above")
                .clone();
            let path = data_path.join("iotwireless").join("snapshot.json");
            let store = fakecloud_persistence::DiskSnapshotStore::new(path);
            match fakecloud_iotwireless::persistence::load_into(&store, &iotwireless_state) {
                Ok(fakecloud_iotwireless::persistence::LoadOutcome::Loaded(accounts)) => {
                    tracing::info!(accounts, "loaded iotwireless persistence snapshot");
                }
                Ok(fakecloud_iotwireless::persistence::LoadOutcome::Empty) => {
                    tracing::info!("no iotwireless persistence snapshot found; starting empty");
                }
                Err(err) => fatal_exit(format_args!("{err}")),
            }
            Some(Arc::new(store) as Arc<dyn fakecloud_persistence::SnapshotStore>)
        } else {
            None
        };
    let mut iotwireless_service =
        fakecloud_iotwireless::IotWirelessService::new(iotwireless_state.clone());
    if let Some(store) = iotwireless_snapshot_store {
        iotwireless_service = iotwireless_service.with_snapshot_store(store);
    }
    if let Some(h) = iotwireless_service.snapshot_hook() {
        cfn_snapshot_hooks.insert("iotwireless", h);
    }
    registry.register(Arc::new(iotwireless_service));

    let sagemaker_snapshot_store: Option<Arc<dyn fakecloud_persistence::SnapshotStore>> =
        if persistence_config.mode == fakecloud_persistence::StorageMode::Persistent {
            let data_path = persistence_config
                .data_path
                .as_ref()
                .expect("validated above")
                .clone();
            let path = data_path.join("sagemaker").join("snapshot.json");
            let store = fakecloud_persistence::DiskSnapshotStore::new(path);
            match fakecloud_sagemaker::persistence::load_into(&store, &sagemaker_state) {
                Ok(fakecloud_sagemaker::persistence::LoadOutcome::Loaded(accounts)) => {
                    tracing::info!(accounts, "loaded sagemaker persistence snapshot");
                }
                Ok(fakecloud_sagemaker::persistence::LoadOutcome::Empty) => {
                    tracing::info!("no sagemaker persistence snapshot found; starting empty");
                }
                Err(err) => fatal_exit(format_args!("{err}")),
            }
            Some(Arc::new(store) as Arc<dyn fakecloud_persistence::SnapshotStore>)
        } else {
            None
        };
    let mut sagemaker_service = fakecloud_sagemaker::SageMakerService::new(sagemaker_state.clone());
    if let Some(store) = sagemaker_snapshot_store {
        sagemaker_service = sagemaker_service.with_snapshot_store(store);
    }
    if let Some(h) = sagemaker_service.snapshot_hook() {
        cfn_snapshot_hooks.insert("sagemaker", h);
    }
    registry.register(Arc::new(sagemaker_service));

    let managedblockchain_snapshot_store: Option<Arc<dyn fakecloud_persistence::SnapshotStore>> =
        if persistence_config.mode == fakecloud_persistence::StorageMode::Persistent {
            let data_path = persistence_config
                .data_path
                .as_ref()
                .expect("validated above")
                .clone();
            let path = data_path.join("managedblockchain").join("snapshot.json");
            let store = fakecloud_persistence::DiskSnapshotStore::new(path);
            match fakecloud_managedblockchain::persistence::load_into(
                &store,
                &managedblockchain_state,
            ) {
                Ok(fakecloud_managedblockchain::persistence::LoadOutcome::Loaded(accounts)) => {
                    tracing::info!(accounts, "loaded managedblockchain persistence snapshot");
                }
                Ok(fakecloud_managedblockchain::persistence::LoadOutcome::Empty) => {
                    tracing::info!(
                        "no managedblockchain persistence snapshot found; starting empty"
                    );
                }
                Err(err) => fatal_exit(format_args!("{err}")),
            }
            Some(Arc::new(store) as Arc<dyn fakecloud_persistence::SnapshotStore>)
        } else {
            None
        };
    let mut managedblockchain_service =
        fakecloud_managedblockchain::ManagedBlockchainService::new(managedblockchain_state.clone());
    if let Some(store) = managedblockchain_snapshot_store {
        managedblockchain_service = managedblockchain_service.with_snapshot_store(store);
    }
    if let Some(h) = managedblockchain_service.snapshot_hook() {
        cfn_snapshot_hooks.insert("managedblockchain", h);
    }
    registry.register(Arc::new(managedblockchain_service));

    let fis_snapshot_store: Option<Arc<dyn fakecloud_persistence::SnapshotStore>> =
        if persistence_config.mode == fakecloud_persistence::StorageMode::Persistent {
            let data_path = persistence_config
                .data_path
                .as_ref()
                .expect("validated above")
                .clone();
            let path = data_path.join("fis").join("snapshot.json");
            let store = fakecloud_persistence::DiskSnapshotStore::new(path);
            match fakecloud_fis::persistence::load_into(&store, &fis_state) {
                Ok(fakecloud_fis::persistence::LoadOutcome::Loaded(accounts)) => {
                    tracing::info!(accounts, "loaded fis persistence snapshot");
                }
                Ok(fakecloud_fis::persistence::LoadOutcome::Empty) => {
                    tracing::info!("no fis persistence snapshot found; starting empty");
                }
                Err(err) => fatal_exit(format_args!("{err}")),
            }
            Some(Arc::new(store) as Arc<dyn fakecloud_persistence::SnapshotStore>)
        } else {
            None
        };
    let mut fis_service = fakecloud_fis::FisService::new(fis_state.clone());
    if let Some(store) = fis_snapshot_store {
        fis_service = fis_service.with_snapshot_store(store);
    }
    if let Some(h) = fis_service.snapshot_hook() {
        cfn_snapshot_hooks.insert("fis", h);
    }
    registry.register(Arc::new(fis_service));

    // Resource Groups Tagging API. Reads aggregate every service's live tags
    // through a shared TagProviderRegistry, plus tags applied directly to
    // arbitrary ARNs via TagResources (stored by the service itself).
    //
    // Per-service TagProvider adapters are wired in incrementally in follow-on
    // batches — the same staged rollout used for the cross-service persistence
    // sweep (each service crate gains an adapter over its own tag state). Until
    // a given service is wired, its native tags simply don't appear here; tags
    // applied through this API always do. The registry starts empty and grows
    // as providers register at startup.
    let tag_provider_registry = fakecloud_core::tag_index::TagProviderRegistry::new();
    let resource_groups_tagging_snapshot_store: Option<
        Arc<dyn fakecloud_persistence::SnapshotStore>,
    > = if persistence_config.mode == fakecloud_persistence::StorageMode::Persistent {
        let data_path = persistence_config
            .data_path
            .as_ref()
            .expect("validated above")
            .clone();
        let path = data_path
            .join("resource-groups-tagging")
            .join("snapshot.json");
        let store = fakecloud_persistence::DiskSnapshotStore::new(path);
        match fakecloud_resource_groups_tagging::persistence::load_into(
            &store,
            &resource_groups_tagging_state,
        ) {
            Ok(fakecloud_resource_groups_tagging::persistence::LoadOutcome::Loaded(accounts)) => {
                tracing::info!(
                    accounts,
                    "loaded resource-groups-tagging persistence snapshot"
                );
            }
            Ok(fakecloud_resource_groups_tagging::persistence::LoadOutcome::Empty) => {
                tracing::info!(
                    "no resource-groups-tagging persistence snapshot found; starting empty"
                );
            }
            Err(err) => fatal_exit(format_args!("{err}")),
        }
        Some(Arc::new(store) as Arc<dyn fakecloud_persistence::SnapshotStore>)
    } else {
        None
    };
    registry.register(Arc::new(
        fakecloud_resource_groups_tagging::ResourceGroupsTaggingService::new(
            resource_groups_tagging_state.clone(),
            tag_provider_registry.clone(),
            resource_groups_tagging_snapshot_store,
        ),
    ));

    let cloudformation_service = cloudformation_service
        .with_s3_store(s3_store.clone())
        .with_snapshot_hooks(cfn_snapshot_hooks);
    // Keep a concrete handle: Cloud Control API drives the same resource
    // provisioners one resource at a time via this service.
    let cloudformation_arc = Arc::new(cloudformation_service);
    registry.register(cloudformation_arc.clone());

    // Cloud Control API (cloudcontrolapi): uniform CRUD+L over every CFN
    // resource type, delegating to the CloudFormation provisioner bridge.
    let cloudcontrol_snapshot_store: Option<Arc<dyn fakecloud_persistence::SnapshotStore>> =
        if persistence_config.mode == fakecloud_persistence::StorageMode::Persistent {
            let data_path = persistence_config
                .data_path
                .as_ref()
                .expect("validated above")
                .clone();
            let path = data_path.join("cloudcontrol").join("snapshot.json");
            let store = fakecloud_persistence::DiskSnapshotStore::new(path);
            match fakecloud_cloudcontrol::persistence::load_into(&store, &cloudcontrol_state) {
                Ok(fakecloud_cloudcontrol::persistence::LoadOutcome::Loaded(accounts)) => {
                    tracing::info!(accounts, "loaded cloudcontrol persistence snapshot");
                }
                Ok(fakecloud_cloudcontrol::persistence::LoadOutcome::Empty) => {
                    tracing::info!("no cloudcontrol persistence snapshot found; starting empty");
                }
                Err(err) => fatal_exit(format_args!("{err}")),
            }
            Some(Arc::new(store) as Arc<dyn fakecloud_persistence::SnapshotStore>)
        } else {
            None
        };
    let mut cloudcontrol_service = fakecloud_cloudcontrol::CloudControlService::new(
        cloudformation_arc.clone(),
        cloudcontrol_state.clone(),
    );
    if let Some(store) = cloudcontrol_snapshot_store {
        cloudcontrol_service = cloudcontrol_service.with_snapshot_store(store);
    }
    registry.register(Arc::new(cloudcontrol_service));
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
        Arc::new(
            stepfunctions_delivery::StepFunctionsDeliveryImpl::new(
                stepfunctions_state.clone(),
                Some(Arc::new(sfn_interpreter_bus)),
                Some(dynamodb_state.clone()),
            )
            .with_registry(sfn_registry_handle.clone()),
        )
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
        // Scheduler-driven Kinesis deliveries must flip the shared dirty flag so
        // the background delivery flusher persists them; `::new` leaves the flag
        // `None` and records delivered to a Kinesis stream target by a schedule
        // would vanish on restart (bug-hunt restart-dataloss).
        let kinesis_delivery_for_scheduler =
            fakecloud_kinesis::delivery::KinesisDeliveryImpl::with_dirty_flag(
                kinesis_state.clone(),
                kinesis_delivery_dirty.clone(),
            );
        let ses_dispatcher_for_scheduler: Arc<
            dyn fakecloud_core::delivery::SesSendEmailDispatcher,
        > = Arc::new(SesSendEmailDispatcherImpl {
            state: ses_state.clone(),
        });
        let ecs_runner_for_scheduler: Arc<dyn fakecloud_core::delivery::EcsTaskRunner> =
            Arc::new(EcsTaskRunnerImpl {
                service: ecs_service_for_scheduler.clone(),
            });
        let sagemaker_pipeline_for_scheduler: Arc<
            dyn fakecloud_core::delivery::SageMakerPipelineDelivery,
        > = Arc::new(hooks::SageMakerPipelineDeliveryImpl {
            state: sagemaker_state.clone(),
        });
        let mut bus = DeliveryBus::new()
            .with_sqs(sqs_delivery.clone())
            .with_sns(sns_delivery_for_scheduler)
            .with_eventbridge(eb_delivery_for_scheduler)
            .with_stepfunctions(sfn_delivery_for_scheduler)
            .with_kinesis(kinesis_delivery_for_scheduler)
            .with_ses_dispatcher(ses_dispatcher_for_scheduler)
            .with_ecs_task_runner(ecs_runner_for_scheduler)
            .with_sagemaker_pipeline(sagemaker_pipeline_for_scheduler);
        if let Some(ref ld) = lambda_delivery {
            bus = bus.with_lambda(ld.clone());
        }
        Arc::new(bus)
    };
    let scheduler_state_for_list = scheduler_state.clone();
    let scheduler_state_for_fire = scheduler_state.clone();
    let glue_state_for_jobs = glue_state.clone();
    let glue_state_for_runs = glue_state.clone();
    let glue_state_for_crawlers = glue_state.clone();
    let cloudwatch_state_for_alarms = cloudwatch_state.clone();
    let cloudwatch_state_for_metrics = cloudwatch_state.clone();
    let delivery_for_scheduler_fire = delivery_for_scheduler.clone();
    let default_account_for_scheduler_fire = cli.account_id.clone();
    let default_region_for_scheduler_fire = cli.region.clone();
    let mut scheduler_ticker =
        fakecloud_scheduler::ticker::Ticker::new(scheduler_state.clone(), delivery_for_scheduler);
    if let Some(store) = scheduler_ticker_snapshot_store {
        scheduler_ticker = scheduler_ticker.with_snapshot_store(store);
    }
    tokio::spawn(scheduler_ticker.run());
    // Spawn background tasks
    let lifecycle_processor = fakecloud_s3::lifecycle::LifecycleProcessor::new(s3_state.clone());
    tokio::spawn(lifecycle_processor.run());
    let mut sqs_lambda_poller = SqsLambdaPoller::new(sqs_state.clone(), lambda_state.clone())
        .with_kms_hook(kms_hook_for_services.clone());
    if let Some(ref ld) = lambda_delivery {
        sqs_lambda_poller = sqs_lambda_poller.with_lambda_delivery(ld.clone());
    }
    if let Some(h) = sqs_poller_snapshot_hook {
        sqs_lambda_poller = sqs_lambda_poller.with_snapshot_hook(h);
    }
    tokio::spawn(sqs_lambda_poller.run());
    let mut kinesis_lambda_poller =
        KinesisLambdaPoller::new(kinesis_state.clone(), lambda_invocations_state.clone());
    if let Some(ref ld) = lambda_delivery {
        kinesis_lambda_poller = kinesis_lambda_poller.with_lambda_delivery(ld.clone());
    }
    if let Some(h) = kinesis_poller_snapshot_hook {
        kinesis_lambda_poller = kinesis_lambda_poller.with_snapshot_hook(h);
    }
    tokio::spawn(kinesis_lambda_poller.run());
    let mut dynamodb_streams_poller =
        DynamoDbStreamsLambdaPoller::new(dynamodb_state.clone(), lambda_invocations_state.clone());
    if let Some(ref ld) = lambda_delivery {
        dynamodb_streams_poller = dynamodb_streams_poller.with_lambda_delivery(ld.clone());
    }
    if let Some(h) = dynamodb_poller_snapshot_hook {
        dynamodb_streams_poller = dynamodb_streams_poller.with_snapshot_hook(h);
    }
    tokio::spawn(Arc::new(dynamodb_streams_poller).run());
    // EventBridge Pipes runner: executes RUNNING pipes with an SQS / Kinesis /
    // DynamoDB-stream source, filtering events, optionally running them through
    // a Lambda enrichment, applying the target InputTemplate, and delivering
    // matches to Lambda/SQS/SNS/Step Functions/EventBridge-bus/Kinesis targets
    // via the shared delivery paths.
    {
        // EventBridge-bus + Step Functions target senders need their own
        // delivery impls (mirroring the scheduler/EB wiring); a minimal inner
        // bus suffices because a pipe delivers *to* these targets rather than
        // driving their downstream fan-out.
        let pipes_eb_delivery = {
            let mut inner = DeliveryBus::new().with_sqs(sqs_delivery.clone());
            if let Some(ref ld) = lambda_delivery {
                inner = inner.with_lambda(ld.clone());
            }
            Arc::new(
                fakecloud_eventbridge::delivery::EventBridgeDeliveryImpl::new(
                    eb_state.clone(),
                    Arc::new(inner),
                ),
            )
        };
        let pipes_sfn_delivery = {
            let mut sfn_bus = DeliveryBus::new().with_sqs(sqs_delivery.clone());
            if let Some(ref ld) = lambda_delivery {
                sfn_bus = sfn_bus.with_lambda(ld.clone());
            }
            Arc::new(
                stepfunctions_delivery::StepFunctionsDeliveryImpl::new(
                    stepfunctions_state.clone(),
                    Some(Arc::new(sfn_bus)),
                    Some(dynamodb_state.clone()),
                )
                .with_registry(sfn_registry_handle.clone()),
            )
        };
        // Pipe-driven Kinesis deliveries must flip the shared dirty flag so the
        // background delivery flusher persists them; `::new` leaves the flag
        // `None` and records delivered to a Kinesis stream target by a pipe would
        // vanish on restart (bug-hunt restart-dataloss).
        let pipes_kinesis_delivery =
            fakecloud_kinesis::delivery::KinesisDeliveryImpl::with_dirty_flag(
                kinesis_state.clone(),
                kinesis_delivery_dirty.clone(),
            );
        let mut pipes_bus = DeliveryBus::new()
            .with_sqs(sqs_delivery.clone())
            .with_sns(sns_delivery.clone())
            .with_eventbridge(pipes_eb_delivery)
            .with_stepfunctions(pipes_sfn_delivery)
            .with_kinesis(pipes_kinesis_delivery);
        if let Some(ref ld) = lambda_delivery {
            pipes_bus = pipes_bus.with_lambda(ld.clone());
        }
        let mut runner = pipes_runner::PipesRunner::new(
            pipes_state.clone(),
            sqs_state.clone(),
            Arc::new(pipes_bus),
        )
        .with_kinesis_state(kinesis_state.clone())
        .with_dynamodb_state(dynamodb_state.clone())
        .with_kms_hook(kms_hook_for_services.clone());
        if let Some(hook) = pipes_persist_hook.clone() {
            runner = runner.with_persist_hook(hook);
        }
        tokio::spawn(runner.run());
    }
    if let Some(ref rt) = container_runtime {
        let rt = rt.clone();
        tokio::spawn(rt.run_cleanup_loop(std::time::Duration::from_secs(300)));
    }
    // Application Auto Scaling watcher: ticks every 15s, walks all
    // DynamoDB scaling targets/policies, reads CloudWatch metrics, and
    // applies capacity changes via the DDB capacity hook. Tests can
    // skip the wall-clock wait via `/_fakecloud/application-autoscaling/tick`.
    let appas_metric_reader = Arc::new(appas_hooks::CloudwatchMetricReader::new(
        cloudwatch_state.clone(),
    ));
    let appas_ddb_hook = Arc::new(appas_hooks::DynamoDbCapacityHookImpl::new(
        dynamodb_state.clone(),
    ));
    let appas_ecs_hook = Arc::new(appas_hooks::EcsServiceHookImpl::new(ecs_state.clone()));
    let appas_watcher_for_admin = Arc::new(
        fakecloud_application_autoscaling::ScalingWatcher::new(
            app_autoscaling_state.clone(),
            appas_metric_reader.clone(),
            appas_ddb_hook.clone(),
            cli.region.clone(),
        )
        .with_ecs_hook(appas_ecs_hook.clone())
        .with_interval(std::time::Duration::from_secs(15)),
    );
    {
        let watcher = fakecloud_application_autoscaling::ScalingWatcher::new(
            app_autoscaling_state.clone(),
            appas_metric_reader,
            appas_ddb_hook.clone(),
            cli.region.clone(),
        )
        .with_ecs_hook(appas_ecs_hook.clone())
        .with_interval(std::time::Duration::from_secs(15));
        tokio::spawn(watcher.run());
    }
    // Application Auto Scaling scheduled action executor: ticks every
    // 30s, walks all ScheduledActions, fires the ones whose Schedule
    // expression is due and applies the configured ScalableTargetAction
    // bounds across linked resources (DDB capacity today; ECS desired
    // count wired alongside). Tests can skip the wall-clock wait via the
    // `/_fakecloud/application-autoscaling/scheduled-tick` admin route.
    let appas_scheduled_executor_for_admin = Arc::new(
        fakecloud_application_autoscaling::ScheduledActionExecutor::new(
            app_autoscaling_state.clone(),
            appas_ddb_hook.clone(),
            cli.region.clone(),
        )
        .with_ecs_hook(appas_ecs_hook.clone())
        .with_interval(std::time::Duration::from_secs(30)),
    );
    {
        let executor = fakecloud_application_autoscaling::ScheduledActionExecutor::new(
            app_autoscaling_state.clone(),
            appas_ddb_hook,
            cli.region.clone(),
        )
        .with_ecs_hook(appas_ecs_hook)
        .with_interval(std::time::Duration::from_secs(30));
        tokio::spawn(executor.run());
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
    if iam_mode.is_enabled() && !cli.verify_sigv4 {
        // Without SigV4 verification a request is authenticated by its
        // access key id alone — no secret or signature is checked, and an
        // access key id that doesn't resolve to a principal falls through
        // unenforced (the same path the local-dev bootstrap relies on). So
        // policy enforcement can be bypassed with an unrecognized key, and a
        // *known* principal's key can be used without its secret. Enable
        // --verify-sigv4 alongside --iam to bind identity to the signing
        // secret and reject unknown keys. (bug-hunt 2026-06-13, finding 5.1)
        tracing::warn!(
            "IAM enforcement is on but SigV4 verification is off: identities are trusted from the access key id alone — enforcement can be bypassed with an unrecognized key, and a known access key id can be used without its secret. Enable --verify-sigv4 to verify signatures and reject unknown keys."
        );
    }
    if iam_mode.is_enabled() {
        let (enforced, skipped) = registry.iam_enforcement_split();
        // warn (not info): the `skipped` services accept any authorized caller
        // even under --iam, a security-relevant gap that should be as loud as
        // the SigV4 caveat above rather than buried at info level.
        tracing::warn!(
            enforced = ?enforced,
            skipped = ?skipped,
            "IAM enforcement surface: listed `enforced` services evaluate policies; `skipped` services are NOT yet wired for enforcement and allow any authorized caller"
        );
    }
    let config = DispatchConfig {
        region: cli.region.clone(),
        account_id: cli.account_id.clone(),
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
                fakecloud_sqs::resource_policy::SqsResourcePolicyProvider::shared(
                    sqs_state.clone(),
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
                fakecloud_eventbridge::resource_policy::EventBridgeResourcePolicyProvider::shared(
                    eb_state.clone(),
                ),
            ],
        )),
        scp_resolver: Some(
            fakecloud_organizations::resolver::OrganizationsScpResolver::shared(
                organizations_state.clone(),
            ),
        ),
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
            "/_fakecloud/kms/usage",
            axum::routing::get({
                let ks = kms_usage_state.clone();
                move || async move {
                    let recs = ks
                        .read()
                        .records()
                        .iter()
                        .map(|r| serde_json::json!({
                            "timestamp": r.timestamp.to_rfc3339(),
                            "operation": r.operation,
                            "servicePrincipal": r.service_principal,
                            "accountId": r.account_id,
                            "keyArn": r.key_arn,
                            "encryptionContext": r.encryption_context,
                        }))
                        .collect::<Vec<_>>();
                    axum::Json(serde_json::json!({"records": recs}))
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
                            dkim_signature: email.dkim_signature.clone(),
                            headers: email.headers.clone(),
                            timestamp: email.timestamp.to_rfc3339(),
                        })
                        .collect();
                    axum::Json(types::SesEmailsResponse { emails })
                }
            }),
        )
        .route(
            "/_fakecloud/ses/metrics",
            axum::routing::get({
                let ss = ses_emails_state.clone();
                move || async move {
                    let mas = ss.read();
                    let state = mas.default_ref();
                    axum::Json(serde_json::json!({
                        "suppressedDropsTotal": state.suppressed_drops_total,
                    }))
                }
            }),
        )
        .route(
            "/_fakecloud/ses/bounces",
            axum::routing::get({
                let ss = ses_emails_state.clone();
                move || async move {
                    let mas = ss.read();
                    let state = mas.default_ref();
                    let bounces: Vec<types::SesBounce> = state
                        .bounces
                        .iter()
                        .map(|b| {
                            let infos: Vec<types::SesBouncedRecipientInfo> = if b
                                .bounced_recipient_info
                                .is_empty()
                            {
                                // Older snapshots store only addresses;
                                // surface them with empty detail fields so
                                // the response shape stays stable.
                                b.bounced_recipients
                                    .iter()
                                    .map(|r| types::SesBouncedRecipientInfo {
                                        recipient: r.clone(),
                                        bounce_type: String::new(),
                                        action: String::new(),
                                        status: String::new(),
                                        diagnostic_code: String::new(),
                                    })
                                    .collect()
                            } else {
                                b.bounced_recipient_info
                                    .iter()
                                    .map(|i| types::SesBouncedRecipientInfo {
                                        recipient: i.recipient.clone(),
                                        bounce_type: i.bounce_type.clone(),
                                        action: i.action.clone(),
                                        status: i.status.clone(),
                                        diagnostic_code: i.diagnostic_code.clone(),
                                    })
                                    .collect()
                            };
                            let primary_type = b
                                .bounced_recipient_info
                                .first()
                                .map(|i| i.bounce_type.clone())
                                .unwrap_or_default();
                            types::SesBounce {
                                message_id: b.bounce_message_id.clone(),
                                bounce_type: primary_type,
                                bounce_sub_type: String::new(),
                                bounced_recipient_info: infos,
                                explanation: b.explanation.clone(),
                                timestamp: b.timestamp.to_rfc3339(),
                                original_message_id: b.original_message_id.clone(),
                                bounce_sender: b.bounce_sender.clone(),
                            }
                        })
                        .collect();
                    axum::Json(types::SesBouncesResponse { bounces })
                }
            }),
        )
        .route(
            "/_fakecloud/ses/messages/{message_id}/insights",
            axum::routing::get({
                let ss = ses_emails_state.clone();
                move |axum::extract::Path(message_id): axum::extract::Path<String>| async move {
                    let mas = ss.read();
                    let state = mas.default_ref();
                    let Some(email) = state
                        .sent_emails
                        .iter()
                        .find(|e| e.message_id == message_id)
                    else {
                        return (
                            axum::http::StatusCode::NOT_FOUND,
                            axum::Json(serde_json::json!({"error": "message not found"})),
                        )
                            .into_response();
                    };
                    let mut sends: Vec<types::SesMessageInsightEvent> = Vec::new();
                    let mut deliveries: Vec<types::SesMessageInsightEvent> = Vec::new();
                    let opens: Vec<types::SesMessageInsightEvent> = Vec::new();
                    let clicks: Vec<types::SesMessageInsightEvent> = Vec::new();
                    let mut bounces: Vec<types::SesMessageInsightEvent> = Vec::new();
                    let mut complaints: Vec<types::SesMessageInsightEvent> = Vec::new();
                    let rejects: Vec<types::SesMessageInsightEvent> = Vec::new();
                    for insight in &email.delivery_insights {
                        for ev in &insight.events {
                            let entry = types::SesMessageInsightEvent {
                                destination: insight.destination.clone(),
                                timestamp: ev.timestamp.to_rfc3339(),
                                bounce_type: ev.bounce_type.clone(),
                                bounce_sub_type: ev.bounce_sub_type.clone(),
                                diagnostic_code: ev.diagnostic_code.clone(),
                                complaint_feedback_type: ev.complaint_feedback_type.clone(),
                            };
                            match ev.event_type.as_str() {
                                "SEND" => sends.push(entry),
                                "DELIVERY" => deliveries.push(entry),
                                "BOUNCE" => bounces.push(entry),
                                "COMPLAINT" => complaints.push(entry),
                                _ => {}
                            }
                        }
                    }
                    axum::Json(types::SesMessageInsightsResponse {
                        message_id: email.message_id.clone(),
                        sends,
                        deliveries,
                        opens,
                        clicks,
                        bounces,
                        complaints,
                        rejects,
                    })
                    .into_response()
                }
            }),
        )
        .route(
            "/_fakecloud/ses/smtp/submissions",
            axum::routing::get({
                let ss = ses_emails_state.clone();
                move || async move {
                    let mas = ss.read();
                    let state = mas.default_ref();
                    let submissions: Vec<types::SesSmtpSubmission> = state
                        .smtp_submissions
                        .iter()
                        .map(|s| types::SesSmtpSubmission {
                            message_id: s.message_id.clone(),
                            from: s.from.clone(),
                            to: s.to.clone(),
                            subject: s.subject.clone(),
                            raw_size_bytes: s.raw_size_bytes,
                            received_at: s.received_at.to_rfc3339(),
                            auth_user: s.auth_user.clone(),
                        })
                        .collect();
                    axum::Json(types::SesSmtpSubmissionsResponse { submissions })
                }
            }),
        )
        .route(
            "/_fakecloud/ses/event-destinations/deliveries",
            axum::routing::get({
                let ss = ses_emails_state.clone();
                move || async move {
                    let mas = ss.read();
                    let state = mas.default_ref();
                    let deliveries: Vec<types::SesEventDestinationDelivery> = state
                        .event_destination_dispatches
                        .iter()
                        .map(|d| types::SesEventDestinationDelivery {
                            destination_name: d.destination_name.clone(),
                            destination_type: d.destination_type.clone(),
                            event_type: d.event_type.clone(),
                            message_id: d.message_id.clone(),
                            dispatched_at: d.dispatched_at.to_rfc3339(),
                            target_arn: d.target_arn.clone(),
                        })
                        .collect();
                    axum::Json(types::SesEventDestinationDeliveriesResponse { deliveries })
                }
            }),
        )
        .route(
            "/_fakecloud/ses/identities/{name}/mail-from-status",
            axum::routing::post({
                let ss = ses_emails_state.clone();
                move |axum::extract::Path(name): axum::extract::Path<String>,
                      axum::Json(body): axum::Json<types::SesMailFromStatusRequest>| async move {
                    let mut accounts = ss.write();
                    let state = accounts.default_mut();
                    let Some(identity) = state.identities.get_mut(&name) else {
                        return (
                            axum::http::StatusCode::NOT_FOUND,
                            axum::Json(serde_json::json!({"error": "identity not found"})),
                        );
                    };
                    let allowed = ["NotStarted", "Pending", "Success", "Failed"];
                    if !allowed.contains(&body.status.as_str()) {
                        return (
                            axum::http::StatusCode::BAD_REQUEST,
                            axum::Json(serde_json::json!({
                                "error": "status must be one of NotStarted/Pending/Success/Failed",
                            })),
                        );
                    }
                    identity.mail_from_domain_status = body.status.clone();
                    (
                        axum::http::StatusCode::OK,
                        axum::Json(serde_json::json!({
                            "identity": name,
                            "mailFromDomainStatus": body.status,
                        })),
                    )
                }
            }),
        )
        .route(
            "/_fakecloud/ses/identities/{name}/dkim-public-key",
            axum::routing::get({
                let ss = ses_emails_state.clone();
                move |axum::extract::Path(name): axum::extract::Path<String>| async move {
                    let mas = ss.read();
                    let state = mas.default_ref();
                    let Some(identity) = state.identities.get(&name) else {
                        return (
                            axum::http::StatusCode::NOT_FOUND,
                            axum::Json(serde_json::json!({"error": "identity not found"})),
                        );
                    };
                    (
                        axum::http::StatusCode::OK,
                        axum::Json(serde_json::json!({
                            "identity": name,
                            "selector": identity.dkim_domain_signing_selector,
                            "publicKeyBase64": identity.dkim_public_key_b64,
                            "signingEnabled": identity.dkim_signing_enabled,
                        })),
                    )
                }
            }),
        )
        .route(
            "/_fakecloud/ses/account/sandbox",
            axum::routing::post({
                let ss = ses_emails_state.clone();
                move |axum::Json(body): axum::Json<types::SesSandboxRequest>| async move {
                    let mut accounts = ss.write();
                    let state = accounts.default_mut();
                    // sandbox=true means production_access disabled (sandbox
                    // semantics on); sandbox=false re-enables production
                    // access (default fakecloud behavior).
                    state.account_settings.production_access_enabled = !body.sandbox;
                    (
                        axum::http::StatusCode::OK,
                        axum::Json(serde_json::json!({
                            "sandbox": body.sandbox,
                            "productionAccessEnabled": state.account_settings.production_access_enabled,
                        })),
                    )
                }
            }),
        )
        .route(
            "/_fakecloud/ses/inbound",
            axum::routing::post({
                let ss = ses_inbound_state.clone();
                let s3_for_inbound = s3_introspection_state.clone();
                let s3_store_for_inbound = s3_store_for_inbound.clone();
                let kms_hook_for_inbound = kms_hook_for_services.clone();
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
                    bus = bus.with_kms_hook(kms_hook_for_inbound);
                    Arc::new(bus)
                };
                let ses_state_for_inbound_actions = ses_inbound_state.clone();
                let region_for_inbound = cli.region.clone();
                move |axum::Json(body): axum::Json<types::InboundEmailRequest>| async move {
                    let (message_id, matched_rules, actions) =
                        fakecloud_ses::v1::evaluate_inbound_email(
                            &ss,
                            &body.from,
                            &body.to,
                            &body.subject,
                            &body.body,
                        );
                    // AddHeader actions are processed inline first so
                    // downstream S3 / Lambda / SNS payloads see the new
                    // headers (matches AWS evaluation order: AddHeader is
                    // applied to the in-flight message).
                    let mut extra_headers: Vec<(String, String)> = Vec::new();
                    for (_rule, action) in &actions {
                        if let fakecloud_ses::ReceiptAction::AddHeader {
                            header_name,
                            header_value,
                        } = action
                        {
                            extra_headers.push((header_name.clone(), header_value.clone()));
                        }
                    }
                    let augmented_body = if extra_headers.is_empty() {
                        body.body.clone()
                    } else {
                        let header_block = extra_headers
                            .iter()
                            .map(|(k, v)| format!("{k}: {v}"))
                            .collect::<Vec<_>>()
                            .join("\r\n");
                        format!("{header_block}\r\n{}", body.body)
                    };
                    // Execute actions for real
                    for (_rule, action) in &actions {
                        match action {
                            fakecloud_ses::ReceiptAction::S3 {
                                bucket_name,
                                object_key_prefix,
                                kms_key_arn,
                                ..
                            } => {
                                let prefix = object_key_prefix.as_deref().unwrap_or("");
                                let key = format!("{prefix}{message_id}");
                                let now = chrono::Utc::now();
                                let data = bytes::Bytes::from(augmented_body.clone());
                                let size = data.len() as u64;
                                let etag = format!("\"{:x}\"", md5::Md5::digest(&data));
                                // Encrypt via KMS when KmsKeyArn is configured.
                                let (body_bytes, sse_algorithm, sse_kms_key_id) =
                                    if let Some(kms_key) = kms_key_arn {
                                        let account_id = ss.read().default_account_id().to_string();
                                        let mut ctx = std::collections::HashMap::new();
                                        ctx.insert(
                                            "aws:s3:arn".to_string(),
                                            fakecloud_aws::arn::Arn::s3(bucket_name).to_string(),
                                        );
                                        match delivery_for_inbound.kms_encrypt(
                                            &account_id,
                                            &region_for_inbound,
                                            kms_key,
                                            &data,
                                            "s3.amazonaws.com",
                                            ctx,
                                        ) {
                                            Ok(envelope) => {
                                                let enc_bytes =
                                                    bytes::Bytes::from(envelope.into_bytes());
                                                (enc_bytes, Some("aws:kms".to_string()), Some(kms_key.clone()))
                                            }
                                            Err(err) => {
                                                tracing::warn!(
                                                    bucket = %bucket_name,
                                                    key = %key,
                                                    error = %err,
                                                    "SES inbound: KMS encrypt failed, storing plaintext"
                                                );
                                                (data.clone(), None, None)
                                            }
                                        }
                                    } else {
                                        (data.clone(), None, None)
                                    };
                                let obj = fakecloud_s3::S3Object {
                                    key: key.clone(),
                                    body: fakecloud_persistence::BodyRef::Memory(body_bytes.clone()),
                                    content_type: "text/plain".to_string(),
                                    etag: etag.clone(),
                                    size,
                                    last_modified: now,
                                    storage_class: "STANDARD".to_string(),
                                    sse_algorithm,
                                    sse_kms_key_id,
                                    ..Default::default()
                                };
                                let mut mas = s3_for_inbound.write();
                                let state = mas.default_mut();
                                if let Some(bucket) = state.buckets.get_mut(bucket_name) {
                                    tracing::info!(
                                        bucket = %bucket_name,
                                        key = %key,
                                        kms = kms_key_arn.is_some(),
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
                                        fakecloud_persistence::BodySource::Bytes(body_bytes),
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
                            fakecloud_ses::ReceiptAction::Sns { topic_arn, .. } => {
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
                                    "content": &augmented_body,
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
                            fakecloud_ses::ReceiptAction::Lambda {
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
                                let function_arn = function_arn.clone();
                                tracing::info!(
                                    function_arn = %function_arn,
                                    invocation_type = ?invocation_type,
                                    "SES inbound: invoking Lambda"
                                );
                                if invocation_type.as_deref() == Some("RequestResponse") {
                                    // Synchronous invocation — await result inline
                                    // so the caller can observe success/failure.
                                    match delivery_for_inbound
                                        .invoke_lambda(&function_arn, &payload)
                                        .await
                                    {
                                        Some(Ok(_)) => {
                                            tracing::info!(
                                                function_arn = %function_arn,
                                                "SES inbound: Lambda RequestResponse succeeded"
                                            );
                                        }
                                        Some(Err(e)) => {
                                            tracing::error!(
                                                function_arn = %function_arn,
                                                error = %e,
                                                "SES inbound: Lambda RequestResponse failed"
                                            );
                                        }
                                        None => {
                                            tracing::warn!(
                                                "SES inbound: no container runtime available for Lambda RequestResponse"
                                            );
                                        }
                                    }
                                } else {
                                    // Fire-and-forget (Event / DryRun).
                                    let delivery = delivery_for_inbound.clone();
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
                            }
                            fakecloud_ses::ReceiptAction::Bounce {
                                smtp_reply_code,
                                message,
                                sender,
                                status_code,
                                topic_arn,
                            } => {
                                // Real AWS sends a bounce email back to the
                                // original sender. Append a SentEmail entry
                                // mirroring the bounce payload so test code
                                // can read it back via /_fakecloud/ses/emails.
                                let bounce_subject = format!(
                                    "Delivery Status Notification (Failure) for {}",
                                    body.from
                                );
                                let bounce_body = format!(
                                    "Your message could not be delivered.\r\n\r\nSMTP code: {smtp_reply_code}\r\nStatus: {}\r\nMessage: {message}\r\n",
                                    status_code.as_deref().unwrap_or("5.0.0")
                                );
                                let bounce_record = fakecloud_ses::SentEmail {
                                    message_id: format!("bounce-{}", uuid::Uuid::new_v4()),
                                    from: sender.clone(),
                                    to: vec![body.from.clone()],
                                    cc: Vec::new(),
                                    bcc: Vec::new(),
                                    subject: Some(bounce_subject),
                                    html_body: None,
                                    text_body: Some(bounce_body),
                                    raw_data: None,
                                    template_name: None,
                                    template_data: None,
                                    dkim_signature: None,
                                    headers: Vec::new(),
                                    timestamp: chrono::Utc::now(),
                                    email_tags: Vec::new(),
                                    delivery_insights: Vec::new(),
                                };
                                {
                                    let mut mas = ses_state_for_inbound_actions.write();
                                    let st = mas.default_mut();
                                    st.sent_emails.push(bounce_record);
                                }
                                // Optional notification topic.
                                if let Some(topic) = topic_arn {
                                    let notification = serde_json::json!({
                                        "notificationType": "Bounce",
                                        "bounce": {
                                            "bounceType": "Permanent",
                                            "bounceSubType": "General",
                                            "bouncedRecipients": [{
                                                "emailAddress": &body.from,
                                                "status": status_code,
                                                "diagnosticCode": message,
                                            }],
                                            "smtpReplyCode": smtp_reply_code,
                                        },
                                        "mail": {
                                            "messageId": message_id,
                                            "source": &body.from,
                                            "destination": &body.to,
                                        },
                                    });
                                    delivery_for_inbound.publish_to_sns(
                                        topic,
                                        &notification.to_string(),
                                        Some("SES Bounce"),
                                    );
                                }
                            }
                            fakecloud_ses::ReceiptAction::Stop { topic_arn, .. } => {
                                if let Some(topic) = topic_arn {
                                    let notification = serde_json::json!({
                                        "notificationType": "ReceiptRuleStop",
                                        "mail": {
                                            "messageId": message_id,
                                            "source": &body.from,
                                            "destination": &body.to,
                                        },
                                    });
                                    delivery_for_inbound.publish_to_sns(
                                        topic,
                                        &notification.to_string(),
                                        Some("SES ReceiptRule Stop"),
                                    );
                                }
                            }
                            fakecloud_ses::ReceiptAction::Workmail {
                                organization_arn,
                                topic_arn,
                            } => {
                                tracing::info!(
                                    organization_arn = %organization_arn,
                                    "SES inbound: Workmail action recorded"
                                );
                                if let Some(topic) = topic_arn {
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
                                        "content": &augmented_body,
                                    });
                                    delivery_for_inbound.publish_to_sns(
                                        topic,
                                        &notification.to_string(),
                                        Some(&body.subject),
                                    );
                                }
                            }
                            // AddHeader is processed inline above
                            fakecloud_ses::ReceiptAction::AddHeader { .. } => {}
                        }
                    }
                    let actions_executed = actions
                        .iter()
                        .map(|(rule, action)| types::InboundActionExecuted {
                            rule: rule.clone(),
                            action_type: match action {
                                fakecloud_ses::ReceiptAction::S3 { .. } => "S3",
                                fakecloud_ses::ReceiptAction::Sns { .. } => "SNS",
                                fakecloud_ses::ReceiptAction::Lambda { .. } => "Lambda",
                                fakecloud_ses::ReceiptAction::Bounce { .. } => "Bounce",
                                fakecloud_ses::ReceiptAction::AddHeader { .. } => {
                                    "AddHeader"
                                }
                                fakecloud_ses::ReceiptAction::Stop { .. } => "Stop",
                                fakecloud_ses::ReceiptAction::Workmail { .. } => {
                                    "Workmail"
                                }
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
            "/_fakecloud/sns/cert.pem",
            axum::routing::get(|| async {
                (
                    [(axum::http::header::CONTENT_TYPE, "application/x-pem-file")],
                    fakecloud_sns::signing::cert_pem(),
                )
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
            "/_fakecloud/sns/sms",
            axum::routing::get({
                let ss = sns_sms_state;
                move || async move {
                    let mas = ss.read();
                    let messages = mas
                        .iter()
                        .flat_map(|(_, state)| state.sms_messages.iter())
                        .map(|(phone_number, message)| types::SnsSmsMessage {
                            phone_number: phone_number.clone(),
                            message: message.clone(),
                        })
                        .collect();
                    axum::Json(types::SnsSmsResponse { messages })
                }
            }),
        )
        .route(
            "/_fakecloud/logs/anomalies/inject",
            axum::routing::post({
                let ls = logs_anomalies_state.clone();
                move |axum::Json(body): axum::Json<types::LogsAnomalyInjectRequest>| async move {
                    let now = chrono::Utc::now().timestamp_millis();
                    let anomaly_id = uuid::Uuid::new_v4().to_string();
                    let pattern_id = format!("{:032x}", uuid::Uuid::new_v4().as_u128());
                    let mut accounts = ls.write();
                    let state = accounts.default_mut();
                    state.anomalies.insert(
                        anomaly_id.clone(),
                        fakecloud_logs::LogAnomaly {
                            anomaly_id: anomaly_id.clone(),
                            anomaly_detector_arn: body.anomaly_detector_arn,
                            log_group_arn_list: body.log_group_arns,
                            pattern_id,
                            pattern_string: body.pattern_string,
                            first_seen: now,
                            last_seen: now,
                            priority: body.priority.unwrap_or_else(|| "MEDIUM".to_string()),
                            state: "ACTIVE".to_string(),
                            suppressed: false,
                        },
                    );
                    (
                        axum::http::StatusCode::OK,
                        axum::Json(types::LogsAnomalyInjectResponse { anomaly_id }),
                    )
                }
            }),
        )
        .route(
            "/_fakecloud/logs/delivery-config",
            axum::routing::get({
                let ls = logs_state.clone();
                move || {
                    let ls = ls.clone();
                    async move {
                        let accounts = ls.read();
                        let mut configurations: Vec<types::LogsDeliveryConfiguration> = Vec::new();
                        for (_account, state) in accounts.iter() {
                            for delivery in state.deliveries.values() {
                                let log_type = state
                                    .delivery_sources
                                    .get(&delivery.delivery_source_name)
                                    .map(|s| s.log_type.clone())
                                    .unwrap_or_default();
                                configurations.push(types::LogsDeliveryConfiguration {
                                    id: delivery.id.clone(),
                                    name: delivery.id.clone(),
                                    delivery_destination_arn: delivery
                                        .delivery_destination_arn
                                        .clone(),
                                    delivery_source_name: delivery.delivery_source_name.clone(),
                                    log_type,
                                    record_fields: delivery.record_fields.clone(),
                                    field_delimiter: delivery.field_delimiter.clone(),
                                    s3_delivery_configuration: delivery
                                        .s3_delivery_configuration
                                        .clone(),
                                    created_at: delivery.created_at,
                                });
                            }
                        }
                        axum::Json(types::LogsDeliveryConfigResponse { configurations })
                    }
                }
            }),
        )
        .route(
            "/_fakecloud/logs/field-indexes/{log_group_name}",
            axum::routing::get({
                let ls = logs_state.clone();
                move |axum::extract::Path(log_group_name): axum::extract::Path<String>| {
                    let ls = ls.clone();
                    async move {
                        let accounts = ls.read();
                        let mut indexes: Vec<types::LogsFieldIndex> = Vec::new();
                        let mut found = false;
                        for (_account, state) in accounts.iter() {
                            let Some(group) = state.log_groups.get(&log_group_name) else {
                                continue;
                            };
                            found = true;
                            for policy in &group.index_policies {
                                let parsed: serde_json::Value =
                                    serde_json::from_str(&policy.policy_document)
                                        .unwrap_or(serde_json::Value::Null);
                                let fields: Vec<String> = parsed
                                    .get("Fields")
                                    .and_then(|v| v.as_array())
                                    .map(|arr| {
                                        arr.iter()
                                            .filter_map(|f| f.as_str().map(|s| s.to_string()))
                                            .collect()
                                    })
                                    .unwrap_or_default();
                                indexes.push(types::LogsFieldIndex {
                                    fields,
                                    created_at: policy.last_updated_time,
                                    last_used_at: policy.last_updated_time,
                                });
                            }
                        }
                        if !found {
                            return axum::http::StatusCode::NOT_FOUND.into_response();
                        }
                        axum::Json(types::LogsFieldIndexesResponse {
                            log_group_name,
                            indexes,
                        })
                        .into_response()
                    }
                }
            }),
        )
        .route(
            "/_fakecloud/sqs/messages",
            axum::routing::get({
                let ss = sqs_introspection_state;
                // Render each queue_url against the caller's Host so the
                // introspection view matches the QueueUrls returned by the SQS
                // API (host-aware, bug-hunt 1.10).
                move |headers: axum::http::HeaderMap| async move {
                    let mas = ss.read();
                    let queues = mas
                        .iter()
                        .flat_map(|(_, state)| {
                            let base =
                                fakecloud_sqs::resolve_endpoint_base(&headers, &state.endpoint);
                            state.queues.values().map(move |queue| {
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
                                    queue_url: fakecloud_sqs::render_queue_url(
                                        &queue.queue_url,
                                        &base,
                                    ),
                                    queue_name: queue.queue_name.clone(),
                                    messages,
                                }
                            })
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
            "/_fakecloud/application-autoscaling/tick",
            axum::routing::post({
                let watcher = appas_watcher_for_admin.clone();
                move || async move {
                    let applied = watcher.tick_once();
                    axum::Json(types::AppAsTickResponse { applied })
                }
            }),
        )
        .route(
            "/_fakecloud/application-autoscaling/scheduled-tick",
            axum::routing::post({
                let executor = appas_scheduled_executor_for_admin.clone();
                move || async move {
                    let fired = executor.tick_once();
                    axum::Json(types::AppAsScheduledTickResponse { fired })
                }
            }),
        )
        .route(
            "/_fakecloud/ssm/commands/{command_id}/status",
            axum::routing::post({
                let ss = ssm_state_for_admin;
                move |axum::extract::Path(command_id): axum::extract::Path<String>,
                      axum::Json(body): axum::Json<types::SetSsmCommandStatusRequest>| async move {
                    let account = body.account_id.as_deref().unwrap_or("000000000000");
                    let svc = fakecloud_ssm::SsmService::new(ss);
                    let updated = svc.set_command_status(account, &command_id, &body.status);
                    axum::Json(types::SetSsmCommandStatusResponse { updated })
                }
            }),
        )
        .route(
            "/_fakecloud/ssm/commands/{command_id}/fail",
            axum::routing::post({
                let ss = ssm_state_for_fail;
                move |axum::extract::Path(command_id): axum::extract::Path<String>,
                      body: Option<axum::Json<types::FailSsmCommandRequest>>| async move {
                    let body = body.map(|b| b.0).unwrap_or_default();
                    // Default to the server's configured account so
                    // tests don't have to thread the ID through every
                    // admin call. Falls back to "000000000000" only on
                    // the off chance state initialisation hasn't yet
                    // populated a default.
                    let default_account = ss.read().default_account_id().to_string();
                    let account = body.account_id.as_deref().unwrap_or(&default_account);
                    let svc = fakecloud_ssm::SsmService::new(ss.clone());
                    let updated = svc.fail_command_invocation(
                        account,
                        &command_id,
                        body.instance_id.as_deref(),
                        body.status_details.as_deref(),
                        body.standard_error_content.as_deref(),
                    );
                    axum::Json(types::FailSsmCommandResponse {
                        updated_invocations: updated,
                    })
                }
            }),
        )
        .route(
            "/_fakecloud/ssm/parameter-policy-events",
            axum::routing::get({
                let ss = ssm_state_for_policy_events.clone();
                move |query: axum::extract::Query<
                    std::collections::HashMap<String, String>,
                >| async move {
                    let default_account = ss.read().default_account_id().to_string();
                    let account = query
                        .get("accountId")
                        .cloned()
                        .unwrap_or(default_account);
                    let svc = fakecloud_ssm::SsmService::new(ss.clone());
                    let events = svc
                        .parameter_policy_events(&account)
                        .into_iter()
                        .map(|e| types::SsmParameterPolicyEvent {
                            parameter_name: e.parameter_name,
                            parameter_arn: e.parameter_arn,
                            event_type: e.event_type,
                            message: e.message,
                            created_at: e.created_at.to_rfc3339(),
                        })
                        .collect();
                    axum::Json(types::SsmParameterPolicyEventsResponse { events })
                }
            }),
        )
        .route(
            "/_fakecloud/ssm/parameter-policy-events",
            axum::routing::delete({
                let ss = ssm_state_for_policy_events;
                move |query: axum::extract::Query<
                    std::collections::HashMap<String, String>,
                >| async move {
                    let default_account = ss.read().default_account_id().to_string();
                    let account = query
                        .get("accountId")
                        .cloned()
                        .unwrap_or(default_account);
                    let svc = fakecloud_ssm::SsmService::new(ss.clone());
                    svc.clear_parameter_policy_events(&account);
                    axum::http::StatusCode::NO_CONTENT
                }
            }),
        )
        .route(
            // Drop a fake SSM session record into state. StartSession
            // returns the Smithy-declared `TargetNotConnected` by default
            // (no real websocket data plane); this endpoint lets tests
            // still exercise DescribeSessions / TerminateSession without
            // flipping FAKECLOUD_SSM_SESSION_ECHO.
            "/_fakecloud/ssm/sessions/inject",
            axum::routing::post({
                let ss = ssm_state_for_session_inject;
                move |axum::Json(body): axum::Json<types::InjectSsmSessionRequest>| async move {
                    let default_account = ss.read().default_account_id().to_string();
                    let account = body.account_id.as_deref().unwrap_or(&default_account);
                    let svc = fakecloud_ssm::SsmService::new(ss.clone());
                    let session_id = svc.inject_session(
                        account,
                        &body.target,
                        body.status.as_deref(),
                        body.owner.as_deref(),
                        body.reason.as_deref(),
                        body.session_id.as_deref(),
                    );
                    axum::Json(types::InjectSsmSessionResponse { session_id })
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
            "/_fakecloud/s3/access-points",
            axum::routing::get({
                let ss = s3_access_points_introspection_state;
                move || async move {
                    let mas = ss.read();
                    let mut access_points: Vec<types::S3AccessPointEntry> = mas
                        .iter()
                        .flat_map(|(account_id, state)| {
                            state.access_points.values().map(move |ap| {
                                types::S3AccessPointEntry {
                                    name: ap.name.clone(),
                                    alias: format!("{}-{}", ap.name, ap.account_id),
                                    bucket: ap.bucket.clone(),
                                    account_id: account_id.to_string(),
                                    network_origin: ap.network_origin.clone(),
                                    vpc_configuration: ap.vpc_configuration.clone(),
                                    public_access_block: ap.public_access_block.clone(),
                                    created_at: ap.creation_date.to_rfc3339(),
                                }
                            })
                        })
                        .collect();
                    access_points.sort_by(|a, b| {
                        a.account_id
                            .cmp(&b.account_id)
                            .then(a.name.cmp(&b.name))
                    });
                    axum::Json(types::S3AccessPointsResponse { access_points })
                }
            }),
        )
        .route(
            "/_fakecloud/s3/object-lambda-responses",
            axum::routing::get({
                let ss = s3_object_lambda_introspection_state;
                move || async move {
                    use base64::Engine as _;
                    let mas = ss.read();
                    let mut responses: Vec<types::S3ObjectLambdaResponse> = mas
                        .iter()
                        .flat_map(|(_, state)| state.object_lambda_responses.values())
                        .map(|r| types::S3ObjectLambdaResponse {
                            request_token: r.token.clone(),
                            request_route: r.route.clone(),
                            status_code: r.fwd_status,
                            body_base64: base64::engine::general_purpose::STANDARD
                                .encode(&r.body),
                            body_size: r.body.len() as u64,
                            content_type: r.content_type.clone(),
                            error_message: r.fwd_error_message.clone(),
                            metadata: r.metadata.clone(),
                        })
                        .collect();
                    responses.sort_by(|a, b| a.request_token.cmp(&b.request_token));
                    axum::Json(types::S3ObjectLambdaResponsesResponse { responses })
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
            "/_fakecloud/glue/jobs",
            axum::routing::get({
                let state = glue_state_for_jobs;
                move || async move {
                    let rows = fakecloud_glue::introspection::list_all_jobs(&state);
                    let jobs = rows
                        .into_iter()
                        .map(|r| types::GlueJob {
                            account_id: r.account_id,
                            name: r.name,
                            role: r.role,
                            command: r.command,
                            default_arguments: r.default_arguments,
                            max_capacity: r.max_capacity,
                            max_retries: r.max_retries,
                            timeout: r.timeout,
                            glue_version: r.glue_version,
                            worker_type: r.worker_type,
                            number_of_workers: r.number_of_workers,
                            created_on: r.created_on.to_rfc3339(),
                            last_modified_on: r.last_modified_on.to_rfc3339(),
                        })
                        .collect();
                    axum::Json(types::GlueJobsResponse { jobs })
                }
            }),
        )
        .route(
            "/_fakecloud/glue/job-runs",
            axum::routing::get({
                let state = glue_state_for_runs;
                move |axum::extract::Query(params): axum::extract::Query<
                    std::collections::HashMap<String, String>,
                >| {
                    let state = state.clone();
                    async move {
                        let filter = params.get("job_name").map(String::as_str);
                        let rows = fakecloud_glue::introspection::list_all_job_runs(&state, filter);
                        let runs = rows
                            .into_iter()
                            .map(|r| types::GlueJobRun {
                                account_id: r.account_id,
                                id: r.id,
                                job_name: r.job_name,
                                attempt: r.attempt,
                                started_on: r.started_on.to_rfc3339(),
                                completed_on: r.completed_on.map(|t| t.to_rfc3339()),
                                job_run_state: r.job_run_state,
                                arguments: r.arguments,
                                error_message: r.error_message,
                                execution_time: r.execution_time,
                            })
                            .collect();
                        axum::Json(types::GlueJobRunsResponse { runs })
                    }
                }
            }),
        )
        .route(
            "/_fakecloud/glue/crawlers",
            axum::routing::get({
                let state = glue_state_for_crawlers;
                move || async move {
                    let rows = fakecloud_glue::introspection::list_all_crawlers(&state);
                    let crawlers = rows
                        .into_iter()
                        .map(|r| types::GlueCrawler {
                            account_id: r.account_id,
                            name: r.name,
                            role: r.role,
                            database_name: r.database_name,
                            state: r.state,
                            target_summary: r.target_summary,
                            schedule: r.schedule,
                            creation_time: r.creation_time.to_rfc3339(),
                            last_updated: r.last_updated.to_rfc3339(),
                        })
                        .collect();
                    axum::Json(types::GlueCrawlersResponse { crawlers })
                }
            }),
        )
        .route(
            "/_fakecloud/cloudwatch/alarms",
            axum::routing::get({
                let state = cloudwatch_state_for_alarms;
                move || async move {
                    let rows = fakecloud_cloudwatch::introspection::list_all_alarms(&state);
                    let alarms = rows
                        .into_iter()
                        .map(|r| types::CloudWatchAlarm {
                            account_id: r.account_id,
                            region: r.region,
                            name: r.name,
                            alarm_type: r.kind.to_string(),
                            state: r.state,
                            state_reason: r.state_reason,
                            state_updated_timestamp: r
                                .state_updated_timestamp
                                .map(|t| t.to_rfc3339()),
                            actions_enabled: r.actions_enabled,
                            alarm_actions: r.alarm_actions,
                            ok_actions: r.ok_actions,
                            insufficient_data_actions: r.insufficient_data_actions,
                            namespace: r.namespace,
                            metric_name: r.metric_name,
                            threshold: r.threshold,
                            comparison_operator: r.comparison_operator,
                            alarm_rule: r.alarm_rule,
                        })
                        .collect();
                    axum::Json(types::CloudWatchAlarmsResponse { alarms })
                }
            }),
        )
        .route(
            "/_fakecloud/cloudwatch/metrics",
            axum::routing::get({
                let state = cloudwatch_state_for_metrics;
                move || async move {
                    let rows = fakecloud_cloudwatch::introspection::list_all_metrics(&state);
                    let metrics = rows
                        .into_iter()
                        .map(|r| types::CloudWatchMetric {
                            account_id: r.account_id,
                            region: r.region,
                            namespace: r.namespace,
                            metric_name: r.metric_name,
                            dimensions: r
                                .dimensions
                                .into_iter()
                                .map(|d| types::CloudWatchDimension {
                                    name: d.name,
                                    value: d.value,
                                })
                                .collect(),
                            datapoint_count: r.datapoint_count,
                            latest: r.latest.map(|l| types::CloudWatchLatestDatapoint {
                                timestamp: l.timestamp.to_rfc3339(),
                                value: l.value,
                                unit: l.unit,
                            }),
                        })
                        .collect();
                    axum::Json(types::CloudWatchMetricsResponse { metrics })
                }
            }),
        )
        .route(
            "/_fakecloud/firehose/delivery-streams",
            axum::routing::get({
                let state = firehose_state.clone();
                move || {
                    let state = state.clone();
                    async move {
                        let rows =
                            fakecloud_firehose::introspection::list_all_delivery_streams(&state);
                        let delivery_streams = rows
                            .into_iter()
                            .map(|r| types::FirehoseDeliveryStream {
                                account_id: r.account_id,
                                name: r.name,
                                arn: r.arn,
                                stream_type: r.stream_type,
                                status: r.status,
                                encryption: types::FirehoseEncryption {
                                    status: r.encryption.status,
                                    key_type: r.encryption.key_type,
                                    key_arn: r.encryption.key_arn,
                                },
                                destination_count: r.destination_count,
                                create_timestamp: r.create_timestamp.to_rfc3339(),
                                last_update_timestamp: r
                                    .last_update_timestamp
                                    .map(|t| t.to_rfc3339()),
                            })
                            .collect();
                        axum::Json(types::FirehoseDeliveryStreamsResponse { delivery_streams })
                    }
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
                let delivery = dynamodb_ttl_delivery;
                let store = dynamodb_ttl_snapshot_store;
                let lock = dynamodb_ttl_snapshot_lock;
                move || async move {
                    let count = fakecloud_dynamodb::ttl::process_ttl_expirations_with(
                        &ds,
                        Some(&delivery),
                    );
                    // Persist the deletions: the tick mutates state outside any
                    // handler, so without this the expired items reappear on the
                    // next restart (the normal mutating API path saves here).
                    if count > 0 {
                        if let Err(err) =
                            fakecloud_dynamodb::save_dynamodb_snapshot(&ds, store.clone(), &lock)
                                .await
                        {
                            tracing::error!(%err, "dynamodb snapshot save failed");
                        }
                    }
                    axum::Json(types::TtlTickResponse {
                        expired_items: count as u64,
                    })
                }
            }),
        )
        .route(
            "/_fakecloud/dynamodb/snapshot/save",
            axum::routing::post({
                let service = dynamodb_service.clone();
                move |body: Option<axum::Json<types::DynamoDbSnapshotSaveRequest>>| {
                    let service = service.clone();
                    async move {
                        let result = if let Some(data_path) =
                            body.and_then(|axum::Json(body)| body.data_path)
                        {
                            let data_path = std::path::PathBuf::from(data_path);
                            let store = fakecloud_persistence::DiskSnapshotStore::new(
                                data_path.join("dynamodb").join("snapshot.json"),
                            );
                            service
                                .save_snapshot_to_store(Arc::new(store))
                                .await
                                .map(|_| true)
                        } else {
                            service.save_snapshot().await
                        };

                        match result {
                            Ok(true) => {
                                axum::Json(serde_json::json!({ "saved": true })).into_response()
                            }
                            Ok(false) => (
                                axum::http::StatusCode::BAD_REQUEST,
                                axum::Json(serde_json::json!({
                                    "error": "dynamodb snapshot store is not configured and request body did not include dataPath"
                                })),
                            )
                                .into_response(),
                            Err(err) => {
                                tracing::error!(%err, "manual dynamodb snapshot save failed");
                                (
                                    axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                                    axum::Json(serde_json::json!({
                                        "error": "failed to save dynamodb snapshot"
                                    })),
                                )
                                    .into_response()
                            }
                        }
                    }
                }
            }),
        )
        .route(
            "/_fakecloud/secretsmanager/rotation-scheduler/tick",
            axum::routing::post({
                let ss = secretsmanager_rotation_state;
                let bus = delivery_for_rotation_scheduler;
                let store = secretsmanager_rotation_snapshot_store;
                move || async move {
                    let rotated = fakecloud_secretsmanager::rotation::check_and_rotate(
                        &ss,
                        Some(&bus),
                        store.clone(),
                    )
                    .await;
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
            "/{pool_id}/.well-known/jwks.json",
            axum::routing::get({
                let cs = cognito_jwks_state;
                move |axum::extract::Path(pool_id): axum::extract::Path<String>| {
                    let cs = cs.clone();
                    async move {
                        match fakecloud_cognito::pool_jwks_document(&cs, &pool_id).await {
                            Some(doc) => (axum::http::StatusCode::OK, axum::Json(doc)),
                            None => (
                                axum::http::StatusCode::NOT_FOUND,
                                axum::Json(serde_json::json!({
                                    "error": "User pool not found",
                                    "pool_id": pool_id,
                                })),
                            ),
                        }
                    }
                }
            }),
        )
        .route(
            "/_fakecloud/cognito/compromised-passwords",
            axum::routing::post({
                let cs = cognito_state.clone();
                move |axum::Json(body): axum::Json<types::CognitoCompromisedPasswordsRequest>| {
                    let cs = cs.clone();
                    async move {
                        use sha2::{Digest, Sha256};
                        let mut mas = cs.write();
                        let state = mas.default_mut();
                        let mut added = 0usize;
                        for p in body.passwords {
                            let mut hasher = Sha256::new();
                            hasher.update(p.as_bytes());
                            let hash = format!("{:x}", hasher.finalize());
                            if state.compromised_password_hashes.insert(hash) {
                                added += 1;
                            }
                        }
                        axum::Json(serde_json::json!({ "added": added }))
                    }
                }
            }),
        )
        .route(
            "/{pool_id}/.well-known/openid-configuration",
            axum::routing::get({
                let cs = cognito_oidc_state;
                move |headers: axum::http::HeaderMap,
                      axum::extract::Path(pool_id): axum::extract::Path<String>| {
                    let cs = cs.clone();
                    async move {
                        let (exists, pool_domain) =
                            fakecloud_cognito::pool_existence_and_domain(&cs, &pool_id);
                        if !exists {
                            return (
                                axum::http::StatusCode::NOT_FOUND,
                                axum::Json(serde_json::json!({
                                    "error": "User pool not found",
                                    "pool_id": pool_id,
                                })),
                            );
                        }
                        let region = pool_id
                            .split_once('_')
                            .map(|(r, _)| r.to_string())
                            .unwrap_or_else(|| "us-east-1".to_string());
                        let host = headers
                            .get(axum::http::header::HOST)
                            .and_then(|v| v.to_str().ok())
                            .unwrap_or("localhost")
                            .to_string();
                        let base_url = format!("http://{host}");
                        (
                            axum::http::StatusCode::OK,
                            axum::Json(fakecloud_cognito::oidc_discovery_document(
                                &pool_id,
                                &region,
                                &base_url,
                                pool_domain.as_deref(),
                            )),
                        )
                    }
                }
            }),
        )
        .route(
            "/oauth2/authorize",
            axum::routing::get({
                let cs = cognito_authorize_state;
                let store = cognito_oauth2_snapshot_store.clone();
                let lock = cognito_oauth2_snapshot_lock.clone();
                move |axum::extract::Query(q): axum::extract::Query<
                    std::collections::BTreeMap<String, String>,
                >| {
                    let cs = cs.clone();
                    let store = store.clone();
                    let lock = lock.clone();
                    async move {
                        let region = std::env::var("AWS_DEFAULT_REGION")
                            .or_else(|_| std::env::var("AWS_REGION"))
                            .unwrap_or_else(|_| "us-east-1".to_string());
                        // RFC 6749 §4.1.1 / §4.2.1 mandate at minimum
                        // `response_type` and `client_id`; `redirect_uri`
                        // is required when the client has multiple
                        // callbacks registered. We require it always
                        // here because we use it as the trust anchor
                        // for the redirect.
                        let Some(response_type) = q.get("response_type") else {
                            return (
                                axum::http::StatusCode::BAD_REQUEST,
                                axum::http::HeaderMap::new(),
                                axum::Json(serde_json::json!({
                                    "error": "invalid_request",
                                    "error_description": "response_type is required"
                                }))
                                .into_response(),
                            );
                        };
                        let Some(client_id) = q.get("client_id") else {
                            return (
                                axum::http::StatusCode::BAD_REQUEST,
                                axum::http::HeaderMap::new(),
                                axum::Json(serde_json::json!({
                                    "error": "invalid_request",
                                    "error_description": "client_id is required"
                                }))
                                .into_response(),
                            );
                        };
                        let Some(redirect_uri) = q.get("redirect_uri") else {
                            return (
                                axum::http::StatusCode::BAD_REQUEST,
                                axum::http::HeaderMap::new(),
                                axum::Json(serde_json::json!({
                                    "error": "invalid_request",
                                    "error_description": "redirect_uri is required"
                                }))
                                .into_response(),
                            );
                        };
                        let req = fakecloud_cognito::OAuth2AuthorizeRequest {
                            response_type: response_type.clone(),
                            client_id: client_id.clone(),
                            redirect_uri: redirect_uri.clone(),
                            scope: q.get("scope").cloned(),
                            state: q.get("state").cloned(),
                            code_challenge: q.get("code_challenge").cloned(),
                            code_challenge_method: q.get("code_challenge_method").cloned(),
                            nonce: q.get("nonce").cloned(),
                            username: q.get("username").cloned(),
                            password: q.get("password").cloned(),
                        };
                        match fakecloud_cognito::handle_oauth2_authorize(&cs, &req, &region).await {
                            Ok(fakecloud_cognito::OAuth2AuthorizeOutcome::Redirect(url)) => {
                                // A successful authorize minted an authorization
                                // code (or implicit-grant token) in state; write
                                // it through so it survives a restart (0.A4).
                                fakecloud_cognito::save_cognito_snapshot(
                                    &cs,
                                    store.clone(),
                                    &lock,
                                )
                                .await;
                                let mut headers = axum::http::HeaderMap::new();
                                if let Ok(loc) = axum::http::HeaderValue::from_str(&url) {
                                    headers.insert(axum::http::header::LOCATION, loc);
                                }
                                (axum::http::StatusCode::FOUND, headers, String::new().into_response())
                            }
                            Ok(fakecloud_cognito::OAuth2AuthorizeOutcome::LoginRequired {
                                html,
                            }) => {
                                let mut headers = axum::http::HeaderMap::new();
                                headers.insert(
                                    axum::http::header::CONTENT_TYPE,
                                    axum::http::HeaderValue::from_static("text/html; charset=utf-8"),
                                );
                                (axum::http::StatusCode::OK, headers, html.into_response())
                            }
                            Err(err) => {
                                let code = match err {
                                    fakecloud_cognito::OAuth2AuthorizeError::InvalidClient => {
                                        "invalid_client"
                                    }
                                    fakecloud_cognito::OAuth2AuthorizeError::InvalidRedirectUri => {
                                        "invalid_request"
                                    }
                                };
                                (
                                    axum::http::StatusCode::BAD_REQUEST,
                                    axum::http::HeaderMap::new(),
                                    axum::Json(serde_json::json!({"error": code}))
                                        .into_response(),
                                )
                            }
                        }
                    }
                }
            }),
        )
        .route(
            "/oauth2/token",
            axum::routing::post({
                let cs = cognito_token_state;
                let store = cognito_oauth2_snapshot_store.clone();
                let lock = cognito_oauth2_snapshot_lock.clone();
                move |headers: axum::http::HeaderMap, body: String| {
                    let cs = cs.clone();
                    let store = store.clone();
                    let lock = lock.clone();
                    async move {
                        let params: std::collections::BTreeMap<String, String> =
                            match serde_urlencoded::from_str::<Vec<(String, String)>>(&body) {
                                Ok(pairs) => pairs.into_iter().collect(),
                                Err(_) => std::collections::BTreeMap::new(),
                            };
                        // RFC 6749 §2.3.1 — confidential clients MAY send
                        // their credentials in the Authorization header
                        // (`Basic base64(client_id:client_secret)`); we
                        // treat that header as authoritative and fail
                        // when it disagrees with the body.
                        let basic = parse_basic_auth(&headers);
                        let region = std::env::var("AWS_DEFAULT_REGION")
                            .or_else(|_| std::env::var("AWS_REGION"))
                            .unwrap_or_else(|_| "us-east-1".to_string());
                        let basic_ref = basic.as_ref().map(|(i, s)| (i.as_str(), s.as_str()));
                        match fakecloud_cognito::handle_oauth2_token(
                            &cs, &params, basic_ref, &region,
                        )
                        .await
                        {
                            Ok(resp) => {
                                // A successful grant minted refresh/access tokens
                                // in state; write them through (0.A4).
                                fakecloud_cognito::save_cognito_snapshot(
                                    &cs,
                                    store.clone(),
                                    &lock,
                                )
                                .await;
                                (axum::http::StatusCode::OK, axum::Json(resp.to_json()))
                            }
                            Err(err) => {
                                let status = axum::http::StatusCode::from_u16(err.status_code())
                                    .unwrap_or(axum::http::StatusCode::BAD_REQUEST);
                                let mut body = serde_json::Map::new();
                                body.insert(
                                    "error".into(),
                                    serde_json::Value::String(err.as_oauth_code().to_string()),
                                );
                                if let Some(desc) = err.description() {
                                    body.insert(
                                        "error_description".into(),
                                        serde_json::Value::String(desc.to_string()),
                                    );
                                }
                                (status, axum::Json(serde_json::Value::Object(body)))
                            }
                        }
                    }
                }
            }),
        )
        .route(
            "/oauth2/userInfo",
            {
                let cs_get = cognito_userinfo_state.clone();
                let cs_post = cognito_userinfo_state;
                axum::routing::get({
                    move |headers: axum::http::HeaderMap| {
                        let cs = cs_get.clone();
                        async move {
                            let bearer = headers
                                .get(axum::http::header::AUTHORIZATION)
                                .and_then(|v| v.to_str().ok())
                                .and_then(|v| v.strip_prefix("Bearer "))
                                .map(|s| s.to_string());
                            let Some(token) = bearer else {
                                let mut body = serde_json::Map::new();
                                body.insert(
                                    "error".into(),
                                    serde_json::Value::String("invalid_token".to_string()),
                                );
                                return (
                                    axum::http::StatusCode::UNAUTHORIZED,
                                    axum::Json(serde_json::Value::Object(body)),
                                );
                            };
                            match fakecloud_cognito::handle_oauth2_userinfo(&cs, &token) {
                                Ok(value) => (axum::http::StatusCode::OK, axum::Json(value)),
                                Err(_) => {
                                    let mut body = serde_json::Map::new();
                                    body.insert(
                                        "error".into(),
                                        serde_json::Value::String("invalid_token".to_string()),
                                    );
                                    (
                                        axum::http::StatusCode::UNAUTHORIZED,
                                        axum::Json(serde_json::Value::Object(body)),
                                    )
                                }
                            }
                        }
                    }
                })
                .post({
                    move |headers: axum::http::HeaderMap| {
                        let cs = cs_post.clone();
                        async move {
                            let bearer = headers
                                .get(axum::http::header::AUTHORIZATION)
                                .and_then(|v| v.to_str().ok())
                                .and_then(|v| v.strip_prefix("Bearer "))
                                .map(|s| s.to_string());
                            let Some(token) = bearer else {
                                let mut body = serde_json::Map::new();
                                body.insert(
                                    "error".into(),
                                    serde_json::Value::String("invalid_token".to_string()),
                                );
                                return (
                                    axum::http::StatusCode::UNAUTHORIZED,
                                    axum::Json(serde_json::Value::Object(body)),
                                );
                            };
                            match fakecloud_cognito::handle_oauth2_userinfo(&cs, &token) {
                                Ok(value) => (axum::http::StatusCode::OK, axum::Json(value)),
                                Err(_) => {
                                    let mut body = serde_json::Map::new();
                                    body.insert(
                                        "error".into(),
                                        serde_json::Value::String("invalid_token".to_string()),
                                    );
                                    (
                                        axum::http::StatusCode::UNAUTHORIZED,
                                        axum::Json(serde_json::Value::Object(body)),
                                    )
                                }
                            }
                        }
                    }
                })
            },
        )
        .route(
            "/oauth2/revoke",
            axum::routing::post({
                let cs = cognito_revoke_state;
                let store = cognito_oauth2_snapshot_store.clone();
                let lock = cognito_oauth2_snapshot_lock.clone();
                move |body: String| {
                    let cs = cs.clone();
                    let store = store.clone();
                    let lock = lock.clone();
                    async move {
                        let params: std::collections::BTreeMap<String, String> =
                            match serde_urlencoded::from_str::<Vec<(String, String)>>(&body) {
                                Ok(pairs) => pairs.into_iter().collect(),
                                Err(_) => std::collections::BTreeMap::new(),
                            };
                        match fakecloud_cognito::handle_oauth2_revoke(&cs, &params) {
                            Ok(()) => {
                                // A successful revoke deleted the token from
                                // state; write that deletion through (0.A4).
                                fakecloud_cognito::save_cognito_snapshot(
                                    &cs,
                                    store.clone(),
                                    &lock,
                                )
                                .await;
                                (
                                    axum::http::StatusCode::OK,
                                    axum::Json(serde_json::Value::Object(serde_json::Map::new())),
                                )
                            }
                            Err(err) => {
                                let (code, status) = match err {
                                    fakecloud_cognito::OAuthRevokeError::InvalidClient => (
                                        "invalid_client",
                                        axum::http::StatusCode::UNAUTHORIZED,
                                    ),
                                    fakecloud_cognito::OAuthRevokeError::UnsupportedTokenType => (
                                        "unsupported_token_type",
                                        axum::http::StatusCode::BAD_REQUEST,
                                    ),
                                };
                                let mut body = serde_json::Map::new();
                                body.insert(
                                    "error".into(),
                                    serde_json::Value::String(code.to_string()),
                                );
                                (status, axum::Json(serde_json::Value::Object(body)))
                            }
                        }
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
            "/_fakecloud/cognito/authorization-codes",
            axum::routing::post({
                let cs = cognito_state.clone();
                let store = cognito_oauth2_snapshot_store.clone();
                let lock = cognito_oauth2_snapshot_lock.clone();
                move |axum::Json(body): axum::Json<types::MintAuthorizationCodeRequest>| {
                    let cs = cs.clone();
                    let store = store.clone();
                    let lock = lock.clone();
                    async move {
                        let req = fakecloud_cognito::MintAuthorizationCodeRequest {
                            user_pool_id: body.user_pool_id,
                            client_id: body.client_id,
                            username: body.username,
                            redirect_uri: body.redirect_uri,
                            scopes: body.scopes,
                            code_challenge: body.code_challenge,
                            code_challenge_method: body.code_challenge_method,
                            nonce: body.nonce,
                        };
                        match fakecloud_cognito::mint_authorization_code(&cs, &req) {
                            Ok(code) => {
                                // The minted authorization code lives in state;
                                // write it through so it survives a restart
                                // (0.A4).
                                fakecloud_cognito::save_cognito_snapshot(
                                    &cs,
                                    store.clone(),
                                    &lock,
                                )
                                .await;
                                (
                                    axum::http::StatusCode::OK,
                                    axum::Json(serde_json::json!(
                                        types::MintAuthorizationCodeResponse { code }
                                    )),
                                )
                            }
                            Err(err) => {
                                let (status, msg) = match err {
                                    fakecloud_cognito::MintAuthorizationCodeError::InvalidClient => (
                                        axum::http::StatusCode::NOT_FOUND,
                                        "client_id not found in any pool",
                                    ),
                                    fakecloud_cognito::MintAuthorizationCodeError::InvalidRedirectUri => (
                                        axum::http::StatusCode::BAD_REQUEST,
                                        "redirect_uri is not a registered callback URL",
                                    ),
                                    fakecloud_cognito::MintAuthorizationCodeError::UserNotFound => (
                                        axum::http::StatusCode::NOT_FOUND,
                                        "user not found in pool",
                                    ),
                                };
                                (
                                    status,
                                    axum::Json(serde_json::json!({"error": msg})),
                                )
                            }
                        }
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
            "/_fakecloud/ec2/instances",
            axum::routing::get({
                let es = ec2_introspection_state;
                move || {
                    let es = es.clone();
                    async move {
                        let accounts = es.read();
                        // Aggregate instances across every account partition —
                        // real callers land under their derived account id, not
                        // the default partition.
                        let mut instances: Vec<types::Ec2Instance> = accounts
                            .iter()
                            .flat_map(|(_, state)| state.instances.values())
                            .map(ec2_instance_response)
                            .collect();
                        instances.sort_by(|a, b| a.instance_id.cmp(&b.instance_id));
                        axum::Json(types::Ec2InstancesResponse { instances })
                    }
                }
            }),
        )
        .route(
            "/_fakecloud/ec2/instance-networks",
            axum::routing::get({
                let es = ec2_networks_state;
                let rt = ec2_networks_runtime;
                move || {
                    let es = es.clone();
                    let rt = rt.clone();
                    async move {
                        let summary = rt.as_ref().map(|r| r.network_isolation_summary());
                        let accounts = es.read();
                        let mut instance_networks: Vec<types::Ec2InstanceNetwork> = accounts
                            .iter()
                            .flat_map(|(_, state)| state.instances.values())
                            .map(|inst| {
                                introspection::ec2_instance_network_response(inst, summary.as_ref())
                            })
                            .collect();
                        instance_networks.sort_by(|a, b| a.instance_id.cmp(&b.instance_id));
                        axum::Json(types::Ec2InstanceNetworksResponse { instance_networks })
                    }
                }
            }),
        )
        .route(
            "/_fakecloud/rds/lambda-invoke",
            axum::routing::post({
                let bridge_lambda = lambda_delivery.clone();
                move |headers: axum::http::HeaderMap,
                      axum::Json(body): axum::Json<types::RdsLambdaInvokeRequest>| {
                    let bridge_lambda = bridge_lambda.clone();
                    async move {
                        let Some(ld) = bridge_lambda else {
                            return (
                                axum::http::StatusCode::SERVICE_UNAVAILABLE,
                                axum::Json(serde_json::json!({
                                    "status_code": 502,
                                    "payload": { "errorMessage": "Lambda runtime not available on this fakecloud server" },
                                    "executed_version": null,
                                    "log_result": null,
                                })),
                            );
                        };
                        let account_id = headers
                            .get("x-fakecloud-account-id")
                            .and_then(|v| v.to_str().ok())
                            .unwrap_or("000000000000")
                            .to_string();
                        let region = body
                            .region
                            .clone()
                            .unwrap_or_else(|| "us-east-1".to_string());
                        let function_arn = if body.function_name.starts_with("arn:") {
                            body.function_name.clone()
                        } else {
                            format!(
                                "arn:aws:lambda:{}:{}:function:{}",
                                region, account_id, body.function_name
                            )
                        };
                        let payload_str = body
                            .payload
                            .as_ref()
                            .map(|v| v.to_string())
                            .unwrap_or_else(|| "null".to_string());
                        let invocation_type = body
                            .invocation_type
                            .as_deref()
                            .unwrap_or("RequestResponse")
                            .to_string();
                        if invocation_type == "Event" {
                            let arn = function_arn.clone();
                            let payload = payload_str.clone();
                            tokio::spawn(async move {
                                let _ = ld.invoke_lambda(&arn, &payload).await;
                            });
                            return (
                                axum::http::StatusCode::OK,
                                axum::Json(serde_json::json!({
                                    "status_code": 202,
                                    "payload": null,
                                    "executed_version": "$LATEST",
                                    "log_result": null,
                                })),
                            );
                        }
                        match ld.invoke_lambda(&function_arn, &payload_str).await {
                            Ok(bytes) => {
                                let payload_value = serde_json::from_slice::<serde_json::Value>(
                                    &bytes,
                                )
                                .unwrap_or_else(|_| {
                                    serde_json::Value::String(
                                        String::from_utf8_lossy(&bytes).to_string(),
                                    )
                                });
                                (
                                    axum::http::StatusCode::OK,
                                    axum::Json(serde_json::json!({
                                        "status_code": 200,
                                        "payload": payload_value,
                                        "executed_version": "$LATEST",
                                        "log_result": null,
                                    })),
                                )
                            }
                            Err(msg) => (
                                axum::http::StatusCode::OK,
                                axum::Json(serde_json::json!({
                                    "status_code": 502,
                                    "payload": { "errorMessage": msg },
                                    "executed_version": null,
                                    "log_result": null,
                                })),
                            ),
                        }
                    }
                }
            }),
        )
        .route(
            "/_fakecloud/rds/s3-import",
            axum::routing::post({
                let s3 = rds_bridge_s3_state.clone();
                move |headers: axum::http::HeaderMap,
                      axum::Json(body): axum::Json<types::RdsS3ImportRequest>| {
                    let s3 = s3.clone();
                    async move {
                        let account_id = headers
                            .get("x-fakecloud-account-id")
                            .and_then(|v| v.to_str().ok())
                            .unwrap_or("000000000000")
                            .to_string();
                        let bytes = {
                            let mas = s3.read();
                            let state = mas.get(&account_id).unwrap_or_else(|| mas.default_ref());
                            let Some(bucket) = state.buckets.get(&body.bucket) else {
                                return (
                                    axum::http::StatusCode::NOT_FOUND,
                                    axum::Json(serde_json::json!({
                                        "error": "NoSuchBucket",
                                        "bucket": body.bucket,
                                    })),
                                );
                            };
                            let Some(object) = bucket.objects.get(&body.key) else {
                                return (
                                    axum::http::StatusCode::NOT_FOUND,
                                    axum::Json(serde_json::json!({
                                        "error": "NoSuchKey",
                                        "bucket": body.bucket,
                                        "key": body.key,
                                    })),
                                );
                            };
                            match state.read_body(&object.body) {
                                Ok(b) => b,
                                Err(e) => {
                                    return (
                                        axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                                        axum::Json(serde_json::json!({
                                            "error": "ReadBodyFailed",
                                            "message": e.to_string(),
                                        })),
                                    );
                                }
                            }
                        };
                        let len = bytes.len() as i64;
                        let resp = types::RdsS3ImportResponse {
                            bucket: body.bucket,
                            key: body.key,
                            body_b64: base64::Engine::encode(
                                &base64::engine::general_purpose::STANDARD,
                                &bytes,
                            ),
                            bytes_processed: len,
                        };
                        (
                            axum::http::StatusCode::OK,
                            axum::Json(serde_json::to_value(resp).unwrap()),
                        )
                    }
                }
            }),
        )
        .route(
            "/_fakecloud/rds/s3-export",
            axum::routing::post({
                let s3 = rds_bridge_s3_state;
                move |headers: axum::http::HeaderMap,
                      axum::Json(body): axum::Json<types::RdsS3ExportRequest>| {
                    let s3 = s3.clone();
                    async move {
                        let account_id = headers
                            .get("x-fakecloud-account-id")
                            .and_then(|v| v.to_str().ok())
                            .unwrap_or("000000000000")
                            .to_string();
                        let bytes = match base64::Engine::decode(
                            &base64::engine::general_purpose::STANDARD,
                            body.body_b64.as_bytes(),
                        ) {
                            Ok(b) => b,
                            Err(e) => {
                                return (
                                    axum::http::StatusCode::BAD_REQUEST,
                                    axum::Json(serde_json::json!({
                                        "error": "InvalidBase64",
                                        "message": e.to_string(),
                                    })),
                                );
                            }
                        };
                        let bytes_uploaded = bytes.len() as i64;
                        let now = chrono::Utc::now();
                        let etag = {
                            use md5::{Digest, Md5};
                            format!("\"{:x}\"", Md5::digest(&bytes))
                        };
                        let body_bytes = bytes::Bytes::from(bytes);
                        {
                            let mut mas = s3.write();
                            let state = mas.get_or_create(&account_id);
                            let Some(bucket) = state.buckets.get_mut(&body.bucket) else {
                                return (
                                    axum::http::StatusCode::NOT_FOUND,
                                    axum::Json(serde_json::json!({
                                        "error": "NoSuchBucket",
                                        "bucket": body.bucket,
                                    })),
                                );
                            };
                            let object = fakecloud_s3::S3Object {
                                key: body.key.clone(),
                                body: fakecloud_s3::memory_body(body_bytes),
                                content_type: "application/octet-stream".to_string(),
                                etag,
                                size: bytes_uploaded as u64,
                                last_modified: now,
                                storage_class: "STANDARD".to_string(),
                                ..Default::default()
                            };
                            bucket.objects.insert(body.key.clone(), object);
                        }
                        let resp = types::RdsS3ExportResponse {
                            bucket: body.bucket,
                            key: body.key,
                            bytes_uploaded,
                        };
                        (
                            axum::http::StatusCode::OK,
                            axum::Json(serde_json::to_value(resp).unwrap()),
                        )
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
                let ec = elasticache_introspection_state.clone();
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
            "/_fakecloud/elasticache/acls",
            axum::routing::get({
                let ec = elasticache_introspection_state;
                move || {
                    let ec = ec.clone();
                    async move {
                        let accounts = ec.read();
                        let state = accounts.default_ref();
                        axum::Json(elasticache_acls_response(state))
                    }
                }
            }),
        )
        .route(
            "/_fakecloud/athena/named-queries",
            axum::routing::get({
                let athena = athena_introspection_state.clone();
                move || {
                    let athena = athena.clone();
                    async move {
                        let accounts = athena.read();
                        let mut queries: Vec<types::AthenaNamedQuery> = accounts
                            .accounts
                            .values()
                            .flat_map(|acc| acc.named_queries.values())
                            .map(athena_named_query_response)
                            .collect();
                        queries.sort_by(|a, b| a.named_query_id.cmp(&b.named_query_id));
                        axum::Json(types::AthenaNamedQueriesResponse { queries })
                    }
                }
            }),
        )
        .route(
            "/_fakecloud/ecr/repositories",
            axum::routing::get({
                let ec = ecr_introspection_state.clone();
                move || {
                    let ec = ec.clone();
                    async move {
                        let accounts = ec.read();
                        let state = accounts.default_ref();
                        let repositories: Vec<types::EcrRepository> = state
                            .repositories
                            .values()
                            .map(ecr_repository_response)
                            .collect();
                        axum::Json(types::EcrRepositoriesResponse { repositories })
                    }
                }
            }),
        )
        .route(
            "/_fakecloud/ecr/images",
            axum::routing::get({
                let ec = ecr_introspection_state.clone();
                move |axum::extract::Query(q): axum::extract::Query<
                    std::collections::HashMap<String, String>,
                >| {
                    let ec = ec.clone();
                    async move {
                        let accounts = ec.read();
                        let state = accounts.default_ref();
                        let repo_filter = q.get("repo").cloned();
                        let mut images: Vec<types::EcrImage> = Vec::new();
                        for repo in state.repositories.values() {
                            if let Some(ref r) = repo_filter {
                                if &repo.repository_name != r {
                                    continue;
                                }
                            }
                            for image in repo.images.values() {
                                images.push(ecr_image_response(repo, image));
                            }
                        }
                        axum::Json(types::EcrImagesResponse { images })
                    }
                }
            }),
        )
        .route(
            "/_fakecloud/ecr/pull-through-rules",
            axum::routing::get({
                let ec = ecr_introspection_state;
                move || {
                    let ec = ec.clone();
                    async move {
                        let accounts = ec.read();
                        let state = accounts.default_ref();
                        let rules: Vec<types::EcrPullThroughRule> = state
                            .pull_through_cache_rules
                            .values()
                            .map(ecr_pull_through_rule_response)
                            .collect();
                        axum::Json(types::EcrPullThroughRulesResponse { rules })
                    }
                }
            }),
        )
        .route(
            "/_fakecloud/ecs/clusters",
            axum::routing::get({
                let ec = ecs_introspection_state.clone();
                move || {
                    let ec = ec.clone();
                    async move {
                        let accounts = ec.read();
                        let mut clusters: Vec<types::EcsCluster> = Vec::new();
                        for (_, state) in accounts.iter() {
                            clusters.extend(state.clusters.values().map(ecs_cluster_response));
                        }
                        clusters.sort_by(|a, b| a.cluster_arn.cmp(&b.cluster_arn));
                        axum::Json(types::EcsClustersResponse { clusters })
                    }
                }
            }),
        )
        .route(
            "/_fakecloud/ecs/tasks",
            axum::routing::get({
                let ec = ecs_introspection_state.clone();
                move |axum::extract::Query(q): axum::extract::Query<
                    std::collections::HashMap<String, String>,
                >| {
                    let ec = ec.clone();
                    async move {
                        let cluster_filter = q.get("cluster").cloned();
                        let status_filter = q.get("status").cloned();
                        let accounts = ec.read();
                        let mut tasks: Vec<types::EcsTask> = Vec::new();
                        for (_, state) in accounts.iter() {
                            for t in state.tasks.values() {
                                if let Some(ref c) = cluster_filter {
                                    if &t.cluster_name != c && &t.cluster_arn != c {
                                        continue;
                                    }
                                }
                                if let Some(ref s) = status_filter {
                                    if &t.last_status != s {
                                        continue;
                                    }
                                }
                                tasks.push(ecs_task_response(t));
                            }
                        }
                        tasks.sort_by(|a, b| a.task_arn.cmp(&b.task_arn));
                        axum::Json(types::EcsTasksResponse { tasks })
                    }
                }
            }),
        )
        .route(
            "/_fakecloud/ecs/tasks/{task_id}",
            axum::routing::get({
                let ec = ecs_introspection_state.clone();
                move |axum::extract::Path(task_id): axum::extract::Path<String>| {
                    let ec = ec.clone();
                    async move {
                        let accounts = ec.read();
                        for (_, state) in accounts.iter() {
                            if let Some(t) = state.tasks.get(&task_id) {
                                return (
                                    axum::http::StatusCode::OK,
                                    axum::Json(serde_json::to_value(ecs_task_response(t)).unwrap()),
                                );
                            }
                        }
                        (
                            axum::http::StatusCode::NOT_FOUND,
                            axum::Json(serde_json::json!({"error": "task not found"})),
                        )
                    }
                }
            }),
        )
        .route(
            "/_fakecloud/ecs/tasks/{task_id}/logs",
            axum::routing::get({
                let ec = ecs_introspection_state.clone();
                move |axum::extract::Path(task_id): axum::extract::Path<String>| {
                    let ec = ec.clone();
                    async move {
                        let accounts = ec.read();
                        for (_, state) in accounts.iter() {
                            if let Some(t) = state.tasks.get(&task_id) {
                                let resp = types::EcsTaskLogsResponse {
                                    task_arn: t.task_arn.clone(),
                                    logs: t.captured_logs.clone(),
                                    last_status: t.last_status.clone(),
                                    exit_code: t
                                        .containers
                                        .iter()
                                        .find_map(|c| c.exit_code),
                                };
                                return (
                                    axum::http::StatusCode::OK,
                                    axum::Json(serde_json::to_value(resp).unwrap()),
                                );
                            }
                        }
                        (
                            axum::http::StatusCode::NOT_FOUND,
                            axum::Json(serde_json::json!({"error": "task not found"})),
                        )
                    }
                }
            }),
        )
        .route(
            // ECS task-role credential endpoint. Containers started by
            // ECS RunTask with a `taskRoleArn` have
            // `AWS_CONTAINER_CREDENTIALS_FULL_URI` pointing here; AWS
            // SDKs following the default credential-provider chain
            // fetch IMDS-format creds from this path. Returns synthetic
            // short-lived credentials since fakecloud STS accepts any
            // access-key/secret.
            "/_fakecloud/ecs/creds/{task_id}",
            axum::routing::get({
                let ec = ecs_introspection_state.clone();
                move |axum::extract::Path(task_id): axum::extract::Path<String>| {
                    let ec = ec.clone();
                    async move {
                        let accounts = ec.read();
                        for (_, state) in accounts.iter() {
                            if let Some(t) = state.tasks.get(&task_id) {
                                let role_arn = t.task_role_arn.clone().unwrap_or_else(|| {
                                    format!(
                                        "arn:aws:iam::{}:role/ecs-task-role",
                                        state.account_id
                                    )
                                });
                                let expiry = chrono::Utc::now() + chrono::Duration::minutes(15);
                                let body = serde_json::json!({
                                    "AccessKeyId": format!("ASIA{}", "F".repeat(16)),
                                    "SecretAccessKey": "fakecloud-ecs-task-role-secret",
                                    "Token": "fakecloud-ecs-task-role-token",
                                    "Expiration": expiry.format("%Y-%m-%dT%H:%M:%SZ").to_string(),
                                    "RoleArn": role_arn,
                                });
                                return (axum::http::StatusCode::OK, axum::Json(body));
                            }
                        }
                        (
                            axum::http::StatusCode::NOT_FOUND,
                            axum::Json(serde_json::json!({"error": "task not found"})),
                        )
                    }
                }
            }),
        )
        .route(
            "/_fakecloud/ecs/v4/{task_id}",
            axum::routing::get({
                let ec = ecs_introspection_state.clone();
                move |axum::extract::Path(task_id): axum::extract::Path<String>| {
                    let ec = ec.clone();
                    async move {
                        let accounts = ec.read();
                        for (_, state) in accounts.iter() {
                            if let Some(t) = state.tasks.get(&task_id) {
                                let body = serde_json::json!({
                                    "Cluster": t.cluster_name,
                                    "TaskARN": t.task_arn,
                                    "Family": t.family,
                                    "Revision": t.revision,
                                    "DesiredStatus": t.desired_status,
                                    "KnownStatus": t.last_status,
                                    "Limits": {
                                        "CPU": t.cpu.as_ref().and_then(|c| c.parse::<f64>().ok()),
                                        "Memory": t.memory.as_ref().and_then(|m| m.parse::<i64>().ok()),
                                    },
                                    "PullStartedAt": t.pull_started_at.map(|d| d.to_rfc3339()),
                                    "PullStoppedAt": t.pull_stopped_at.map(|d| d.to_rfc3339()),
                                    "CreatedAt": t.created_at.to_rfc3339(),
                                    "StartedAt": t.started_at.map(|d| d.to_rfc3339()),
                                    "StoppedAt": t.stopped_at.map(|d| d.to_rfc3339()),
                                    "AvailabilityZone": "us-east-1a",
                                    "Containers": t.containers.iter().map(|c| serde_json::json!({
                                        "DockerId": c.runtime_id,
                                        "Name": c.name,
                                        "DockerName": c.runtime_id.as_ref().map(|id| format!("ecs-{}", id)),
                                        "Image": c.image,
                                        "ImageID": c.image_digest,
                                        "Ports": c.network_bindings,
                                        "Labels": {},
                                        "DesiredStatus": c.last_status,
                                        "KnownStatus": c.last_status,
                                        "ExitCode": c.exit_code,
                                        "Health": {
                                            "status": c.health_status.as_deref().unwrap_or("UNKNOWN"),
                                        },
                                    })).collect::<Vec<_>>(),
                                });
                                return (axum::http::StatusCode::OK, axum::Json(body));
                            }
                        }
                        (
                            axum::http::StatusCode::NOT_FOUND,
                            axum::Json(serde_json::json!({"error": "task not found"})),
                        )
                    }
                }
            }),
        )
        .route(
            "/_fakecloud/ecs/v3/{task_id}",
            axum::routing::get({
                let ec = ecs_introspection_state.clone();
                move |axum::extract::Path(task_id): axum::extract::Path<String>| {
                    let ec = ec.clone();
                    async move {
                        let accounts = ec.read();
                        for (_, state) in accounts.iter() {
                            if let Some(t) = state.tasks.get(&task_id) {
                                let body = serde_json::json!({
                                    "Cluster": t.cluster_name,
                                    "TaskARN": t.task_arn,
                                    "Family": t.family,
                                    "Revision": t.revision,
                                    "DesiredStatus": t.desired_status,
                                    "KnownStatus": t.last_status,
                                    "Containers": t.containers.iter().map(|c| serde_json::json!({
                                        "DockerId": c.runtime_id,
                                        "Name": c.name,
                                        "DockerName": c.runtime_id.as_ref().map(|id| format!("ecs-{}", id)),
                                        "Image": c.image,
                                        "ImageID": c.image_digest,
                                        "Ports": c.network_bindings,
                                        "Labels": {},
                                        "DesiredStatus": c.last_status,
                                        "KnownStatus": c.last_status,
                                        "ExitCode": c.exit_code,
                                    })).collect::<Vec<_>>(),
                                });
                                return (axum::http::StatusCode::OK, axum::Json(body));
                            }
                        }
                        (
                            axum::http::StatusCode::NOT_FOUND,
                            axum::Json(serde_json::json!({"error": "task not found"})),
                        )
                    }
                }
            }),
        )
        .route(
            "/_fakecloud/ecs/tasks/{task_id}/force-stop",
            axum::routing::post({
                let ec = ecs_introspection_state.clone();
                let rt = ecs_runtime.clone();
                move |axum::extract::Path(task_id): axum::extract::Path<String>| {
                    let ec = ec.clone();
                    let rt = rt.clone();
                    async move {
                        if let Some(runtime) = rt {
                            runtime
                                .stop_task(&task_id, "IntrospectionForceStop")
                                .await;
                        }
                        let accounts = ec.read();
                        for (_, state) in accounts.iter() {
                            if let Some(t) = state.tasks.get(&task_id) {
                                return (
                                    axum::http::StatusCode::OK,
                                    axum::Json(serde_json::to_value(ecs_task_response(t)).unwrap()),
                                );
                            }
                        }
                        (
                            axum::http::StatusCode::NOT_FOUND,
                            axum::Json(serde_json::json!({"error": "task not found"})),
                        )
                    }
                }
            }),
        )
        .route(
            "/_fakecloud/ecs/tasks/{task_id}/mark-failed",
            axum::routing::post({
                let ec = ecs_introspection_state.clone();
                move |axum::extract::Path(task_id): axum::extract::Path<String>,
                      axum::Json(req): axum::Json<types::EcsMarkFailedRequest>| {
                    let ec = ec.clone();
                    async move {
                        let mut accounts = ec.write();
                        for (_, state) in accounts.iter_mut() {
                            if state.tasks.contains_key(&task_id) {
                                let event_detail = serde_json::json!({
                                    "exitCode": req.exit_code.unwrap_or(-1),
                                    "stopCode": "IntrospectionMarkFailed",
                                });
                                let (task_arn, cluster_arn) = {
                                    let t = state.tasks.get_mut(&task_id).unwrap();
                                    t.last_status = "STOPPED".into();
                                    t.desired_status = "STOPPED".into();
                                    t.stopped_at = Some(chrono::Utc::now());
                                    t.stop_code = Some("IntrospectionMarkFailed".into());
                                    t.stopped_reason = req
                                        .reason
                                        .clone()
                                        .or(Some("Forced by introspection".into()));
                                    for c in t.containers.iter_mut() {
                                        c.last_status = "STOPPED".into();
                                        c.exit_code =
                                            Some(req.exit_code.unwrap_or(-1));
                                    }
                                    (t.task_arn.clone(), t.cluster_arn.clone())
                                };
                                state.push_event(fakecloud_ecs::LifecycleEvent {
                                    at: chrono::Utc::now(),
                                    event_type: "TaskStateChange".into(),
                                    task_arn: Some(task_arn),
                                    cluster_arn: Some(cluster_arn),
                                    last_status: Some("STOPPED".into()),
                                    detail: event_detail,
                                });
                                let t = state.tasks.get(&task_id).unwrap();
                                return (
                                    axum::http::StatusCode::OK,
                                    axum::Json(serde_json::to_value(ecs_task_response(t)).unwrap()),
                                );
                            }
                        }
                        (
                            axum::http::StatusCode::NOT_FOUND,
                            axum::Json(serde_json::json!({"error": "task not found"})),
                        )
                    }
                }
            }),
        )
        .route(
            // ECS task-metadata introspection (v4 dump). Looks up a task by
            // its full ARN (URL-encoded) and returns the aggregated shape a
            // container would see at `ECS_CONTAINER_METADATA_URI_V4`. Unlike
            // `/_fakecloud/ecs/v4/{task_id}` (which is what the container
            // itself hits), this is keyed by ARN for assertion-friendly use
            // from tests holding the RunTask response.
            "/_fakecloud/ecs/metadata/{task_arn}",
            axum::routing::get({
                let ec = ecs_introspection_state.clone();
                move |axum::extract::Path(task_arn): axum::extract::Path<String>| {
                    let ec = ec.clone();
                    async move {
                        let accounts = ec.read();
                        for (_, state) in accounts.iter() {
                            for t in state.tasks.values() {
                                if t.task_arn == task_arn {
                                    return (
                                        axum::http::StatusCode::OK,
                                        axum::Json(
                                            serde_json::to_value(ecs_task_metadata_response(t))
                                                .unwrap(),
                                        ),
                                    );
                                }
                            }
                        }
                        (
                            axum::http::StatusCode::NOT_FOUND,
                            axum::Json(serde_json::json!({"error": "task not found"})),
                        )
                    }
                }
            }),
        )
        .route(
            "/_fakecloud/ecs/events",
            axum::routing::get({
                let ec = ecs_introspection_state.clone();
                move || {
                    let ec = ec.clone();
                    async move {
                        let accounts = ec.read();
                        let mut events: Vec<types::EcsLifecycleEvent> = Vec::new();
                        for (_, state) in accounts.iter() {
                            events.extend(state.events.iter().map(ecs_lifecycle_event));
                        }
                        events.sort_by(|a, b| a.at.cmp(&b.at));
                        axum::Json(types::EcsEventsResponse { events })
                    }
                }
            }),
        )
        .route(
            "/_fakecloud/cloudfront/distributions",
            axum::routing::get({
                let st = cloudfront_introspection_state.clone();
                move || {
                    let st = st.clone();
                    async move {
                        let accounts = st.read();
                        let mut distributions: Vec<types::CloudFrontDistribution> = accounts
                            .all_distributions()
                            .map(|(_, d)| cloudfront_distribution_response(d))
                            .collect();
                        distributions.sort_by(|a, b| a.id.cmp(&b.id));
                        axum::Json(types::CloudFrontDistributionsResponse { distributions })
                    }
                }
            }),
        )
        .route(
            "/_fakecloud/elbv2/load-balancers",
            axum::routing::get({
                let st = elbv2_introspection_state.clone();
                move || {
                    let st = st.clone();
                    async move {
                        let accounts = st.read();
                        let mut load_balancers: Vec<types::Elbv2LoadBalancer> = Vec::new();
                        for (_, s) in accounts.iter() {
                            load_balancers.extend(
                                s.load_balancers.values().map(elbv2_load_balancer_response),
                            );
                        }
                        load_balancers.sort_by(|a, b| a.arn.cmp(&b.arn));
                        axum::Json(types::Elbv2LoadBalancersResponse { load_balancers })
                    }
                }
            }),
        )
        .route(
            "/_fakecloud/elbv2/target-groups",
            axum::routing::get({
                let st = elbv2_introspection_state.clone();
                move || {
                    let st = st.clone();
                    async move {
                        let accounts = st.read();
                        let mut target_groups: Vec<types::Elbv2TargetGroup> = Vec::new();
                        for (_, s) in accounts.iter() {
                            target_groups.extend(
                                s.target_groups.values().map(elbv2_target_group_response),
                            );
                        }
                        target_groups.sort_by(|a, b| a.arn.cmp(&b.arn));
                        axum::Json(types::Elbv2TargetGroupsResponse { target_groups })
                    }
                }
            }),
        )
        .route(
            "/_fakecloud/elbv2/listeners",
            axum::routing::get({
                let st = elbv2_introspection_state.clone();
                move || {
                    let st = st.clone();
                    async move {
                        let accounts = st.read();
                        let mut listeners: Vec<types::Elbv2Listener> = Vec::new();
                        for (_, s) in accounts.iter() {
                            listeners
                                .extend(s.listeners.values().map(elbv2_listener_response));
                        }
                        listeners.sort_by(|a, b| a.arn.cmp(&b.arn));
                        axum::Json(types::Elbv2ListenersResponse { listeners })
                    }
                }
            }),
        )
        .route(
            "/_fakecloud/elbv2/rules",
            axum::routing::get({
                let st = elbv2_introspection_state.clone();
                move || {
                    let st = st.clone();
                    async move {
                        let accounts = st.read();
                        let mut rules: Vec<types::Elbv2Rule> = Vec::new();
                        for (_, s) in accounts.iter() {
                            rules.extend(s.rules.values().map(elbv2_rule_response));
                        }
                        rules.sort_by(|a, b| a.arn.cmp(&b.arn));
                        axum::Json(types::Elbv2RulesResponse { rules })
                    }
                }
            }),
        )
        .route(
            "/_fakecloud/elbv2/access-logs/flush",
            axum::routing::post({
                let svc = elbv2_service_for_admin.clone();
                move || {
                    let svc = svc.clone();
                    async move {
                        let flushed = svc.flush_access_logs();
                        axum::Json(serde_json::json!({ "flushed": flushed }))
                    }
                }
            }),
        )
        .route(
            "/_fakecloud/elbv2/waf-counts",
            axum::routing::get({
                let svc = elbv2_service_for_admin.clone();
                move || {
                    let svc = svc.clone();
                    async move {
                        let counts = svc.waf_count_metrics_snapshot();
                        axum::Json(serde_json::json!({ "counts": counts }))
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
            "/_fakecloud/stepfunctions/sync-executions",
            axum::routing::get({
                let ss = stepfunctions_state.clone();
                move || {
                    let ss = ss.clone();
                    async move {
                        let accounts = ss.read();
                        let state = accounts.default_ref();
                        let mut executions: Vec<types::StepFunctionsSyncExecution> = state
                            .executions
                            .values()
                            .filter(|exec| exec.is_sync)
                            .map(|exec| {
                                let duration_ms = exec.billed_duration_ms.unwrap_or_else(|| {
                                    exec.stop_date.map_or(0, |stop| {
                                        (stop - exec.start_date).num_milliseconds().max(0)
                                    })
                                });
                                types::StepFunctionsSyncExecution {
                                    execution_arn: exec.execution_arn.clone(),
                                    state_machine_arn: exec.state_machine_arn.clone(),
                                    name: exec.name.clone(),
                                    status: exec.status.as_str().to_string(),
                                    input: exec.input.clone(),
                                    output: exec.output.clone(),
                                    started_at: exec.start_date.to_rfc3339(),
                                    stopped_at: exec.stop_date.map(|d| d.to_rfc3339()),
                                    duration_ms,
                                    billing_details: types::StepFunctionsSyncBillingDetails {
                                        billed_duration_in_milliseconds: duration_ms,
                                        billed_memory_used_in_mb: exec
                                            .billed_memory_mb
                                            .unwrap_or(64),
                                    },
                                }
                            })
                            .collect();
                        executions.sort_by(|a, b| b.started_at.cmp(&a.started_at));
                        axum::Json(types::StepFunctionsSyncExecutionsResponse { executions })
                    }
                }
            }),
        )
        .route(
            "/_fakecloud/stepfunctions/execution-tree/{arn}",
            axum::routing::get({
                let ss = stepfunctions_state.clone();
                move |axum::extract::Path(arn): axum::extract::Path<String>| {
                    let ss = ss.clone();
                    async move {
                        let accounts = ss.read();
                        let state = accounts.default_ref();
                        // Index children by parent arn for O(N) tree build.
                        let mut children_by_parent: std::collections::HashMap<
                            String,
                            Vec<&fakecloud_stepfunctions::Execution>,
                        > = std::collections::HashMap::new();
                        for exec in state.executions.values() {
                            if let Some(parent) = exec.parent_execution_arn.as_ref() {
                                children_by_parent
                                    .entry(parent.clone())
                                    .or_default()
                                    .push(exec);
                            }
                        }
                        fn build_node(
                            exec: &fakecloud_stepfunctions::Execution,
                            children_by_parent: &std::collections::HashMap<
                                String,
                                Vec<&fakecloud_stepfunctions::Execution>,
                            >,
                        ) -> types::StepFunctionsExecutionTreeNode {
                            let kids = children_by_parent
                                .get(&exec.execution_arn)
                                .map(|v| {
                                    let mut sorted = v.clone();
                                    sorted.sort_by_key(|a| a.start_date);
                                    sorted
                                        .into_iter()
                                        .map(|c| build_node(c, children_by_parent))
                                        .collect()
                                })
                                .unwrap_or_default();
                            types::StepFunctionsExecutionTreeNode {
                                arn: exec.execution_arn.clone(),
                                state_machine_arn: exec.state_machine_arn.clone(),
                                status: exec.status.as_str().to_string(),
                                started_at: exec.start_date.to_rfc3339(),
                                stopped_at: exec.stop_date.map(|d| d.to_rfc3339()),
                                children: kids,
                            }
                        }
                        match state.executions.get(&arn) {
                            Some(root) => (
                                axum::http::StatusCode::OK,
                                axum::Json(serde_json::to_value(
                                    types::StepFunctionsExecutionTreeResponse {
                                        root_arn: root.execution_arn.clone(),
                                        tree: build_node(root, &children_by_parent),
                                    },
                                )
                                .unwrap_or_else(|_| serde_json::json!({}))),
                            ),
                            None => (
                                axum::http::StatusCode::NOT_FOUND,
                                axum::Json(serde_json::json!({
                                    "error": "ExecutionDoesNotExist",
                                    "message": format!("Execution Does Not Exist: '{}'", arn),
                                })),
                            ),
                        }
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
            "/_fakecloud/apigatewayv2/connections",
            axum::routing::get({
                let reg = apigatewayv2_ws_registry.clone();
                move || {
                    let reg = reg.clone();
                    async move {
                        let r = reg.read();
                        let conns: Vec<_> = r
                            .connections
                            .values()
                            .map(|c| {
                                serde_json::json!({
                                    "connectionId": c.connection_id,
                                    "apiId": c.api_id,
                                    "stage": c.stage,
                                    "connectedAt": c.connected_at.to_rfc3339(),
                                    "lastActiveAt": c.last_active_at.to_rfc3339(),
                                    "sourceIp": c.source_ip,
                                })
                            })
                            .collect();
                        axum::Json(serde_json::json!({ "connections": conns }))
                    }
                }
            }),
        )
        .route(
            "/_fakecloud/apigatewayv2/ws/{api_id}",
            axum::routing::get({
                let reg = apigatewayv2_ws_registry.clone();
                let apigw_state = apigatewayv2_state.clone();
                let lambda_delivery = lambda_delivery.clone();
                let account_id = cli.account_id.clone();
                let region = cli.region.clone();
                move |ws: axum::extract::WebSocketUpgrade,
                      axum::extract::Path(api_id): axum::extract::Path<String>,
                      axum::extract::Query(params): axum::extract::Query<
                    std::collections::HashMap<String, String>,
                >,
                      headers: axum::http::HeaderMap,
                      axum::extract::ConnectInfo(addr): axum::extract::ConnectInfo<
                    std::net::SocketAddr,
                >| {
                    let reg = reg.clone();
                    let apigw_state = apigw_state.clone();
                    let lambda_delivery = lambda_delivery.clone();
                    let account_id = account_id.clone();
                    let region = region.clone();
                    async move {
                        let stage = params
                            .get("stage")
                            .cloned()
                            .unwrap_or_else(|| "$default".to_string());
                        let user_agent = headers
                            .get(axum::http::header::USER_AGENT)
                            .and_then(|v| v.to_str().ok())
                            .map(|s| s.to_string());
                        let source_ip = addr.ip().to_string();
                        let mut header_map = std::collections::HashMap::new();
                        for (k, v) in &headers {
                            if let Ok(s) = v.to_str() {
                                header_map.insert(k.to_string(), s.to_string());
                            }
                        }
                        let query_map: std::collections::HashMap<String, String> = params
                            .into_iter()
                            .filter(|(k, _)| k != "stage")
                            .collect();
                        ws.on_upgrade(move |socket| async move {
                            let (conn_id, rx) = fakecloud_apigatewayv2::websocket::register(
                                reg.clone(),
                                api_id.clone(),
                                stage.clone(),
                                source_ip.clone(),
                                user_agent.clone(),
                            );
                            let conn_id_for_lifecycle = conn_id.clone();
                            let connected_at = chrono::Utc::now();
                            // Dispatch $connect route
                            fakecloud_apigatewayv2::websocket_dispatch::dispatch_websocket_event(
                                &apigw_state,
                                lambda_delivery.as_ref(),
                                &account_id,
                                &region,
                                &api_id,
                                &stage,
                                &conn_id,
                                "$connect",
                                "CONNECT",
                                None,
                                &source_ip,
                                user_agent.as_deref(),
                                connected_at,
                                Some(&header_map),
                                Some(&query_map),
                            )
                            .await;
                            let on_disconnect = {
                                let apigw_state = apigw_state.clone();
                                let lambda_delivery = lambda_delivery.clone();
                                let account_id = account_id.clone();
                                let region = region.clone();
                                let api_id = api_id.clone();
                                let stage = stage.clone();
                                let conn_id = conn_id.clone();
                                let source_ip = source_ip.clone();
                                let user_agent = user_agent.clone();
                                // connected_at is Copy (DateTime<Utc>) — no rebind needed
                                move || async move {
                                    fakecloud_apigatewayv2::websocket_dispatch::dispatch_websocket_event(
                                        &apigw_state,
                                        lambda_delivery.as_ref(),
                                        &account_id,
                                        &region,
                                        &api_id,
                                        &stage,
                                        &conn_id,
                                        "$disconnect",
                                        "DISCONNECT",
                                        None,
                                        &source_ip,
                                        user_agent.as_deref(),
                                        connected_at,
                                        None,
                                        None,
                                    )
                                    .await;
                                }
                            };
                            let on_message = {
                                let apigw_state = apigw_state.clone();
                                let lambda_delivery = lambda_delivery.clone();
                                let account_id = account_id.clone();
                                let region = region.clone();
                                let api_id = api_id.clone();
                                let stage = stage.clone();
                                let conn_id = conn_id.clone();
                                let source_ip = source_ip.clone();
                                let user_agent = user_agent.clone();
                                // connected_at is Copy (DateTime<Utc>) — no rebind needed
                                move |bytes: Vec<u8>, is_text: bool| {
                                    let apigw_state = apigw_state.clone();
                                    let lambda_delivery = lambda_delivery.clone();
                                    let account_id = account_id.clone();
                                    let region = region.clone();
                                    let api_id = api_id.clone();
                                    let stage = stage.clone();
                                    let conn_id = conn_id.clone();
                                    let source_ip = source_ip.clone();
                                    let user_agent = user_agent.clone();
                                    // connected_at is Copy (DateTime<Utc>) — no rebind needed
                                    async move {
                                        let body_str = if is_text {
                                            String::from_utf8_lossy(&bytes).into_owned()
                                        } else {
                                            use base64::prelude::*;
                                            BASE64_STANDARD.encode(&bytes)
                                        };
                                        let is_base64 = !is_text;
                                        // Resolve route key via RouteSelectionExpression
                                        let route_key = {
                                            let accounts = apigw_state.read();
                                            let empty = fakecloud_apigatewayv2::ApiGatewayV2State::new(
                                                &account_id, &region,
                                            );
                                            let state = accounts.get(&account_id).unwrap_or(&empty);
                                            let expression = state
                                                .apis
                                                .get(&api_id)
                                                .map(|a| a.route_selection_expression.as_str())
                                                .unwrap_or("$request.body.action");
                                            if is_base64 {
                                                "$default".to_string()
                                            } else {
                                                fakecloud_apigatewayv2::websocket_dispatch::resolve_route_key(
                                                    expression, &body_str,
                                                )
                                            }
                                        };
                                        fakecloud_apigatewayv2::websocket_dispatch::dispatch_websocket_event(
                                            &apigw_state,
                                            lambda_delivery.as_ref(),
                                            &account_id,
                                            &region,
                                            &api_id,
                                            &stage,
                                            &conn_id,
                                            &route_key,
                                            "MESSAGE",
                                            Some(&body_str),
                                            &source_ip,
                                            user_agent.as_deref(),
                                            connected_at,
                                            None,
                                            None,
                                        )
                                        .await;
                                    }
                                }
                            };
                            fakecloud_apigatewayv2::websocket::run_lifecycle_tracked_with_disconnect(
                                socket,
                                rx,
                                on_message,
                                reg.clone(),
                                conn_id_for_lifecycle,
                                on_disconnect,
                            )
                            .await;
                            fakecloud_apigatewayv2::websocket::deregister(&reg, &conn_id,
                            );
                        })
                    }
                }
            }),
        )
        .merge(fakecloud_apigatewayv2::management::router_with_stage_prefix(
            apigatewayv2_ws_registry.clone(),
        ))
        .route(
            // Direct injection of an activity task (skipping a state-machine
            // execution). Used by tests that want to exercise the worker
            // pool API surface without spinning up an ASL workflow.
            "/_fakecloud/stepfunctions/enqueue-activity-task",
            axum::routing::post({
                let ss = stepfunctions_state.clone();
                move |axum::Json(req): axum::Json<types::SfnEnqueueActivityTaskRequest>| {
                    let ss = ss.clone();
                    async move {
                        let activity_arn = req.activity_arn;
                        let token = format!(
                            "FCToken-injected-{}-{}",
                            chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0),
                            uuid::Uuid::new_v4().simple(),
                        );
                        let mut accounts = ss.write();
                        // Default-account namespace keeps the introspection
                        // endpoint simple. Multi-account callers can switch
                        // FAKECLOUD's default account before calling, or
                        // create the activity in the default account.
                        let state = accounts.default_mut();
                        if !state.activities.contains_key(&activity_arn) {
                            return (
                                axum::http::StatusCode::NOT_FOUND,
                                axum::Json(serde_json::json!({
                                    "error": "ActivityDoesNotExist"
                                })),
                            );
                        }
                        state.task_tokens.insert(
                            token.clone(),
                            fakecloud_stepfunctions::TaskTokenState {
                                activity_arn: activity_arn.clone(),
                                status: "PENDING".to_string(),
                                output: None,
                                error: None,
                                cause: None,
                                input: Some(req.input.unwrap_or_else(|| "{}".to_string())),
                                created_at: chrono::Utc::now(),
                                last_heartbeat_at: None,
                                heartbeat_seconds: req.heartbeat_seconds,
                                timeout_seconds: req.timeout_seconds,
                            },
                        );
                        (
                            axum::http::StatusCode::OK,
                            axum::Json(serde_json::json!({ "taskToken": token })),
                        )
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
            "/_fakecloud/lambda/layer-content/{account_id}/{layer_name}/{file}",
            axum::routing::get({
                let ls = lambda_layer_content_state;
                move |axum::extract::Path((account_id, layer_name, file)): axum::extract::Path<(String, String, String)>| {
                    let ls = ls.clone();
                    async move {
                        let version: Option<i64> = file
                            .strip_suffix(".zip")
                            .and_then(|v| v.parse().ok());
                        let Some(version) = version else {
                            return (
                                axum::http::StatusCode::NOT_FOUND,
                                [(axum::http::header::CONTENT_TYPE, "text/plain")],
                                axum::body::Bytes::from_static(b"layer version not found"),
                            );
                        };
                        let bytes_opt: Option<Vec<u8>> = {
                            let accounts = ls.read();
                            accounts
                                .get(&account_id)
                                .and_then(|s| s.layers.get(&layer_name))
                                .and_then(|l| l.versions.iter().find(|v| v.version == version))
                                .and_then(|v| v.code_zip.clone())
                        };
                        match bytes_opt {
                            Some(bytes) => (
                                axum::http::StatusCode::OK,
                                [(axum::http::header::CONTENT_TYPE, "application/zip")],
                                axum::body::Bytes::from(bytes),
                            ),
                            None => (
                                axum::http::StatusCode::NOT_FOUND,
                                [(axum::http::header::CONTENT_TYPE, "text/plain")],
                                axum::body::Bytes::from_static(b"layer version not found"),
                            ),
                        }
                    }
                }
            }),
        )
        .route(
            "/_fakecloud/lambda/function-code/{account_id}/{function_name}/{file}",
            axum::routing::get({
                let ls = lambda_state.clone();
                move |axum::extract::Path((account_id, function_name, file)): axum::extract::Path<(String, String, String)>| {
                    let ls = ls.clone();
                    async move {
                        let qualifier: Option<String> = file
                            .strip_suffix(".zip")
                            .map(|s| s.to_string());
                        let Some(qualifier) = qualifier else {
                            return (
                                axum::http::StatusCode::NOT_FOUND,
                                [(axum::http::header::CONTENT_TYPE, "text/plain")],
                                axum::body::Bytes::from_static(b"function code not found"),
                            );
                        };
                        let bytes_opt: Option<Vec<u8>> = {
                            let accounts = ls.read();
                            accounts.get(&account_id).and_then(|s| {
                                if qualifier == "latest" {
                                    s.functions
                                        .get(&function_name)
                                        .and_then(|f| f.code_zip.clone())
                                } else {
                                    s.function_version_snapshots
                                        .get(&function_name)
                                        .and_then(|m| m.get(&qualifier))
                                        .and_then(|f| f.code_zip.clone())
                                }
                            })
                        };
                        match bytes_opt {
                            Some(bytes) => (
                                axum::http::StatusCode::OK,
                                [(axum::http::header::CONTENT_TYPE, "application/zip")],
                                axum::body::Bytes::from(bytes),
                            ),
                            None => (
                                axum::http::StatusCode::NOT_FOUND,
                                [(axum::http::header::CONTENT_TYPE, "text/plain")],
                                axum::body::Bytes::from_static(b"function code not found"),
                            ),
                        }
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
                        parsed.push(fakecloud_bedrock::ResponseRule {
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
                        .push(fakecloud_bedrock::FaultRule {
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
        // Bedrock Agent (control plane) introspection: list agents with
        // their aliases, versions, knowledge-base attachments, and
        // collaborators flattened into one shape. Pure read-only and
        // bypasses IAM since it's an admin/test endpoint.
        .route(
            "/_fakecloud/bedrock-agent/agents",
            axum::routing::get({
                let bas = bedrock_agent_state.clone();
                move || async move {
                    let accounts = bas.read();
                    let mut out: Vec<serde_json::Value> = Vec::new();
                    for state in accounts.accounts.values() {
                        for (agent_id, agent) in state.agents.iter() {
                            let aliases: Vec<serde_json::Value> = state
                                .agent_aliases
                                .values()
                                .filter(|a| a.agent_id == *agent_id)
                                .map(|a| serde_json::json!({
                                    "aliasId": a.alias_id,
                                    "aliasName": a.alias_name,
                                    "agentVersion": a.agent_version,
                                    "aliasArn": a.alias_arn,
                                    "status": a.agent_alias_status,
                                    "createdAt": a.created_at.to_rfc3339(),
                                    "updatedAt": a.updated_at.to_rfc3339(),
                                }))
                                .collect();
                            let versions: Vec<serde_json::Value> = state
                                .agent_versions
                                .get(agent_id)
                                .map(|vs| vs.iter().map(|v| serde_json::json!({
                                    "agentVersion": v.agent_version,
                                    "createdAt": v.created_at.to_rfc3339(),
                                    "instruction": v.instruction,
                                    "foundationModel": v.foundation_model,
                                })).collect())
                                .unwrap_or_default();
                            let kbs: Vec<serde_json::Value> = state
                                .agent_knowledge_bases
                                .get(agent_id)
                                .map(|ks| ks.iter().map(|k| serde_json::json!({
                                    "knowledgeBaseId": k.knowledge_base_id,
                                    "state": k.knowledge_base_state,
                                    "description": k.description,
                                })).collect())
                                .unwrap_or_default();
                            let collaborators: Vec<serde_json::Value> = state
                                .agent_collaborators
                                .get(agent_id)
                                .map(|cs| cs.iter().map(|c| serde_json::json!({
                                    "collaboratorId": c.collaborator_id,
                                    "collaboratorName": c.collaborator_name,
                                    "agentDescriptor": c.agent_descriptor,
                                    "collaborationInstruction": c.collaboration_instruction,
                                    "relayConversationHistory": c.relay_conversation_history,
                                })).collect())
                                .unwrap_or_default();
                            let action_groups: Vec<serde_json::Value> = state
                                .agent_action_groups
                                .values()
                                .filter(|ag| ag.agent_id == *agent_id)
                                .map(|ag| serde_json::json!({
                                    "actionGroupId": ag.action_group_id,
                                    "actionGroupName": ag.action_group_name,
                                    "actionGroupState": ag.action_group_state,
                                    "description": ag.description,
                                }))
                                .collect();
                            out.push(serde_json::json!({
                                "agentId": agent.agent_id,
                                "agentName": agent.agent_name,
                                "agentArn": agent.agent_arn,
                                "agentStatus": agent.agent_status,
                                "foundationModel": agent.foundation_model,
                                "instruction": agent.instruction,
                                "knowledgeBases": kbs,
                                "actionGroups": action_groups,
                                "collaborators": collaborators,
                                "aliases": aliases,
                                "versions": versions,
                                "promptOverrides": agent.prompt_override_configuration,
                                "createdAt": agent.created_at.to_rfc3339(),
                                "updatedAt": agent.updated_at.to_rfc3339(),
                            }));
                        }
                    }
                    axum::Json(serde_json::json!({ "agents": out }))
                }
            }),
        )
        // Bedrock Agent Runtime (data plane) introspection: log of
        // InvokeAgent / InvokeInlineAgent / InvokeFlow / Retrieve /
        // RetrieveAndGenerate (and bookkeeping CreateInvocation) calls.
        .route(
            "/_fakecloud/bedrock-agent-runtime/invocations",
            axum::routing::get({
                let bars = bedrock_agent_runtime_state.clone();
                move || async move {
                    let accounts = bars.read();
                    let mut out: Vec<serde_json::Value> = Vec::new();
                    for state in accounts.accounts.values() {
                        for inv in state.invocations.iter() {
                            out.push(serde_json::json!({
                                "invocationId": inv.invocation_id,
                                "op": inv.op,
                                "agentId": inv.agent_id,
                                "flowId": inv.flow_id,
                                "sessionId": inv.session_id,
                                "input": inv.input,
                                "output": inv.output,
                                "outputChunks": inv.output_chunks,
                                "trace": inv.trace,
                                "citations": inv.citations,
                                "invokedAt": inv.timestamp.to_rfc3339(),
                                "durationMs": inv.duration_ms,
                            }));
                        }
                    }
                    axum::Json(serde_json::json!({ "invocations": out }))
                }
            }),
        )
        .route(
            "/_fakecloud/iam/create-admin",
            axum::routing::post({
                let iam = iam_state.clone();
                let orgs = organizations_state.clone();
                let persist = organizations_persist_hook.clone();
                move |axum::Json(body): axum::Json<types::CreateAdminRequest>| {
                    let iam = iam.clone();
                    let orgs = orgs.clone();
                    let persist = persist.clone();
                    async move {
                        let resp = reset::create_admin_in_account(
                            &iam,
                            &orgs,
                            &body.account_id,
                            &body.user_name,
                        );
                        // The helper may auto-enroll the account into the org;
                        // persist that mutation through to disk.
                        if let Some(hook) = &persist {
                            hook().await;
                        }
                        axum::Json(resp)
                    }
                }
            }),
        )
        // Organizations introspection: list every member account with
        // lifecycle state, parent OU, tags, and SCPs directly attached
        // to the account. IAM-bypass admin route — tests assert on org
        // shape without needing management-account credentials.
        .route(
            "/_fakecloud/organizations/accounts",
            axum::routing::get({
                let orgs = organizations_state.clone();
                move || {
                    let orgs = orgs.clone();
                    async move { axum::Json(organizations_accounts_snapshot(&orgs)) }
                }
            }),
        )
        // Organizations introspection: list every billing-responsibility
        // transfer in the org with direction, lifecycle status, and the
        // active handshake. IAM-bypass admin route.
        .route(
            "/_fakecloud/organizations/responsibility-transfers",
            axum::routing::get({
                let orgs = organizations_state.clone();
                move || {
                    let orgs = orgs.clone();
                    async move {
                        let rows =
                            fakecloud_organizations::introspection::list_all_responsibility_transfers(
                                &orgs,
                            );
                        let responsibility_transfers = rows
                            .into_iter()
                            .map(|r| types::OrganizationsResponsibilityTransfer {
                                id: r.id,
                                arn: r.arn,
                                name: r.name,
                                transfer_type: r.transfer_type,
                                status: r.status,
                                direction: r.direction,
                                source_management_account_id: r.source_management_account_id,
                                source_management_account_email: r.source_management_account_email,
                                target_management_account_id: r.target_management_account_id,
                                target_management_account_email: r.target_management_account_email,
                                start_timestamp: r.start_timestamp.to_rfc3339(),
                                end_timestamp: r.end_timestamp.map(|t| t.to_rfc3339()),
                                active_handshake_id: r.active_handshake_id,
                            })
                            .collect();
                        axum::Json(types::OrganizationsResponsibilityTransfersResponse {
                            responsibility_transfers,
                        })
                    }
                }
            }),
        )
        // WAFv2 evaluator admin endpoint. Phase W1 ships only the
        // evaluator; the dataplane integrations (ALB, API Gateway,
        // CloudFront) land in W2. This endpoint lets tests call into the
        // evaluator directly without spinning up a real dataplane: pass a
        // synthetic request and a `WebACL` ARN, get back the WafVerdict.
        .route(
            "/_fakecloud/wafv2/evaluate",
            axum::routing::post({
                let waf_state = wafv2_state.clone();
                let limiter = wafv2_rate_limiter.clone();
                let default_account = cli.account_id.clone();
                move |axum::Json(body): axum::Json<serde_json::Value>| {
                    let waf_state = waf_state.clone();
                    let limiter = limiter.clone();
                    let default_account = default_account.clone();
                    async move {
                        wafv2_evaluate_admin(&waf_state, &limiter, &default_account, &body)
                    }
                }
            }),
        )
        .route(
            "/_fakecloud/route53/health-checks/{id}/status",
            axum::routing::post({
                let svc = route53_service.clone();
                move |axum::extract::Path(id): axum::extract::Path<String>,
                      axum::Json(body): axum::Json<types::Route53HealthCheckStatusRequest>| {
                    let svc = svc.clone();
                    async move {
                        let status = match body.status {
                            types::Route53HealthCheckStatusValue::Success => {
                                fakecloud_route53::HealthCheckStatus::Success
                            }
                            types::Route53HealthCheckStatusValue::Failure => {
                                fakecloud_route53::HealthCheckStatus::Failure
                            }
                            types::Route53HealthCheckStatusValue::Timeout => {
                                fakecloud_route53::HealthCheckStatus::Timeout
                            }
                            types::Route53HealthCheckStatusValue::DnsError => {
                                fakecloud_route53::HealthCheckStatus::DnsError
                            }
                            types::Route53HealthCheckStatusValue::InsufficientDataPoints => {
                                fakecloud_route53::HealthCheckStatus::InsufficientDataPoints
                            }
                            types::Route53HealthCheckStatusValue::Unknown => {
                                fakecloud_route53::HealthCheckStatus::Unknown
                            }
                        };
                        if svc
                            .set_health_check_status_persistent(&id, status, body.reason)
                            .await
                        {
                            axum::http::StatusCode::NO_CONTENT
                        } else {
                            axum::http::StatusCode::NOT_FOUND
                        }
                    }
                }
            }),
        )
        .route(
            "/_fakecloud/route53/zones/{id}/dnssec",
            axum::routing::get({
                let svc = route53_service.clone();
                move |axum::extract::Path(id): axum::extract::Path<String>| {
                    let svc = svc.clone();
                    async move {
                        match svc.dnssec_material_for_zone(&id) {
                            Some((ksk, dnskey_public_key, key_tag, ds_hex)) => {
                                let body = types::Route53DnssecMaterialResponse {
                                    hosted_zone_id: ksk.hosted_zone_id,
                                    key_signing_key_name: ksk.name,
                                    algorithm: fakecloud_route53::dnssec::DNSSEC_ALGORITHM,
                                    flags: fakecloud_route53::dnssec::DNSKEY_FLAGS_KSK,
                                    key_tag,
                                    dnskey_public_key_b64: fakecloud_route53::dnssec::b64(
                                        &dnskey_public_key,
                                    ),
                                    ds_digest_sha256_hex: ds_hex,
                                };
                                (axum::http::StatusCode::OK, axum::Json(body)).into_response()
                            }
                            None => (
                                axum::http::StatusCode::NOT_FOUND,
                                "no ACTIVE KSK for zone",
                            )
                                .into_response(),
                        }
                    }
                }
            }),
        )
        .route(
            "/_fakecloud/route53/zones/{id}/dnssec/sign",
            axum::routing::post({
                let svc = route53_service.clone();
                move |axum::extract::Path(id): axum::extract::Path<String>,
                      axum::Json(body): axum::Json<types::Route53DnssecSignRequest>| {
                    let svc = svc.clone();
                    async move {
                        match svc.sign_rrset_with_zone_ksk(
                            &id,
                            &body.name,
                            &body.record_type,
                            body.ttl,
                            &body.rdatas,
                        ) {
                            Some(sig) => {
                                let resp = types::Route53DnssecSignResponse {
                                    signature_b64: sig.signature_b64,
                                    algorithm: sig.algorithm,
                                    key_tag: sig.key_tag,
                                    signer_name: sig.signer_name,
                                    inception: sig.inception,
                                    expiration: sig.expiration,
                                    labels: sig.labels,
                                    original_ttl: sig.original_ttl,
                                    rrset_type: sig.rrset_type,
                                };
                                (axum::http::StatusCode::OK, axum::Json(resp)).into_response()
                            }
                            None => (
                                axum::http::StatusCode::NOT_FOUND,
                                "no ACTIVE KSK or unknown record type",
                            )
                                .into_response(),
                        }
                    }
                }
            }),
        )
        .route(
            "/_fakecloud/acm/certificates/{arn_or_id}/status",
            axum::routing::post({
                let svc = acm_service.clone();
                move |axum::extract::Path(arn_or_id): axum::extract::Path<String>,
                      axum::Json(body): axum::Json<types::AcmCertificateStatusRequest>| {
                    let svc = svc.clone();
                    async move {
                        if svc
                            .set_certificate_status_persistent(&arn_or_id, &body.status, body.reason)
                            .await
                        {
                            axum::http::StatusCode::NO_CONTENT
                        } else {
                            axum::http::StatusCode::NOT_FOUND
                        }
                    }
                }
            }),
        )
        .route(
            "/_fakecloud/acm/certificates/{arn_or_id}/chain-info",
            axum::routing::get({
                let svc = acm_service.clone();
                move |axum::extract::Path(arn_or_id): axum::extract::Path<String>| {
                    let svc = svc.clone();
                    async move {
                        match svc.chain_info(&arn_or_id) {
                            Some(v) => (axum::http::StatusCode::OK, axum::Json(v)).into_response(),
                            None => axum::http::StatusCode::NOT_FOUND.into_response(),
                        }
                    }
                }
            }),
        )
        .route(
            "/_fakecloud/apigatewayv2/domain-names/{name}/mtls-info",
            axum::routing::get({
                let svc = apigatewayv2_service.clone();
                move |axum::extract::Path(name): axum::extract::Path<String>| {
                    let svc = svc.clone();
                    async move {
                        match svc.mtls_info(&name) {
                            Some(v) => (axum::http::StatusCode::OK, axum::Json(v)).into_response(),
                            None => axum::http::StatusCode::NOT_FOUND.into_response(),
                        }
                    }
                }
            }),
        )
        .route(
            "/_fakecloud/cognito/pretokengen/invocations",
            axum::routing::get({
                let cs = cognito_state.clone();
                move || {
                    let cs = cs.clone();
                    async move {
                        let accounts = cs.read();
                        let mut out: Vec<types::PreTokenGenInvocation> = Vec::new();
                        for (_account_id, state) in accounts.iter() {
                            for inv in &state.pre_token_gen_invocations {
                                out.push(types::PreTokenGenInvocation {
                                    pool_id: inv.pool_id.clone(),
                                    user_pool_arn: inv.user_pool_arn.clone(),
                                    username: inv.username.clone(),
                                    trigger_source: inv.trigger_source.clone(),
                                    lambda_arn: inv.lambda_arn.clone(),
                                    request_payload: inv.request_payload.clone(),
                                    response_payload: inv.response_payload.clone(),
                                    claims_added: inv.claims_added.clone(),
                                    claims_overridden: inv.claims_overridden.clone(),
                                    group_overrides: inv.group_overrides.clone(),
                                    invoked_at: inv.invoked_at.to_rfc3339(),
                                    duration_ms: inv.duration_ms,
                                });
                            }
                        }
                        axum::Json(types::PreTokenGenInvocationsResponse {
                            invocations: out,
                        })
                    }
                }
            }),
        )
        .route(
            "/_fakecloud/cognito/webauthn-credentials",
            axum::routing::get({
                let cs = cognito_state.clone();
                move || {
                    let cs = cs.clone();
                    async move {
                        let accounts = cs.read();
                        let mut out = Vec::new();
                        for (account_id, state) in accounts.iter() {
                            for (pool_user_key, creds) in &state.webauthn_credentials {
                                for c in creds {
                                    out.push(serde_json::json!({
                                        "account_id": account_id,
                                        "pool_user": pool_user_key,
                                        "credential_id": c.credential_id,
                                        "relying_party_id": c.relying_party_id,
                                        "attestation_info": c.attestation_info,
                                    }));
                                }
                            }
                        }
                        axum::Json(serde_json::json!({ "credentials": out }))
                    }
                }
            }),
        )
        .route(
            "/_fakecloud/acm/certificates/{arn_or_id}/approve",
            axum::routing::post({
                let svc = acm_service.clone();
                move |axum::extract::Path(arn_or_id): axum::extract::Path<String>| {
                    let svc = svc.clone();
                    async move {
                        if svc.approve_certificate_persistent(&arn_or_id).await {
                            axum::http::StatusCode::NO_CONTENT
                        } else {
                            axum::http::StatusCode::NOT_FOUND
                        }
                    }
                }
            }),
        )
        .route(
            "/_fakecloud/cloudfront/distributions/{id}/status",
            axum::routing::post({
                let svc = cloudfront_service.clone();
                move |axum::extract::Path(id): axum::extract::Path<String>,
                      axum::Json(body): axum::Json<
                    types::CloudFrontDistributionStatusRequest,
                >| {
                    let svc = svc.clone();
                    async move {
                        if svc
                            .set_distribution_status_persistent(&id, &body.status)
                            .await
                        {
                            axum::http::StatusCode::NO_CONTENT
                        } else {
                            axum::http::StatusCode::NOT_FOUND
                        }
                    }
                }
            }),
        )
        .merge({
            // K8s Lambda backend needs in-cluster Pod init containers to
            // pull function code + layers over HTTP. The routes are
            // mounted unconditionally (gated by bearer-token check, never
            // exposed without auth) so the K8s backend can boot at any
            // time after server start.
            if lambda_backend_is_k8s {
                admin_lambda_artifacts::router(
                    admin_lambda_artifacts::ArtifactRoutesContext {
                        lambda_state: lambda_state.clone(),
                        bearer_token: k8s_internal_token.clone(),
                    },
                )
            } else {
                axum::Router::new()
            }
        })
        .merge({
            // Internal RDB endpoint for the ElastiCache Kubernetes backend:
            // restoring Redis Pods fetch their snapshot here. Present only
            // when the k8s backend is active (Docker stages RDBs via the
            // daemon, not HTTP). Bearer-token guarded like the Lambda routes.
            match elasticache_runtime
                .as_ref()
                .and_then(|rt| rt.pending_rdb())
            {
                Some(pending_rdb) => admin_elasticache_artifacts::router(
                    admin_elasticache_artifacts::RdbRoutesContext {
                        pending_rdb,
                        bearer_token: k8s_internal_token.clone(),
                    },
                ),
                None => axum::Router::new(),
            }
        })
        .fallback(dispatch::dispatch)
        .layer({
            let registry_arc = Arc::new(registry);
            // Now that every service has been registered, give the
            // Step Functions interpreter a handle to the finalised
            // registry so `arn:aws:states:::aws-sdk:*` Tasks can
            // dispatch back into other services. `set` returns Err
            // if already populated (only possible on hot reload),
            // which we silently ignore.
            let _ = sfn_registry_handle.set(registry_arc.clone());
            let _ = apigw_v1_registry_handle.set(registry_arc.clone());
            Extension(registry_arc)
        })
        .layer(Extension(Arc::new(config)))
        .layer(TraceLayer::new_for_http())
        // Outermost: CloudFront viewer routing. Requests whose `Host` matches an
        // enabled distribution are served by the data plane; everything else
        // (the AWS API, `/_fakecloud/*`, health) falls straight through.
        .layer(axum::middleware::from_fn_with_state(
            cloudfront_dataplane,
            cloudfront_viewer_middleware,
        ));
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
    if let Some(rt) = mq_runtime {
        rt.stop_all().await;
    }
    if let Some(rt) = kafka_runtime {
        rt.stop_all().await;
    }
    if let Some(rt) = ec2_runtime {
        rt.stop_all().await;
    }
}
