use std::sync::Arc;

use fakecloud_aws::arn::Arn;
use fakecloud_sdk::types;

// Make pub so main.rs can construct it
#[derive(Clone)]
pub(crate) struct ResetState {
    pub iam: fakecloud_iam::SharedIamState,
    pub sqs: fakecloud_sqs::SharedSqsState,
    pub sns: fakecloud_sns::SharedSnsState,
    pub eb: fakecloud_eventbridge::SharedEventBridgeState,
    pub ssm: fakecloud_ssm::SharedSsmState,
    pub dynamodb: fakecloud_dynamodb::SharedDynamoDbState,
    pub lambda: fakecloud_lambda::SharedLambdaState,
    pub secretsmanager: fakecloud_secretsmanager::SharedSecretsManagerState,
    pub s3: fakecloud_s3::SharedS3State,
    pub logs: fakecloud_logs::SharedLogsState,
    pub kms: fakecloud_kms::SharedKmsState,
    pub cloudformation: fakecloud_cloudformation::SharedCloudFormationState,
    pub ses: fakecloud_ses::SharedSesState,
    pub cognito: fakecloud_cognito::SharedCognitoState,
    pub kinesis: fakecloud_kinesis::SharedKinesisState,
    pub rds: fakecloud_rds::SharedRdsState,
    pub elasticache: fakecloud_elasticache::SharedElastiCacheState,
    pub ecr: fakecloud_ecr::SharedEcrState,
    pub ecs: fakecloud_ecs::SharedEcsState,
    pub stepfunctions: fakecloud_stepfunctions::SharedStepFunctionsState,
    pub scheduler: fakecloud_scheduler::SharedSchedulerState,
    pub apigatewayv1: fakecloud_apigateway::SharedApiGatewayState,
    pub apigatewayv2: fakecloud_apigatewayv2::SharedApiGatewayV2State,
    pub bedrock: fakecloud_bedrock::SharedBedrockState,
    pub bedrock_agent: fakecloud_bedrock_agent::SharedBedrockAgentState,
    pub bedrock_agent_runtime: fakecloud_bedrock_agent_runtime::SharedBedrockAgentRuntimeState,
    pub cloudfront: fakecloud_cloudfront::SharedCloudFrontState,
    pub route53: fakecloud_route53::SharedRoute53State,
    pub acm: fakecloud_acm::SharedAcmState,
    pub acmpca: fakecloud_acmpca::SharedAcmPcaState,
    pub config: fakecloud_config::SharedConfigState,
    pub route53resolver: fakecloud_route53resolver::SharedRoute53ResolverState,
    pub firehose: fakecloud_firehose::SharedFirehoseState,
    pub glue: fakecloud_glue::SharedGlueState,
    pub cloudwatch: fakecloud_cloudwatch::SharedCloudWatchState,
    pub application_autoscaling:
        fakecloud_application_autoscaling::SharedApplicationAutoScalingState,
    pub wafv2: fakecloud_wafv2::SharedWafv2State,
    pub athena: fakecloud_athena::SharedAthenaState,
    pub organizations: fakecloud_organizations::SharedOrganizationsState,
    pub container_runtime: Option<Arc<fakecloud_lambda::runtime::ContainerRuntime>>,
    pub rds_runtime: Option<Arc<fakecloud_rds::runtime::RdsRuntime>>,
    pub elasticache_runtime: Option<Arc<fakecloud_elasticache::runtime::ElastiCacheRuntime>>,
    pub ecs_runtime: Option<Arc<fakecloud_ecs::runtime::EcsRuntime>>,
    pub ec2: fakecloud_ec2::SharedEc2State,
    pub ec2_runtime: Option<Arc<fakecloud_ec2::runtime::Ec2Runtime>>,
}

// A reset snapshots the reset rows' incarnation ids and volumes and clears
// the state under one write lock, then tears down by those ids. Runtime
// records are keyed by incarnation, so a resource created after the reset
// (even under a reset one's identifier) is never reached, and a start still in
// flight for a reset incarnation reaps itself once it finds its row gone.

/// `(DbiResourceId, data volume)` of every RDS instance in an account.
fn rds_incarnations(state: &fakecloud_rds::RdsState) -> Vec<(String, String)> {
    let tag = fakecloud_core::data_volume::current_scope().tag();
    state
        .instances
        .values()
        .map(|inst| {
            (
                inst.dbi_resource_id.clone(),
                inst.data_volume_name(tag, &state.account_id),
            )
        })
        .collect()
}

/// `(account, instance id)` of every EC2 instance in an account, whose
/// containers and data volumes a reset removes (stopped ones included).
fn ec2_instances(state: &fakecloud_ec2::Ec2State) -> Vec<(String, String)> {
    state
        .instances
        .keys()
        .map(|id| (state.account_id.clone(), id.clone()))
        .collect()
}

/// `(incarnation, data volume)` of every cache cluster, replication group and
/// serverless cache in an account (no volume for memcached).
fn elasticache_incarnations(
    state: &fakecloud_elasticache::ElastiCacheState,
) -> Vec<(String, Option<String>)> {
    let tag = fakecloud_core::data_volume::current_scope().tag();
    let account = &state.account_id;
    let clusters = state.cache_clusters.values().map(|c| {
        (
            c.incarnation(),
            (c.engine != "memcached").then(|| c.data_volume_name(tag, account)),
        )
    });
    let groups = state.replication_groups.values().map(|g| {
        (
            g.incarnation(),
            (g.engine != "memcached").then(|| g.data_volume_name(tag, account)),
        )
    });
    let serverless = state
        .serverless_caches
        .values()
        .map(|c| (c.incarnation(), Some(c.data_volume_name(tag, account))));
    clusters.chain(groups).chain(serverless).collect()
}

/// How long a reset response waits for its container teardown.
const TEARDOWN_WAIT: std::time::Duration = std::time::Duration::from_secs(120);

/// Container and data-volume teardown a reset queued. The reset handlers
/// await it before replying, so once a reset returns, a resource recreated
/// under a reset one's identifier can't race the teardown and mount the old
/// data volume (or have its new container stopped).
#[derive(Default)]
pub(crate) struct Teardown(Vec<std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>>);

impl Teardown {
    fn push(&mut self, f: impl std::future::Future<Output = ()> + Send + 'static) {
        self.0.push(Box::pin(f));
    }

    /// Run the teardown on its own task (a client that hangs up mid-reset
    /// can't cancel it half way, leaving containers untracked and volumes
    /// behind) and wait for it, bounded so a wedged daemon can't hang the
    /// reset response; past the bound it keeps running in the background.
    pub(crate) async fn run(self) {
        // Each service's teardown runs on its own task, concurrently, so a
        // slow one can't eat the others' share of the wait.
        let tasks: Vec<_> = self.0.into_iter().map(tokio::spawn).collect();
        let deadline = tokio::time::Instant::now() + TEARDOWN_WAIT;
        for task in tasks {
            match tokio::time::timeout_at(deadline, task).await {
                Ok(Ok(())) => {}
                Ok(Err(err)) => {
                    tracing::error!(%err, "reset teardown failed; containers or volumes may be left behind");
                }
                Err(_) => {
                    tracing::warn!(
                        "reset teardown still running after {}s; continuing in the background",
                        TEARDOWN_WAIT.as_secs()
                    );
                }
            }
        }
    }
}

impl ResetState {
    /// Reset RDS in every account, stopping the backing containers and
    /// dropping the instances' data volumes: the instances are gone for good,
    /// so one recreated under the same identifier must start clean (the
    /// volumes would otherwise outlive the state, #2630).
    fn reset_rds(&self, teardown: &mut Teardown) {
        let gone: Vec<(String, String)> = {
            let mut mas = self.rds.write();
            let gone = mas.iter().flat_map(|(_, s)| rds_incarnations(s)).collect();
            mas.reset();
            gone
        };
        if let Some(rt) = self.rds_runtime.clone() {
            teardown.push(async move {
                for (incarnation, volume) in gone {
                    rt.stop(&incarnation).await;
                    rt.remove_data_volume_named(&volume).await;
                }
            });
        }
    }

    /// Reset ElastiCache in every account, stopping the backing containers
    /// and dropping the resources' data volumes (see [`Self::reset_rds`]).
    fn reset_elasticache(&self, teardown: &mut Teardown) {
        let gone: Vec<(String, Option<String>)> = {
            let mut mas = self.elasticache.write();
            let gone = mas
                .iter()
                .flat_map(|(_, s)| elasticache_incarnations(s))
                .collect();
            mas.reset();
            gone
        };
        if let Some(rt) = self.elasticache_runtime.clone() {
            teardown.push(async move {
                for (incarnation, volume) in gone {
                    rt.stop(&incarnation).await;
                    if let Some(volume) = volume {
                        rt.remove_data_volume_named(&volume).await;
                    }
                }
            });
        }
    }

    /// Reset EC2 in every account, tearing down every instance's container
    /// and data volume by instance id (see [`Self::reset_rds`]).
    fn reset_ec2(&self, teardown: &mut Teardown) {
        let gone: Vec<(String, String)> = {
            let mut mas = self.ec2.write();
            let gone = mas.iter().flat_map(|(_, s)| ec2_instances(s)).collect();
            mas.reset();
            gone
        };
        if let Some(rt) = self.ec2_runtime.clone() {
            teardown.push(async move { rt.remove_instances(gone).await });
        }
    }

    pub(crate) fn reset_service(&self, service: &str) -> Result<Teardown, String> {
        let mut teardown = Teardown::default();
        match service {
            "iam" | "sts" => {
                // The reset drops the execution-role sessions warm Lambda
                // instances hold: stop handing them invocations in the same
                // step, then tear the free ones down in the background.
                if let Some(ref rt) = self.container_runtime {
                    rt.mark_credentials_revoked(None);
                }
                self.iam.write().reset();
                if let Some(ref rt) = self.container_runtime {
                    let rt = rt.clone();
                    tokio::spawn(async move { rt.retire_released().await });
                }
            }
            "sqs" => {
                self.sqs.write().reset();
            }
            "sns" => {
                let mut s = self.sns.write();
                s.reset();
                s.default_mut().seed_default_opted_out();
            }
            "events" | "eventbridge" => {
                let mut eb_accounts = self.eb.write();
                let eb = eb_accounts.default_mut();
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
            "rds" => self.reset_rds(&mut teardown),
            "elasticache" => self.reset_elasticache(&mut teardown),
            "ec2" => self.reset_ec2(&mut teardown),
            "ecr" => {
                self.ecr.write().reset();
            }
            "ecs" => {
                self.ecs.write().reset();
                if let Some(ref rt) = self.ecs_runtime {
                    let rt = rt.clone();
                    tokio::spawn(async move { rt.stop_all().await });
                }
            }
            "states" | "stepfunctions" => {
                self.stepfunctions.write().reset();
            }
            "scheduler" => {
                self.scheduler.write().reset();
            }
            "apigateway" => {
                // Both v1 (REST) and v2 (HTTP) share the SigV4 service
                // identifier `apigateway`; resetting the service clears
                // both crates' state.
                self.apigatewayv1.write().reset();
                self.apigatewayv2.write().reset();
            }
            "apigatewayv1" | "apigatewayrest" => {
                self.apigatewayv1.write().reset();
            }
            "apigatewayv2" => {
                self.apigatewayv2.write().reset();
            }
            "bedrock" | "bedrock-runtime" => {
                self.bedrock.write().reset();
            }
            "bedrock-agent" => {
                self.bedrock_agent.write().reset();
            }
            "bedrock-agent-runtime" => {
                self.bedrock_agent_runtime.write().reset();
            }
            "cloudfront" => {
                *self.cloudfront.write() = fakecloud_cloudfront::CloudFrontAccounts::new();
            }
            "route53" => {
                *self.route53.write() = fakecloud_route53::Route53Accounts::new();
            }
            "acm" => {
                *self.acm.write() = fakecloud_acm::AcmAccounts::new();
            }
            "acm-pca" | "acmpca" => {
                *self.acmpca.write() = fakecloud_acmpca::AcmPcaAccounts::new();
            }
            "config" => {
                *self.config.write() = fakecloud_config::ConfigAccounts::new();
            }
            "route53resolver" => {
                *self.route53resolver.write() =
                    fakecloud_route53resolver::Route53ResolverAccounts::new();
            }
            "firehose" => {
                *self.firehose.write() = fakecloud_firehose::FirehoseAccounts::new();
            }
            "glue" => {
                *self.glue.write() = fakecloud_glue::GlueAccounts::new();
            }
            "monitoring" | "cloudwatch" => {
                *self.cloudwatch.write() = fakecloud_cloudwatch::CloudWatchAccounts::new();
            }
            "application-autoscaling" => {
                *self.application_autoscaling.write() =
                    fakecloud_application_autoscaling::ApplicationAutoScalingAccounts::new();
            }
            "wafv2" => {
                *self.wafv2.write() = fakecloud_wafv2::Wafv2Accounts::new();
            }
            "athena" => {
                *self.athena.write() = fakecloud_athena::AthenaAccounts::new();
            }
            "organizations" => {
                self.organizations.write().clear();
            }
            _ => {
                return Err(format!("Unknown service: {service}"));
            }
        }
        tracing::info!(service = %service, "service state reset via per-service reset API");
        Ok(teardown)
    }

    /// Reset a single service's state for a specific account only.
    pub(crate) fn reset_service_for_account(
        &self,
        service: &str,
        account_id: &str,
    ) -> Result<Teardown, String> {
        let mut teardown = Teardown::default();
        match service {
            "iam" | "sts" => {
                if let Some(ref rt) = self.container_runtime {
                    rt.mark_credentials_revoked(Some(account_id));
                }
                {
                    let mut mas = self.iam.write();
                    let region = mas.region().to_string();
                    if let Some(state) = mas.get_mut(account_id) {
                        state.reset(&region);
                    }
                }
                if let Some(ref rt) = self.container_runtime {
                    let rt = rt.clone();
                    tokio::spawn(async move { rt.retire_released().await });
                }
            }
            "sqs" => {
                // Every region of the account.
                let mut mas = self.sqs.write();
                if let Some(state) = mas.get_mut(account_id) {
                    state.clear();
                }
            }
            "sns" => {
                let mut mas = self.sns.write();
                if let Some(state) = mas.get_mut(account_id) {
                    state.reset();
                    state.seed_default_opted_out();
                }
            }
            "events" | "eventbridge" => {
                let mut mas = self.eb.write();
                if let Some(eb) = mas.get_mut(account_id) {
                    eb.reset();
                }
            }
            "ssm" => {
                let mut mas = self.ssm.write();
                if let Some(state) = mas.get_mut(account_id) {
                    state.reset();
                }
            }
            "dynamodb" => {
                let mut mas = self.dynamodb.write();
                if let Some(state) = mas.get_mut(account_id) {
                    state.reset();
                }
            }
            "lambda" => {
                let mut mas = self.lambda.write();
                if let Some(state) = mas.get_mut(account_id) {
                    state.reset();
                }
            }
            "secretsmanager" => {
                let mut mas = self.secretsmanager.write();
                if let Some(state) = mas.get_mut(account_id) {
                    state.reset();
                }
            }
            "s3" => {
                let mut mas = self.s3.write();
                if let Some(state) = mas.get_mut(account_id) {
                    state.reset();
                }
            }
            "logs" => {
                let mut mas = self.logs.write();
                if let Some(state) = mas.get_mut(account_id) {
                    state.reset();
                }
            }
            "kms" => {
                let mut mas = self.kms.write();
                if let Some(state) = mas.get_mut(account_id) {
                    state.reset();
                }
            }
            "cloudformation" => {
                let mut mas = self.cloudformation.write();
                if let Some(state) = mas.get_mut(account_id) {
                    state.reset();
                }
            }
            "ses" => {
                let mut mas = self.ses.write();
                if let Some(state) = mas.get_mut(account_id) {
                    state.reset();
                }
            }
            "cognito" => {
                let mut mas = self.cognito.write();
                if let Some(state) = mas.get_mut(account_id) {
                    state.reset();
                }
            }
            "kinesis" => {
                // Every region of the account.
                let mut mas = self.kinesis.write();
                if let Some(state) = mas.get_mut(account_id) {
                    state.clear();
                }
            }
            "rds" => {
                let mut mas = self.rds.write();
                if let Some(state) = mas.get_mut(account_id) {
                    let gone = rds_incarnations(state);
                    state.reset();
                    // The account's instances are gone: stop their containers
                    // and drop their data volumes by incarnation, so an
                    // instance recreated under the same identifier (or another
                    // account's same-named one) is never reached.
                    if let Some(rt) = self.rds_runtime.clone() {
                        teardown.push(async move {
                            for (incarnation, volume) in gone {
                                rt.stop(&incarnation).await;
                                rt.remove_data_volume_named(&volume).await;
                            }
                        });
                    }
                }
            }
            "elasticache" => {
                let mut mas = self.elasticache.write();
                if let Some(state) = mas.get_mut(account_id) {
                    let gone = elasticache_incarnations(state);
                    state.reset();
                    if let Some(rt) = self.elasticache_runtime.clone() {
                        teardown.push(async move {
                            for (incarnation, volume) in gone {
                                rt.stop(&incarnation).await;
                                if let Some(volume) = volume {
                                    rt.remove_data_volume_named(&volume).await;
                                }
                            }
                        });
                    }
                }
            }
            "ecr" => {
                let mut mas = self.ecr.write();
                if let Some(state) = mas.get_mut(account_id) {
                    state.reset();
                }
            }
            "ecs" => {
                let mut mas = self.ecs.write();
                if let Some(state) = mas.get_mut(account_id) {
                    state.reset();
                }
            }
            "states" | "stepfunctions" => {
                let mut mas = self.stepfunctions.write();
                if let Some(state) = mas.get_mut(account_id) {
                    state.reset();
                }
            }
            "scheduler" => {
                let mut mas = self.scheduler.write();
                if let Some(state) = mas.get_mut(account_id) {
                    state.reset();
                }
            }
            "apigateway" => {
                let mut v1 = self.apigatewayv1.write();
                if let Some(state) = v1.get_mut(account_id) {
                    state.reset();
                }
                let mut v2 = self.apigatewayv2.write();
                if let Some(state) = v2.get_mut(account_id) {
                    state.reset();
                }
            }
            "apigatewayv1" | "apigatewayrest" => {
                let mut mas = self.apigatewayv1.write();
                if let Some(state) = mas.get_mut(account_id) {
                    state.reset();
                }
            }
            "apigatewayv2" => {
                let mut mas = self.apigatewayv2.write();
                if let Some(state) = mas.get_mut(account_id) {
                    state.reset();
                }
            }
            "bedrock" | "bedrock-runtime" => {
                let mut mas = self.bedrock.write();
                if let Some(state) = mas.get_mut(account_id) {
                    state.reset();
                }
            }
            "bedrock-agent" => {
                let mut state = self.bedrock_agent.write();
                state.accounts.remove(account_id);
            }
            "bedrock-agent-runtime" => {
                let mut state = self.bedrock_agent_runtime.write();
                state.accounts.remove(account_id);
            }
            "cloudfront" => {
                // CloudFront is global (no region) but its resources are
                // owned by the creating account; drop only that account's.
                let mut state = self.cloudfront.write();
                state.accounts.remove(account_id);
            }
            "route53" => {
                // Route 53 is global (no region) but its resources are owned
                // by the creating account; drop only that account's.
                let mut state = self.route53.write();
                state.accounts.remove(account_id);
            }
            "acm" => {
                let mut state = self.acm.write();
                state.accounts.remove(account_id);
            }
            "acm-pca" | "acmpca" => {
                let mut state = self.acmpca.write();
                state.accounts.remove(account_id);
            }
            "config" => {
                let mut state = self.config.write();
                state.accounts.remove(account_id);
            }
            "route53resolver" => {
                let mut state = self.route53resolver.write();
                state.accounts.remove(account_id);
            }
            "firehose" => {
                let mut state = self.firehose.write();
                state.accounts.remove(account_id);
            }
            "glue" => {
                let mut state = self.glue.write();
                state.accounts.remove(account_id);
            }
            "monitoring" | "cloudwatch" => {
                let mut state = self.cloudwatch.write();
                state.accounts.remove(account_id);
            }
            "application-autoscaling" => {
                let mut state = self.application_autoscaling.write();
                state.accounts.remove(account_id);
            }
            "wafv2" => {
                let mut state = self.wafv2.write();
                state.accounts.remove(account_id);
            }
            "athena" => {
                let mut state = self.athena.write();
                state.accounts.remove(account_id);
            }
            _ => {
                return Err(format!("Unknown service: {service}"));
            }
        }
        tracing::info!(service = %service, account_id = %account_id, "service state reset for account via per-account reset API");
        Ok(teardown)
    }

    pub(crate) fn reset(&self) -> (axum::Json<types::ResetResponse>, Teardown) {
        let mut teardown = Teardown::default();
        self.iam.write().reset();
        self.sqs.write().reset();
        {
            let mut sns = self.sns.write();
            sns.reset();
            sns.default_mut().seed_default_opted_out();
        }
        {
            let mut eb_accounts = self.eb.write();
            let eb = eb_accounts.default_mut();
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
        self.lambda.write().default_mut().reset();
        // Stop all Lambda containers on reset
        if let Some(ref rt) = self.container_runtime {
            let rt = rt.clone();
            tokio::spawn(async move { rt.stop_all().await });
        }
        self.secretsmanager.write().reset();
        self.s3.write().reset();
        self.logs.write().default_mut().reset();
        self.kms.write().reset();
        self.cloudformation.write().reset();
        self.ses.write().reset();
        self.cognito.write().reset();
        self.kinesis.write().reset();
        self.reset_rds(&mut teardown);
        self.reset_elasticache(&mut teardown);
        self.reset_ec2(&mut teardown);
        self.ecr.write().reset();
        self.ecs.write().reset();
        if let Some(ref rt) = self.ecs_runtime {
            let rt = rt.clone();
            tokio::spawn(async move { rt.stop_all().await });
        }
        self.stepfunctions.write().reset();
        self.scheduler.write().reset();
        self.apigatewayv1.write().reset();
        self.apigatewayv2.write().reset();
        self.bedrock.write().reset();
        self.bedrock_agent.write().reset();
        self.bedrock_agent_runtime.write().reset();
        *self.cloudfront.write() = fakecloud_cloudfront::CloudFrontAccounts::new();
        *self.route53.write() = fakecloud_route53::Route53Accounts::new();
        *self.acm.write() = fakecloud_acm::AcmAccounts::new();
        *self.acmpca.write() = fakecloud_acmpca::AcmPcaAccounts::new();
        *self.config.write() = fakecloud_config::ConfigAccounts::new();
        *self.route53resolver.write() = fakecloud_route53resolver::Route53ResolverAccounts::new();
        *self.firehose.write() = fakecloud_firehose::FirehoseAccounts::new();
        *self.glue.write() = fakecloud_glue::GlueAccounts::new();
        *self.cloudwatch.write() = fakecloud_cloudwatch::CloudWatchAccounts::new();
        *self.application_autoscaling.write() =
            fakecloud_application_autoscaling::ApplicationAutoScalingAccounts::new();
        *self.wafv2.write() = fakecloud_wafv2::Wafv2Accounts::new();
        *self.athena.write() = fakecloud_athena::AthenaAccounts::new();
        // Organizations is a cross-account registry (not MultiAccountState);
        // a full reset drops every organization so subsequent runs start
        // with none, matching the no-in-use default state.
        self.organizations.write().clear();
        tracing::info!("state reset via reset API");
        (
            axum::Json(types::ResetResponse {
                status: "ok".to_string(),
            }),
            teardown,
        )
    }
}

/// Bootstrap an IAM admin user in a specific account. Creates the user,
/// access key, and an inline admin policy (`Allow */*`) in the target
/// account's IAM state. Returns the credentials so the caller can sign
/// requests as that user.
///
/// This solves the multi-account bootstrap problem: the `test*` root
/// bypass only targets the default account, so there's no way to create
/// credentials for a non-default account via the normal AWS API.
///
/// The account is standalone unless `organization_id` names an existing
/// organization, in which case it is enrolled into that organization's
/// root OU. That mirrors AWS: a freshly vended account belongs to no
/// organization until it is invited and accepts, or is created through
/// `CreateAccount`. Bootstrapping an admin must never silently pull the
/// account into an unrelated organization — that account then inherits
/// SCPs it never agreed to, can read the organization's metadata, and
/// becomes a stack-set auto-deployment target.
pub(crate) fn create_admin_in_account(
    iam: &fakecloud_iam::SharedIamState,
    organizations: &fakecloud_organizations::SharedOrganizationsState,
    account_id: &str,
    user_name: &str,
    organization_id: Option<&str>,
) -> Result<types::CreateAdminResponse, CreateAdminError> {
    if let Some(org_id) = organization_id {
        let mut guard = organizations.write();
        if !guard.contains_org(org_id) {
            return Err(CreateAdminError::UnknownOrganization(org_id.to_string()));
        }
        // An account belongs to at most one organization. Without this the
        // shortcut would enroll it into a second registry entry, and which
        // organization's SCP ceiling, DescribeOrganization view and
        // stack-set targeting applied would come down to org-id sort order.
        // `CreateOrganization` and `InviteAccountToOrganization` both reject
        // this; so does the shortcut.
        if let Some(other) = guard.claimed_by_other_org(account_id, org_id) {
            return Err(CreateAdminError::AccountInAnotherOrganization {
                account_id: account_id.to_string(),
                organization_id: other,
            });
        }
        let org = guard
            .org_by_id_mut(org_id)
            .expect("checked just above that the organization exists");
        org.enroll_account_if_missing(account_id);
    }

    let mut accounts = iam.write();
    let region = accounts.region().to_string();
    let state = accounts.get_or_create(account_id);

    let user_id = format!(
        "AIDA{}",
        &uuid::Uuid::new_v4()
            .to_string()
            .replace('-', "")
            .to_uppercase()[..16]
    );
    let arn = Arn::global_in(&region, "iam", account_id, &format!("user/{user_name}")).to_string();
    let akid = format!(
        "FKIA{}",
        &uuid::Uuid::new_v4()
            .to_string()
            .replace('-', "")
            .to_uppercase()[..20]
    );
    let secret = uuid::Uuid::new_v4().to_string();

    state.users.insert(
        user_name.to_string(),
        fakecloud_iam::IamUser {
            user_name: user_name.to_string(),
            user_id,
            arn: arn.clone(),
            path: "/".to_string(),
            created_at: chrono::Utc::now(),
            tags: Vec::new(),
            permissions_boundary: None,
        },
    );
    state.access_keys.insert(
        user_name.to_string(),
        vec![fakecloud_iam::IamAccessKey {
            access_key_id: akid.clone(),
            secret_access_key: secret.clone(),
            user_name: user_name.to_string(),
            status: "Active".to_string(),
            created_at: chrono::Utc::now(),
        }],
    );
    state.user_inline_policies.insert(
        user_name.to_string(),
        std::collections::BTreeMap::from([(
            "fakecloud-admin".to_string(),
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}"#.to_string(),
        )]),
    );

    Ok(types::CreateAdminResponse {
        access_key_id: akid,
        secret_access_key: secret,
        account_id: account_id.to_string(),
        arn,
    })
}

/// Why a `/_fakecloud/iam/create-admin` call could not be satisfied.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CreateAdminError {
    /// `organizationId` was supplied but no organization with that id
    /// exists. Enrolling into "whatever org happens to exist" is what
    /// the caller is explicitly avoiding by naming one, so this is an
    /// error rather than a silent fallback.
    UnknownOrganization(String),
    /// The account is already a member of a different organization, and
    /// an account can only ever be in one.
    AccountInAnotherOrganization {
        account_id: String,
        organization_id: String,
    },
}

impl CreateAdminError {
    pub(crate) fn message(&self) -> String {
        match self {
            Self::UnknownOrganization(id) => {
                format!("no organization with id {id} exists")
            }
            Self::AccountInAnotherOrganization {
                account_id,
                organization_id,
            } => format!(
                "account {account_id} is already a member of organization {organization_id}"
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use chrono::Utc;
    use fakecloud_rds::{DbInstance, RdsState};

    use super::ResetState;

    #[test]
    fn reset_service_clears_rds_state() {
        let mut rds_mas: fakecloud_core::multi_account::MultiAccountState<RdsState> =
            fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", "");
        let rds = rds_mas.default_mut();
        let created_at = Utc::now();
        rds.instances.insert(
            "db-1".to_string(),
            DbInstance {
                associated_roles: Vec::new(),
                db_instance_identifier: "db-1".to_string(),
                db_instance_arn: "arn:aws:rds:us-east-1:123456789012:db:db-1".to_string(),
                db_instance_class: "db.t3.micro".to_string(),
                engine: "postgres".to_string(),
                engine_version: "16.3".to_string(),
                db_instance_status: "available".to_string(),
                master_username: "admin".to_string(),
                db_name: Some("postgres".to_string()),
                db_subnet_group_name: None,
                endpoint_address: "127.0.0.1".to_string(),
                port: 5432,
                allocated_storage: 20,
                publicly_accessible: true,
                deletion_protection: false,
                created_at,
                dbi_resource_id: "db-test".to_string(),
                master_user_password: "secret123".to_string(),
                container_id: "container-id".to_string(),
                host_port: 15432,
                data_volume: None,
                tags: Vec::new(),
                read_replica_source_db_instance_identifier: None,
                read_replica_db_instance_identifiers: Vec::new(),
                vpc_security_group_ids: Vec::new(),
                db_parameter_group_name: None,
                backup_retention_period: 1,
                preferred_backup_window: "03:00-04:00".to_string(),
                preferred_maintenance_window: None,
                latest_restorable_time: Some(created_at),
                option_group_name: None,
                multi_az: false,
                pending_modified_values: None,
                availability_zone: None,
                storage_type: None,
                storage_encrypted: false,
                kms_key_id: None,
                iam_database_authentication_enabled: false,
                iops: None,
                monitoring_interval: None,
                monitoring_role_arn: None,
                performance_insights_enabled: false,
                performance_insights_kms_key_id: None,
                performance_insights_retention_period: None,
                enabled_cloudwatch_logs_exports: Vec::new(),
                ca_certificate_identifier: None,
                network_type: None,
                character_set_name: None,
                auto_minor_version_upgrade: None,
                copy_tags_to_snapshot: None,
                master_user_secret_arn: None,
                master_user_secret_kms_key_id: None,
                license_model: None,
                max_allocated_storage: None,
                multi_tenant: None,
                storage_throughput: None,
                tde_credential_arn: None,
                delete_automated_backups: None,
                db_security_groups: Vec::new(),
                domain: None,
                domain_fqdn: None,
                domain_ou: None,
                domain_iam_role_name: None,
                domain_auth_secret_arn: None,
                domain_dns_ips: Vec::new(),
                db_cluster_identifier: None,
                activity_stream: None,
            },
        );

        let state = ResetState {
            iam: Arc::new(parking_lot::RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new(
                    "123456789012",
                    "us-east-1",
                    "http://localhost:4566",
                ),
            )),
            sqs: Arc::new(parking_lot::RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new(
                    "123456789012",
                    "us-east-1",
                    "http://localhost:4566",
                ),
            )),
            sns: Arc::new(parking_lot::RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new(
                    "123456789012",
                    "us-east-1",
                    "http://localhost:4566",
                ),
            )),
            eb: Arc::new(parking_lot::RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new(
                    "123456789012",
                    "us-east-1",
                    "",
                ),
            )),
            ssm: Arc::new(parking_lot::RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new(
                    "123456789012",
                    "us-east-1",
                    "http://localhost:4566",
                ),
            )),
            dynamodb: Arc::new(parking_lot::RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new(
                    "123456789012",
                    "us-east-1",
                    "",
                ),
            )),
            lambda: Arc::new(parking_lot::RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new(
                    "123456789012",
                    "us-east-1",
                    "",
                ),
            )),
            secretsmanager: Arc::new(parking_lot::RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new(
                    "123456789012",
                    "us-east-1",
                    "http://localhost:4566",
                ),
            )),
            s3: Arc::new(parking_lot::RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new(
                    "123456789012",
                    "us-east-1",
                    "http://localhost:4566",
                ),
            )),
            logs: Arc::new(parking_lot::RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new(
                    "123456789012",
                    "us-east-1",
                    "",
                ),
            )),
            kms: Arc::new(parking_lot::RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new(
                    "123456789012",
                    "us-east-1",
                    "http://localhost:4566",
                ),
            )),
            cloudformation: Arc::new(parking_lot::RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new(
                    "123456789012",
                    "us-east-1",
                    "http://localhost:4566",
                ),
            )),
            ses: Arc::new(parking_lot::RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new(
                    "123456789012",
                    "us-east-1",
                    "http://localhost:4566",
                ),
            )),
            cognito: Arc::new(parking_lot::RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new(
                    "123456789012",
                    "us-east-1",
                    "http://localhost:4566",
                ),
            )),
            kinesis: Arc::new(parking_lot::RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new(
                    "123456789012",
                    "us-east-1",
                    "http://localhost:4566",
                ),
            )),
            rds: Arc::new(parking_lot::RwLock::new(rds_mas)),
            elasticache: Arc::new(parking_lot::RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new(
                    "123456789012",
                    "us-east-1",
                    "",
                ),
            )),
            ecr: Arc::new(parking_lot::RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new(
                    "123456789012",
                    "us-east-1",
                    "http://localhost:4566",
                ),
            )),
            ecs: Arc::new(parking_lot::RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new(
                    "123456789012",
                    "us-east-1",
                    "http://localhost:4566",
                ),
            )),
            stepfunctions: Arc::new(parking_lot::RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new(
                    "123456789012",
                    "us-east-1",
                    "",
                ),
            )),
            scheduler: Arc::new(parking_lot::RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new(
                    "123456789012",
                    "us-east-1",
                    "",
                ),
            )),
            apigatewayv1: Arc::new(parking_lot::RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new(
                    "123456789012",
                    "us-east-1",
                    "",
                ),
            )),
            apigatewayv2: Arc::new(parking_lot::RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new(
                    "123456789012",
                    "us-east-1",
                    "",
                ),
            )),
            bedrock: Arc::new(parking_lot::RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new(
                    "123456789012",
                    "us-east-1",
                    "http://localhost:4566",
                ),
            )),
            bedrock_agent: Arc::new(parking_lot::RwLock::new(
                fakecloud_bedrock_agent::BedrockAgentAccounts::new(),
            )),
            bedrock_agent_runtime: Arc::new(parking_lot::RwLock::new(
                fakecloud_bedrock_agent_runtime::BedrockAgentRuntimeAccounts::new(),
            )),
            cloudfront: Arc::new(parking_lot::RwLock::new(
                fakecloud_cloudfront::CloudFrontAccounts::new(),
            )),
            route53: Arc::new(parking_lot::RwLock::new(
                fakecloud_route53::Route53Accounts::new(),
            )),
            acm: Arc::new(parking_lot::RwLock::new(fakecloud_acm::AcmAccounts::new())),
            acmpca: Arc::new(parking_lot::RwLock::new(
                fakecloud_acmpca::AcmPcaAccounts::new(),
            )),
            config: Arc::new(parking_lot::RwLock::new(
                fakecloud_config::ConfigAccounts::new(),
            )),
            route53resolver: Arc::new(parking_lot::RwLock::new(
                fakecloud_route53resolver::Route53ResolverAccounts::new(),
            )),
            firehose: Arc::new(parking_lot::RwLock::new(
                fakecloud_firehose::FirehoseAccounts::new(),
            )),
            glue: Arc::new(parking_lot::RwLock::new(fakecloud_glue::GlueAccounts::new())),
            cloudwatch: Arc::new(parking_lot::RwLock::new(
                fakecloud_cloudwatch::CloudWatchAccounts::new(),
            )),
            application_autoscaling: Arc::new(parking_lot::RwLock::new(
                fakecloud_application_autoscaling::ApplicationAutoScalingAccounts::new(),
            )),
            wafv2: Arc::new(parking_lot::RwLock::new(
                fakecloud_wafv2::Wafv2Accounts::new(),
            )),
            athena: Arc::new(parking_lot::RwLock::new(
                fakecloud_athena::AthenaAccounts::new(),
            )),
            organizations: Arc::new(parking_lot::RwLock::new(
                fakecloud_organizations::OrganizationsRegistry::default(),
            )),
            container_runtime: None,
            rds_runtime: None,
            elasticache_runtime: None,
            ecs_runtime: None,
            ec2: Arc::new(parking_lot::RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new(
                    "123456789012",
                    "us-east-1",
                    "",
                ),
            )),
            ec2_runtime: None,
        };

        state.reset_service("ec2").expect("reset ec2");
        state.reset_service("rds").expect("reset rds");

        assert!(state.rds.read().default_ref().instances.is_empty());
    }

    #[test]
    fn create_admin_in_default_account() {
        let iam: fakecloud_iam::SharedIamState = Arc::new(parking_lot::RwLock::new(
            fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
        ));
        let orgs: fakecloud_organizations::SharedOrganizationsState = Arc::new(
            parking_lot::RwLock::new(fakecloud_organizations::OrganizationsRegistry::default()),
        );
        let resp = super::create_admin_in_account(&iam, &orgs, "123456789012", "admin", None)
            .expect("create admin");
        assert_eq!(resp.account_id, "123456789012");
        assert!(resp.access_key_id.starts_with("FKIA"));
        assert!(resp.arn.contains("123456789012"));
        assert!(resp.arn.contains("admin"));

        // Verify state was populated
        let accounts = iam.read();
        let state = accounts.get("123456789012").unwrap();
        assert!(state.users.contains_key("admin"));
        assert!(state.access_keys.contains_key("admin"));
        assert!(state.user_inline_policies.contains_key("admin"));
    }

    #[test]
    fn create_admin_on_a_china_server_uses_the_aws_cn_partition() {
        let iam: fakecloud_iam::SharedIamState = Arc::new(parking_lot::RwLock::new(
            fakecloud_core::multi_account::MultiAccountState::new("123456789012", "cn-north-1", ""),
        ));
        let orgs: fakecloud_organizations::SharedOrganizationsState = Arc::new(
            parking_lot::RwLock::new(fakecloud_organizations::OrganizationsRegistry::default()),
        );
        let resp = super::create_admin_in_account(&iam, &orgs, "222222222222", "admin", None)
            .expect("create admin");
        assert_eq!(resp.arn, "arn:aws-cn:iam::222222222222:user/admin");
        assert_eq!(
            iam.read().get("222222222222").unwrap().users["admin"].arn,
            resp.arn
        );
    }

    #[test]
    fn create_admin_in_new_account() {
        let iam: fakecloud_iam::SharedIamState = Arc::new(parking_lot::RwLock::new(
            fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
        ));
        let orgs: fakecloud_organizations::SharedOrganizationsState = Arc::new(
            parking_lot::RwLock::new(fakecloud_organizations::OrganizationsRegistry::default()),
        );
        let resp = super::create_admin_in_account(&iam, &orgs, "999999999999", "bob", None)
            .expect("create admin");
        assert_eq!(resp.account_id, "999999999999");
        assert!(resp.arn.contains("999999999999"));

        // New account was created
        let accounts = iam.read();
        assert!(accounts.get("999999999999").is_some());
        let state = accounts.get("999999999999").unwrap();
        assert!(state.users.contains_key("bob"));

        // Default account untouched
        let default = accounts.get("123456789012").unwrap();
        assert!(default.users.is_empty());
    }

    #[test]
    fn create_admin_policy_allows_all() {
        use fakecloud_core::auth::{
            ConditionContext, IamAction, IamDecision, IamPolicyEvaluator, Principal, PrincipalType,
        };
        let iam: fakecloud_iam::SharedIamState = Arc::new(parking_lot::RwLock::new(
            fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
        ));
        let orgs: fakecloud_organizations::SharedOrganizationsState = Arc::new(
            parking_lot::RwLock::new(fakecloud_organizations::OrganizationsRegistry::default()),
        );
        let resp = super::create_admin_in_account(&iam, &orgs, "222222222222", "admin", None)
            .expect("create admin");

        let evaluator = fakecloud_iam::policy_evaluator::IamPolicyEvaluatorImpl::new(iam.clone());
        let principal = Principal {
            arn: resp.arn.clone(),
            user_id: "AIDATEST".to_string(),
            account_id: "222222222222".to_string(),
            principal_type: PrincipalType::User,
            source_identity: None,
            tags: None,
        };
        let action = IamAction {
            service: "s3",
            action: "ListBuckets",
            resource: "*".to_string(),
        };
        let decision =
            evaluator.evaluate(&principal, &action, &ConditionContext::default(), &[], None);
        assert_eq!(
            decision,
            IamDecision::Allow,
            "admin policy should Allow */*"
        );
    }

    /// Regression for #2543: bootstrapping an admin must not silently
    /// pull the account into an organization someone else created. An
    /// auto-joined account cannot become a management account of its
    /// own, which broke multi-organization setups.
    #[test]
    fn create_admin_does_not_join_existing_organization() {
        let iam: fakecloud_iam::SharedIamState = Arc::new(parking_lot::RwLock::new(
            fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
        ));
        let orgs: fakecloud_organizations::SharedOrganizationsState =
            Arc::new(parking_lot::RwLock::new(
                fakecloud_organizations::OrganizationState::bootstrap("111111111111").into(),
            ));

        super::create_admin_in_account(&iam, &orgs, "222222222222", "admin", None)
            .expect("create admin");

        let guard = orgs.read();
        let org = guard.sole().unwrap();
        assert!(
            !org.accounts.contains_key("222222222222"),
            "a standalone bootstrap must leave the account outside the org"
        );
        assert!(org.accounts.contains_key("111111111111"));
    }

    #[test]
    fn create_admin_with_organization_id_enrolls_account() {
        let iam: fakecloud_iam::SharedIamState = Arc::new(parking_lot::RwLock::new(
            fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
        ));
        let org = fakecloud_organizations::OrganizationState::bootstrap("111111111111");
        let org_id = org.org_id.clone();
        let root_id = org.root_id.clone();
        let orgs: fakecloud_organizations::SharedOrganizationsState =
            Arc::new(parking_lot::RwLock::new(org.into()));

        super::create_admin_in_account(&iam, &orgs, "222222222222", "admin", Some(&org_id))
            .expect("create admin");

        let guard = orgs.read();
        let member = guard
            .sole()
            .unwrap()
            .accounts
            .get("222222222222")
            .expect("account enrolled");
        assert_eq!(member.parent_id, root_id);
        assert_eq!(member.status, "ACTIVE");
    }

    /// An account belongs to at most one organization. The bootstrap
    /// shortcut must reject a second enrollment rather than putting the
    /// account in two registries at once, where which organization's SCP
    /// ceiling and stack-set targeting applied would be arbitrary.
    #[test]
    fn create_admin_cannot_enroll_an_account_into_a_second_organization() {
        let iam: fakecloud_iam::SharedIamState = Arc::new(parking_lot::RwLock::new(
            fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
        ));
        let first = fakecloud_organizations::OrganizationState::bootstrap("111111111111");
        let second = fakecloud_organizations::OrganizationState::bootstrap("999999999999");
        let first_id = first.org_id.clone();
        let second_id = second.org_id.clone();
        let mut registry = fakecloud_organizations::OrganizationsRegistry::default();
        registry.insert(first);
        registry.insert(second);
        let orgs: fakecloud_organizations::SharedOrganizationsState =
            Arc::new(parking_lot::RwLock::new(registry));

        super::create_admin_in_account(&iam, &orgs, "222222222222", "admin", Some(&first_id))
            .expect("first enrollment");

        let err =
            super::create_admin_in_account(&iam, &orgs, "222222222222", "admin", Some(&second_id))
                .expect_err("a second organization must be rejected");
        assert_eq!(
            err,
            super::CreateAdminError::AccountInAnotherOrganization {
                account_id: "222222222222".to_string(),
                organization_id: first_id.clone(),
            }
        );

        let guard = orgs.read();
        assert!(guard
            .org_by_id(&first_id)
            .unwrap()
            .accounts
            .contains_key("222222222222"));
        assert!(!guard
            .org_by_id(&second_id)
            .unwrap()
            .accounts
            .contains_key("222222222222"));
    }

    /// Re-naming the organization the account is already in is a no-op
    /// rather than an error — bootstrapping admin credentials twice for
    /// the same member must keep working.
    #[test]
    fn create_admin_into_the_account_s_own_organization_is_idempotent() {
        let iam: fakecloud_iam::SharedIamState = Arc::new(parking_lot::RwLock::new(
            fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
        ));
        let org = fakecloud_organizations::OrganizationState::bootstrap("111111111111");
        let org_id = org.org_id.clone();
        let orgs: fakecloud_organizations::SharedOrganizationsState =
            Arc::new(parking_lot::RwLock::new(org.into()));

        for _ in 0..2 {
            super::create_admin_in_account(&iam, &orgs, "222222222222", "admin", Some(&org_id))
                .expect("repeat enrollment is a no-op");
        }
        assert_eq!(orgs.read().org_by_id(&org_id).unwrap().accounts.len(), 2);
    }

    #[test]
    fn create_admin_with_unknown_organization_id_errors() {
        let iam: fakecloud_iam::SharedIamState = Arc::new(parking_lot::RwLock::new(
            fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
        ));
        let orgs: fakecloud_organizations::SharedOrganizationsState =
            Arc::new(parking_lot::RwLock::new(
                fakecloud_organizations::OrganizationState::bootstrap("111111111111").into(),
            ));

        let err = super::create_admin_in_account(
            &iam,
            &orgs,
            "222222222222",
            "admin",
            Some("o-doesnotexist"),
        )
        .expect_err("unknown org id must be rejected");
        assert_eq!(
            err,
            super::CreateAdminError::UnknownOrganization("o-doesnotexist".to_string())
        );
        // The IAM user is not created when the enrollment target is bogus.
        assert!(iam.read().get("222222222222").is_none());
    }

    #[test]
    fn create_admin_credentials_resolve() {
        let iam: fakecloud_iam::SharedIamState = Arc::new(parking_lot::RwLock::new(
            fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
        ));
        let orgs: fakecloud_organizations::SharedOrganizationsState = Arc::new(
            parking_lot::RwLock::new(fakecloud_organizations::OrganizationsRegistry::default()),
        );
        let resp = super::create_admin_in_account(&iam, &orgs, "222222222222", "alice", None)
            .expect("create admin");

        // Verify the credential resolver can find this key
        let mut accounts = iam.write();
        let state = accounts.get_or_create("222222222222");
        let lookup = state.credential_secret(&resp.access_key_id);
        assert!(lookup.is_some());
        let lookup = lookup.unwrap();
        assert_eq!(lookup.account_id, "222222222222");
        assert_eq!(lookup.secret_access_key, resp.secret_access_key);
    }
}
