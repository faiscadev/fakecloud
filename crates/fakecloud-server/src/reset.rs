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

impl ResetState {
    pub(crate) fn reset_service(&self, service: &str) -> Result<(), String> {
        match service {
            "iam" | "sts" => {
                self.iam.write().reset();
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
            "ec2" => {
                self.ec2.write().reset();
                if let Some(ref rt) = self.ec2_runtime {
                    let rt = rt.clone();
                    tokio::spawn(async move { rt.stop_all().await });
                }
            }
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
                *self.organizations.write() = None;
            }
            _ => {
                return Err(format!("Unknown service: {service}"));
            }
        }
        tracing::info!(service = %service, "service state reset via per-service reset API");
        Ok(())
    }

    /// Reset a single service's state for a specific account only.
    pub(crate) fn reset_service_for_account(
        &self,
        service: &str,
        account_id: &str,
    ) -> Result<(), String> {
        match service {
            "iam" | "sts" => {
                let mut mas = self.iam.write();
                if let Some(state) = mas.get_mut(account_id) {
                    state.reset();
                }
            }
            "sqs" => {
                let mut mas = self.sqs.write();
                if let Some(state) = mas.get_mut(account_id) {
                    state.reset();
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
                let mut mas = self.kinesis.write();
                if let Some(state) = mas.get_mut(account_id) {
                    state.reset();
                }
            }
            "rds" => {
                let mut mas = self.rds.write();
                if let Some(state) = mas.get_mut(account_id) {
                    state.reset();
                }
            }
            "elasticache" => {
                let mut mas = self.elasticache.write();
                if let Some(state) = mas.get_mut(account_id) {
                    state.reset();
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
                // CloudFront is a global service in AWS; per-account resets
                // simply drop that account's distribution / invalidation /
                // tag map, matching the multi-account semantics other
                // services use here.
                let mut state = self.cloudfront.write();
                state.accounts.remove(account_id);
            }
            "route53" => {
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
        Ok(())
    }

    pub(crate) fn reset(&self) -> axum::Json<types::ResetResponse> {
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
        *self.firehose.write() = fakecloud_firehose::FirehoseAccounts::new();
        *self.glue.write() = fakecloud_glue::GlueAccounts::new();
        *self.cloudwatch.write() = fakecloud_cloudwatch::CloudWatchAccounts::new();
        *self.application_autoscaling.write() =
            fakecloud_application_autoscaling::ApplicationAutoScalingAccounts::new();
        *self.wafv2.write() = fakecloud_wafv2::Wafv2Accounts::new();
        *self.athena.write() = fakecloud_athena::AthenaAccounts::new();
        // Organizations is a cross-account singleton (not MultiAccountState);
        // a full reset drops the org entirely so subsequent runs start
        // with no org, matching the no-in-use default state.
        *self.organizations.write() = None;
        tracing::info!("state reset via reset API");
        axum::Json(types::ResetResponse {
            status: "ok".to_string(),
        })
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
pub(crate) fn create_admin_in_account(
    iam: &fakecloud_iam::SharedIamState,
    organizations: &fakecloud_organizations::SharedOrganizationsState,
    account_id: &str,
    user_name: &str,
) -> types::CreateAdminResponse {
    // Auto-enroll the account into the organization's root OU if an
    // org exists. Matches AWS's InviteAccount path in spirit: tests
    // bootstrapping admin credentials for a second account expect
    // that account to immediately participate in SCP evaluation.
    if let Some(org) = organizations.write().as_mut() {
        org.enroll_account_if_missing(account_id);
    }

    let mut accounts = iam.write();
    let state = accounts.get_or_create(account_id);

    let user_id = format!(
        "AIDA{}",
        &uuid::Uuid::new_v4()
            .to_string()
            .replace('-', "")
            .to_uppercase()[..16]
    );
    let arn = Arn::global("iam", account_id, &format!("user/{user_name}")).to_string();
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

    types::CreateAdminResponse {
        access_key_id: akid,
        secret_access_key: secret,
        account_id: account_id.to_string(),
        arn,
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
                created_at,
                dbi_resource_id: "db-test".to_string(),
                master_user_password: "secret123".to_string(),
                container_id: "container-id".to_string(),
                host_port: 15432,
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
            organizations: Arc::new(parking_lot::RwLock::new(None)),
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
        let orgs: fakecloud_organizations::SharedOrganizationsState =
            Arc::new(parking_lot::RwLock::new(None));
        let resp = super::create_admin_in_account(&iam, &orgs, "123456789012", "admin");
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
    fn create_admin_in_new_account() {
        let iam: fakecloud_iam::SharedIamState = Arc::new(parking_lot::RwLock::new(
            fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
        ));
        let orgs: fakecloud_organizations::SharedOrganizationsState =
            Arc::new(parking_lot::RwLock::new(None));
        let resp = super::create_admin_in_account(&iam, &orgs, "999999999999", "bob");
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
        let orgs: fakecloud_organizations::SharedOrganizationsState =
            Arc::new(parking_lot::RwLock::new(None));
        let resp = super::create_admin_in_account(&iam, &orgs, "222222222222", "admin");

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

    #[test]
    fn create_admin_credentials_resolve() {
        let iam: fakecloud_iam::SharedIamState = Arc::new(parking_lot::RwLock::new(
            fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
        ));
        let orgs: fakecloud_organizations::SharedOrganizationsState =
            Arc::new(parking_lot::RwLock::new(None));
        let resp = super::create_admin_in_account(&iam, &orgs, "222222222222", "alice");

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
