use crate::error::Error;
use crate::types::*;
use percent_encoding::{utf8_percent_encode, NON_ALPHANUMERIC};

/// Client for the fakecloud introspection and simulation API (`/_fakecloud/*`).
pub struct FakeCloud {
    base_url: String,
    client: reqwest::Client,
}

impl FakeCloud {
    /// Create a new client pointing at the given fakecloud base URL (e.g. `http://localhost:4566`).
    pub fn new(base_url: &str) -> Self {
        Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            client: reqwest::Client::new(),
        }
    }

    // ── Health & Reset ──────────────────────────────────────────────

    /// Check server health.
    pub async fn health(&self) -> Result<HealthResponse, Error> {
        let resp = self
            .client
            .get(format!("{}/_fakecloud/health", self.base_url))
            .send()
            .await?;
        Self::parse(resp).await
    }

    /// Reset all service state. Uses the legacy `/_reset` endpoint.
    pub async fn reset(&self) -> Result<ResetResponse, Error> {
        let resp = self
            .client
            .post(format!("{}/_reset", self.base_url))
            .send()
            .await?;
        Self::parse(resp).await
    }

    /// Create an IAM admin user in a specific account. Returns credentials
    /// for the new user. Solves the multi-account bootstrap problem: the
    /// root bypass only targets the default account, so this endpoint lets
    /// callers create credentials for any account.
    pub async fn create_admin(
        &self,
        account_id: &str,
        user_name: &str,
    ) -> Result<CreateAdminResponse, Error> {
        let resp = self
            .client
            .post(format!("{}/_fakecloud/iam/create-admin", self.base_url))
            .json(&CreateAdminRequest {
                account_id: account_id.to_string(),
                user_name: user_name.to_string(),
            })
            .send()
            .await?;
        Self::parse(resp).await
    }

    /// Reset a single service's state.
    pub async fn reset_service(&self, service: &str) -> Result<ResetServiceResponse, Error> {
        let resp = self
            .client
            .post(format!("{}/_fakecloud/reset/{}", self.base_url, service))
            .send()
            .await?;
        Self::parse(resp).await
    }

    /// Reset a single service's state for a specific account only.
    pub async fn reset_service_for_account(
        &self,
        service: &str,
        account_id: &str,
    ) -> Result<ResetServiceResponse, Error> {
        let resp = self
            .client
            .post(format!(
                "{}/_fakecloud/reset/{}/{}",
                self.base_url, service, account_id
            ))
            .send()
            .await?;
        Self::parse(resp).await
    }

    // ── Sub-clients ─────────────────────────────────────────────────

    pub fn lambda(&self) -> LambdaClient<'_> {
        LambdaClient { fc: self }
    }

    pub fn ses(&self) -> SesClient<'_> {
        SesClient { fc: self }
    }

    pub fn sns(&self) -> SnsClient<'_> {
        SnsClient { fc: self }
    }

    pub fn sqs(&self) -> SqsClient<'_> {
        SqsClient { fc: self }
    }

    pub fn events(&self) -> EventsClient<'_> {
        EventsClient { fc: self }
    }

    pub fn s3(&self) -> S3Client<'_> {
        S3Client { fc: self }
    }

    pub fn dynamodb(&self) -> DynamoDbClient<'_> {
        DynamoDbClient { fc: self }
    }

    pub fn secretsmanager(&self) -> SecretsManagerClient<'_> {
        SecretsManagerClient { fc: self }
    }

    pub fn cognito(&self) -> CognitoClient<'_> {
        CognitoClient { fc: self }
    }

    pub fn rds(&self) -> RdsClient<'_> {
        RdsClient { fc: self }
    }

    pub fn ec2(&self) -> Ec2Client<'_> {
        Ec2Client { fc: self }
    }

    pub fn elasticache(&self) -> ElastiCacheClient<'_> {
        ElastiCacheClient { fc: self }
    }

    pub fn apigatewayv2(&self) -> ApiGatewayV2Client<'_> {
        ApiGatewayV2Client { fc: self }
    }

    pub fn stepfunctions(&self) -> StepFunctionsClient<'_> {
        StepFunctionsClient { fc: self }
    }

    pub fn bedrock(&self) -> BedrockClient<'_> {
        BedrockClient { fc: self }
    }

    pub fn bedrock_agent(&self) -> BedrockAgentClient<'_> {
        BedrockAgentClient { fc: self }
    }

    pub fn bedrock_agent_runtime(&self) -> BedrockAgentRuntimeClient<'_> {
        BedrockAgentRuntimeClient { fc: self }
    }

    pub fn ecs(&self) -> EcsClient<'_> {
        EcsClient { fc: self }
    }

    pub fn application_autoscaling(&self) -> ApplicationAutoScalingClient<'_> {
        ApplicationAutoScalingClient { fc: self }
    }

    pub fn athena(&self) -> AthenaClient<'_> {
        AthenaClient { fc: self }
    }

    pub fn organizations(&self) -> OrganizationsClient<'_> {
        OrganizationsClient { fc: self }
    }

    pub fn acm(&self) -> AcmClient<'_> {
        AcmClient { fc: self }
    }

    pub fn ecr(&self) -> EcrClient<'_> {
        EcrClient { fc: self }
    }

    pub fn elbv2(&self) -> Elbv2Client<'_> {
        Elbv2Client { fc: self }
    }

    pub fn glue(&self) -> GlueClient<'_> {
        GlueClient { fc: self }
    }

    pub fn cloudwatch(&self) -> CloudWatchClient<'_> {
        CloudWatchClient { fc: self }
    }

    pub fn firehose(&self) -> FirehoseClient<'_> {
        FirehoseClient { fc: self }
    }

    pub fn logs(&self) -> LogsClient<'_> {
        LogsClient { fc: self }
    }

    pub fn route53(&self) -> Route53Client<'_> {
        Route53Client { fc: self }
    }

    pub fn scheduler(&self) -> SchedulerClient<'_> {
        SchedulerClient { fc: self }
    }

    pub fn ssm(&self) -> SsmClient<'_> {
        SsmClient { fc: self }
    }

    pub fn kms(&self) -> KmsClient<'_> {
        KmsClient { fc: self }
    }

    pub fn wafv2(&self) -> WafV2Client<'_> {
        WafV2Client { fc: self }
    }

    pub fn cloudfront(&self) -> CloudFrontClient<'_> {
        CloudFrontClient { fc: self }
    }

    // ── Internal helpers ────────────────────────────────────────────

    async fn parse<T: serde::de::DeserializeOwned>(resp: reqwest::Response) -> Result<T, Error> {
        let status = resp.status().as_u16();
        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(Error::Api { status, body });
        }
        Ok(resp.json::<T>().await?)
    }
}

// ── RDS ─────────────────────────────────────────────────────────────

pub struct Ec2Client<'a> {
    fc: &'a FakeCloud,
}

impl Ec2Client<'_> {
    /// List fakecloud-managed EC2 instances with control-plane metadata.
    pub async fn get_instances(&self) -> Result<Ec2InstancesResponse, Error> {
        let resp = self
            .fc
            .client
            .get(format!("{}/_fakecloud/ec2/instances", self.fc.base_url))
            .send()
            .await?;
        FakeCloud::parse(resp).await
    }
}

pub struct RdsClient<'a> {
    fc: &'a FakeCloud,
}

impl RdsClient<'_> {
    /// List fakecloud-managed RDS DB instances with runtime metadata.
    pub async fn get_instances(&self) -> Result<RdsInstancesResponse, Error> {
        let resp = self
            .fc
            .client
            .get(format!("{}/_fakecloud/rds/instances", self.fc.base_url))
            .send()
            .await?;
        FakeCloud::parse(resp).await
    }

    /// Bridge endpoint used by the PostgreSQL `aws_lambda` extension
    /// running inside an RDS container to invoke a Lambda function. A
    /// `status_code` of `502` is returned (with the response body still
    /// JSON-decodable) when the target Lambda is unavailable.
    pub async fn lambda_invoke(
        &self,
        req: &RdsLambdaInvokeRequest,
    ) -> Result<RdsLambdaInvokeResponse, Error> {
        let resp = self
            .fc
            .client
            .post(format!("{}/_fakecloud/rds/lambda-invoke", self.fc.base_url))
            .json(req)
            .send()
            .await?;
        let status = resp.status().as_u16();
        // 502 is a valid bridge response (SERVICE_UNAVAILABLE) — body is
        // still a well-formed RdsLambdaInvokeResponse, so parse it
        // rather than treating the call as a transport error.
        if status == 502 || resp.status().is_success() {
            return Ok(resp.json::<RdsLambdaInvokeResponse>().await?);
        }
        let body = resp.text().await.unwrap_or_default();
        Err(Error::Api { status, body })
    }

    /// Bridge endpoint for the PostgreSQL `aws_s3` extension's import
    /// path. Mirrors `aws_s3.table_import_from_s3` at the transport layer.
    pub async fn s3_import(&self, req: &RdsS3ImportRequest) -> Result<RdsS3ImportResponse, Error> {
        let resp = self
            .fc
            .client
            .post(format!("{}/_fakecloud/rds/s3-import", self.fc.base_url))
            .json(req)
            .send()
            .await?;
        FakeCloud::parse(resp).await
    }

    /// Bridge endpoint for the PostgreSQL `aws_s3` extension's export
    /// path. Mirrors `aws_s3.query_export_to_s3` at the transport layer.
    pub async fn s3_export(&self, req: &RdsS3ExportRequest) -> Result<RdsS3ExportResponse, Error> {
        let resp = self
            .fc
            .client
            .post(format!("{}/_fakecloud/rds/s3-export", self.fc.base_url))
            .json(req)
            .send()
            .await?;
        FakeCloud::parse(resp).await
    }
}

// ── ElastiCache ─────────────────────────────────────────────────────

pub struct ElastiCacheClient<'a> {
    fc: &'a FakeCloud,
}

impl ElastiCacheClient<'_> {
    /// List fakecloud-managed ElastiCache cache clusters with runtime metadata.
    pub async fn get_clusters(&self) -> Result<ElastiCacheClustersResponse, Error> {
        let resp = self
            .fc
            .client
            .get(format!(
                "{}/_fakecloud/elasticache/clusters",
                self.fc.base_url
            ))
            .send()
            .await?;
        FakeCloud::parse(resp).await
    }

    /// List fakecloud-managed ElastiCache replication groups with runtime metadata.
    pub async fn get_replication_groups(
        &self,
    ) -> Result<ElastiCacheReplicationGroupsResponse, Error> {
        let resp = self
            .fc
            .client
            .get(format!(
                "{}/_fakecloud/elasticache/replication-groups",
                self.fc.base_url
            ))
            .send()
            .await?;
        FakeCloud::parse(resp).await
    }

    /// List fakecloud-managed ElastiCache serverless caches with runtime metadata.
    pub async fn get_serverless_caches(
        &self,
    ) -> Result<ElastiCacheServerlessCachesResponse, Error> {
        let resp = self
            .fc
            .client
            .get(format!(
                "{}/_fakecloud/elasticache/serverless-caches",
                self.fc.base_url
            ))
            .send()
            .await?;
        FakeCloud::parse(resp).await
    }

    /// List ACL state (users + user groups) for ElastiCache replication groups
    /// that have one or more user groups attached.
    pub async fn get_acls(&self) -> Result<ElastiCacheAclsResponse, Error> {
        let resp = self
            .fc
            .client
            .get(format!("{}/_fakecloud/elasticache/acls", self.fc.base_url))
            .send()
            .await?;
        FakeCloud::parse(resp).await
    }
}

// ── Lambda ──────────────────────────────────────────────────────────

pub struct LambdaClient<'a> {
    fc: &'a FakeCloud,
}

impl LambdaClient<'_> {
    /// List recorded Lambda invocations.
    pub async fn get_invocations(&self) -> Result<LambdaInvocationsResponse, Error> {
        let resp = self
            .fc
            .client
            .get(format!(
                "{}/_fakecloud/lambda/invocations",
                self.fc.base_url
            ))
            .send()
            .await?;
        FakeCloud::parse(resp).await
    }

    /// List warm (cached) Lambda containers.
    pub async fn get_warm_containers(&self) -> Result<WarmContainersResponse, Error> {
        let resp = self
            .fc
            .client
            .get(format!(
                "{}/_fakecloud/lambda/warm-containers",
                self.fc.base_url
            ))
            .send()
            .await?;
        FakeCloud::parse(resp).await
    }

    /// Evict the warm container for a specific function.
    pub async fn evict_container(
        &self,
        function_name: &str,
    ) -> Result<EvictContainerResponse, Error> {
        let resp = self
            .fc
            .client
            .post(format!(
                "{}/_fakecloud/lambda/{}/evict-container",
                self.fc.base_url, function_name
            ))
            .send()
            .await?;
        FakeCloud::parse(resp).await
    }

    /// Download the raw zip bytes for a function's code. Pass `"latest"`
    /// for the live function code, or a published version string (e.g.
    /// `"1"`) for a version snapshot.
    pub async fn download_function_code(
        &self,
        account_id: &str,
        function_name: &str,
        qualifier_or_latest: &str,
    ) -> Result<Vec<u8>, Error> {
        let resp = self
            .fc
            .client
            .get(format!(
                "{}/_fakecloud/lambda/function-code/{}/{}/{}.zip",
                self.fc.base_url, account_id, function_name, qualifier_or_latest
            ))
            .send()
            .await?;
        let status = resp.status().as_u16();
        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(Error::Api { status, body });
        }
        Ok(resp.bytes().await?.to_vec())
    }

    /// Download the raw zip bytes for a published Lambda layer version.
    pub async fn download_layer_content(
        &self,
        account_id: &str,
        layer_name: &str,
        version: i64,
    ) -> Result<Vec<u8>, Error> {
        let resp = self
            .fc
            .client
            .get(format!(
                "{}/_fakecloud/lambda/layer-content/{}/{}/{}.zip",
                self.fc.base_url, account_id, layer_name, version
            ))
            .send()
            .await?;
        let status = resp.status().as_u16();
        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(Error::Api { status, body });
        }
        Ok(resp.bytes().await?.to_vec())
    }
}

// ── SES ─────────────────────────────────────────────────────────────

pub struct SesClient<'a> {
    fc: &'a FakeCloud,
}

impl SesClient<'_> {
    /// List all sent emails.
    pub async fn get_emails(&self) -> Result<SesEmailsResponse, Error> {
        let resp = self
            .fc
            .client
            .get(format!("{}/_fakecloud/ses/emails", self.fc.base_url))
            .send()
            .await?;
        FakeCloud::parse(resp).await
    }

    /// Simulate an inbound email (SES receipt rules).
    pub async fn simulate_inbound(
        &self,
        req: &InboundEmailRequest,
    ) -> Result<InboundEmailResponse, Error> {
        let resp = self
            .fc
            .client
            .post(format!("{}/_fakecloud/ses/inbound", self.fc.base_url))
            .json(req)
            .send()
            .await?;
        FakeCloud::parse(resp).await
    }

    /// Snapshot the running SES counters (currently only
    /// `suppressed_drops_total`).
    pub async fn get_metrics(&self) -> Result<SesMetricsResponse, Error> {
        let resp = self
            .fc
            .client
            .get(format!("{}/_fakecloud/ses/metrics", self.fc.base_url))
            .send()
            .await?;
        FakeCloud::parse(resp).await
    }

    /// List recorded SES bounces with per-recipient bounce metadata.
    pub async fn get_bounces(&self) -> Result<SesBouncesResponse, Error> {
        let resp = self
            .fc
            .client
            .get(format!("{}/_fakecloud/ses/bounces", self.fc.base_url))
            .send()
            .await?;
        FakeCloud::parse(resp).await
    }

    /// Flip the SES account-level `production_access_enabled` flag.
    /// `sandbox=true` puts the account back into sandbox mode (production
    /// access disabled); `sandbox=false` re-enables production access.
    pub async fn set_sandbox(&self, sandbox: bool) -> Result<SesSandboxResponse, Error> {
        let resp = self
            .fc
            .client
            .post(format!(
                "{}/_fakecloud/ses/account/sandbox",
                self.fc.base_url
            ))
            .json(&SesSandboxRequest { sandbox })
            .send()
            .await?;
        FakeCloud::parse(resp).await
    }

    /// List event-destination delivery dispatches recorded by the
    /// SES sender (one row per dispatched event-destination target).
    pub async fn get_event_destination_deliveries(
        &self,
    ) -> Result<SesEventDestinationDeliveriesResponse, Error> {
        let resp = self
            .fc
            .client
            .get(format!(
                "{}/_fakecloud/ses/event-destinations/deliveries",
                self.fc.base_url
            ))
            .send()
            .await?;
        FakeCloud::parse(resp).await
    }

    /// Get the deterministic DKIM public key + selector + signing-enabled
    /// flag for an identity. 404 if the identity is unknown.
    pub async fn get_dkim_public_key(
        &self,
        identity: &str,
    ) -> Result<SesDkimPublicKeyResponse, Error> {
        let resp = self
            .fc
            .client
            .get(format!(
                "{}/_fakecloud/ses/identities/{}/dkim-public-key",
                self.fc.base_url, identity
            ))
            .send()
            .await?;
        FakeCloud::parse(resp).await
    }

    /// Flip an identity's `MailFromDomainStatus`. Must be one of
    /// `NotStarted` / `Pending` / `Success` / `Failed`.
    pub async fn set_mail_from_status(
        &self,
        identity: &str,
        status: &str,
    ) -> Result<SesMailFromStatusResponse, Error> {
        let resp = self
            .fc
            .client
            .post(format!(
                "{}/_fakecloud/ses/identities/{}/mail-from-status",
                self.fc.base_url, identity
            ))
            .json(&SesMailFromStatusRequest {
                status: status.to_string(),
            })
            .send()
            .await?;
        FakeCloud::parse(resp).await
    }

    /// Get a per-message insights snapshot (sends, deliveries, bounces,
    /// complaints, ...). 404 if the message id is unknown.
    pub async fn get_message_insights(
        &self,
        message_id: &str,
    ) -> Result<SesMessageInsightsResponse, Error> {
        let resp = self
            .fc
            .client
            .get(format!(
                "{}/_fakecloud/ses/messages/{}/insights",
                self.fc.base_url, message_id
            ))
            .send()
            .await?;
        FakeCloud::parse(resp).await
    }

    /// List submissions received via the SES SMTP endpoint.
    pub async fn get_smtp_submissions(&self) -> Result<SesSmtpSubmissionsResponse, Error> {
        let resp = self
            .fc
            .client
            .get(format!(
                "{}/_fakecloud/ses/smtp/submissions",
                self.fc.base_url
            ))
            .send()
            .await?;
        FakeCloud::parse(resp).await
    }
}

// ── SNS ─────────────────────────────────────────────────────────────

pub struct SnsClient<'a> {
    fc: &'a FakeCloud,
}

impl SnsClient<'_> {
    /// List all published SNS messages.
    pub async fn get_messages(&self) -> Result<SnsMessagesResponse, Error> {
        let resp = self
            .fc
            .client
            .get(format!("{}/_fakecloud/sns/messages", self.fc.base_url))
            .send()
            .await?;
        FakeCloud::parse(resp).await
    }

    /// List subscriptions pending confirmation.
    pub async fn get_pending_confirmations(&self) -> Result<PendingConfirmationsResponse, Error> {
        let resp = self
            .fc
            .client
            .get(format!(
                "{}/_fakecloud/sns/pending-confirmations",
                self.fc.base_url
            ))
            .send()
            .await?;
        FakeCloud::parse(resp).await
    }

    /// Confirm a pending subscription.
    pub async fn confirm_subscription(
        &self,
        req: &ConfirmSubscriptionRequest,
    ) -> Result<ConfirmSubscriptionResponse, Error> {
        let resp = self
            .fc
            .client
            .post(format!(
                "{}/_fakecloud/sns/confirm-subscription",
                self.fc.base_url
            ))
            .json(req)
            .send()
            .await?;
        FakeCloud::parse(resp).await
    }

    /// Fetch the PEM-encoded SNS signing certificate used to sign
    /// outbound notification payloads. Served as
    /// `application/x-pem-file`; returned as the raw PEM string so
    /// callers can hand it straight to an X.509 parser.
    pub async fn cert_pem(&self) -> Result<String, Error> {
        let resp = self
            .fc
            .client
            .get(format!("{}/_fakecloud/sns/cert.pem", self.fc.base_url))
            .send()
            .await?;
        let status = resp.status().as_u16();
        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(Error::Api { status, body });
        }
        Ok(resp.text().await?)
    }

    /// List recorded SMS publications (phone number + message body).
    pub async fn sms(&self) -> Result<SnsSmsResponse, Error> {
        let resp = self
            .fc
            .client
            .get(format!("{}/_fakecloud/sns/sms", self.fc.base_url))
            .send()
            .await?;
        FakeCloud::parse(resp).await
    }
}

// ── SQS ─────────────────────────────────────────────────────────────

pub struct SqsClient<'a> {
    fc: &'a FakeCloud,
}

impl SqsClient<'_> {
    /// List all messages across all queues.
    pub async fn get_messages(&self) -> Result<SqsMessagesResponse, Error> {
        let resp = self
            .fc
            .client
            .get(format!("{}/_fakecloud/sqs/messages", self.fc.base_url))
            .send()
            .await?;
        FakeCloud::parse(resp).await
    }

    /// Tick the message expiration processor (expire visibility-timed-out messages).
    pub async fn tick_expiration(&self) -> Result<ExpirationTickResponse, Error> {
        let resp = self
            .fc
            .client
            .post(format!(
                "{}/_fakecloud/sqs/expiration-processor/tick",
                self.fc.base_url
            ))
            .send()
            .await?;
        FakeCloud::parse(resp).await
    }

    /// Force all messages in a queue to its DLQ.
    pub async fn force_dlq(&self, queue_name: &str) -> Result<ForceDlqResponse, Error> {
        let resp = self
            .fc
            .client
            .post(format!(
                "{}/_fakecloud/sqs/{}/force-dlq",
                self.fc.base_url, queue_name
            ))
            .send()
            .await?;
        FakeCloud::parse(resp).await
    }
}

// ── Application Auto Scaling ────────────────────────────────────────

pub struct ApplicationAutoScalingClient<'a> {
    fc: &'a FakeCloud,
}

impl ApplicationAutoScalingClient<'_> {
    /// Force the watcher to evaluate every scaling policy now. Returns
    /// the number of policies that applied a capacity change on this
    /// tick. Useful in tests so callers don't have to wait for the
    /// wall-clock 15s interval.
    pub async fn tick(&self) -> Result<AppAsTickResponse, Error> {
        let resp = self
            .fc
            .client
            .post(format!(
                "{}/_fakecloud/application-autoscaling/tick",
                self.fc.base_url
            ))
            .send()
            .await?;
        FakeCloud::parse(resp).await
    }

    /// Force the scheduled-action executor to evaluate every
    /// `ScheduledAction` now. Returns the number of actions that
    /// fired this tick. Useful in tests so callers don't have to wait
    /// for the wall-clock 30s interval.
    pub async fn scheduled_tick(&self) -> Result<AppAsScheduledTickResponse, Error> {
        let resp = self
            .fc
            .client
            .post(format!(
                "{}/_fakecloud/application-autoscaling/scheduled-tick",
                self.fc.base_url
            ))
            .send()
            .await?;
        FakeCloud::parse(resp).await
    }
}

// ── EventBridge ─────────────────────────────────────────────────────

pub struct EventsClient<'a> {
    fc: &'a FakeCloud,
}

impl EventsClient<'_> {
    /// Get event history and delivery records.
    pub async fn get_history(&self) -> Result<EventHistoryResponse, Error> {
        let resp = self
            .fc
            .client
            .get(format!("{}/_fakecloud/events/history", self.fc.base_url))
            .send()
            .await?;
        FakeCloud::parse(resp).await
    }

    /// Fire a specific EventBridge rule manually.
    pub async fn fire_rule(&self, req: &FireRuleRequest) -> Result<FireRuleResponse, Error> {
        let resp = self
            .fc
            .client
            .post(format!("{}/_fakecloud/events/fire-rule", self.fc.base_url))
            .json(req)
            .send()
            .await?;
        FakeCloud::parse(resp).await
    }
}

// ── S3 ──────────────────────────────────────────────────────────────

pub struct S3Client<'a> {
    fc: &'a FakeCloud,
}

impl S3Client<'_> {
    /// List S3 notification events.
    pub async fn get_notifications(&self) -> Result<S3NotificationsResponse, Error> {
        let resp = self
            .fc
            .client
            .get(format!("{}/_fakecloud/s3/notifications", self.fc.base_url))
            .send()
            .await?;
        FakeCloud::parse(resp).await
    }

    /// Tick the S3 lifecycle processor.
    pub async fn tick_lifecycle(&self) -> Result<LifecycleTickResponse, Error> {
        let resp = self
            .fc
            .client
            .post(format!(
                "{}/_fakecloud/s3/lifecycle-processor/tick",
                self.fc.base_url
            ))
            .send()
            .await?;
        FakeCloud::parse(resp).await
    }

    /// List S3 access points across all accounts.
    pub async fn get_access_points(&self) -> Result<S3AccessPointsResponse, Error> {
        let resp = self
            .fc
            .client
            .get(format!("{}/_fakecloud/s3/access-points", self.fc.base_url))
            .send()
            .await?;
        FakeCloud::parse(resp).await
    }

    /// List stored WriteGetObjectResponse bodies (S3 Object Lambda).
    pub async fn get_object_lambda_responses(
        &self,
    ) -> Result<S3ObjectLambdaResponsesResponse, Error> {
        let resp = self
            .fc
            .client
            .get(format!(
                "{}/_fakecloud/s3/object-lambda-responses",
                self.fc.base_url
            ))
            .send()
            .await?;
        FakeCloud::parse(resp).await
    }
}

// ── DynamoDB ────────────────────────────────────────────────────────

pub struct DynamoDbClient<'a> {
    fc: &'a FakeCloud,
}

impl DynamoDbClient<'_> {
    /// Tick the DynamoDB TTL processor.
    pub async fn tick_ttl(&self) -> Result<TtlTickResponse, Error> {
        let resp = self
            .fc
            .client
            .post(format!(
                "{}/_fakecloud/dynamodb/ttl-processor/tick",
                self.fc.base_url
            ))
            .send()
            .await?;
        FakeCloud::parse(resp).await
    }
}

// ── SecretsManager ──────────────────────────────────────────────────

pub struct SecretsManagerClient<'a> {
    fc: &'a FakeCloud,
}

impl SecretsManagerClient<'_> {
    /// Tick the SecretsManager rotation scheduler.
    pub async fn tick_rotation(&self) -> Result<RotationTickResponse, Error> {
        let resp = self
            .fc
            .client
            .post(format!(
                "{}/_fakecloud/secretsmanager/rotation-scheduler/tick",
                self.fc.base_url
            ))
            .send()
            .await?;
        FakeCloud::parse(resp).await
    }
}

// ── Cognito ─────────────────────────────────────────────────────────

pub struct CognitoClient<'a> {
    fc: &'a FakeCloud,
}

impl CognitoClient<'_> {
    /// Get confirmation codes for a specific user.
    pub async fn get_user_codes(
        &self,
        pool_id: &str,
        username: &str,
    ) -> Result<UserConfirmationCodes, Error> {
        let resp = self
            .fc
            .client
            .get(format!(
                "{}/_fakecloud/cognito/confirmation-codes/{}/{}",
                self.fc.base_url, pool_id, username
            ))
            .send()
            .await?;
        FakeCloud::parse(resp).await
    }

    /// List all confirmation codes across all pools.
    pub async fn get_confirmation_codes(&self) -> Result<ConfirmationCodesResponse, Error> {
        let resp = self
            .fc
            .client
            .get(format!(
                "{}/_fakecloud/cognito/confirmation-codes",
                self.fc.base_url
            ))
            .send()
            .await?;
        FakeCloud::parse(resp).await
    }

    /// Confirm a user (bypass email/phone verification).
    pub async fn confirm_user(
        &self,
        req: &ConfirmUserRequest,
    ) -> Result<ConfirmUserResponse, Error> {
        let resp = self
            .fc
            .client
            .post(format!(
                "{}/_fakecloud/cognito/confirm-user",
                self.fc.base_url
            ))
            .json(req)
            .send()
            .await?;
        let status = resp.status().as_u16();
        let body: ConfirmUserResponse = resp.json().await?;
        if status >= 400 {
            return Err(Error::Api {
                status,
                body: body.error.unwrap_or_default(),
            });
        }
        Ok(body)
    }

    /// List all active tokens.
    pub async fn get_tokens(&self) -> Result<TokensResponse, Error> {
        let resp = self
            .fc
            .client
            .get(format!("{}/_fakecloud/cognito/tokens", self.fc.base_url))
            .send()
            .await?;
        FakeCloud::parse(resp).await
    }

    /// Expire tokens (optionally filtered by pool/user).
    pub async fn expire_tokens(
        &self,
        req: &ExpireTokensRequest,
    ) -> Result<ExpireTokensResponse, Error> {
        let resp = self
            .fc
            .client
            .post(format!(
                "{}/_fakecloud/cognito/expire-tokens",
                self.fc.base_url
            ))
            .json(req)
            .send()
            .await?;
        FakeCloud::parse(resp).await
    }

    /// List auth events.
    pub async fn get_auth_events(&self) -> Result<AuthEventsResponse, Error> {
        let resp = self
            .fc
            .client
            .get(format!(
                "{}/_fakecloud/cognito/auth-events",
                self.fc.base_url
            ))
            .send()
            .await?;
        FakeCloud::parse(resp).await
    }

    /// List PreTokenGeneration Lambda trigger invocations recorded
    /// during `InitiateAuth`. Each entry includes the full request /
    /// response payloads plus pre-parsed `claims_added`,
    /// `claims_overridden`, and `group_overrides` so tests can assert
    /// the claim mutation flow without inspecting the issued JWT.
    pub async fn get_pre_token_gen_invocations(
        &self,
    ) -> Result<PreTokenGenInvocationsResponse, Error> {
        let resp = self
            .fc
            .client
            .get(format!(
                "{}/_fakecloud/cognito/pretokengen/invocations",
                self.fc.base_url
            ))
            .send()
            .await?;
        FakeCloud::parse(resp).await
    }

    /// Mint an OAuth2 `authorization_code` for the given `(client_id,
    /// redirect_uri, scopes, PKCE)` binding. Lets tests drive the
    /// `authorization_code` grant before the hosted-UI lands.
    pub async fn mint_authorization_code(
        &self,
        req: &MintAuthorizationCodeRequest,
    ) -> Result<MintAuthorizationCodeResponse, Error> {
        let resp = self
            .fc
            .client
            .post(format!(
                "{}/_fakecloud/cognito/authorization-codes",
                self.fc.base_url
            ))
            .json(req)
            .send()
            .await?;
        FakeCloud::parse(resp).await
    }

    /// Register one or more plaintext passwords with the compromised-
    /// credentials set so subsequent `InitiateAuth` /
    /// `AdminInitiateAuth` calls trip the `BLOCK` action when the pool's
    /// `CompromisedCredentialsRiskConfiguration` is enabled.
    pub async fn set_compromised_passwords(
        &self,
        req: &CognitoCompromisedPasswordsRequest,
    ) -> Result<CompromisedPasswordsResponse, Error> {
        let resp = self
            .fc
            .client
            .post(format!(
                "{}/_fakecloud/cognito/compromised-passwords",
                self.fc.base_url
            ))
            .json(req)
            .send()
            .await?;
        FakeCloud::parse(resp).await
    }

    /// List every registered WebAuthn credential across pools.
    pub async fn get_webauthn_credentials(&self) -> Result<WebAuthnCredentialsResponse, Error> {
        let resp = self
            .fc
            .client
            .get(format!(
                "{}/_fakecloud/cognito/webauthn-credentials",
                self.fc.base_url
            ))
            .send()
            .await?;
        FakeCloud::parse(resp).await
    }
}

// ── API Gateway v2 ──────────────────────────────────────────────────

pub struct ApiGatewayV2Client<'a> {
    fc: &'a FakeCloud,
}

impl ApiGatewayV2Client<'_> {
    /// List all HTTP API requests that were received and processed.
    pub async fn get_requests(&self) -> Result<ApiGatewayV2RequestsResponse, Error> {
        let resp = self
            .fc
            .client
            .get(format!(
                "{}/_fakecloud/apigatewayv2/requests",
                self.fc.base_url
            ))
            .send()
            .await?;
        FakeCloud::parse(resp).await
    }

    /// List currently-active WebSocket connections across all WebSocket
    /// APIs the server has seen.
    pub async fn connections(&self) -> Result<ApiGatewayV2ConnectionsResponse, Error> {
        let resp = self
            .fc
            .client
            .get(format!(
                "{}/_fakecloud/apigatewayv2/connections",
                self.fc.base_url
            ))
            .send()
            .await?;
        FakeCloud::parse(resp).await
    }

    /// Fetch the mTLS configuration recorded for a custom domain name
    /// (truststore bundle, version, validity). Pass-through JSON — the
    /// shape mirrors what the server emits unmodified.
    pub async fn mtls_info(&self, name: &str) -> Result<serde_json::Value, Error> {
        let resp = self
            .fc
            .client
            .get(format!(
                "{}/_fakecloud/apigatewayv2/domain-names/{}/mtls-info",
                self.fc.base_url, name
            ))
            .send()
            .await?;
        FakeCloud::parse(resp).await
    }

    /// Build the WebSocket upgrade URL for `api_id`. The SDK doesn't
    /// open the socket itself — pair this with `tokio-tungstenite` or
    /// any other WebSocket client in test code.
    pub fn ws_url(&self, api_id: &str, stage: Option<&str>) -> String {
        let base = self
            .fc
            .base_url
            .replacen("https://", "wss://", 1)
            .replacen("http://", "ws://", 1);
        // API Gateway v2 API IDs are 10-char alphanumeric so encoding is
        // a no-op, but defend against future changes by passing through
        // reqwest's URL builder for the query parameter.
        let url = reqwest::Url::parse(&format!("{}/_fakecloud/apigatewayv2/ws/{}", base, api_id))
            .expect("base url + api id form a valid URL");
        match stage {
            Some(s) => {
                let mut u = url;
                u.query_pairs_mut().append_pair("stage", s);
                u.to_string()
            }
            None => url.to_string(),
        }
    }
}

// ── Step Functions ──────────────────────────────────────────────────

pub struct StepFunctionsClient<'a> {
    fc: &'a FakeCloud,
}

impl StepFunctionsClient<'_> {
    /// List all Step Functions executions with status, input, output, and timestamps.
    pub async fn get_executions(&self) -> Result<StepFunctionsExecutionsResponse, Error> {
        let resp = self
            .fc
            .client
            .get(format!(
                "{}/_fakecloud/stepfunctions/executions",
                self.fc.base_url
            ))
            .send()
            .await?;
        FakeCloud::parse(resp).await
    }

    /// List `StartSyncExecution` invocations with billing details. EXPRESS state
    /// machines only — async (`StartExecution`) calls show up under
    /// [`Self::get_executions`] instead.
    pub async fn get_sync_executions(&self) -> Result<StepFunctionsSyncExecutionsResponse, Error> {
        let resp = self
            .fc
            .client
            .get(format!(
                "{}/_fakecloud/stepfunctions/sync-executions",
                self.fc.base_url
            ))
            .send()
            .await?;
        FakeCloud::parse(resp).await
    }

    /// Return the nested call tree rooted at `execution_arn`. Children are
    /// executions that were started by their parent via
    /// `arn:aws:states:::states:startExecution[.sync]`.
    /// Inject an activity task into the worker pool, skipping a
    /// state-machine execution. Used by tests that want to exercise the
    /// worker-pool API surface (`GetActivityTask` / `SendTaskSuccess`)
    /// without spinning up an ASL workflow.
    pub async fn enqueue_activity_task(
        &self,
        req: &SfnEnqueueActivityTaskRequest,
    ) -> Result<SfnEnqueueActivityTaskResponse, Error> {
        let resp = self
            .fc
            .client
            .post(format!(
                "{}/_fakecloud/stepfunctions/enqueue-activity-task",
                self.fc.base_url
            ))
            .json(req)
            .send()
            .await?;
        FakeCloud::parse(resp).await
    }

    pub async fn get_execution_tree(
        &self,
        execution_arn: &str,
    ) -> Result<StepFunctionsExecutionTreeResponse, Error> {
        let mut encoded = String::with_capacity(execution_arn.len());
        for b in execution_arn.bytes() {
            match b {
                b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                    encoded.push(b as char);
                }
                _ => encoded.push_str(&format!("%{:02X}", b)),
            }
        }
        let resp = self
            .fc
            .client
            .get(format!(
                "{}/_fakecloud/stepfunctions/execution-tree/{}",
                self.fc.base_url, encoded
            ))
            .send()
            .await?;
        FakeCloud::parse(resp).await
    }
}

// ── Bedrock ─────────────────────────────────────────────────────────

pub struct BedrockClient<'a> {
    fc: &'a FakeCloud,
}

impl BedrockClient<'_> {
    /// List recorded Bedrock runtime invocations. Each invocation has an optional
    /// `error` field that is set for calls faulted via [`Self::queue_fault`].
    pub async fn get_invocations(&self) -> Result<BedrockInvocationsResponse, Error> {
        let resp = self
            .fc
            .client
            .get(format!(
                "{}/_fakecloud/bedrock/invocations",
                self.fc.base_url
            ))
            .send()
            .await?;
        FakeCloud::parse(resp).await
    }

    /// Configure a single canned response for a Bedrock model.
    pub async fn set_model_response(
        &self,
        model_id: &str,
        response: &str,
    ) -> Result<BedrockModelResponseConfig, Error> {
        let resp = self
            .fc
            .client
            .post(format!(
                "{}/_fakecloud/bedrock/models/{}/response",
                self.fc.base_url, model_id
            ))
            .header("content-type", "text/plain")
            .body(response.to_string())
            .send()
            .await?;
        FakeCloud::parse(resp).await
    }

    /// Replace the prompt-conditional response rule list for a Bedrock model.
    pub async fn set_response_rules(
        &self,
        model_id: &str,
        rules: &[BedrockResponseRule],
    ) -> Result<BedrockModelResponseConfig, Error> {
        let body = serde_json::json!({ "rules": rules });
        let resp = self
            .fc
            .client
            .post(format!(
                "{}/_fakecloud/bedrock/models/{}/responses",
                self.fc.base_url, model_id
            ))
            .json(&body)
            .send()
            .await?;
        FakeCloud::parse(resp).await
    }

    /// Clear all prompt-conditional response rules for a Bedrock model.
    pub async fn clear_response_rules(
        &self,
        model_id: &str,
    ) -> Result<BedrockModelResponseConfig, Error> {
        let resp = self
            .fc
            .client
            .delete(format!(
                "{}/_fakecloud/bedrock/models/{}/responses",
                self.fc.base_url, model_id
            ))
            .send()
            .await?;
        FakeCloud::parse(resp).await
    }

    /// Queue a fault rule that will cause the next matching Bedrock runtime call(s) to fail.
    pub async fn queue_fault(
        &self,
        rule: &BedrockFaultRule,
    ) -> Result<BedrockStatusResponse, Error> {
        let resp = self
            .fc
            .client
            .post(format!("{}/_fakecloud/bedrock/faults", self.fc.base_url))
            .json(rule)
            .send()
            .await?;
        FakeCloud::parse(resp).await
    }

    /// List currently queued fault rules.
    pub async fn get_faults(&self) -> Result<BedrockFaultsResponse, Error> {
        let resp = self
            .fc
            .client
            .get(format!("{}/_fakecloud/bedrock/faults", self.fc.base_url))
            .send()
            .await?;
        FakeCloud::parse(resp).await
    }

    /// Clear all queued fault rules.
    pub async fn clear_faults(&self) -> Result<BedrockStatusResponse, Error> {
        let resp = self
            .fc
            .client
            .delete(format!("{}/_fakecloud/bedrock/faults", self.fc.base_url))
            .send()
            .await?;
        FakeCloud::parse(resp).await
    }
}

// ── Bedrock Agent (control plane) ───────────────────────────────────

pub struct BedrockAgentClient<'a> {
    fc: &'a FakeCloud,
}

impl BedrockAgentClient<'_> {
    /// List every recorded Bedrock Agent with its aliases, versions,
    /// knowledge-base attachments, and collaborators flattened into one
    /// row each.
    pub async fn get_agents(&self) -> Result<BedrockAgentAgentsResponse, Error> {
        let resp = self
            .fc
            .client
            .get(format!(
                "{}/_fakecloud/bedrock-agent/agents",
                self.fc.base_url
            ))
            .send()
            .await?;
        FakeCloud::parse(resp).await
    }
}

// ── Bedrock Agent Runtime (data plane) ──────────────────────────────

pub struct BedrockAgentRuntimeClient<'a> {
    fc: &'a FakeCloud,
}

impl BedrockAgentRuntimeClient<'_> {
    /// List every recorded InvokeAgent / InvokeInlineAgent / InvokeFlow
    /// / Retrieve / RetrieveAndGenerate / CreateInvocation call.
    pub async fn get_invocations(&self) -> Result<BedrockAgentRuntimeInvocationsResponse, Error> {
        let resp = self
            .fc
            .client
            .get(format!(
                "{}/_fakecloud/bedrock-agent-runtime/invocations",
                self.fc.base_url
            ))
            .send()
            .await?;
        FakeCloud::parse(resp).await
    }
}

// ── ECS ─────────────────────────────────────────────────────────────

pub struct EcsClient<'a> {
    fc: &'a FakeCloud,
}

impl EcsClient<'_> {
    /// List all ECS clusters across every account the server has seen.
    /// Deterministic, sorted by cluster ARN. Bypasses the ECS control-plane
    /// auth and pagination so tests can assert directly on raw state.
    pub async fn get_clusters(&self) -> Result<EcsClustersResponse, Error> {
        let resp = self
            .fc
            .client
            .get(format!("{}/_fakecloud/ecs/clusters", self.fc.base_url))
            .send()
            .await?;
        FakeCloud::parse(resp).await
    }

    /// List every task the server has seen. Optional `cluster` / `status`
    /// filters restrict the dump when supplied.
    pub async fn get_tasks(
        &self,
        cluster: Option<&str>,
        status: Option<&str>,
    ) -> Result<EcsTasksResponse, Error> {
        fn encode(s: &str) -> String {
            let mut out = String::with_capacity(s.len());
            for b in s.bytes() {
                match b {
                    b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                        out.push(b as char);
                    }
                    _ => out.push_str(&format!("%{:02X}", b)),
                }
            }
            out
        }
        let mut url = format!("{}/_fakecloud/ecs/tasks", self.fc.base_url);
        let mut sep = '?';
        if let Some(c) = cluster {
            url.push(sep);
            url.push_str("cluster=");
            url.push_str(&encode(c));
            sep = '&';
        }
        if let Some(s) = status {
            url.push(sep);
            url.push_str("status=");
            url.push_str(&encode(s));
        }
        let resp = self.fc.client.get(url).send().await?;
        FakeCloud::parse(resp).await
    }

    /// Tail stored container stdout/stderr for a single task. Works even
    /// when no `awslogs` driver is configured — fakecloud always captures
    /// docker stdout/stderr on exit and keeps it on the task.
    pub async fn get_task_logs(&self, task_id: &str) -> Result<EcsTaskLogsResponse, Error> {
        let resp = self
            .fc
            .client
            .get(format!(
                "{}/_fakecloud/ecs/tasks/{}/logs",
                self.fc.base_url, task_id
            ))
            .send()
            .await?;
        FakeCloud::parse(resp).await
    }

    /// Force the running container behind a task to stop.
    pub async fn force_stop_task(&self, task_id: &str) -> Result<EcsTask, Error> {
        let resp = self
            .fc
            .client
            .post(format!(
                "{}/_fakecloud/ecs/tasks/{}/force-stop",
                self.fc.base_url, task_id
            ))
            .send()
            .await?;
        FakeCloud::parse(resp).await
    }

    /// Flip the task to STOPPED without killing the underlying container
    /// — useful for simulating task failures in tests.
    pub async fn mark_task_failed(
        &self,
        task_id: &str,
        req: &EcsMarkFailedRequest,
    ) -> Result<EcsTask, Error> {
        let resp = self
            .fc
            .client
            .post(format!(
                "{}/_fakecloud/ecs/tasks/{}/mark-failed",
                self.fc.base_url, task_id
            ))
            .json(req)
            .send()
            .await?;
        FakeCloud::parse(resp).await
    }

    /// Replay the lifecycle event log.
    pub async fn get_events(&self) -> Result<EcsEventsResponse, Error> {
        let resp = self
            .fc
            .client
            .get(format!("{}/_fakecloud/ecs/events", self.fc.base_url))
            .send()
            .await?;
        FakeCloud::parse(resp).await
    }

    /// Get the ECS task-metadata v4 dump keyed by full task ARN. Unlike
    /// the per-container `/_fakecloud/ecs/v4/{task_id}` endpoint, this
    /// is keyed by ARN for assertion-friendly use from tests that hold
    /// the `RunTask` response. Returned as raw JSON because the shape
    /// is the aggregated container-metadata document AWS surfaces at
    /// `ECS_CONTAINER_METADATA_URI_V4`.
    pub async fn get_metadata_by_arn(&self, task_arn: &str) -> Result<serde_json::Value, Error> {
        let mut encoded = String::with_capacity(task_arn.len());
        for b in task_arn.bytes() {
            match b {
                b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                    encoded.push(b as char);
                }
                _ => encoded.push_str(&format!("%{:02X}", b)),
            }
        }
        let resp = self
            .fc
            .client
            .get(format!(
                "{}/_fakecloud/ecs/metadata/{}",
                self.fc.base_url, encoded
            ))
            .send()
            .await?;
        FakeCloud::parse(resp).await
    }

    /// Fetch the IAM task-role credentials that fakecloud hands out via
    /// `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI` to the container. Fields
    /// use AWS's native PascalCase (`AccessKeyId`, `SecretAccessKey`,
    /// `Token`, `Expiration`, `RoleArn`).
    pub async fn task_credentials(
        &self,
        task_id: &str,
    ) -> Result<EcsTaskCredentialsResponse, Error> {
        let resp = self
            .fc
            .client
            .get(format!(
                "{}/_fakecloud/ecs/creds/{}",
                self.fc.base_url, task_id
            ))
            .send()
            .await?;
        FakeCloud::parse(resp).await
    }

    /// Fetch the v3 ECS task metadata document for `task_id`. Returned
    /// as raw JSON (`Cluster`, `TaskARN`, `Family`, `Revision`,
    /// `DesiredStatus`, `KnownStatus`, `Containers`, `Limits`,
    /// `Networks`, ...) so callers can pick the fields they need
    /// without dragging in the entire metadata schema.
    pub async fn task_metadata_v3(&self, task_id: &str) -> Result<serde_json::Value, Error> {
        let resp = self
            .fc
            .client
            .get(format!(
                "{}/_fakecloud/ecs/v3/{}",
                self.fc.base_url, task_id
            ))
            .send()
            .await?;
        FakeCloud::parse(resp).await
    }

    /// Fetch the v4 ECS task metadata document for `task_id`. Same
    /// pass-through shape as the v3 endpoint; v4 adds extra container
    /// runtime fields the server emits as-is.
    pub async fn task_metadata_v4(&self, task_id: &str) -> Result<serde_json::Value, Error> {
        let resp = self
            .fc
            .client
            .get(format!(
                "{}/_fakecloud/ecs/v4/{}",
                self.fc.base_url, task_id
            ))
            .send()
            .await?;
        FakeCloud::parse(resp).await
    }
}

// ── Route 53 ────────────────────────────────────────────────────────

pub struct Route53Client<'a> {
    fc: &'a FakeCloud,
}

impl Route53Client<'_> {
    /// Fetch the deterministic DNSSEC chain-of-trust material for a
    /// hosted zone. Returns `Err(Error::Api { status: 404, .. })` when
    /// the zone has no ACTIVE KSK.
    pub async fn dnssec_material(
        &self,
        zone_id: &str,
    ) -> Result<Route53DnssecMaterialResponse, Error> {
        let resp = self
            .fc
            .client
            .get(format!(
                "{}/_fakecloud/route53/zones/{}/dnssec",
                self.fc.base_url, zone_id
            ))
            .send()
            .await?;
        FakeCloud::parse(resp).await
    }

    /// Sign an RRset under the zone's first ACTIVE KSK and return the
    /// raw RRSIG fields so tests can verify the signature against the
    /// zone's DNSKEY material.
    pub async fn dnssec_sign(
        &self,
        zone_id: &str,
        req: &Route53DnssecSignRequest,
    ) -> Result<Route53DnssecSignResponse, Error> {
        let resp = self
            .fc
            .client
            .post(format!(
                "{}/_fakecloud/route53/zones/{}/dnssec/sign",
                self.fc.base_url, zone_id
            ))
            .json(req)
            .send()
            .await?;
        FakeCloud::parse(resp).await
    }
}

// ── Athena ──────────────────────────────────────────────────────────

pub struct AthenaClient<'a> {
    fc: &'a FakeCloud,
}

impl AthenaClient<'_> {
    /// List every named query stored in the Athena named-query registry
    /// across all workgroups for the default account. Bumps `last_used_at`
    /// each time `StartQueryExecution` resolves a query by id so test
    /// authors can assert that a saved query was actually exercised.
    pub async fn get_named_queries(&self) -> Result<AthenaNamedQueriesResponse, Error> {
        let resp = self
            .fc
            .client
            .get(format!(
                "{}/_fakecloud/athena/named-queries",
                self.fc.base_url
            ))
            .send()
            .await?;
        FakeCloud::parse(resp).await
    }
}

// ── Organizations ───────────────────────────────────────────────────

pub struct OrganizationsClient<'a> {
    fc: &'a FakeCloud,
}

impl OrganizationsClient<'_> {
    /// List every member account in the org with lifecycle state,
    /// parent OU, tags, and directly-attached SCPs. Returns an empty
    /// `accounts` list (and `None` for management/master ids) when no
    /// organization has been created yet.
    pub async fn get_accounts(&self) -> Result<OrganizationsAccountsResponse, Error> {
        let resp = self
            .fc
            .client
            .get(format!(
                "{}/_fakecloud/organizations/accounts",
                self.fc.base_url
            ))
            .send()
            .await?;
        FakeCloud::parse(resp).await
    }

    /// List every billing-responsibility transfer in the org, with
    /// direction, lifecycle status, and the active handshake. Returns an
    /// empty list when no organization has been created.
    pub async fn get_responsibility_transfers(
        &self,
    ) -> Result<OrganizationsResponsibilityTransfersResponse, Error> {
        let resp = self
            .fc
            .client
            .get(format!(
                "{}/_fakecloud/organizations/responsibility-transfers",
                self.fc.base_url
            ))
            .send()
            .await?;
        FakeCloud::parse(resp).await
    }
}

// ── ACM ─────────────────────────────────────────────────────────────

pub struct AcmClient<'a> {
    fc: &'a FakeCloud,
}

impl AcmClient<'_> {
    fn certificate_id(arn_or_id: &str) -> String {
        match arn_or_id.rfind("certificate/") {
            Some(idx) => arn_or_id[idx + "certificate/".len()..].to_string(),
            None => arn_or_id.to_string(),
        }
    }

    /// Flip a stored ACM certificate's status (and optionally record a
    /// failure reason). Accepts either the full ACM ARN or just the
    /// trailing UUID. Returns `Error::Api { status: 404, .. }` if the
    /// certificate is unknown.
    pub async fn set_certificate_status(
        &self,
        arn_or_id: &str,
        req: &AcmCertificateStatusRequest,
    ) -> Result<(), Error> {
        let id = Self::certificate_id(arn_or_id);
        let resp = self
            .fc
            .client
            .post(format!(
                "{}/_fakecloud/acm/certificates/{}/status",
                self.fc.base_url, id
            ))
            .json(req)
            .send()
            .await?;
        if !resp.status().is_success() {
            let status = resp.status().as_u16();
            let body = resp.text().await.unwrap_or_default();
            return Err(Error::Api { status, body });
        }
        Ok(())
    }

    /// Inspect a stored certificate's PEM block counts and byte sizes.
    /// `external_ca_validated` is always `false` — fakecloud does not run
    /// real X.509 verification.
    pub async fn get_certificate_chain_info(
        &self,
        arn_or_id: &str,
    ) -> Result<AcmCertificateChainInfo, Error> {
        let id = Self::certificate_id(arn_or_id);
        let resp = self
            .fc
            .client
            .get(format!(
                "{}/_fakecloud/acm/certificates/{}/chain-info",
                self.fc.base_url, id
            ))
            .send()
            .await?;
        FakeCloud::parse(resp).await
    }

    /// Approve a `PENDING_VALIDATION` certificate (synchronous equivalent
    /// of "user clicked the validation link"). Flips the cert to `ISSUED`
    /// and refreshes its renewal eligibility / RenewalSummary.
    pub async fn approve_certificate(&self, arn_or_id: &str) -> Result<(), Error> {
        let id = Self::certificate_id(arn_or_id);
        let resp = self
            .fc
            .client
            .post(format!(
                "{}/_fakecloud/acm/certificates/{}/approve",
                self.fc.base_url, id
            ))
            .send()
            .await?;
        if !resp.status().is_success() {
            let status = resp.status().as_u16();
            let body = resp.text().await.unwrap_or_default();
            return Err(Error::Api { status, body });
        }
        Ok(())
    }
}

// ── ECR ─────────────────────────────────────────────────────────────

pub struct EcrClient<'a> {
    fc: &'a FakeCloud,
}

impl EcrClient<'_> {
    /// List every ECR image across every repository.
    pub async fn get_images(&self) -> Result<EcrImagesResponse, Error> {
        let resp = self
            .fc
            .client
            .get(format!("{}/_fakecloud/ecr/images", self.fc.base_url))
            .send()
            .await?;
        FakeCloud::parse(resp).await
    }

    /// List every ECR repository.
    pub async fn get_repositories(&self) -> Result<EcrRepositoriesResponse, Error> {
        let resp = self
            .fc
            .client
            .get(format!("{}/_fakecloud/ecr/repositories", self.fc.base_url))
            .send()
            .await?;
        FakeCloud::parse(resp).await
    }

    /// List configured ECR pull-through-cache rules.
    pub async fn get_pull_through_rules(&self) -> Result<EcrPullThroughRulesResponse, Error> {
        let resp = self
            .fc
            .client
            .get(format!(
                "{}/_fakecloud/ecr/pull-through-rules",
                self.fc.base_url
            ))
            .send()
            .await?;
        FakeCloud::parse(resp).await
    }
}

// ── ELBv2 ───────────────────────────────────────────────────────────

pub struct Elbv2Client<'a> {
    fc: &'a FakeCloud,
}

impl Elbv2Client<'_> {
    /// List every ELBv2 load balancer (ALB / NLB / GWLB).
    pub async fn get_load_balancers(&self) -> Result<Elbv2LoadBalancersResponse, Error> {
        let resp = self
            .fc
            .client
            .get(format!(
                "{}/_fakecloud/elbv2/load-balancers",
                self.fc.base_url
            ))
            .send()
            .await?;
        FakeCloud::parse(resp).await
    }

    /// List every ELBv2 listener.
    pub async fn get_listeners(&self) -> Result<Elbv2ListenersResponse, Error> {
        let resp = self
            .fc
            .client
            .get(format!("{}/_fakecloud/elbv2/listeners", self.fc.base_url))
            .send()
            .await?;
        FakeCloud::parse(resp).await
    }

    /// List every ELBv2 routing rule.
    pub async fn get_rules(&self) -> Result<Elbv2RulesResponse, Error> {
        let resp = self
            .fc
            .client
            .get(format!("{}/_fakecloud/elbv2/rules", self.fc.base_url))
            .send()
            .await?;
        FakeCloud::parse(resp).await
    }

    /// List every ELBv2 target group with its registered targets and
    /// health-check configuration.
    pub async fn get_target_groups(&self) -> Result<Elbv2TargetGroupsResponse, Error> {
        let resp = self
            .fc
            .client
            .get(format!(
                "{}/_fakecloud/elbv2/target-groups",
                self.fc.base_url
            ))
            .send()
            .await?;
        FakeCloud::parse(resp).await
    }

    /// Flush buffered access-log records to the configured S3 bucket
    /// now. Returns the number of records that were flushed.
    pub async fn flush_access_logs(&self) -> Result<Elbv2AccessLogsFlushResponse, Error> {
        let resp = self
            .fc
            .client
            .post(format!(
                "{}/_fakecloud/elbv2/access-logs/flush",
                self.fc.base_url
            ))
            .send()
            .await?;
        FakeCloud::parse(resp).await
    }
}

// ── Glue ────────────────────────────────────────────────────────────

pub struct GlueClient<'a> {
    fc: &'a FakeCloud,
}

impl GlueClient<'_> {
    /// List every configured Glue job.
    pub async fn get_jobs(&self) -> Result<GlueJobsResponse, Error> {
        let resp = self
            .fc
            .client
            .get(format!("{}/_fakecloud/glue/jobs", self.fc.base_url))
            .send()
            .await?;
        FakeCloud::parse(resp).await
    }

    /// List Glue JobRun records. Optionally scope to a single job by
    /// name (matches the `job_name` query parameter).
    pub async fn get_job_runs(&self, job_name: Option<&str>) -> Result<GlueJobRunsResponse, Error> {
        fn encode(s: &str) -> String {
            let mut out = String::with_capacity(s.len());
            for b in s.bytes() {
                match b {
                    b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                        out.push(b as char);
                    }
                    _ => out.push_str(&format!("%{:02X}", b)),
                }
            }
            out
        }
        let mut url = format!("{}/_fakecloud/glue/job-runs", self.fc.base_url);
        if let Some(name) = job_name {
            url.push_str("?job_name=");
            url.push_str(&encode(name));
        }
        let resp = self.fc.client.get(url).send().await?;
        FakeCloud::parse(resp).await
    }

    /// List every configured Glue crawler across all accounts.
    pub async fn get_crawlers(&self) -> Result<GlueCrawlersResponse, Error> {
        let resp = self
            .fc
            .client
            .get(format!("{}/_fakecloud/glue/crawlers", self.fc.base_url))
            .send()
            .await?;
        FakeCloud::parse(resp).await
    }
}

// ── CloudWatch ──────────────────────────────────────────────────────

pub struct CloudWatchClient<'a> {
    fc: &'a FakeCloud,
}

impl CloudWatchClient<'_> {
    /// List every metric and composite alarm across all accounts and
    /// regions, flattened with current state.
    pub async fn get_alarms(&self) -> Result<CloudWatchAlarmsResponse, Error> {
        let resp = self
            .fc
            .client
            .get(format!("{}/_fakecloud/cloudwatch/alarms", self.fc.base_url))
            .send()
            .await?;
        FakeCloud::parse(resp).await
    }

    /// List every unique metric series (account, region, namespace,
    /// metric, dimensions) with its datapoint count and latest value.
    pub async fn get_metrics(&self) -> Result<CloudWatchMetricsResponse, Error> {
        let resp = self
            .fc
            .client
            .get(format!(
                "{}/_fakecloud/cloudwatch/metrics",
                self.fc.base_url
            ))
            .send()
            .await?;
        FakeCloud::parse(resp).await
    }
}

// ── Firehose ────────────────────────────────────────────────────────

pub struct FirehoseClient<'a> {
    fc: &'a FakeCloud,
}

impl FirehoseClient<'_> {
    /// List every Firehose delivery stream across all accounts and
    /// regions, with stream type, lifecycle status, encryption summary,
    /// and destination count.
    pub async fn get_delivery_streams(&self) -> Result<FirehoseDeliveryStreamsResponse, Error> {
        let resp = self
            .fc
            .client
            .get(format!(
                "{}/_fakecloud/firehose/delivery-streams",
                self.fc.base_url
            ))
            .send()
            .await?;
        FakeCloud::parse(resp).await
    }
}

// ── Logs ────────────────────────────────────────────────────────────

pub struct LogsClient<'a> {
    fc: &'a FakeCloud,
}

impl LogsClient<'_> {
    /// Seed a synthetic CloudWatch Logs anomaly so tests can exercise
    /// `ListAnomalies` / `UpdateAnomaly` deterministically. Returns the
    /// minted anomaly id.
    pub async fn inject_anomaly(
        &self,
        req: &LogsAnomalyInjectRequest,
    ) -> Result<LogsAnomalyInjectResponse, Error> {
        let resp = self
            .fc
            .client
            .post(format!(
                "{}/_fakecloud/logs/anomalies/inject",
                self.fc.base_url
            ))
            .json(req)
            .send()
            .await?;
        FakeCloud::parse(resp).await
    }

    /// Snapshot the per-delivery configuration (one row per
    /// `Delivery`), joined with the `log_type` of its associated
    /// `DeliverySource`.
    pub async fn get_delivery_config(&self) -> Result<LogsDeliveryConfigResponse, Error> {
        let resp = self
            .fc
            .client
            .get(format!(
                "{}/_fakecloud/logs/delivery-config",
                self.fc.base_url
            ))
            .send()
            .await?;
        FakeCloud::parse(resp).await
    }

    /// Get the parsed `Fields` lists from a single log group's index
    /// policies. Returns `Error::Api { status: 404, .. }` if the log
    /// group is unknown.
    pub async fn get_field_indexes(
        &self,
        log_group_name: &str,
    ) -> Result<LogsFieldIndexesResponse, Error> {
        let resp = self
            .fc
            .client
            .get(format!(
                "{}/_fakecloud/logs/field-indexes/{}",
                self.fc.base_url,
                utf8_percent_encode(log_group_name, NON_ALPHANUMERIC)
            ))
            .send()
            .await?;
        FakeCloud::parse(resp).await
    }
}

impl Route53Client<'_> {
    /// Flip a stored Route 53 health check's reported status (and
    /// optionally its last-failure observation) so tests can simulate
    /// failover scenarios without a live checker. Returns
    /// `Error::Api { status: 404, .. }` if the health check is unknown.
    pub async fn set_health_check_status(
        &self,
        id: &str,
        req: &Route53HealthCheckStatusRequest,
    ) -> Result<(), Error> {
        let resp = self
            .fc
            .client
            .post(format!(
                "{}/_fakecloud/route53/health-checks/{}/status",
                self.fc.base_url, id
            ))
            .json(req)
            .send()
            .await?;
        if !resp.status().is_success() {
            let status = resp.status().as_u16();
            let body = resp.text().await.unwrap_or_default();
            return Err(Error::Api { status, body });
        }
        Ok(())
    }
}

// ── Scheduler ───────────────────────────────────────────────────────

pub struct SchedulerClient<'a> {
    fc: &'a FakeCloud,
}

impl SchedulerClient<'_> {
    /// List every EventBridge Scheduler schedule across every account
    /// and group.
    pub async fn get_schedules(&self) -> Result<SchedulerSchedulesResponse, Error> {
        let resp = self
            .fc
            .client
            .get(format!(
                "{}/_fakecloud/scheduler/schedules",
                self.fc.base_url
            ))
            .send()
            .await?;
        FakeCloud::parse(resp).await
    }

    /// Fire a single schedule by `(group, name)` immediately, bypassing
    /// the cron tick. Returns the schedule + target ARN that received the
    /// invocation.
    pub async fn fire_schedule(
        &self,
        group: &str,
        name: &str,
    ) -> Result<FireScheduleResponse, Error> {
        let resp = self
            .fc
            .client
            .post(format!(
                "{}/_fakecloud/scheduler/fire/{}/{}",
                self.fc.base_url, group, name
            ))
            .send()
            .await?;
        FakeCloud::parse(resp).await
    }
}

// ── SSM ─────────────────────────────────────────────────────────────

pub struct SsmClient<'a> {
    fc: &'a FakeCloud,
}

impl SsmClient<'_> {
    /// Force a specific SSM command's status. Useful for driving the
    /// `Pending`/`InProgress`/`Success` lifecycle synchronously in tests.
    pub async fn set_command_status(
        &self,
        command_id: &str,
        req: &SetSsmCommandStatusRequest,
    ) -> Result<SetSsmCommandStatusResponse, Error> {
        let resp = self
            .fc
            .client
            .post(format!(
                "{}/_fakecloud/ssm/commands/{}/status",
                self.fc.base_url, command_id
            ))
            .json(req)
            .send()
            .await?;
        FakeCloud::parse(resp).await
    }

    /// Flip a command's invocations to `Failed`. `req` is optional; when
    /// `None`, the server uses default values (all invocations, default
    /// account, generic "Failed" status detail).
    pub async fn fail_command(
        &self,
        command_id: &str,
        req: Option<&FailSsmCommandRequest>,
    ) -> Result<FailSsmCommandResponse, Error> {
        let mut builder = self.fc.client.post(format!(
            "{}/_fakecloud/ssm/commands/{}/fail",
            self.fc.base_url, command_id
        ));
        if let Some(body) = req {
            builder = builder.json(body);
        }
        let resp = builder.send().await?;
        FakeCloud::parse(resp).await
    }

    /// List recorded parameter-policy events for an account. Pass `None`
    /// to use the server's default account.
    pub async fn parameter_policy_events(
        &self,
        account_id: Option<&str>,
    ) -> Result<SsmParameterPolicyEventsResponse, Error> {
        let mut url = format!(
            "{}/_fakecloud/ssm/parameter-policy-events",
            self.fc.base_url
        );
        if let Some(id) = account_id {
            url.push_str("?accountId=");
            url.push_str(id);
        }
        let resp = self.fc.client.get(url).send().await?;
        FakeCloud::parse(resp).await
    }

    /// Inject a fake SSM session record so tests can exercise
    /// `DescribeSessions`/`TerminateSession` without going through
    /// `StartSession`.
    pub async fn inject_session(
        &self,
        req: &InjectSsmSessionRequest,
    ) -> Result<InjectSsmSessionResponse, Error> {
        let resp = self
            .fc
            .client
            .post(format!(
                "{}/_fakecloud/ssm/sessions/inject",
                self.fc.base_url
            ))
            .json(req)
            .send()
            .await?;
        FakeCloud::parse(resp).await
    }
}

// ── KMS ─────────────────────────────────────────────────────────────

pub struct KmsClient<'a> {
    fc: &'a FakeCloud,
}

impl KmsClient<'_> {
    /// List recorded KMS data-plane invocations.
    pub async fn usage(&self) -> Result<KmsUsageResponse, Error> {
        let resp = self
            .fc
            .client
            .get(format!("{}/_fakecloud/kms/usage", self.fc.base_url))
            .send()
            .await?;
        FakeCloud::parse(resp).await
    }
}

// ── WAFv2 ───────────────────────────────────────────────────────────

pub struct WafV2Client<'a> {
    fc: &'a FakeCloud,
}

impl WafV2Client<'_> {
    /// Evaluate a synthetic request against a `WebACL` and return the
    /// raw verdict JSON. Both request and response shapes are
    /// intentionally free-form so the SDK doesn't have to track every
    /// new field the evaluator emits.
    pub async fn evaluate(&self, body: &serde_json::Value) -> Result<serde_json::Value, Error> {
        let resp = self
            .fc
            .client
            .post(format!("{}/_fakecloud/wafv2/evaluate", self.fc.base_url))
            .json(body)
            .send()
            .await?;
        FakeCloud::parse(resp).await
    }
}

// ── CloudFront ──────────────────────────────────────────────────────

pub struct CloudFrontClient<'a> {
    fc: &'a FakeCloud,
}

impl CloudFrontClient<'_> {
    /// Force a stored CloudFront Distribution into a new status (typically
    /// `"Deployed"` or `"InProgress"`). Returns an `Api { status: 404, .. }`
    /// error when the distribution doesn't exist.
    pub async fn set_distribution_status(
        &self,
        distribution_id: &str,
        req: &CloudFrontDistributionStatusRequest,
    ) -> Result<(), Error> {
        let resp = self
            .fc
            .client
            .post(format!(
                "{}/_fakecloud/cloudfront/distributions/{}/status",
                self.fc.base_url, distribution_id
            ))
            .json(req)
            .send()
            .await?;
        let status = resp.status().as_u16();
        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(Error::Api { status, body });
        }
        Ok(())
    }
}

impl Elbv2Client<'_> {
    /// Snapshot the ELBv2 WAF count-metric registry. The `counts` field
    /// is intentionally free-form JSON because its shape tracks the
    /// service-internal metric layout.
    pub async fn waf_counts(&self) -> Result<Elbv2WafCountsResponse, Error> {
        let resp = self
            .fc
            .client
            .get(format!("{}/_fakecloud/elbv2/waf-counts", self.fc.base_url))
            .send()
            .await?;
        FakeCloud::parse(resp).await
    }
}
