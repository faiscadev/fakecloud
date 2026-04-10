use crate::error::Error;
use crate::types::*;

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

    /// Reset a single service's state.
    pub async fn reset_service(&self, service: &str) -> Result<ResetServiceResponse, Error> {
        let resp = self
            .client
            .post(format!("{}/_fakecloud/reset/{}", self.base_url, service))
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

    pub fn elasticache(&self) -> ElastiCacheClient<'_> {
        ElastiCacheClient { fc: self }
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
        // This endpoint returns 404 for missing users but still has a JSON body
        let status = resp.status().as_u16();
        let body: ConfirmUserResponse = resp.json().await?;
        if status == 404 {
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
}
