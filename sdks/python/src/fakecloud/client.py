"""Async and sync clients for the fakecloud introspection API."""

from __future__ import annotations

import httpx

from fakecloud.types import (
    ApiGatewayV2RequestsResponse,
    AuthEventsResponse,
    BedrockInvocationsResponse,
    BedrockModelResponseConfig,
    ConfirmationCodesResponse,
    ConfirmSubscriptionRequest,
    ConfirmSubscriptionResponse,
    ConfirmUserRequest,
    ConfirmUserResponse,
    ElastiCacheClustersResponse,
    ElastiCacheReplicationGroupsResponse,
    ElastiCacheServerlessCachesResponse,
    EventHistoryResponse,
    EvictContainerResponse,
    ExpirationTickResponse,
    ExpireTokensRequest,
    ExpireTokensResponse,
    FireRuleRequest,
    FireRuleResponse,
    ForceDlqResponse,
    HealthResponse,
    InboundEmailRequest,
    InboundEmailResponse,
    LambdaInvocationsResponse,
    LifecycleTickResponse,
    PendingConfirmationsResponse,
    RdsInstancesResponse,
    ResetResponse,
    ResetServiceResponse,
    RotationTickResponse,
    S3NotificationsResponse,
    SesEmailsResponse,
    SnsMessagesResponse,
    SqsMessagesResponse,
    StepFunctionsExecutionsResponse,
    TokensResponse,
    TtlTickResponse,
    UserConfirmationCodes,
    WarmContainersResponse,
)


class FakeCloudError(Exception):
    """Raised when the fakecloud API returns a non-success status."""

    def __init__(self, status: int, body: str) -> None:
        self.status = status
        self.body = body
        super().__init__(f"fakecloud API error {status}: {body}")


# ── Async sub-clients ───────────────────────────────────────────────


class LambdaClient:
    """Async Lambda introspection client."""

    def __init__(self, client: httpx.AsyncClient, base_url: str) -> None:
        self._client = client
        self._base = base_url

    async def get_invocations(self) -> LambdaInvocationsResponse:
        resp = await self._client.get(f"{self._base}/_fakecloud/lambda/invocations")
        _check(resp)
        return LambdaInvocationsResponse.from_dict(resp.json())

    async def get_warm_containers(self) -> WarmContainersResponse:
        resp = await self._client.get(f"{self._base}/_fakecloud/lambda/warm-containers")
        _check(resp)
        return WarmContainersResponse.from_dict(resp.json())

    async def evict_container(self, function_name: str) -> EvictContainerResponse:
        resp = await self._client.post(
            f"{self._base}/_fakecloud/lambda/{function_name}/evict-container"
        )
        _check(resp)
        return EvictContainerResponse.from_dict(resp.json())


class RdsClient:
    """Async RDS introspection client."""

    def __init__(self, client: httpx.AsyncClient, base_url: str) -> None:
        self._client = client
        self._base = base_url

    async def get_instances(self) -> RdsInstancesResponse:
        resp = await self._client.get(f"{self._base}/_fakecloud/rds/instances")
        _check(resp)
        return RdsInstancesResponse.from_dict(resp.json())


class ElastiCacheClient:
    """Async ElastiCache introspection client."""

    def __init__(self, client: httpx.AsyncClient, base_url: str) -> None:
        self._client = client
        self._base = base_url

    async def get_clusters(self) -> ElastiCacheClustersResponse:
        resp = await self._client.get(f"{self._base}/_fakecloud/elasticache/clusters")
        _check(resp)
        return ElastiCacheClustersResponse.from_dict(resp.json())

    async def get_replication_groups(self) -> ElastiCacheReplicationGroupsResponse:
        resp = await self._client.get(
            f"{self._base}/_fakecloud/elasticache/replication-groups"
        )
        _check(resp)
        return ElastiCacheReplicationGroupsResponse.from_dict(resp.json())

    async def get_serverless_caches(self) -> ElastiCacheServerlessCachesResponse:
        resp = await self._client.get(
            f"{self._base}/_fakecloud/elasticache/serverless-caches"
        )
        _check(resp)
        return ElastiCacheServerlessCachesResponse.from_dict(resp.json())


class SesClient:
    """Async SES introspection client."""

    def __init__(self, client: httpx.AsyncClient, base_url: str) -> None:
        self._client = client
        self._base = base_url

    async def get_emails(self) -> SesEmailsResponse:
        resp = await self._client.get(f"{self._base}/_fakecloud/ses/emails")
        _check(resp)
        return SesEmailsResponse.from_dict(resp.json())

    async def simulate_inbound(self, req: InboundEmailRequest) -> InboundEmailResponse:
        resp = await self._client.post(
            f"{self._base}/_fakecloud/ses/inbound", json=req.to_dict()
        )
        _check(resp)
        return InboundEmailResponse.from_dict(resp.json())


class SnsClient:
    """Async SNS introspection client."""

    def __init__(self, client: httpx.AsyncClient, base_url: str) -> None:
        self._client = client
        self._base = base_url

    async def get_messages(self) -> SnsMessagesResponse:
        resp = await self._client.get(f"{self._base}/_fakecloud/sns/messages")
        _check(resp)
        return SnsMessagesResponse.from_dict(resp.json())

    async def get_pending_confirmations(self) -> PendingConfirmationsResponse:
        resp = await self._client.get(
            f"{self._base}/_fakecloud/sns/pending-confirmations"
        )
        _check(resp)
        return PendingConfirmationsResponse.from_dict(resp.json())

    async def confirm_subscription(
        self, req: ConfirmSubscriptionRequest
    ) -> ConfirmSubscriptionResponse:
        resp = await self._client.post(
            f"{self._base}/_fakecloud/sns/confirm-subscription",
            json=req.to_dict(),
        )
        _check(resp)
        return ConfirmSubscriptionResponse.from_dict(resp.json())


class SqsClient:
    """Async SQS introspection client."""

    def __init__(self, client: httpx.AsyncClient, base_url: str) -> None:
        self._client = client
        self._base = base_url

    async def get_messages(self) -> SqsMessagesResponse:
        resp = await self._client.get(f"{self._base}/_fakecloud/sqs/messages")
        _check(resp)
        return SqsMessagesResponse.from_dict(resp.json())

    async def tick_expiration(self) -> ExpirationTickResponse:
        resp = await self._client.post(
            f"{self._base}/_fakecloud/sqs/expiration-processor/tick"
        )
        _check(resp)
        return ExpirationTickResponse.from_dict(resp.json())

    async def force_dlq(self, queue_name: str) -> ForceDlqResponse:
        resp = await self._client.post(
            f"{self._base}/_fakecloud/sqs/{queue_name}/force-dlq"
        )
        _check(resp)
        return ForceDlqResponse.from_dict(resp.json())


class EventsClient:
    """Async EventBridge introspection client."""

    def __init__(self, client: httpx.AsyncClient, base_url: str) -> None:
        self._client = client
        self._base = base_url

    async def get_history(self) -> EventHistoryResponse:
        resp = await self._client.get(f"{self._base}/_fakecloud/events/history")
        _check(resp)
        return EventHistoryResponse.from_dict(resp.json())

    async def fire_rule(self, req: FireRuleRequest) -> FireRuleResponse:
        resp = await self._client.post(
            f"{self._base}/_fakecloud/events/fire-rule", json=req.to_dict()
        )
        _check(resp)
        return FireRuleResponse.from_dict(resp.json())


class S3Client:
    """Async S3 introspection client."""

    def __init__(self, client: httpx.AsyncClient, base_url: str) -> None:
        self._client = client
        self._base = base_url

    async def get_notifications(self) -> S3NotificationsResponse:
        resp = await self._client.get(f"{self._base}/_fakecloud/s3/notifications")
        _check(resp)
        return S3NotificationsResponse.from_dict(resp.json())

    async def tick_lifecycle(self) -> LifecycleTickResponse:
        resp = await self._client.post(
            f"{self._base}/_fakecloud/s3/lifecycle-processor/tick"
        )
        _check(resp)
        return LifecycleTickResponse.from_dict(resp.json())


class DynamoDbClient:
    """Async DynamoDB introspection client."""

    def __init__(self, client: httpx.AsyncClient, base_url: str) -> None:
        self._client = client
        self._base = base_url

    async def tick_ttl(self) -> TtlTickResponse:
        resp = await self._client.post(
            f"{self._base}/_fakecloud/dynamodb/ttl-processor/tick"
        )
        _check(resp)
        return TtlTickResponse.from_dict(resp.json())


class SecretsManagerClient:
    """Async SecretsManager introspection client."""

    def __init__(self, client: httpx.AsyncClient, base_url: str) -> None:
        self._client = client
        self._base = base_url

    async def tick_rotation(self) -> RotationTickResponse:
        resp = await self._client.post(
            f"{self._base}/_fakecloud/secretsmanager/rotation-scheduler/tick"
        )
        _check(resp)
        return RotationTickResponse.from_dict(resp.json())


class CognitoClient:
    """Async Cognito introspection client."""

    def __init__(self, client: httpx.AsyncClient, base_url: str) -> None:
        self._client = client
        self._base = base_url

    async def get_user_codes(
        self, pool_id: str, username: str
    ) -> UserConfirmationCodes:
        resp = await self._client.get(
            f"{self._base}/_fakecloud/cognito/confirmation-codes/{pool_id}/{username}"
        )
        _check(resp)
        return UserConfirmationCodes.from_dict(resp.json())

    async def get_confirmation_codes(self) -> ConfirmationCodesResponse:
        resp = await self._client.get(
            f"{self._base}/_fakecloud/cognito/confirmation-codes"
        )
        _check(resp)
        return ConfirmationCodesResponse.from_dict(resp.json())

    async def confirm_user(self, req: ConfirmUserRequest) -> ConfirmUserResponse:
        resp = await self._client.post(
            f"{self._base}/_fakecloud/cognito/confirm-user",
            json=req.to_dict(),
        )
        _check(resp)
        return ConfirmUserResponse.from_dict(resp.json())

    async def get_tokens(self) -> TokensResponse:
        resp = await self._client.get(f"{self._base}/_fakecloud/cognito/tokens")
        _check(resp)
        return TokensResponse.from_dict(resp.json())

    async def expire_tokens(self, req: ExpireTokensRequest) -> ExpireTokensResponse:
        resp = await self._client.post(
            f"{self._base}/_fakecloud/cognito/expire-tokens",
            json=req.to_dict(),
        )
        _check(resp)
        return ExpireTokensResponse.from_dict(resp.json())

    async def get_auth_events(self) -> AuthEventsResponse:
        resp = await self._client.get(f"{self._base}/_fakecloud/cognito/auth-events")
        _check(resp)
        return AuthEventsResponse.from_dict(resp.json())


class ApiGatewayV2Client:
    """Async API Gateway v2 introspection client."""

    def __init__(self, client: httpx.AsyncClient, base_url: str) -> None:
        self._client = client
        self._base = base_url

    async def get_requests(self) -> ApiGatewayV2RequestsResponse:
        resp = await self._client.get(f"{self._base}/_fakecloud/apigatewayv2/requests")
        _check(resp)
        return ApiGatewayV2RequestsResponse.from_dict(resp.json())


class StepFunctionsClient:
    """Async Step Functions introspection client."""

    def __init__(self, client: httpx.AsyncClient, base_url: str) -> None:
        self._client = client
        self._base = base_url

    async def get_executions(self) -> StepFunctionsExecutionsResponse:
        resp = await self._client.get(
            f"{self._base}/_fakecloud/stepfunctions/executions"
        )
        _check(resp)
        return StepFunctionsExecutionsResponse.from_dict(resp.json())


class BedrockClient:
    """Async Bedrock introspection client."""

    def __init__(self, client: httpx.AsyncClient, base_url: str) -> None:
        self._client = client
        self._base = base_url

    async def get_invocations(self) -> BedrockInvocationsResponse:
        resp = await self._client.get(f"{self._base}/_fakecloud/bedrock/invocations")
        _check(resp)
        return BedrockInvocationsResponse.from_dict(resp.json())

    async def set_model_response(
        self, model_id: str, response: str
    ) -> BedrockModelResponseConfig:
        resp = await self._client.post(
            f"{self._base}/_fakecloud/bedrock/models/{model_id}/response",
            content=response,
            headers={"Content-Type": "text/plain"},
        )
        _check(resp)
        return BedrockModelResponseConfig.from_dict(resp.json())


# ── Sync sub-clients ────────────────────────────────────────────────


class _SyncLambdaClient:
    def __init__(self, client: httpx.Client, base_url: str) -> None:
        self._client = client
        self._base = base_url

    def get_invocations(self) -> LambdaInvocationsResponse:
        resp = self._client.get(f"{self._base}/_fakecloud/lambda/invocations")
        _check(resp)
        return LambdaInvocationsResponse.from_dict(resp.json())

    def get_warm_containers(self) -> WarmContainersResponse:
        resp = self._client.get(f"{self._base}/_fakecloud/lambda/warm-containers")
        _check(resp)
        return WarmContainersResponse.from_dict(resp.json())

    def evict_container(self, function_name: str) -> EvictContainerResponse:
        resp = self._client.post(
            f"{self._base}/_fakecloud/lambda/{function_name}/evict-container"
        )
        _check(resp)
        return EvictContainerResponse.from_dict(resp.json())


class _SyncRdsClient:
    """Sync RDS introspection client."""

    def __init__(self, client: httpx.Client, base_url: str) -> None:
        self._client = client
        self._base = base_url

    def get_instances(self) -> RdsInstancesResponse:
        resp = self._client.get(f"{self._base}/_fakecloud/rds/instances")
        _check(resp)
        return RdsInstancesResponse.from_dict(resp.json())


class _SyncElastiCacheClient:
    """Sync ElastiCache introspection client."""

    def __init__(self, client: httpx.Client, base_url: str) -> None:
        self._client = client
        self._base = base_url

    def get_clusters(self) -> ElastiCacheClustersResponse:
        resp = self._client.get(f"{self._base}/_fakecloud/elasticache/clusters")
        _check(resp)
        return ElastiCacheClustersResponse.from_dict(resp.json())

    def get_replication_groups(self) -> ElastiCacheReplicationGroupsResponse:
        resp = self._client.get(
            f"{self._base}/_fakecloud/elasticache/replication-groups"
        )
        _check(resp)
        return ElastiCacheReplicationGroupsResponse.from_dict(resp.json())

    def get_serverless_caches(self) -> ElastiCacheServerlessCachesResponse:
        resp = self._client.get(
            f"{self._base}/_fakecloud/elasticache/serverless-caches"
        )
        _check(resp)
        return ElastiCacheServerlessCachesResponse.from_dict(resp.json())


class _SyncSesClient:
    def __init__(self, client: httpx.Client, base_url: str) -> None:
        self._client = client
        self._base = base_url

    def get_emails(self) -> SesEmailsResponse:
        resp = self._client.get(f"{self._base}/_fakecloud/ses/emails")
        _check(resp)
        return SesEmailsResponse.from_dict(resp.json())

    def simulate_inbound(self, req: InboundEmailRequest) -> InboundEmailResponse:
        resp = self._client.post(
            f"{self._base}/_fakecloud/ses/inbound", json=req.to_dict()
        )
        _check(resp)
        return InboundEmailResponse.from_dict(resp.json())


class _SyncSnsClient:
    def __init__(self, client: httpx.Client, base_url: str) -> None:
        self._client = client
        self._base = base_url

    def get_messages(self) -> SnsMessagesResponse:
        resp = self._client.get(f"{self._base}/_fakecloud/sns/messages")
        _check(resp)
        return SnsMessagesResponse.from_dict(resp.json())

    def get_pending_confirmations(self) -> PendingConfirmationsResponse:
        resp = self._client.get(f"{self._base}/_fakecloud/sns/pending-confirmations")
        _check(resp)
        return PendingConfirmationsResponse.from_dict(resp.json())

    def confirm_subscription(
        self, req: ConfirmSubscriptionRequest
    ) -> ConfirmSubscriptionResponse:
        resp = self._client.post(
            f"{self._base}/_fakecloud/sns/confirm-subscription",
            json=req.to_dict(),
        )
        _check(resp)
        return ConfirmSubscriptionResponse.from_dict(resp.json())


class _SyncSqsClient:
    def __init__(self, client: httpx.Client, base_url: str) -> None:
        self._client = client
        self._base = base_url

    def get_messages(self) -> SqsMessagesResponse:
        resp = self._client.get(f"{self._base}/_fakecloud/sqs/messages")
        _check(resp)
        return SqsMessagesResponse.from_dict(resp.json())

    def tick_expiration(self) -> ExpirationTickResponse:
        resp = self._client.post(
            f"{self._base}/_fakecloud/sqs/expiration-processor/tick"
        )
        _check(resp)
        return ExpirationTickResponse.from_dict(resp.json())

    def force_dlq(self, queue_name: str) -> ForceDlqResponse:
        resp = self._client.post(f"{self._base}/_fakecloud/sqs/{queue_name}/force-dlq")
        _check(resp)
        return ForceDlqResponse.from_dict(resp.json())


class _SyncEventsClient:
    def __init__(self, client: httpx.Client, base_url: str) -> None:
        self._client = client
        self._base = base_url

    def get_history(self) -> EventHistoryResponse:
        resp = self._client.get(f"{self._base}/_fakecloud/events/history")
        _check(resp)
        return EventHistoryResponse.from_dict(resp.json())

    def fire_rule(self, req: FireRuleRequest) -> FireRuleResponse:
        resp = self._client.post(
            f"{self._base}/_fakecloud/events/fire-rule", json=req.to_dict()
        )
        _check(resp)
        return FireRuleResponse.from_dict(resp.json())


class _SyncS3Client:
    def __init__(self, client: httpx.Client, base_url: str) -> None:
        self._client = client
        self._base = base_url

    def get_notifications(self) -> S3NotificationsResponse:
        resp = self._client.get(f"{self._base}/_fakecloud/s3/notifications")
        _check(resp)
        return S3NotificationsResponse.from_dict(resp.json())

    def tick_lifecycle(self) -> LifecycleTickResponse:
        resp = self._client.post(f"{self._base}/_fakecloud/s3/lifecycle-processor/tick")
        _check(resp)
        return LifecycleTickResponse.from_dict(resp.json())


class _SyncDynamoDbClient:
    def __init__(self, client: httpx.Client, base_url: str) -> None:
        self._client = client
        self._base = base_url

    def tick_ttl(self) -> TtlTickResponse:
        resp = self._client.post(f"{self._base}/_fakecloud/dynamodb/ttl-processor/tick")
        _check(resp)
        return TtlTickResponse.from_dict(resp.json())


class _SyncSecretsManagerClient:
    def __init__(self, client: httpx.Client, base_url: str) -> None:
        self._client = client
        self._base = base_url

    def tick_rotation(self) -> RotationTickResponse:
        resp = self._client.post(
            f"{self._base}/_fakecloud/secretsmanager/rotation-scheduler/tick"
        )
        _check(resp)
        return RotationTickResponse.from_dict(resp.json())


class _SyncCognitoClient:
    def __init__(self, client: httpx.Client, base_url: str) -> None:
        self._client = client
        self._base = base_url

    def get_user_codes(self, pool_id: str, username: str) -> UserConfirmationCodes:
        resp = self._client.get(
            f"{self._base}/_fakecloud/cognito/confirmation-codes/{pool_id}/{username}"
        )
        _check(resp)
        return UserConfirmationCodes.from_dict(resp.json())

    def get_confirmation_codes(self) -> ConfirmationCodesResponse:
        resp = self._client.get(f"{self._base}/_fakecloud/cognito/confirmation-codes")
        _check(resp)
        return ConfirmationCodesResponse.from_dict(resp.json())

    def confirm_user(self, req: ConfirmUserRequest) -> ConfirmUserResponse:
        resp = self._client.post(
            f"{self._base}/_fakecloud/cognito/confirm-user",
            json=req.to_dict(),
        )
        _check(resp)
        return ConfirmUserResponse.from_dict(resp.json())

    def get_tokens(self) -> TokensResponse:
        resp = self._client.get(f"{self._base}/_fakecloud/cognito/tokens")
        _check(resp)
        return TokensResponse.from_dict(resp.json())

    def expire_tokens(self, req: ExpireTokensRequest) -> ExpireTokensResponse:
        resp = self._client.post(
            f"{self._base}/_fakecloud/cognito/expire-tokens",
            json=req.to_dict(),
        )
        _check(resp)
        return ExpireTokensResponse.from_dict(resp.json())

    def get_auth_events(self) -> AuthEventsResponse:
        resp = self._client.get(f"{self._base}/_fakecloud/cognito/auth-events")
        _check(resp)
        return AuthEventsResponse.from_dict(resp.json())


class _SyncApiGatewayV2Client:
    def __init__(self, client: httpx.Client, base_url: str) -> None:
        self._client = client
        self._base = base_url

    def get_requests(self) -> ApiGatewayV2RequestsResponse:
        resp = self._client.get(f"{self._base}/_fakecloud/apigatewayv2/requests")
        _check(resp)
        return ApiGatewayV2RequestsResponse.from_dict(resp.json())


class _SyncStepFunctionsClient:
    def __init__(self, client: httpx.Client, base_url: str) -> None:
        self._client = client
        self._base = base_url

    def get_executions(self) -> StepFunctionsExecutionsResponse:
        resp = self._client.get(f"{self._base}/_fakecloud/stepfunctions/executions")
        _check(resp)
        return StepFunctionsExecutionsResponse.from_dict(resp.json())


class _SyncBedrockClient:
    def __init__(self, client: httpx.Client, base_url: str) -> None:
        self._client = client
        self._base = base_url

    def get_invocations(self) -> BedrockInvocationsResponse:
        resp = self._client.get(f"{self._base}/_fakecloud/bedrock/invocations")
        _check(resp)
        return BedrockInvocationsResponse.from_dict(resp.json())

    def set_model_response(
        self, model_id: str, response: str
    ) -> BedrockModelResponseConfig:
        resp = self._client.post(
            f"{self._base}/_fakecloud/bedrock/models/{model_id}/response",
            content=response,
            headers={"Content-Type": "text/plain"},
        )
        _check(resp)
        return BedrockModelResponseConfig.from_dict(resp.json())


# ── Main clients ────────────────────────────────────────────────────


class FakeCloud:
    """Async client for the fakecloud introspection API.

    Usage::

        async with httpx.AsyncClient() as http:
            fc = FakeCloud()
            health = await fc.health()

    The client creates its own ``httpx.AsyncClient`` internally.
    """

    def __init__(self, base_url: str = "http://localhost:4566") -> None:
        self._base = base_url.rstrip("/")
        self._client = httpx.AsyncClient()

    # ── Top-level operations ────────────────────────────────────────

    async def health(self) -> HealthResponse:
        """Check server health."""
        resp = await self._client.get(f"{self._base}/_fakecloud/health")
        _check(resp)
        return HealthResponse.from_dict(resp.json())

    async def reset(self) -> ResetResponse:
        """Reset all service state."""
        resp = await self._client.post(f"{self._base}/_reset")
        _check(resp)
        return ResetResponse.from_dict(resp.json())

    async def reset_service(self, service: str) -> ResetServiceResponse:
        """Reset a single service's state."""
        resp = await self._client.post(f"{self._base}/_fakecloud/reset/{service}")
        _check(resp)
        return ResetServiceResponse.from_dict(resp.json())

    # ── Service sub-clients ─────────────────────────────────────────

    @property
    def lambda_(self) -> LambdaClient:
        """Lambda introspection client.

        Named ``lambda_`` to avoid shadowing Python's ``lambda`` keyword.
        """
        return LambdaClient(self._client, self._base)

    @property
    def rds(self) -> RdsClient:
        return RdsClient(self._client, self._base)

    @property
    def elasticache(self) -> ElastiCacheClient:
        return ElastiCacheClient(self._client, self._base)

    @property
    def ses(self) -> SesClient:
        return SesClient(self._client, self._base)

    @property
    def sns(self) -> SnsClient:
        return SnsClient(self._client, self._base)

    @property
    def sqs(self) -> SqsClient:
        return SqsClient(self._client, self._base)

    @property
    def events(self) -> EventsClient:
        return EventsClient(self._client, self._base)

    @property
    def s3(self) -> S3Client:
        return S3Client(self._client, self._base)

    @property
    def dynamodb(self) -> DynamoDbClient:
        return DynamoDbClient(self._client, self._base)

    @property
    def secretsmanager(self) -> SecretsManagerClient:
        return SecretsManagerClient(self._client, self._base)

    @property
    def cognito(self) -> CognitoClient:
        return CognitoClient(self._client, self._base)

    @property
    def apigatewayv2(self) -> ApiGatewayV2Client:
        return ApiGatewayV2Client(self._client, self._base)

    @property
    def stepfunctions(self) -> StepFunctionsClient:
        return StepFunctionsClient(self._client, self._base)

    @property
    def bedrock(self) -> BedrockClient:
        return BedrockClient(self._client, self._base)

    # ── Lifecycle ───────────────────────────────────────────────────

    async def aclose(self) -> None:
        """Close the underlying HTTP client."""
        await self._client.aclose()

    async def __aenter__(self) -> "FakeCloud":
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.aclose()


class FakeCloudSync:
    """Synchronous client for the fakecloud introspection API.

    Usage::

        fc = FakeCloudSync()
        health = fc.health()
    """

    def __init__(self, base_url: str = "http://localhost:4566") -> None:
        self._base = base_url.rstrip("/")
        self._client = httpx.Client()

    # ── Top-level operations ────────────────────────────────────────

    def health(self) -> HealthResponse:
        resp = self._client.get(f"{self._base}/_fakecloud/health")
        _check(resp)
        return HealthResponse.from_dict(resp.json())

    def reset(self) -> ResetResponse:
        resp = self._client.post(f"{self._base}/_reset")
        _check(resp)
        return ResetResponse.from_dict(resp.json())

    def reset_service(self, service: str) -> ResetServiceResponse:
        resp = self._client.post(f"{self._base}/_fakecloud/reset/{service}")
        _check(resp)
        return ResetServiceResponse.from_dict(resp.json())

    # ── Service sub-clients ─────────────────────────────────────────

    @property
    def lambda_(self) -> _SyncLambdaClient:
        return _SyncLambdaClient(self._client, self._base)

    @property
    def rds(self) -> _SyncRdsClient:
        return _SyncRdsClient(self._client, self._base)

    @property
    def elasticache(self) -> _SyncElastiCacheClient:
        return _SyncElastiCacheClient(self._client, self._base)

    @property
    def ses(self) -> _SyncSesClient:
        return _SyncSesClient(self._client, self._base)

    @property
    def sns(self) -> _SyncSnsClient:
        return _SyncSnsClient(self._client, self._base)

    @property
    def sqs(self) -> _SyncSqsClient:
        return _SyncSqsClient(self._client, self._base)

    @property
    def events(self) -> _SyncEventsClient:
        return _SyncEventsClient(self._client, self._base)

    @property
    def s3(self) -> _SyncS3Client:
        return _SyncS3Client(self._client, self._base)

    @property
    def dynamodb(self) -> _SyncDynamoDbClient:
        return _SyncDynamoDbClient(self._client, self._base)

    @property
    def secretsmanager(self) -> _SyncSecretsManagerClient:
        return _SyncSecretsManagerClient(self._client, self._base)

    @property
    def cognito(self) -> _SyncCognitoClient:
        return _SyncCognitoClient(self._client, self._base)

    @property
    def apigatewayv2(self) -> _SyncApiGatewayV2Client:
        return _SyncApiGatewayV2Client(self._client, self._base)

    @property
    def stepfunctions(self) -> _SyncStepFunctionsClient:
        return _SyncStepFunctionsClient(self._client, self._base)

    @property
    def bedrock(self) -> _SyncBedrockClient:
        return _SyncBedrockClient(self._client, self._base)

    # ── Lifecycle ───────────────────────────────────────────────────

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> "FakeCloudSync":
        return self

    def __exit__(self, *args: object) -> None:
        self.close()


# ── Helpers ─────────────────────────────────────────────────────────


def _check(resp: httpx.Response) -> None:
    """Raise ``FakeCloudError`` on non-2xx responses."""
    if resp.status_code >= 400:
        raise FakeCloudError(resp.status_code, resp.text)
