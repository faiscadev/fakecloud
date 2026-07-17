"""Async and sync clients for the fakecloud introspection API."""

from __future__ import annotations

from typing import Any, Dict, Optional, cast
from urllib.parse import quote as _urlquote

import httpx

from fakecloud.types import (
    AcmCertificateChainInfo,
    ApiGatewayV2ConnectionsResponse,
    ApiGatewayV2RequestsResponse,
    AppAsScheduledTickResponse,
    AppAsTickResponse,
    AthenaNamedQueriesResponse,
    AuthEventsResponse,
    BedrockAgentAgentsResponse,
    BedrockAgentRuntimeInvocationsResponse,
    BedrockFaultRule,
    BedrockFaultsResponse,
    BedrockInvocationsResponse,
    BedrockModelResponseConfig,
    BedrockResponseRule,
    BedrockStatusResponse,
    CloudFrontDistributionsResponse,
    CloudFrontDistributionStatusRequest,
    CloudWatchAlarmsResponse,
    CloudWatchMetricsResponse,
    CompromisedPasswordsRequest,
    CompromisedPasswordsResponse,
    ConfirmationCodesResponse,
    ConfirmSubscriptionRequest,
    ConfirmSubscriptionResponse,
    ConfirmUserRequest,
    ConfirmUserResponse,
    ContainerCredentials,
    CreateAdminResponse,
    DynamoDbSnapshotSaveResponse,
    Ec2InstanceNetworksResponse,
    Ec2InstancesResponse,
    EcrImagesResponse,
    EcrPullThroughRulesResponse,
    EcrRepositoriesResponse,
    EcsClustersResponse,
    EcsEventsResponse,
    EcsMarkFailedRequest,
    EcsTask,
    EcsTaskCredentials,
    EcsTaskLogsResponse,
    EcsTaskMetadataResponse,
    EcsTasksResponse,
    ElastiCacheAclsResponse,
    ElastiCacheClustersResponse,
    ElastiCacheReplicationGroupsResponse,
    ElastiCacheServerlessCachesResponse,
    Elbv2ListenersResponse,
    Elbv2LoadBalancersResponse,
    Elbv2RulesResponse,
    Elbv2TargetGroupsResponse,
    Elbv2WafCountsResponse,
    EventHistoryResponse,
    EvictContainerResponse,
    ExpirationTickResponse,
    ExpireTokensRequest,
    ExpireTokensResponse,
    FailSsmCommandRequest,
    FailSsmCommandResponse,
    FirehoseDeliveryStreamsResponse,
    FireRuleRequest,
    FireRuleResponse,
    FireScheduleResponse,
    ForceDlqResponse,
    GlueCrawlersResponse,
    GlueJobRunsResponse,
    GlueJobsResponse,
    HealthResponse,
    InboundEmailRequest,
    InboundEmailResponse,
    InjectSsmSessionRequest,
    InjectSsmSessionResponse,
    KmsUsageResponse,
    LambdaInvocationsResponse,
    LifecycleTickResponse,
    LogsAnomalyInjectRequest,
    LogsAnomalyInjectResponse,
    LogsDeliveryConfigResponse,
    LogsFieldIndexesResponse,
    MintAuthorizationCodeRequest,
    MintAuthorizationCodeResponse,
    OrganizationsAccountsResponse,
    OrganizationsResponsibilityTransfersResponse,
    PendingConfirmationsResponse,
    PreTokenGenInvocationsResponse,
    RdsInstancesResponse,
    RdsLambdaInvokeRequest,
    RdsLambdaInvokeResponse,
    RdsS3ExportRequest,
    RdsS3ExportResponse,
    RdsS3ImportRequest,
    RdsS3ImportResponse,
    ResetResponse,
    ResetServiceResponse,
    RotationTickResponse,
    Route53DnssecMaterial,
    Route53DnssecSignRequest,
    Route53DnssecSignResponse,
    S3AccessPointsResponse,
    S3NotificationsResponse,
    S3ObjectLambdaResponsesResponse,
    SchedulerSchedulesResponse,
    SesBouncesResponse,
    SesDkimPublicKey,
    SesEmailsResponse,
    SesEventDestinationDeliveriesResponse,
    SesMailFromStatusResponse,
    SesMessageInsightsResponse,
    SesMetrics,
    SesSandboxResponse,
    SesSmtpSubmissionsResponse,
    SetSsmCommandStatusRequest,
    SetSsmCommandStatusResponse,
    SfnEnqueueActivityTaskRequest,
    SfnEnqueueActivityTaskResponse,
    SnsMessagesResponse,
    SnsSmsResponse,
    SqsMessagesResponse,
    SsmParameterPolicyEventsResponse,
    StepFunctionsExecutionsResponse,
    StepFunctionsExecutionTreeResponse,
    StepFunctionsSyncExecutionsResponse,
    TokensResponse,
    TtlTickResponse,
    UserConfirmationCodes,
    WarmContainersResponse,
    WebAuthnCredentialsResponse,
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

    async def download_function_code(
        self,
        account_id: str,
        function_name: str,
        qualifier_or_latest: str = "latest",
    ) -> bytes:
        """Download a function-code zip blob.

        ``qualifier_or_latest`` is ``"latest"`` for the most recent
        publish or a numeric version string. The server file name is
        ``<qualifier>.zip``.
        """
        acct = _urlquote(account_id, safe="")
        name = _urlquote(function_name, safe="")
        qual = _urlquote(qualifier_or_latest, safe="")
        resp = await self._client.get(
            f"{self._base}/_fakecloud/lambda/function-code/{acct}/{name}/{qual}.zip"
        )
        _check(resp)
        return resp.content

    async def download_layer_content(
        self, account_id: str, layer_name: str, version: int
    ) -> bytes:
        """Download a layer-version zip blob."""
        acct = _urlquote(account_id, safe="")
        name = _urlquote(layer_name, safe="")
        resp = await self._client.get(
            f"{self._base}/_fakecloud/lambda/layer-content/{acct}/{name}/{version}.zip"
        )
        _check(resp)
        return resp.content


class RdsClient:
    """Async RDS introspection client."""

    def __init__(self, client: httpx.AsyncClient, base_url: str) -> None:
        self._client = client
        self._base = base_url

    async def get_instances(self) -> RdsInstancesResponse:
        resp = await self._client.get(f"{self._base}/_fakecloud/rds/instances")
        _check(resp)
        return RdsInstancesResponse.from_dict(resp.json())

    async def lambda_invoke(
        self, req: RdsLambdaInvokeRequest
    ) -> RdsLambdaInvokeResponse:
        """Invoke a Lambda function via the RDS ``aws_lambda`` bridge.

        Used internally by the PostgreSQL ``aws_lambda`` extension inside
        RDS containers. The wire format is snake_case to match the
        extension's calling convention.
        """
        resp = await self._client.post(
            f"{self._base}/_fakecloud/rds/lambda-invoke",
            json=req.to_dict(),
        )
        _check(resp)
        return RdsLambdaInvokeResponse.from_dict(resp.json())

    async def s3_import(self, req: RdsS3ImportRequest) -> RdsS3ImportResponse:
        """Fetch an S3 object via the RDS ``aws_s3`` extension bridge."""
        resp = await self._client.post(
            f"{self._base}/_fakecloud/rds/s3-import",
            json=req.to_dict(),
        )
        _check(resp)
        return RdsS3ImportResponse.from_dict(resp.json())

    async def s3_export(self, req: RdsS3ExportRequest) -> RdsS3ExportResponse:
        """Upload an object via the RDS ``aws_s3`` extension bridge."""
        resp = await self._client.post(
            f"{self._base}/_fakecloud/rds/s3-export",
            json=req.to_dict(),
        )
        _check(resp)
        return RdsS3ExportResponse.from_dict(resp.json())


class Ec2Client:
    """Async EC2 introspection client."""

    def __init__(self, client: httpx.AsyncClient, base_url: str) -> None:
        self._client = client
        self._base = base_url

    async def get_instances(self) -> Ec2InstancesResponse:
        resp = await self._client.get(f"{self._base}/_fakecloud/ec2/instances")
        _check(resp)
        return Ec2InstancesResponse.from_dict(resp.json())

    async def get_instance_networks(self) -> Ec2InstanceNetworksResponse:
        """Inspect the real backing network of each EC2 instance — which
        Docker/Podman network or k8s NetworkPolicy backs it, its container IP,
        and whether security-group enforcement is active or degraded."""
        resp = await self._client.get(f"{self._base}/_fakecloud/ec2/instance-networks")
        _check(resp)
        return Ec2InstanceNetworksResponse.from_dict(resp.json())


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

    async def get_elasti_cache_acls(self) -> ElastiCacheAclsResponse:
        resp = await self._client.get(f"{self._base}/_fakecloud/elasticache/acls")
        _check(resp)
        return ElastiCacheAclsResponse.from_dict(resp.json())


class AthenaClient:
    """Async Athena introspection client."""

    def __init__(self, client: httpx.AsyncClient, base_url: str) -> None:
        self._client = client
        self._base = base_url

    async def get_named_queries(self) -> AthenaNamedQueriesResponse:
        """List every named query across workgroups for the default account.

        The response includes a ``last_used_at`` timestamp the server bumps
        each time ``StartQueryExecution`` resolves the query by id.
        """
        resp = await self._client.get(f"{self._base}/_fakecloud/athena/named-queries")
        _check(resp)
        return AthenaNamedQueriesResponse.from_dict(resp.json())


class _SyncAthenaClient:
    """Sync Athena introspection client."""

    def __init__(self, client: httpx.Client, base_url: str) -> None:
        self._client = client
        self._base = base_url

    def get_named_queries(self) -> AthenaNamedQueriesResponse:
        resp = self._client.get(f"{self._base}/_fakecloud/athena/named-queries")
        _check(resp)
        return AthenaNamedQueriesResponse.from_dict(resp.json())


class EcrClient:
    """Async ECR introspection client."""

    def __init__(self, client: httpx.AsyncClient, base_url: str) -> None:
        self._client = client
        self._base = base_url

    async def get_repositories(self) -> EcrRepositoriesResponse:
        resp = await self._client.get(f"{self._base}/_fakecloud/ecr/repositories")
        _check(resp)
        return EcrRepositoriesResponse.from_dict(resp.json())

    async def get_images(
        self, repository_name: Optional[str] = None
    ) -> EcrImagesResponse:
        path = f"{self._base}/_fakecloud/ecr/images"
        if repository_name:
            path += f"?repo={repository_name}"
        resp = await self._client.get(path)
        _check(resp)
        return EcrImagesResponse.from_dict(resp.json())

    async def get_pull_through_rules(self) -> EcrPullThroughRulesResponse:
        resp = await self._client.get(f"{self._base}/_fakecloud/ecr/pull-through-rules")
        _check(resp)
        return EcrPullThroughRulesResponse.from_dict(resp.json())


class EcsClient:
    """Async ECS introspection client."""

    def __init__(self, client: httpx.AsyncClient, base_url: str) -> None:
        self._client = client
        self._base = base_url

    async def get_clusters(self) -> EcsClustersResponse:
        resp = await self._client.get(f"{self._base}/_fakecloud/ecs/clusters")
        _check(resp)
        return EcsClustersResponse.from_dict(resp.json())

    async def get_tasks(
        self,
        cluster: Optional[str] = None,
        status: Optional[str] = None,
    ) -> EcsTasksResponse:
        params: Dict[str, str] = {}
        if cluster is not None:
            params["cluster"] = cluster
        if status is not None:
            params["status"] = status
        resp = await self._client.get(
            f"{self._base}/_fakecloud/ecs/tasks", params=params
        )
        _check(resp)
        return EcsTasksResponse.from_dict(resp.json())

    async def get_task(self, task_id: str) -> EcsTask:
        resp = await self._client.get(f"{self._base}/_fakecloud/ecs/tasks/{task_id}")
        _check(resp)
        return EcsTask.from_dict(resp.json())

    async def get_task_logs(self, task_id: str) -> EcsTaskLogsResponse:
        resp = await self._client.get(
            f"{self._base}/_fakecloud/ecs/tasks/{task_id}/logs"
        )
        _check(resp)
        return EcsTaskLogsResponse.from_dict(resp.json())

    async def force_stop_task(self, task_id: str) -> EcsTask:
        resp = await self._client.post(
            f"{self._base}/_fakecloud/ecs/tasks/{task_id}/force-stop"
        )
        _check(resp)
        return EcsTask.from_dict(resp.json())

    async def mark_task_failed(
        self, task_id: str, req: EcsMarkFailedRequest
    ) -> EcsTask:
        resp = await self._client.post(
            f"{self._base}/_fakecloud/ecs/tasks/{task_id}/mark-failed",
            json=req.to_dict(),
        )
        _check(resp)
        return EcsTask.from_dict(resp.json())

    async def get_events(self) -> EcsEventsResponse:
        resp = await self._client.get(f"{self._base}/_fakecloud/ecs/events")
        _check(resp)
        return EcsEventsResponse.from_dict(resp.json())

    async def get_task_metadata(self, task_arn: str) -> EcsTaskMetadataResponse:
        """Return the v4 metadata-URI dump for the task with the given ARN."""
        encoded = _urlquote(task_arn, safe="")
        resp = await self._client.get(f"{self._base}/_fakecloud/ecs/metadata/{encoded}")
        _check(resp)
        return EcsTaskMetadataResponse.from_dict(resp.json())

    async def get_task_credentials(self, task_id: str) -> EcsTaskCredentials:
        """Fetch the IAM credentials a running ECS task would see at
        ``$AWS_CONTAINER_CREDENTIALS_RELATIVE_URI``."""
        resp = await self._client.get(f"{self._base}/_fakecloud/ecs/creds/{task_id}")
        _check(resp)
        return EcsTaskCredentials.from_dict(resp.json())

    async def get_task_metadata_v3(self, task_id: str) -> Dict[str, Any]:
        """Return the v3 task metadata document. Pass-through dict — the
        shape mirrors the real ECS v3 metadata endpoint."""
        resp = await self._client.get(f"{self._base}/_fakecloud/ecs/v3/{task_id}")
        _check(resp)
        return cast(Dict[str, Any], resp.json())

    async def get_task_metadata_v4(self, task_id: str) -> Dict[str, Any]:
        """Return the v4 task metadata document. Pass-through dict — the
        shape mirrors the real ECS v4 metadata endpoint."""
        resp = await self._client.get(f"{self._base}/_fakecloud/ecs/v4/{task_id}")
        _check(resp)
        return cast(Dict[str, Any], resp.json())


class _SyncEcsClient:
    """Sync ECS introspection client."""

    def __init__(self, client: httpx.Client, base_url: str) -> None:
        self._client = client
        self._base = base_url

    def get_clusters(self) -> EcsClustersResponse:
        resp = self._client.get(f"{self._base}/_fakecloud/ecs/clusters")
        _check(resp)
        return EcsClustersResponse.from_dict(resp.json())

    def get_tasks(
        self,
        cluster: Optional[str] = None,
        status: Optional[str] = None,
    ) -> EcsTasksResponse:
        params: Dict[str, str] = {}
        if cluster is not None:
            params["cluster"] = cluster
        if status is not None:
            params["status"] = status
        resp = self._client.get(f"{self._base}/_fakecloud/ecs/tasks", params=params)
        _check(resp)
        return EcsTasksResponse.from_dict(resp.json())

    def get_task(self, task_id: str) -> EcsTask:
        resp = self._client.get(f"{self._base}/_fakecloud/ecs/tasks/{task_id}")
        _check(resp)
        return EcsTask.from_dict(resp.json())

    def get_task_logs(self, task_id: str) -> EcsTaskLogsResponse:
        resp = self._client.get(f"{self._base}/_fakecloud/ecs/tasks/{task_id}/logs")
        _check(resp)
        return EcsTaskLogsResponse.from_dict(resp.json())

    def force_stop_task(self, task_id: str) -> EcsTask:
        resp = self._client.post(
            f"{self._base}/_fakecloud/ecs/tasks/{task_id}/force-stop"
        )
        _check(resp)
        return EcsTask.from_dict(resp.json())

    def mark_task_failed(self, task_id: str, req: EcsMarkFailedRequest) -> EcsTask:
        resp = self._client.post(
            f"{self._base}/_fakecloud/ecs/tasks/{task_id}/mark-failed",
            json=req.to_dict(),
        )
        _check(resp)
        return EcsTask.from_dict(resp.json())

    def get_events(self) -> EcsEventsResponse:
        resp = self._client.get(f"{self._base}/_fakecloud/ecs/events")
        _check(resp)
        return EcsEventsResponse.from_dict(resp.json())

    def get_task_metadata(self, task_arn: str) -> EcsTaskMetadataResponse:
        """Return the v4 metadata-URI dump for the task with the given ARN."""
        encoded = _urlquote(task_arn, safe="")
        resp = self._client.get(f"{self._base}/_fakecloud/ecs/metadata/{encoded}")
        _check(resp)
        return EcsTaskMetadataResponse.from_dict(resp.json())

    def get_task_credentials(self, task_id: str) -> EcsTaskCredentials:
        resp = self._client.get(f"{self._base}/_fakecloud/ecs/creds/{task_id}")
        _check(resp)
        return EcsTaskCredentials.from_dict(resp.json())

    def get_task_metadata_v3(self, task_id: str) -> Dict[str, Any]:
        resp = self._client.get(f"{self._base}/_fakecloud/ecs/v3/{task_id}")
        _check(resp)
        return cast(Dict[str, Any], resp.json())

    def get_task_metadata_v4(self, task_id: str) -> Dict[str, Any]:
        resp = self._client.get(f"{self._base}/_fakecloud/ecs/v4/{task_id}")
        _check(resp)
        return cast(Dict[str, Any], resp.json())


class Elbv2Client:
    """Async ELBv2 (Elastic Load Balancing v2) introspection client."""

    def __init__(self, client: httpx.AsyncClient, base_url: str) -> None:
        self._client = client
        self._base = base_url

    async def get_load_balancers(self) -> Elbv2LoadBalancersResponse:
        resp = await self._client.get(f"{self._base}/_fakecloud/elbv2/load-balancers")
        _check(resp)
        return Elbv2LoadBalancersResponse.from_dict(resp.json())

    async def get_target_groups(self) -> Elbv2TargetGroupsResponse:
        resp = await self._client.get(f"{self._base}/_fakecloud/elbv2/target-groups")
        _check(resp)
        return Elbv2TargetGroupsResponse.from_dict(resp.json())

    async def get_listeners(self) -> Elbv2ListenersResponse:
        resp = await self._client.get(f"{self._base}/_fakecloud/elbv2/listeners")
        _check(resp)
        return Elbv2ListenersResponse.from_dict(resp.json())

    async def get_rules(self) -> Elbv2RulesResponse:
        resp = await self._client.get(f"{self._base}/_fakecloud/elbv2/rules")
        _check(resp)
        return Elbv2RulesResponse.from_dict(resp.json())

    async def flush_access_logs(self) -> dict[str, Any]:
        """Force every buffered access-log + connection-log line to flush to S3."""
        resp = await self._client.post(
            f"{self._base}/_fakecloud/elbv2/access-logs/flush"
        )
        _check(resp)
        return cast("dict[str, Any]", resp.json())

    async def get_waf_counts(self) -> Elbv2WafCountsResponse:
        """Snapshot the WAF-association count metrics across ALBs."""
        resp = await self._client.get(f"{self._base}/_fakecloud/elbv2/waf-counts")
        _check(resp)
        return Elbv2WafCountsResponse.from_dict(resp.json())


class _SyncElbv2Client:
    """Sync ELBv2 introspection client."""

    def __init__(self, client: httpx.Client, base_url: str) -> None:
        self._client = client
        self._base = base_url

    def get_load_balancers(self) -> Elbv2LoadBalancersResponse:
        resp = self._client.get(f"{self._base}/_fakecloud/elbv2/load-balancers")
        _check(resp)
        return Elbv2LoadBalancersResponse.from_dict(resp.json())

    def get_target_groups(self) -> Elbv2TargetGroupsResponse:
        resp = self._client.get(f"{self._base}/_fakecloud/elbv2/target-groups")
        _check(resp)
        return Elbv2TargetGroupsResponse.from_dict(resp.json())

    def get_listeners(self) -> Elbv2ListenersResponse:
        resp = self._client.get(f"{self._base}/_fakecloud/elbv2/listeners")
        _check(resp)
        return Elbv2ListenersResponse.from_dict(resp.json())

    def get_rules(self) -> Elbv2RulesResponse:
        resp = self._client.get(f"{self._base}/_fakecloud/elbv2/rules")
        _check(resp)
        return Elbv2RulesResponse.from_dict(resp.json())

    def flush_access_logs(self) -> dict[str, Any]:
        """Force every buffered access-log + connection-log line to flush to S3."""
        resp = self._client.post(f"{self._base}/_fakecloud/elbv2/access-logs/flush")
        _check(resp)
        return cast("dict[str, Any]", resp.json())

    def get_waf_counts(self) -> Elbv2WafCountsResponse:
        """Snapshot the WAF-association count metrics across ALBs."""
        resp = self._client.get(f"{self._base}/_fakecloud/elbv2/waf-counts")
        _check(resp)
        return Elbv2WafCountsResponse.from_dict(resp.json())


class Route53Client:
    """Async Route 53 admin client.

    Wraps the per-health-check status admin endpoint that lets tests flip a
    stored health check between healthy and unhealthy without a live prober,
    so failover and multi-value routing can be exercised end-to-end.
    """

    def __init__(self, client: httpx.AsyncClient, base_url: str) -> None:
        self._client = client
        self._base = base_url

    async def set_health_check_status(
        self,
        health_check_id: str,
        status: str,
        reason: Optional[str] = None,
    ) -> None:
        """Flip a health check's reported status.

        ``status`` is one of ``"Success"``, ``"Failure"``, ``"Timeout"``,
        ``"DnsError"``, ``"InsufficientDataPoints"``, ``"Unknown"``.
        ``reason`` is appended to the ``<Status>`` element returned by
        ``GetHealthCheckStatus`` for failure-flavoured statuses
        (``Failure``, ``Timeout``, ``DnsError``); ignored otherwise.
        """
        body: Dict[str, str] = {"status": status}
        if reason is not None:
            body["reason"] = reason
        resp = await self._client.post(
            f"{self._base}/_fakecloud/route53/health-checks/{health_check_id}/status",
            json=body,
        )
        _check(resp)

    async def get_dnssec_material(self, zone_id: str) -> Route53DnssecMaterial:
        """Return the deterministic DNSSEC KSK material for ``zone_id``.

        Raises ``FakeCloudError`` with status 404 if the zone has no
        ACTIVE Key Signing Key.
        """
        resp = await self._client.get(
            f"{self._base}/_fakecloud/route53/zones/{zone_id}/dnssec",
        )
        _check(resp)
        return Route53DnssecMaterial.from_dict(resp.json())

    async def sign_dnssec_rrset(
        self, zone_id: str, req: Route53DnssecSignRequest
    ) -> Route53DnssecSignResponse:
        """Sign an RRset under the zone's first ACTIVE KSK and return the
        raw RRSIG fields. Useful for verifier-side tests."""
        resp = await self._client.post(
            f"{self._base}/_fakecloud/route53/zones/{zone_id}/dnssec/sign",
            json=req.to_dict(),
        )
        _check(resp)
        return Route53DnssecSignResponse.from_dict(resp.json())


class _SyncRoute53Client:
    """Sync Route 53 admin client."""

    def __init__(self, client: httpx.Client, base_url: str) -> None:
        self._client = client
        self._base = base_url

    def set_health_check_status(
        self,
        health_check_id: str,
        status: str,
        reason: Optional[str] = None,
    ) -> None:
        body: Dict[str, str] = {"status": status}
        if reason is not None:
            body["reason"] = reason
        resp = self._client.post(
            f"{self._base}/_fakecloud/route53/health-checks/{health_check_id}/status",
            json=body,
        )
        _check(resp)

    def get_dnssec_material(self, zone_id: str) -> Route53DnssecMaterial:
        resp = self._client.get(
            f"{self._base}/_fakecloud/route53/zones/{zone_id}/dnssec",
        )
        _check(resp)
        return Route53DnssecMaterial.from_dict(resp.json())

    def sign_dnssec_rrset(
        self, zone_id: str, req: Route53DnssecSignRequest
    ) -> Route53DnssecSignResponse:
        resp = self._client.post(
            f"{self._base}/_fakecloud/route53/zones/{zone_id}/dnssec/sign",
            json=req.to_dict(),
        )
        _check(resp)
        return Route53DnssecSignResponse.from_dict(resp.json())


class SsmClient:
    """Async SSM admin client."""

    def __init__(self, client: httpx.AsyncClient, base_url: str) -> None:
        self._client = client
        self._base = base_url

    async def set_command_status(
        self,
        command_id: str,
        status: str,
        account_id: Optional[str] = None,
    ) -> SetSsmCommandStatusResponse:
        """Force a stored ``SendCommand`` command into the given status."""
        req = SetSsmCommandStatusRequest(status=status, account_id=account_id)
        resp = await self._client.post(
            f"{self._base}/_fakecloud/ssm/commands/{_urlquote(command_id, safe='')}/status",  # noqa: E501
            json=req.to_dict(),
        )
        _check(resp)
        return SetSsmCommandStatusResponse.from_dict(resp.json())

    async def fail_command(
        self,
        command_id: str,
        req: Optional[FailSsmCommandRequest] = None,
    ) -> FailSsmCommandResponse:
        """Flip every (or one) invocation on a command to ``Failed``."""
        body = req.to_dict() if req is not None else {}
        resp = await self._client.post(
            f"{self._base}/_fakecloud/ssm/commands/{_urlquote(command_id, safe='')}/fail",  # noqa: E501
            json=body,
        )
        _check(resp)
        return FailSsmCommandResponse.from_dict(resp.json())

    async def get_parameter_policy_events(
        self, account_id: Optional[str] = None
    ) -> SsmParameterPolicyEventsResponse:
        params: Dict[str, str] = {}
        if account_id is not None:
            params["accountId"] = account_id
        resp = await self._client.get(
            f"{self._base}/_fakecloud/ssm/parameter-policy-events", params=params
        )
        _check(resp)
        return SsmParameterPolicyEventsResponse.from_dict(resp.json())

    async def inject_session(
        self, req: InjectSsmSessionRequest
    ) -> InjectSsmSessionResponse:
        """Drop a fake Session Manager record into state."""
        resp = await self._client.post(
            f"{self._base}/_fakecloud/ssm/sessions/inject",
            json=req.to_dict(),
        )
        _check(resp)
        return InjectSsmSessionResponse.from_dict(resp.json())


class _SyncSsmClient:
    """Sync SSM admin client."""

    def __init__(self, client: httpx.Client, base_url: str) -> None:
        self._client = client
        self._base = base_url

    def set_command_status(
        self,
        command_id: str,
        status: str,
        account_id: Optional[str] = None,
    ) -> SetSsmCommandStatusResponse:
        req = SetSsmCommandStatusRequest(status=status, account_id=account_id)
        resp = self._client.post(
            f"{self._base}/_fakecloud/ssm/commands/{_urlquote(command_id, safe='')}/status",  # noqa: E501
            json=req.to_dict(),
        )
        _check(resp)
        return SetSsmCommandStatusResponse.from_dict(resp.json())

    def fail_command(
        self,
        command_id: str,
        req: Optional[FailSsmCommandRequest] = None,
    ) -> FailSsmCommandResponse:
        body = req.to_dict() if req is not None else {}
        resp = self._client.post(
            f"{self._base}/_fakecloud/ssm/commands/{_urlquote(command_id, safe='')}/fail",  # noqa: E501
            json=body,
        )
        _check(resp)
        return FailSsmCommandResponse.from_dict(resp.json())

    def get_parameter_policy_events(
        self, account_id: Optional[str] = None
    ) -> SsmParameterPolicyEventsResponse:
        params: Dict[str, str] = {}
        if account_id is not None:
            params["accountId"] = account_id
        resp = self._client.get(
            f"{self._base}/_fakecloud/ssm/parameter-policy-events", params=params
        )
        _check(resp)
        return SsmParameterPolicyEventsResponse.from_dict(resp.json())

    def inject_session(self, req: InjectSsmSessionRequest) -> InjectSsmSessionResponse:
        resp = self._client.post(
            f"{self._base}/_fakecloud/ssm/sessions/inject",
            json=req.to_dict(),
        )
        _check(resp)
        return InjectSsmSessionResponse.from_dict(resp.json())


class KmsClient:
    """Async KMS introspection client."""

    def __init__(self, client: httpx.AsyncClient, base_url: str) -> None:
        self._client = client
        self._base = base_url

    async def get_usage(self) -> KmsUsageResponse:
        """Snapshot the recorded KMS usage events across services."""
        resp = await self._client.get(f"{self._base}/_fakecloud/kms/usage")
        _check(resp)
        return KmsUsageResponse.from_dict(resp.json())


class _SyncKmsClient:
    """Sync KMS introspection client."""

    def __init__(self, client: httpx.Client, base_url: str) -> None:
        self._client = client
        self._base = base_url

    def get_usage(self) -> KmsUsageResponse:
        resp = self._client.get(f"{self._base}/_fakecloud/kms/usage")
        _check(resp)
        return KmsUsageResponse.from_dict(resp.json())


class WafV2Client:
    """Async WAFv2 admin client."""

    def __init__(self, client: httpx.AsyncClient, base_url: str) -> None:
        self._client = client
        self._base = base_url

    async def evaluate(self, body: Dict[str, Any]) -> Dict[str, Any]:
        """Run a synthetic request through the WAFv2 evaluator.

        Body and response shapes mirror the admin endpoint and are
        passed through as arbitrary dicts.
        """
        resp = await self._client.post(
            f"{self._base}/_fakecloud/wafv2/evaluate", json=body
        )
        _check(resp)
        return cast("Dict[str, Any]", resp.json())


class _SyncWafV2Client:
    """Sync WAFv2 admin client."""

    def __init__(self, client: httpx.Client, base_url: str) -> None:
        self._client = client
        self._base = base_url

    def evaluate(self, body: Dict[str, Any]) -> Dict[str, Any]:
        resp = self._client.post(f"{self._base}/_fakecloud/wafv2/evaluate", json=body)
        _check(resp)
        return cast("Dict[str, Any]", resp.json())


class CloudFrontClient:
    """Async CloudFront admin client."""

    def __init__(self, client: httpx.AsyncClient, base_url: str) -> None:
        self._client = client
        self._base = base_url

    async def get_distributions(self) -> CloudFrontDistributionsResponse:
        """List every stored CloudFront distribution with its reachability.

        Each entry carries the ``<id>.cloudfront.net`` ``domain_name`` to send as
        the ``Host`` header to fakecloud's main endpoint to reach the data plane,
        the ``enabled`` flag, and whether the in-process data plane currently
        ``served`` it (``True`` when enabled and the data plane is not disabled via
        ``FAKECLOUD_CLOUDFRONT_DISABLE_DATAPLANE``).
        """
        resp = await self._client.get(
            f"{self._base}/_fakecloud/cloudfront/distributions"
        )
        _check(resp)
        return CloudFrontDistributionsResponse.from_dict(resp.json())

    async def set_distribution_status(self, distribution_id: str, status: str) -> None:
        """Force a stored CloudFront distribution into a given status.

        Returns ``None`` on success (HTTP 204) and raises
        ``FakeCloudError`` if the distribution does not exist.
        """
        req = CloudFrontDistributionStatusRequest(status=status)
        resp = await self._client.post(
            f"{self._base}/_fakecloud/cloudfront/distributions/{_urlquote(distribution_id, safe='')}/status",  # noqa: E501
            json=req.to_dict(),
        )
        _check(resp)


class _SyncCloudFrontClient:
    """Sync CloudFront admin client."""

    def __init__(self, client: httpx.Client, base_url: str) -> None:
        self._client = client
        self._base = base_url

    def get_distributions(self) -> CloudFrontDistributionsResponse:
        """List every stored CloudFront distribution with its reachability.

        Each entry carries the ``<id>.cloudfront.net`` ``domain_name`` to send as
        the ``Host`` header to fakecloud's main endpoint to reach the data plane,
        the ``enabled`` flag, and whether the in-process data plane currently
        ``served`` it (``True`` when enabled and the data plane is not disabled via
        ``FAKECLOUD_CLOUDFRONT_DISABLE_DATAPLANE``).
        """
        resp = self._client.get(f"{self._base}/_fakecloud/cloudfront/distributions")
        _check(resp)
        return CloudFrontDistributionsResponse.from_dict(resp.json())

    def set_distribution_status(self, distribution_id: str, status: str) -> None:
        req = CloudFrontDistributionStatusRequest(status=status)
        resp = self._client.post(
            f"{self._base}/_fakecloud/cloudfront/distributions/{_urlquote(distribution_id, safe='')}/status",  # noqa: E501
            json=req.to_dict(),
        )
        _check(resp)


def _acm_id(arn_or_id: str) -> str:
    """Extract the trailing UUID from an ACM ARN, or return ``arn_or_id``
    unchanged when no ``certificate/`` segment is present."""
    marker = "certificate/"
    idx = arn_or_id.rfind(marker)
    if idx >= 0:
        return arn_or_id[idx + len(marker) :]
    return arn_or_id


class AcmClient:
    """Async ACM admin client.

    Wraps the per-certificate status admin endpoint that lets tests flip a
    stored certificate between ``PENDING_VALIDATION``, ``ISSUED``,
    ``FAILED``, and ``VALIDATION_TIMED_OUT`` synchronously, without
    waiting on the auto-issue tick.
    """

    def __init__(self, client: httpx.AsyncClient, base_url: str) -> None:
        self._client = client
        self._base = base_url

    async def set_certificate_status(
        self,
        arn_or_id: str,
        status: str,
        reason: Optional[str] = None,
    ) -> None:
        """Flip a certificate's status.

        ``status`` is one of ``"ISSUED"``, ``"FAILED"``, or
        ``"VALIDATION_TIMED_OUT"``. ``reason`` is recorded as
        ``FailureReason`` on ``DescribeCertificate`` for non-``ISSUED``
        statuses; ignored when ``ISSUED``. ``arn_or_id`` accepts either
        the full ACM ARN or the trailing UUID portion.
        """
        body: Dict[str, str] = {"status": status}
        if reason is not None:
            body["reason"] = reason
        resp = await self._client.post(
            f"{self._base}/_fakecloud/acm/certificates/{_acm_id(arn_or_id)}/status",
            json=body,
        )
        _check(resp)

    async def approve_certificate(self, arn_or_id: str) -> None:
        """Approve a ``PENDING_VALIDATION`` certificate.

        Synchronous equivalent of "the user clicked the approval link in
        the validation email" — flips the cert to ``ISSUED`` and
        refreshes its renewal eligibility / RenewalSummary. Used to
        drive the EMAIL validation flow in tests, where the auto-issue
        tick intentionally doesn't fire.
        """
        resp = await self._client.post(
            f"{self._base}/_fakecloud/acm/certificates/{_acm_id(arn_or_id)}/approve",
        )
        _check(resp)

    async def get_certificate_chain_info(
        self, arn_or_id: str
    ) -> AcmCertificateChainInfo:
        """Inspect a stored certificate's PEM block counts and byte sizes.

        Returns the PEM block / byte counts for the certificate and its
        chain plus a constant ``external_ca_validated=False`` marker —
        fakecloud doesn't run a real X.509 verifier, so the field
        documents the emulator gap rather than reporting a verification
        result. Use this to confirm that the chain you uploaded round-
        trips intact, especially for ``ImportCertificate`` flows.
        ``arn_or_id`` accepts the full ACM ARN or the trailing UUID.
        """
        resp = await self._client.get(
            f"{self._base}/_fakecloud/acm/certificates/{_acm_id(arn_or_id)}/chain-info",
        )
        _check(resp)
        return AcmCertificateChainInfo.from_dict(resp.json())


class _SyncAcmClient:
    """Sync ACM admin client."""

    def __init__(self, client: httpx.Client, base_url: str) -> None:
        self._client = client
        self._base = base_url

    def set_certificate_status(
        self,
        arn_or_id: str,
        status: str,
        reason: Optional[str] = None,
    ) -> None:
        body: Dict[str, str] = {"status": status}
        if reason is not None:
            body["reason"] = reason
        resp = self._client.post(
            f"{self._base}/_fakecloud/acm/certificates/{_acm_id(arn_or_id)}/status",
            json=body,
        )
        _check(resp)

    def approve_certificate(self, arn_or_id: str) -> None:
        resp = self._client.post(
            f"{self._base}/_fakecloud/acm/certificates/{_acm_id(arn_or_id)}/approve",
        )
        _check(resp)

    def get_certificate_chain_info(self, arn_or_id: str) -> AcmCertificateChainInfo:
        resp = self._client.get(
            f"{self._base}/_fakecloud/acm/certificates/{_acm_id(arn_or_id)}/chain-info",
        )
        _check(resp)
        return AcmCertificateChainInfo.from_dict(resp.json())


class LogsClient:
    """Async CloudWatch Logs admin/introspection client."""

    def __init__(self, client: httpx.AsyncClient, base_url: str) -> None:
        self._client = client
        self._base = base_url

    async def inject_anomaly(
        self, req: LogsAnomalyInjectRequest
    ) -> LogsAnomalyInjectResponse:
        """Seed a synthetic anomaly for ListAnomalies/UpdateAnomaly tests."""
        resp = await self._client.post(
            f"{self._base}/_fakecloud/logs/anomalies/inject", json=req.to_dict()
        )
        _check(resp)
        return LogsAnomalyInjectResponse.from_dict(resp.json())

    async def get_delivery_config(self) -> LogsDeliveryConfigResponse:
        """Return persisted CloudWatch Logs delivery configurations."""
        resp = await self._client.get(f"{self._base}/_fakecloud/logs/delivery-config")
        _check(resp)
        return LogsDeliveryConfigResponse.from_dict(resp.json())

    async def get_field_indexes(self, log_group_name: str) -> LogsFieldIndexesResponse:
        """Return parsed `Fields` from index policies on a log group."""
        from urllib.parse import quote

        resp = await self._client.get(
            f"{self._base}/_fakecloud/logs/field-indexes/"
            f"{quote(log_group_name, safe='')}"
        )
        _check(resp)
        return LogsFieldIndexesResponse.from_dict(resp.json())


class OrganizationsClient:
    """Async AWS Organizations admin/introspection client.

    Bypasses IAM so tests can assert on org shape without
    management-account credentials.
    """

    def __init__(self, client: httpx.AsyncClient, base_url: str) -> None:
        self._client = client
        self._base = base_url

    async def get_accounts(self) -> OrganizationsAccountsResponse:
        """List every member account with lifecycle state, parent OU,
        tags, and directly-attached SCPs. Returns an empty list when
        no org has been created yet."""
        resp = await self._client.get(f"{self._base}/_fakecloud/organizations/accounts")
        _check(resp)
        return OrganizationsAccountsResponse.from_dict(resp.json())

    async def get_responsibility_transfers(
        self,
    ) -> OrganizationsResponsibilityTransfersResponse:
        """List every billing responsibility transfer in the org, with
        direction (INBOUND/OUTBOUND), lifecycle status, and the active
        handshake. Returns an empty list when no org has been created."""
        resp = await self._client.get(
            f"{self._base}/_fakecloud/organizations/responsibility-transfers"
        )
        _check(resp)
        return OrganizationsResponsibilityTransfersResponse.from_dict(resp.json())


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

    async def get_metrics(self) -> SesMetrics:
        resp = await self._client.get(f"{self._base}/_fakecloud/ses/metrics")
        _check(resp)
        return SesMetrics.from_dict(resp.json())

    async def set_mail_from_status(
        self, identity: str, status: str
    ) -> SesMailFromStatusResponse:
        resp = await self._client.post(
            f"{self._base}/_fakecloud/ses/identities/{identity}/mail-from-status",
            json={"status": status},
        )
        _check(resp)
        return SesMailFromStatusResponse.from_dict(resp.json())

    async def get_dkim_public_key(self, identity: str) -> SesDkimPublicKey:
        resp = await self._client.get(
            f"{self._base}/_fakecloud/ses/identities/{identity}/dkim-public-key"
        )
        _check(resp)
        return SesDkimPublicKey.from_dict(resp.json())

    async def set_sandbox(self, sandbox: bool) -> SesSandboxResponse:
        resp = await self._client.post(
            f"{self._base}/_fakecloud/ses/account/sandbox",
            json={"sandbox": sandbox},
        )
        _check(resp)
        return SesSandboxResponse.from_dict(resp.json())

    async def get_bounces(self) -> SesBouncesResponse:
        resp = await self._client.get(f"{self._base}/_fakecloud/ses/bounces")
        _check(resp)
        return SesBouncesResponse.from_dict(resp.json())

    async def get_message_insights(self, message_id: str) -> SesMessageInsightsResponse:
        resp = await self._client.get(
            f"{self._base}/_fakecloud/ses/messages/{message_id}/insights"
        )
        _check(resp)
        return SesMessageInsightsResponse.from_dict(resp.json())

    async def get_smtp_submissions(self) -> SesSmtpSubmissionsResponse:
        resp = await self._client.get(f"{self._base}/_fakecloud/ses/smtp/submissions")
        _check(resp)
        return SesSmtpSubmissionsResponse.from_dict(resp.json())

    async def get_event_destination_deliveries(
        self,
    ) -> SesEventDestinationDeliveriesResponse:
        resp = await self._client.get(
            f"{self._base}/_fakecloud/ses/event-destinations/deliveries"
        )
        _check(resp)
        return SesEventDestinationDeliveriesResponse.from_dict(resp.json())


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

    async def get_cert_pem(self) -> str:
        """Return the SNS signing certificate as a PEM-encoded string.

        The response body is text (``application/x-pem-file``), not JSON.
        """
        resp = await self._client.get(f"{self._base}/_fakecloud/sns/cert.pem")
        _check(resp)
        return resp.text

    async def get_sms(self) -> SnsSmsResponse:
        """Return all SMS messages the SNS fake has accepted."""
        resp = await self._client.get(f"{self._base}/_fakecloud/sns/sms")
        _check(resp)
        return SnsSmsResponse.from_dict(resp.json())


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


class ApplicationAutoScalingClient:
    """Async Application Auto Scaling watcher introspection client."""

    def __init__(self, client: httpx.AsyncClient, base_url: str) -> None:
        self._client = client
        self._base = base_url

    async def tick(self) -> AppAsTickResponse:
        """Force the watcher to evaluate every scaling policy now.

        Returns the number of policies that applied a capacity change
        on this tick. Useful in tests so callers don't have to wait
        for the wall-clock 15s interval.
        """
        resp = await self._client.post(
            f"{self._base}/_fakecloud/application-autoscaling/tick"
        )
        _check(resp)
        return AppAsTickResponse.from_dict(resp.json())

    async def scheduled_tick(self) -> AppAsScheduledTickResponse:
        """Force the scheduled-action executor to evaluate every action now.

        Returns the number of scheduled actions that fired on this
        tick. Useful in tests so callers don't have to wait for the
        wall-clock 30s interval.
        """
        resp = await self._client.post(
            f"{self._base}/_fakecloud/application-autoscaling/scheduled-tick"
        )
        _check(resp)
        return AppAsScheduledTickResponse.from_dict(resp.json())


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


class SchedulerClient:
    """Async EventBridge Scheduler introspection client."""

    def __init__(self, client: httpx.AsyncClient, base_url: str) -> None:
        self._client = client
        self._base = base_url

    async def get_schedules(self) -> SchedulerSchedulesResponse:
        resp = await self._client.get(f"{self._base}/_fakecloud/scheduler/schedules")
        _check(resp)
        return SchedulerSchedulesResponse.from_dict(resp.json())

    async def fire_schedule(self, group: str, name: str) -> FireScheduleResponse:
        resp = await self._client.post(
            f"{self._base}/_fakecloud/scheduler/fire/{group}/{name}"
        )
        _check(resp)
        return FireScheduleResponse.from_dict(resp.json())


class GlueClient:
    """Async Glue introspection client."""

    def __init__(self, client: httpx.AsyncClient, base_url: str) -> None:
        self._client = client
        self._base = base_url

    async def get_jobs(self) -> GlueJobsResponse:
        resp = await self._client.get(f"{self._base}/_fakecloud/glue/jobs")
        _check(resp)
        return GlueJobsResponse.from_dict(resp.json())

    async def get_job_runs(self, job_name: Optional[str] = None) -> GlueJobRunsResponse:
        params = {"job_name": job_name} if job_name else None
        resp = await self._client.get(
            f"{self._base}/_fakecloud/glue/job-runs", params=params
        )
        _check(resp)
        return GlueJobRunsResponse.from_dict(resp.json())

    async def get_crawlers(self) -> GlueCrawlersResponse:
        resp = await self._client.get(f"{self._base}/_fakecloud/glue/crawlers")
        _check(resp)
        return GlueCrawlersResponse.from_dict(resp.json())


class CloudWatchClient:
    """Async CloudWatch metrics/alarms introspection client."""

    def __init__(self, client: httpx.AsyncClient, base_url: str) -> None:
        self._client = client
        self._base = base_url

    async def get_alarms(self) -> CloudWatchAlarmsResponse:
        resp = await self._client.get(f"{self._base}/_fakecloud/cloudwatch/alarms")
        _check(resp)
        return CloudWatchAlarmsResponse.from_dict(resp.json())

    async def get_metrics(self) -> CloudWatchMetricsResponse:
        resp = await self._client.get(f"{self._base}/_fakecloud/cloudwatch/metrics")
        _check(resp)
        return CloudWatchMetricsResponse.from_dict(resp.json())


class FirehoseClient:
    """Async Firehose delivery-streams introspection client."""

    def __init__(self, client: httpx.AsyncClient, base_url: str) -> None:
        self._client = client
        self._base = base_url

    async def get_delivery_streams(self) -> FirehoseDeliveryStreamsResponse:
        """List every delivery stream across accounts and regions, with
        stream type, lifecycle status, encryption summary, and
        destination count. Sorted by account, then name."""
        resp = await self._client.get(
            f"{self._base}/_fakecloud/firehose/delivery-streams"
        )
        _check(resp)
        return FirehoseDeliveryStreamsResponse.from_dict(resp.json())


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

    async def get_access_points(self) -> S3AccessPointsResponse:
        resp = await self._client.get(f"{self._base}/_fakecloud/s3/access-points")
        _check(resp)
        return S3AccessPointsResponse.from_dict(resp.json())

    async def get_object_lambda_responses(self) -> S3ObjectLambdaResponsesResponse:
        resp = await self._client.get(
            f"{self._base}/_fakecloud/s3/object-lambda-responses"
        )
        _check(resp)
        return S3ObjectLambdaResponsesResponse.from_dict(resp.json())


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

    async def save_snapshot(
        self, data_path: Optional[str] = None
    ) -> DynamoDbSnapshotSaveResponse:
        """Write the current DynamoDB state as a canonical snapshot on demand.

        With ``data_path`` set, the snapshot is written to
        ``<data_path>/dynamodb/snapshot.json``; with ``None`` it is written to
        the server's configured persistent store (an error if none is
        configured).
        """
        body = {"dataPath": data_path} if data_path is not None else None
        resp = await self._client.post(
            f"{self._base}/_fakecloud/dynamodb/snapshot/save", json=body
        )
        _check(resp)
        return DynamoDbSnapshotSaveResponse.from_dict(resp.json())


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

    async def get_pre_token_gen_invocations(
        self,
    ) -> PreTokenGenInvocationsResponse:
        """Return the PreTokenGeneration Lambda trigger invocation log."""
        resp = await self._client.get(
            f"{self._base}/_fakecloud/cognito/pretokengen/invocations"
        )
        _check(resp)
        return PreTokenGenInvocationsResponse.from_dict(resp.json())

    async def mint_authorization_code(
        self, req: MintAuthorizationCodeRequest
    ) -> MintAuthorizationCodeResponse:
        resp = await self._client.post(
            f"{self._base}/_fakecloud/cognito/authorization-codes",
            json=req.to_dict(),
        )
        _check(resp)
        return MintAuthorizationCodeResponse.from_dict(resp.json())

    async def set_compromised_passwords(
        self, req: CompromisedPasswordsRequest
    ) -> CompromisedPasswordsResponse:
        resp = await self._client.post(
            f"{self._base}/_fakecloud/cognito/compromised-passwords",
            json=req.to_dict(),
        )
        _check(resp)
        return CompromisedPasswordsResponse.from_dict(resp.json())

    async def get_webauthn_credentials(self) -> WebAuthnCredentialsResponse:
        resp = await self._client.get(
            f"{self._base}/_fakecloud/cognito/webauthn-credentials"
        )
        _check(resp)
        return WebAuthnCredentialsResponse.from_dict(resp.json())


class ApiGatewayV2Client:
    """Async API Gateway v2 introspection client."""

    def __init__(self, client: httpx.AsyncClient, base_url: str) -> None:
        self._client = client
        self._base = base_url

    async def get_requests(self) -> ApiGatewayV2RequestsResponse:
        resp = await self._client.get(f"{self._base}/_fakecloud/apigatewayv2/requests")
        _check(resp)
        return ApiGatewayV2RequestsResponse.from_dict(resp.json())

    async def get_connections(self) -> ApiGatewayV2ConnectionsResponse:
        """List active WebSocket connections tracked by the API Gateway v2 fake."""
        resp = await self._client.get(
            f"{self._base}/_fakecloud/apigatewayv2/connections"
        )
        _check(resp)
        return ApiGatewayV2ConnectionsResponse.from_dict(resp.json())

    async def get_mtls_info(self, domain_name: str) -> Dict[str, Any]:
        """Return the mTLS trust-store summary for a custom domain.

        The shape is service-internal and may evolve, so this returns a
        pass-through dict rather than a typed dataclass.
        """
        resp = await self._client.get(
            f"{self._base}/_fakecloud/apigatewayv2/domain-names/{domain_name}/mtls-info"
        )
        _check(resp)
        return cast(Dict[str, Any], resp.json())

    def ws_url(self, api_id: str, stage: Optional[str] = None) -> str:
        """Build the WebSocket URL for ``api_id`` at ``stage``.

        Switches the scheme from ``http(s)://`` to ``ws(s)://``, appends
        the server's ``/_fakecloud/apigatewayv2/ws/{api_id}`` path, and
        passes the stage as a query parameter (the server reads it from
        the query string; when omitted the server defaults to
        ``$default``).
        """
        if self._base.startswith("https://"):
            ws = "wss://" + self._base[len("https://") :]
        elif self._base.startswith("http://"):
            ws = "ws://" + self._base[len("http://") :]
        else:
            ws = self._base
        from urllib.parse import quote as _q

        api_id_enc = _q(api_id, safe="")
        if stage is None:
            return f"{ws}/_fakecloud/apigatewayv2/ws/{api_id_enc}"
        return (
            f"{ws}/_fakecloud/apigatewayv2/ws/{api_id_enc}?stage={_q(stage, safe='')}"
        )


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

    async def get_sync_executions(self) -> StepFunctionsSyncExecutionsResponse:
        resp = await self._client.get(
            f"{self._base}/_fakecloud/stepfunctions/sync-executions"
        )
        _check(resp)
        return StepFunctionsSyncExecutionsResponse.from_dict(resp.json())

    async def get_execution_tree(self, arn: str) -> StepFunctionsExecutionTreeResponse:
        encoded = _urlquote(arn, safe="")
        resp = await self._client.get(
            f"{self._base}/_fakecloud/stepfunctions/execution-tree/{encoded}"
        )
        _check(resp)
        return StepFunctionsExecutionTreeResponse.from_dict(resp.json())

    async def enqueue_activity_task(
        self, req: SfnEnqueueActivityTaskRequest
    ) -> SfnEnqueueActivityTaskResponse:
        """Insert a pending task into an activity worker queue without
        running an ASL execution."""
        resp = await self._client.post(
            f"{self._base}/_fakecloud/stepfunctions/enqueue-activity-task",
            json=req.to_dict(),
        )
        _check(resp)
        return SfnEnqueueActivityTaskResponse.from_dict(resp.json())


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

    async def set_response_rules(
        self, model_id: str, rules: list[BedrockResponseRule]
    ) -> BedrockModelResponseConfig:
        """Replace the prompt-conditional response rule list for a model."""
        resp = await self._client.post(
            f"{self._base}/_fakecloud/bedrock/models/{model_id}/responses",
            json={"rules": [r.to_dict() for r in rules]},
        )
        _check(resp)
        return BedrockModelResponseConfig.from_dict(resp.json())

    async def clear_response_rules(self, model_id: str) -> BedrockModelResponseConfig:
        """Clear all prompt-conditional response rules for a model."""
        resp = await self._client.delete(
            f"{self._base}/_fakecloud/bedrock/models/{model_id}/responses",
        )
        _check(resp)
        return BedrockModelResponseConfig.from_dict(resp.json())

    async def queue_fault(self, rule: BedrockFaultRule) -> BedrockStatusResponse:
        """Queue a fault rule for the next matching runtime call(s)."""
        resp = await self._client.post(
            f"{self._base}/_fakecloud/bedrock/faults",
            json=rule.to_dict(),
        )
        _check(resp)
        return BedrockStatusResponse.from_dict(resp.json())

    async def get_faults(self) -> BedrockFaultsResponse:
        resp = await self._client.get(f"{self._base}/_fakecloud/bedrock/faults")
        _check(resp)
        return BedrockFaultsResponse.from_dict(resp.json())

    async def clear_faults(self) -> BedrockStatusResponse:
        resp = await self._client.delete(f"{self._base}/_fakecloud/bedrock/faults")
        _check(resp)
        return BedrockStatusResponse.from_dict(resp.json())


class BedrockAgentClient:
    """Async Bedrock Agent (control plane) introspection sub-client."""

    def __init__(self, client: httpx.AsyncClient, base_url: str) -> None:
        self._client = client
        self._base = base_url

    async def get_agents(self) -> BedrockAgentAgentsResponse:
        resp = await self._client.get(f"{self._base}/_fakecloud/bedrock-agent/agents")
        _check(resp)
        return BedrockAgentAgentsResponse.from_dict(resp.json())


class BedrockAgentRuntimeClient:
    """Async Bedrock Agent Runtime (data plane) introspection sub-client."""

    def __init__(self, client: httpx.AsyncClient, base_url: str) -> None:
        self._client = client
        self._base = base_url

    async def get_invocations(self) -> BedrockAgentRuntimeInvocationsResponse:
        resp = await self._client.get(
            f"{self._base}/_fakecloud/bedrock-agent-runtime/invocations"
        )
        _check(resp)
        return BedrockAgentRuntimeInvocationsResponse.from_dict(resp.json())


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

    def download_function_code(
        self,
        account_id: str,
        function_name: str,
        qualifier_or_latest: str = "latest",
    ) -> bytes:
        acct = _urlquote(account_id, safe="")
        name = _urlquote(function_name, safe="")
        qual = _urlquote(qualifier_or_latest, safe="")
        resp = self._client.get(
            f"{self._base}/_fakecloud/lambda/function-code/{acct}/{name}/{qual}.zip"
        )
        _check(resp)
        return resp.content

    def download_layer_content(
        self, account_id: str, layer_name: str, version: int
    ) -> bytes:
        acct = _urlquote(account_id, safe="")
        name = _urlquote(layer_name, safe="")
        resp = self._client.get(
            f"{self._base}/_fakecloud/lambda/layer-content/{acct}/{name}/{version}.zip"
        )
        _check(resp)
        return resp.content


class _SyncRdsClient:
    """Sync RDS introspection client."""

    def __init__(self, client: httpx.Client, base_url: str) -> None:
        self._client = client
        self._base = base_url

    def get_instances(self) -> RdsInstancesResponse:
        resp = self._client.get(f"{self._base}/_fakecloud/rds/instances")
        _check(resp)
        return RdsInstancesResponse.from_dict(resp.json())

    def lambda_invoke(self, req: RdsLambdaInvokeRequest) -> RdsLambdaInvokeResponse:
        resp = self._client.post(
            f"{self._base}/_fakecloud/rds/lambda-invoke",
            json=req.to_dict(),
        )
        _check(resp)
        return RdsLambdaInvokeResponse.from_dict(resp.json())

    def s3_import(self, req: RdsS3ImportRequest) -> RdsS3ImportResponse:
        resp = self._client.post(
            f"{self._base}/_fakecloud/rds/s3-import",
            json=req.to_dict(),
        )
        _check(resp)
        return RdsS3ImportResponse.from_dict(resp.json())

    def s3_export(self, req: RdsS3ExportRequest) -> RdsS3ExportResponse:
        resp = self._client.post(
            f"{self._base}/_fakecloud/rds/s3-export",
            json=req.to_dict(),
        )
        _check(resp)
        return RdsS3ExportResponse.from_dict(resp.json())


class _SyncEc2Client:
    """Sync EC2 introspection client."""

    def __init__(self, client: httpx.Client, base_url: str) -> None:
        self._client = client
        self._base = base_url

    def get_instances(self) -> Ec2InstancesResponse:
        resp = self._client.get(f"{self._base}/_fakecloud/ec2/instances")
        _check(resp)
        return Ec2InstancesResponse.from_dict(resp.json())

    def get_instance_networks(self) -> Ec2InstanceNetworksResponse:
        """Inspect the real backing network of each EC2 instance — which
        Docker/Podman network or k8s NetworkPolicy backs it, its container IP,
        and whether security-group enforcement is active or degraded."""
        resp = self._client.get(f"{self._base}/_fakecloud/ec2/instance-networks")
        _check(resp)
        return Ec2InstanceNetworksResponse.from_dict(resp.json())


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

    def get_elasti_cache_acls(self) -> ElastiCacheAclsResponse:
        resp = self._client.get(f"{self._base}/_fakecloud/elasticache/acls")
        _check(resp)
        return ElastiCacheAclsResponse.from_dict(resp.json())


class _SyncLogsClient:
    def __init__(self, client: httpx.Client, base_url: str) -> None:
        self._client = client
        self._base = base_url

    def inject_anomaly(
        self, req: LogsAnomalyInjectRequest
    ) -> LogsAnomalyInjectResponse:
        resp = self._client.post(
            f"{self._base}/_fakecloud/logs/anomalies/inject", json=req.to_dict()
        )
        _check(resp)
        return LogsAnomalyInjectResponse.from_dict(resp.json())

    def get_delivery_config(self) -> LogsDeliveryConfigResponse:
        resp = self._client.get(f"{self._base}/_fakecloud/logs/delivery-config")
        _check(resp)
        return LogsDeliveryConfigResponse.from_dict(resp.json())

    def get_field_indexes(self, log_group_name: str) -> LogsFieldIndexesResponse:
        from urllib.parse import quote

        resp = self._client.get(
            f"{self._base}/_fakecloud/logs/field-indexes/"
            f"{quote(log_group_name, safe='')}"
        )
        _check(resp)
        return LogsFieldIndexesResponse.from_dict(resp.json())


class _SyncOrganizationsClient:
    def __init__(self, client: httpx.Client, base_url: str) -> None:
        self._client = client
        self._base = base_url

    def get_accounts(self) -> OrganizationsAccountsResponse:
        resp = self._client.get(f"{self._base}/_fakecloud/organizations/accounts")
        _check(resp)
        return OrganizationsAccountsResponse.from_dict(resp.json())

    def get_responsibility_transfers(
        self,
    ) -> OrganizationsResponsibilityTransfersResponse:
        resp = self._client.get(
            f"{self._base}/_fakecloud/organizations/responsibility-transfers"
        )
        _check(resp)
        return OrganizationsResponsibilityTransfersResponse.from_dict(resp.json())


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

    def get_metrics(self) -> SesMetrics:
        resp = self._client.get(f"{self._base}/_fakecloud/ses/metrics")
        _check(resp)
        return SesMetrics.from_dict(resp.json())

    def set_mail_from_status(
        self, identity: str, status: str
    ) -> SesMailFromStatusResponse:
        resp = self._client.post(
            f"{self._base}/_fakecloud/ses/identities/{identity}/mail-from-status",
            json={"status": status},
        )
        _check(resp)
        return SesMailFromStatusResponse.from_dict(resp.json())

    def get_dkim_public_key(self, identity: str) -> SesDkimPublicKey:
        resp = self._client.get(
            f"{self._base}/_fakecloud/ses/identities/{identity}/dkim-public-key"
        )
        _check(resp)
        return SesDkimPublicKey.from_dict(resp.json())

    def set_sandbox(self, sandbox: bool) -> SesSandboxResponse:
        resp = self._client.post(
            f"{self._base}/_fakecloud/ses/account/sandbox",
            json={"sandbox": sandbox},
        )
        _check(resp)
        return SesSandboxResponse.from_dict(resp.json())

    def get_bounces(self) -> SesBouncesResponse:
        resp = self._client.get(f"{self._base}/_fakecloud/ses/bounces")
        _check(resp)
        return SesBouncesResponse.from_dict(resp.json())

    def get_message_insights(self, message_id: str) -> SesMessageInsightsResponse:
        resp = self._client.get(
            f"{self._base}/_fakecloud/ses/messages/{message_id}/insights"
        )
        _check(resp)
        return SesMessageInsightsResponse.from_dict(resp.json())

    def get_smtp_submissions(self) -> SesSmtpSubmissionsResponse:
        resp = self._client.get(f"{self._base}/_fakecloud/ses/smtp/submissions")
        _check(resp)
        return SesSmtpSubmissionsResponse.from_dict(resp.json())

    def get_event_destination_deliveries(
        self,
    ) -> SesEventDestinationDeliveriesResponse:
        resp = self._client.get(
            f"{self._base}/_fakecloud/ses/event-destinations/deliveries"
        )
        _check(resp)
        return SesEventDestinationDeliveriesResponse.from_dict(resp.json())


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

    def get_cert_pem(self) -> str:
        resp = self._client.get(f"{self._base}/_fakecloud/sns/cert.pem")
        _check(resp)
        return resp.text

    def get_sms(self) -> SnsSmsResponse:
        resp = self._client.get(f"{self._base}/_fakecloud/sns/sms")
        _check(resp)
        return SnsSmsResponse.from_dict(resp.json())


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


class _SyncApplicationAutoScalingClient:
    def __init__(self, client: httpx.Client, base_url: str) -> None:
        self._client = client
        self._base = base_url

    def tick(self) -> AppAsTickResponse:
        resp = self._client.post(
            f"{self._base}/_fakecloud/application-autoscaling/tick"
        )
        _check(resp)
        return AppAsTickResponse.from_dict(resp.json())

    def scheduled_tick(self) -> AppAsScheduledTickResponse:
        resp = self._client.post(
            f"{self._base}/_fakecloud/application-autoscaling/scheduled-tick"
        )
        _check(resp)
        return AppAsScheduledTickResponse.from_dict(resp.json())


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


class _SyncSchedulerClient:
    def __init__(self, client: httpx.Client, base_url: str) -> None:
        self._client = client
        self._base = base_url

    def get_schedules(self) -> SchedulerSchedulesResponse:
        resp = self._client.get(f"{self._base}/_fakecloud/scheduler/schedules")
        _check(resp)
        return SchedulerSchedulesResponse.from_dict(resp.json())

    def fire_schedule(self, group: str, name: str) -> FireScheduleResponse:
        resp = self._client.post(
            f"{self._base}/_fakecloud/scheduler/fire/{group}/{name}"
        )
        _check(resp)
        return FireScheduleResponse.from_dict(resp.json())


class _SyncGlueClient:
    def __init__(self, client: httpx.Client, base_url: str) -> None:
        self._client = client
        self._base = base_url

    def get_jobs(self) -> GlueJobsResponse:
        resp = self._client.get(f"{self._base}/_fakecloud/glue/jobs")
        _check(resp)
        return GlueJobsResponse.from_dict(resp.json())

    def get_job_runs(self, job_name: Optional[str] = None) -> GlueJobRunsResponse:
        params = {"job_name": job_name} if job_name else None
        resp = self._client.get(f"{self._base}/_fakecloud/glue/job-runs", params=params)
        _check(resp)
        return GlueJobRunsResponse.from_dict(resp.json())

    def get_crawlers(self) -> GlueCrawlersResponse:
        resp = self._client.get(f"{self._base}/_fakecloud/glue/crawlers")
        _check(resp)
        return GlueCrawlersResponse.from_dict(resp.json())


class _SyncCloudWatchClient:
    def __init__(self, client: httpx.Client, base_url: str) -> None:
        self._client = client
        self._base = base_url

    def get_alarms(self) -> CloudWatchAlarmsResponse:
        resp = self._client.get(f"{self._base}/_fakecloud/cloudwatch/alarms")
        _check(resp)
        return CloudWatchAlarmsResponse.from_dict(resp.json())

    def get_metrics(self) -> CloudWatchMetricsResponse:
        resp = self._client.get(f"{self._base}/_fakecloud/cloudwatch/metrics")
        _check(resp)
        return CloudWatchMetricsResponse.from_dict(resp.json())


class _SyncFirehoseClient:
    def __init__(self, client: httpx.Client, base_url: str) -> None:
        self._client = client
        self._base = base_url

    def get_delivery_streams(self) -> FirehoseDeliveryStreamsResponse:
        resp = self._client.get(f"{self._base}/_fakecloud/firehose/delivery-streams")
        _check(resp)
        return FirehoseDeliveryStreamsResponse.from_dict(resp.json())


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

    def get_access_points(self) -> S3AccessPointsResponse:
        resp = self._client.get(f"{self._base}/_fakecloud/s3/access-points")
        _check(resp)
        return S3AccessPointsResponse.from_dict(resp.json())

    def get_object_lambda_responses(self) -> S3ObjectLambdaResponsesResponse:
        resp = self._client.get(f"{self._base}/_fakecloud/s3/object-lambda-responses")
        _check(resp)
        return S3ObjectLambdaResponsesResponse.from_dict(resp.json())


class _SyncDynamoDbClient:
    def __init__(self, client: httpx.Client, base_url: str) -> None:
        self._client = client
        self._base = base_url

    def tick_ttl(self) -> TtlTickResponse:
        resp = self._client.post(f"{self._base}/_fakecloud/dynamodb/ttl-processor/tick")
        _check(resp)
        return TtlTickResponse.from_dict(resp.json())

    def save_snapshot(
        self, data_path: Optional[str] = None
    ) -> DynamoDbSnapshotSaveResponse:
        """Write the current DynamoDB state as a canonical snapshot on demand.

        With ``data_path`` set, the snapshot is written to
        ``<data_path>/dynamodb/snapshot.json``; with ``None`` it is written to
        the server's configured persistent store (an error if none is
        configured).
        """
        body = {"dataPath": data_path} if data_path is not None else None
        resp = self._client.post(
            f"{self._base}/_fakecloud/dynamodb/snapshot/save", json=body
        )
        _check(resp)
        return DynamoDbSnapshotSaveResponse.from_dict(resp.json())


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

    def get_pre_token_gen_invocations(self) -> PreTokenGenInvocationsResponse:
        """Return the PreTokenGeneration Lambda trigger invocation log."""
        resp = self._client.get(
            f"{self._base}/_fakecloud/cognito/pretokengen/invocations"
        )
        _check(resp)
        return PreTokenGenInvocationsResponse.from_dict(resp.json())

    def mint_authorization_code(
        self, req: MintAuthorizationCodeRequest
    ) -> MintAuthorizationCodeResponse:
        resp = self._client.post(
            f"{self._base}/_fakecloud/cognito/authorization-codes",
            json=req.to_dict(),
        )
        _check(resp)
        return MintAuthorizationCodeResponse.from_dict(resp.json())

    def set_compromised_passwords(
        self, req: CompromisedPasswordsRequest
    ) -> CompromisedPasswordsResponse:
        resp = self._client.post(
            f"{self._base}/_fakecloud/cognito/compromised-passwords",
            json=req.to_dict(),
        )
        _check(resp)
        return CompromisedPasswordsResponse.from_dict(resp.json())

    def get_webauthn_credentials(self) -> WebAuthnCredentialsResponse:
        resp = self._client.get(f"{self._base}/_fakecloud/cognito/webauthn-credentials")
        _check(resp)
        return WebAuthnCredentialsResponse.from_dict(resp.json())


class _SyncApiGatewayV2Client:
    def __init__(self, client: httpx.Client, base_url: str) -> None:
        self._client = client
        self._base = base_url

    def get_requests(self) -> ApiGatewayV2RequestsResponse:
        resp = self._client.get(f"{self._base}/_fakecloud/apigatewayv2/requests")
        _check(resp)
        return ApiGatewayV2RequestsResponse.from_dict(resp.json())

    def get_connections(self) -> ApiGatewayV2ConnectionsResponse:
        resp = self._client.get(f"{self._base}/_fakecloud/apigatewayv2/connections")
        _check(resp)
        return ApiGatewayV2ConnectionsResponse.from_dict(resp.json())

    def get_mtls_info(self, domain_name: str) -> Dict[str, Any]:
        resp = self._client.get(
            f"{self._base}/_fakecloud/apigatewayv2/domain-names/{domain_name}/mtls-info"
        )
        _check(resp)
        return cast(Dict[str, Any], resp.json())

    def ws_url(self, api_id: str, stage: Optional[str] = None) -> str:
        if self._base.startswith("https://"):
            ws = "wss://" + self._base[len("https://") :]
        elif self._base.startswith("http://"):
            ws = "ws://" + self._base[len("http://") :]
        else:
            ws = self._base
        from urllib.parse import quote as _q

        api_id_enc = _q(api_id, safe="")
        if stage is None:
            return f"{ws}/_fakecloud/apigatewayv2/ws/{api_id_enc}"
        return (
            f"{ws}/_fakecloud/apigatewayv2/ws/{api_id_enc}?stage={_q(stage, safe='')}"
        )


class _SyncStepFunctionsClient:
    def __init__(self, client: httpx.Client, base_url: str) -> None:
        self._client = client
        self._base = base_url

    def get_executions(self) -> StepFunctionsExecutionsResponse:
        resp = self._client.get(f"{self._base}/_fakecloud/stepfunctions/executions")
        _check(resp)
        return StepFunctionsExecutionsResponse.from_dict(resp.json())

    def get_sync_executions(self) -> StepFunctionsSyncExecutionsResponse:
        resp = self._client.get(
            f"{self._base}/_fakecloud/stepfunctions/sync-executions"
        )
        _check(resp)
        return StepFunctionsSyncExecutionsResponse.from_dict(resp.json())

    def get_execution_tree(self, arn: str) -> StepFunctionsExecutionTreeResponse:
        encoded = _urlquote(arn, safe="")
        resp = self._client.get(
            f"{self._base}/_fakecloud/stepfunctions/execution-tree/{encoded}"
        )
        _check(resp)
        return StepFunctionsExecutionTreeResponse.from_dict(resp.json())

    def enqueue_activity_task(
        self, req: SfnEnqueueActivityTaskRequest
    ) -> SfnEnqueueActivityTaskResponse:
        resp = self._client.post(
            f"{self._base}/_fakecloud/stepfunctions/enqueue-activity-task",
            json=req.to_dict(),
        )
        _check(resp)
        return SfnEnqueueActivityTaskResponse.from_dict(resp.json())


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

    def set_response_rules(
        self, model_id: str, rules: list[BedrockResponseRule]
    ) -> BedrockModelResponseConfig:
        resp = self._client.post(
            f"{self._base}/_fakecloud/bedrock/models/{model_id}/responses",
            json={"rules": [r.to_dict() for r in rules]},
        )
        _check(resp)
        return BedrockModelResponseConfig.from_dict(resp.json())

    def clear_response_rules(self, model_id: str) -> BedrockModelResponseConfig:
        resp = self._client.delete(
            f"{self._base}/_fakecloud/bedrock/models/{model_id}/responses",
        )
        _check(resp)
        return BedrockModelResponseConfig.from_dict(resp.json())

    def queue_fault(self, rule: BedrockFaultRule) -> BedrockStatusResponse:
        resp = self._client.post(
            f"{self._base}/_fakecloud/bedrock/faults",
            json=rule.to_dict(),
        )
        _check(resp)
        return BedrockStatusResponse.from_dict(resp.json())

    def get_faults(self) -> BedrockFaultsResponse:
        resp = self._client.get(f"{self._base}/_fakecloud/bedrock/faults")
        _check(resp)
        return BedrockFaultsResponse.from_dict(resp.json())

    def clear_faults(self) -> BedrockStatusResponse:
        resp = self._client.delete(f"{self._base}/_fakecloud/bedrock/faults")
        _check(resp)
        return BedrockStatusResponse.from_dict(resp.json())


class _SyncBedrockAgentClient:
    def __init__(self, client: httpx.Client, base_url: str) -> None:
        self._client = client
        self._base = base_url

    def get_agents(self) -> BedrockAgentAgentsResponse:
        resp = self._client.get(f"{self._base}/_fakecloud/bedrock-agent/agents")
        _check(resp)
        return BedrockAgentAgentsResponse.from_dict(resp.json())


class _SyncBedrockAgentRuntimeClient:
    def __init__(self, client: httpx.Client, base_url: str) -> None:
        self._client = client
        self._base = base_url

    def get_invocations(self) -> BedrockAgentRuntimeInvocationsResponse:
        resp = self._client.get(
            f"{self._base}/_fakecloud/bedrock-agent-runtime/invocations"
        )
        _check(resp)
        return BedrockAgentRuntimeInvocationsResponse.from_dict(resp.json())


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

    async def credentials(self) -> ContainerCredentials:
        """Fetch temporary credentials from the general-purpose
        container/instance credential endpoint
        (``GET /_fakecloud/credentials``).

        This is the same JSON an app's AWS SDK fetches when
        ``AWS_CONTAINER_CREDENTIALS_FULL_URI`` points at fakecloud, letting a
        real binary that expects an instance/task role resolve the default
        credential chain locally with no code change.
        """
        resp = await self._client.get(f"{self._base}/_fakecloud/credentials")
        _check(resp)
        return ContainerCredentials.from_dict(resp.json())

    async def create_admin(
        self, account_id: str, user_name: str
    ) -> CreateAdminResponse:
        """Create an IAM admin user in a specific account."""
        resp = await self._client.post(
            f"{self._base}/_fakecloud/iam/create-admin",
            json={"accountId": account_id, "userName": user_name},
        )
        _check(resp)
        return CreateAdminResponse.from_dict(resp.json())

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
    def ec2(self) -> Ec2Client:
        return Ec2Client(self._client, self._base)

    @property
    def elasticache(self) -> ElastiCacheClient:
        return ElastiCacheClient(self._client, self._base)

    @property
    def ecr(self) -> EcrClient:
        return EcrClient(self._client, self._base)

    @property
    def logs(self) -> LogsClient:
        return LogsClient(self._client, self._base)

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
    def scheduler(self) -> SchedulerClient:
        return SchedulerClient(self._client, self._base)

    @property
    def glue(self) -> GlueClient:
        return GlueClient(self._client, self._base)

    @property
    def cloudwatch(self) -> CloudWatchClient:
        return CloudWatchClient(self._client, self._base)

    @property
    def firehose(self) -> FirehoseClient:
        return FirehoseClient(self._client, self._base)

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

    @property
    def bedrock_agent(self) -> BedrockAgentClient:
        return BedrockAgentClient(self._client, self._base)

    @property
    def bedrock_agent_runtime(self) -> BedrockAgentRuntimeClient:
        return BedrockAgentRuntimeClient(self._client, self._base)

    @property
    def ecs(self) -> EcsClient:
        return EcsClient(self._client, self._base)

    @property
    def elbv2(self) -> Elbv2Client:
        return Elbv2Client(self._client, self._base)

    @property
    def route53(self) -> Route53Client:
        return Route53Client(self._client, self._base)

    @property
    def ssm(self) -> SsmClient:
        return SsmClient(self._client, self._base)

    @property
    def kms(self) -> KmsClient:
        return KmsClient(self._client, self._base)

    @property
    def wafv2(self) -> WafV2Client:
        return WafV2Client(self._client, self._base)

    @property
    def cloudfront(self) -> CloudFrontClient:
        return CloudFrontClient(self._client, self._base)

    @property
    def acm(self) -> AcmClient:
        return AcmClient(self._client, self._base)

    @property
    def application_autoscaling(self) -> ApplicationAutoScalingClient:
        return ApplicationAutoScalingClient(self._client, self._base)

    @property
    def athena(self) -> AthenaClient:
        return AthenaClient(self._client, self._base)

    def organizations(self) -> OrganizationsClient:
        return OrganizationsClient(self._client, self._base)

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

    def credentials(self) -> ContainerCredentials:
        """Fetch temporary credentials from the general-purpose
        container/instance credential endpoint
        (``GET /_fakecloud/credentials``)."""
        resp = self._client.get(f"{self._base}/_fakecloud/credentials")
        _check(resp)
        return ContainerCredentials.from_dict(resp.json())

    def create_admin(self, account_id: str, user_name: str) -> CreateAdminResponse:
        """Create an IAM admin user in a specific account."""
        resp = self._client.post(
            f"{self._base}/_fakecloud/iam/create-admin",
            json={"accountId": account_id, "userName": user_name},
        )
        _check(resp)
        return CreateAdminResponse.from_dict(resp.json())

    # ── Service sub-clients ─────────────────────────────────────────

    @property
    def lambda_(self) -> _SyncLambdaClient:
        return _SyncLambdaClient(self._client, self._base)

    @property
    def rds(self) -> _SyncRdsClient:
        return _SyncRdsClient(self._client, self._base)

    @property
    def ec2(self) -> _SyncEc2Client:
        return _SyncEc2Client(self._client, self._base)

    @property
    def elasticache(self) -> _SyncElastiCacheClient:
        return _SyncElastiCacheClient(self._client, self._base)

    @property
    def logs(self) -> _SyncLogsClient:
        return _SyncLogsClient(self._client, self._base)

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
    def scheduler(self) -> _SyncSchedulerClient:
        return _SyncSchedulerClient(self._client, self._base)

    @property
    def glue(self) -> _SyncGlueClient:
        return _SyncGlueClient(self._client, self._base)

    @property
    def cloudwatch(self) -> _SyncCloudWatchClient:
        return _SyncCloudWatchClient(self._client, self._base)

    @property
    def firehose(self) -> _SyncFirehoseClient:
        return _SyncFirehoseClient(self._client, self._base)

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

    @property
    def bedrock_agent(self) -> _SyncBedrockAgentClient:
        return _SyncBedrockAgentClient(self._client, self._base)

    @property
    def bedrock_agent_runtime(self) -> _SyncBedrockAgentRuntimeClient:
        return _SyncBedrockAgentRuntimeClient(self._client, self._base)

    @property
    def ecs(self) -> _SyncEcsClient:
        return _SyncEcsClient(self._client, self._base)

    @property
    def elbv2(self) -> _SyncElbv2Client:
        return _SyncElbv2Client(self._client, self._base)

    @property
    def route53(self) -> _SyncRoute53Client:
        return _SyncRoute53Client(self._client, self._base)

    @property
    def ssm(self) -> _SyncSsmClient:
        return _SyncSsmClient(self._client, self._base)

    @property
    def kms(self) -> _SyncKmsClient:
        return _SyncKmsClient(self._client, self._base)

    @property
    def wafv2(self) -> _SyncWafV2Client:
        return _SyncWafV2Client(self._client, self._base)

    @property
    def cloudfront(self) -> _SyncCloudFrontClient:
        return _SyncCloudFrontClient(self._client, self._base)

    @property
    def acm(self) -> _SyncAcmClient:
        return _SyncAcmClient(self._client, self._base)

    @property
    def application_autoscaling(self) -> _SyncApplicationAutoScalingClient:
        return _SyncApplicationAutoScalingClient(self._client, self._base)

    @property
    def athena(self) -> _SyncAthenaClient:
        return _SyncAthenaClient(self._client, self._base)

    def organizations(self) -> _SyncOrganizationsClient:
        return _SyncOrganizationsClient(self._client, self._base)

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
