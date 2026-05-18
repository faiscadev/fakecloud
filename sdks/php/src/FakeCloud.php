<?php

declare(strict_types=1);

namespace FakeCloud;

/**
 * Top-level client for the fakecloud introspection and simulation API.
 *
 * ```php
 * $fc = new FakeCloud('http://localhost:4566');
 * $fc->reset();
 * $emails = $fc->ses()->getEmails()->emails;
 * ```
 */
final class FakeCloud
{
    private const DEFAULT_BASE_URL = 'http://localhost:4566';

    private HttpTransport $http;
    private LambdaClient $lambda;
    private RdsClient $rds;
    private ElastiCacheClient $elasticache;
    private EcrClient $ecr;
    private LogsClient $logs;
    private SesClient $ses;
    private SnsClient $sns;
    private SqsClient $sqs;
    private EventsClient $events;
    private SchedulerClient $scheduler;
    private GlueClient $glue;
    private S3Client $s3;
    private DynamoDbClient $dynamodb;
    private SecretsManagerClient $secretsmanager;
    private CognitoClient $cognito;
    private ApiGatewayV2Client $apigatewayv2;
    private StepFunctionsClient $stepfunctions;
    private BedrockClient $bedrock;
    private BedrockAgentClient $bedrockAgent;
    private BedrockAgentRuntimeClient $bedrockAgentRuntime;
    private EcsClient $ecs;
    private Elbv2Client $elbv2;
    private Route53Client $route53;
    private AcmClient $acm;
    private ApplicationAutoScalingClient $applicationAutoscaling;
    private AthenaClient $athena;
    private OrganizationsClient $organizations;
    private SsmClient $ssm;
    private KmsClient $kms;
    private WafV2Client $wafv2;
    private CloudFrontClient $cloudfront;

    public function __construct(string $baseUrl = self::DEFAULT_BASE_URL)
    {
        $this->http = new HttpTransport($baseUrl);
        $this->lambda = new LambdaClient($this->http);
        $this->rds = new RdsClient($this->http);
        $this->elasticache = new ElastiCacheClient($this->http);
        $this->ecr = new EcrClient($this->http);
        $this->logs = new LogsClient($this->http);
        $this->ses = new SesClient($this->http);
        $this->sns = new SnsClient($this->http);
        $this->sqs = new SqsClient($this->http);
        $this->events = new EventsClient($this->http);
        $this->scheduler = new SchedulerClient($this->http);
        $this->glue = new GlueClient($this->http);
        $this->s3 = new S3Client($this->http);
        $this->dynamodb = new DynamoDbClient($this->http);
        $this->secretsmanager = new SecretsManagerClient($this->http);
        $this->cognito = new CognitoClient($this->http);
        $this->apigatewayv2 = new ApiGatewayV2Client($this->http);
        $this->stepfunctions = new StepFunctionsClient($this->http);
        $this->bedrock = new BedrockClient($this->http);
        $this->bedrockAgent = new BedrockAgentClient($this->http);
        $this->bedrockAgentRuntime = new BedrockAgentRuntimeClient($this->http);
        $this->ecs = new EcsClient($this->http);
        $this->elbv2 = new Elbv2Client($this->http);
        $this->route53 = new Route53Client($this->http);
        $this->acm = new AcmClient($this->http);
        $this->applicationAutoscaling = new ApplicationAutoScalingClient($this->http);
        $this->athena = new AthenaClient($this->http);
        $this->organizations = new OrganizationsClient($this->http);
        $this->ssm = new SsmClient($this->http);
        $this->kms = new KmsClient($this->http);
        $this->wafv2 = new WafV2Client($this->http);
        $this->cloudfront = new CloudFrontClient($this->http);
    }

    public function baseUrl(): string
    {
        return $this->http->baseUrl();
    }

    // ── Health & Reset ─────────────────────────────────────────────

    public function health(): HealthResponse
    {
        return HealthResponse::fromArray($this->http->get('/_fakecloud/health'));
    }

    public function reset(): ResetResponse
    {
        return ResetResponse::fromArray($this->http->postEmpty('/_reset'));
    }

    public function resetService(string $service): ResetServiceResponse
    {
        return ResetServiceResponse::fromArray(
            $this->http->postEmpty('/_fakecloud/reset/' . HttpTransport::encodePath($service))
        );
    }

    // ── IAM ───────────────────────────────────────────────────────

    public function createAdmin(string $accountId, string $userName): CreateAdminResponse
    {
        return CreateAdminResponse::fromArray(
            $this->http->postJson('/_fakecloud/iam/create-admin', [
                'accountId' => $accountId,
                'userName' => $userName,
            ])
        );
    }

    // ── Sub-client accessors ───────────────────────────────────────

    public function lambda(): LambdaClient { return $this->lambda; }
    public function rds(): RdsClient { return $this->rds; }
    public function elasticache(): ElastiCacheClient { return $this->elasticache; }
    public function ecr(): EcrClient { return $this->ecr; }
    public function logs(): LogsClient { return $this->logs; }
    public function ses(): SesClient { return $this->ses; }
    public function sns(): SnsClient { return $this->sns; }
    public function sqs(): SqsClient { return $this->sqs; }
    public function events(): EventsClient { return $this->events; }

    public function scheduler(): SchedulerClient { return $this->scheduler; }
    public function glue(): GlueClient { return $this->glue; }
    public function s3(): S3Client { return $this->s3; }
    public function dynamodb(): DynamoDbClient { return $this->dynamodb; }
    public function secretsmanager(): SecretsManagerClient { return $this->secretsmanager; }
    public function cognito(): CognitoClient { return $this->cognito; }
    public function apigatewayv2(): ApiGatewayV2Client { return $this->apigatewayv2; }
    public function stepfunctions(): StepFunctionsClient { return $this->stepfunctions; }
    public function bedrock(): BedrockClient { return $this->bedrock; }
    public function bedrockAgent(): BedrockAgentClient { return $this->bedrockAgent; }
    public function bedrockAgentRuntime(): BedrockAgentRuntimeClient { return $this->bedrockAgentRuntime; }
    public function ecs(): EcsClient { return $this->ecs; }
    public function elbv2(): Elbv2Client { return $this->elbv2; }
    public function route53(): Route53Client { return $this->route53; }
    public function acm(): AcmClient { return $this->acm; }
    public function applicationAutoscaling(): ApplicationAutoScalingClient { return $this->applicationAutoscaling; }    public function athena(): AthenaClient { return $this->athena; }
    public function organizations(): OrganizationsClient { return $this->organizations; }
    public function ssm(): SsmClient { return $this->ssm; }
    public function kms(): KmsClient { return $this->kms; }
    public function wafv2(): WafV2Client { return $this->wafv2; }
    public function cloudfront(): CloudFrontClient { return $this->cloudfront; }
}

// ── Sub-clients ────────────────────────────────────────────────

final class LambdaClient
{
    public function __construct(private readonly HttpTransport $http) {}

    public function getInvocations(): LambdaInvocationsResponse
    {
        return LambdaInvocationsResponse::fromArray(
            $this->http->get('/_fakecloud/lambda/invocations')
        );
    }

    public function getWarmContainers(): WarmContainersResponse
    {
        return WarmContainersResponse::fromArray(
            $this->http->get('/_fakecloud/lambda/warm-containers')
        );
    }

    public function evictContainer(string $functionName): EvictContainerResponse
    {
        return EvictContainerResponse::fromArray(
            $this->http->postEmpty('/_fakecloud/lambda/' . HttpTransport::encodePath($functionName) . '/evict-container')
        );
    }

    /**
     * Download the stored zip bundle for a Lambda function's code.
     *
     * `$qualifierOrLatest` is either the literal string `"latest"` (the
     * value the server stores under `latest.zip`) or a numeric version
     * string (e.g. `"3"`) to fetch `<version>.zip`. Returns the raw zip
     * bytes; throws {@see FakeCloudError} on 404 / non-2xx.
     */
    public function downloadFunctionCode(string $accountId, string $functionName, string $qualifierOrLatest): string
    {
        $file = $qualifierOrLatest === 'latest' ? 'latest.zip' : $qualifierOrLatest . '.zip';
        return $this->http->getRaw(
            '/_fakecloud/lambda/function-code/'
                . HttpTransport::encodePath($accountId) . '/'
                . HttpTransport::encodePath($functionName) . '/'
                . HttpTransport::encodePath($file)
        );
    }

    /**
     * Download the stored zip bundle for a specific Lambda layer
     * version. Returns the raw zip bytes; throws {@see FakeCloudError}
     * on 404 / non-2xx.
     */
    public function downloadLayerContent(string $accountId, string $layerName, int|string $version): string
    {
        $file = ((string) $version) . '.zip';
        return $this->http->getRaw(
            '/_fakecloud/lambda/layer-content/'
                . HttpTransport::encodePath($accountId) . '/'
                . HttpTransport::encodePath($layerName) . '/'
                . HttpTransport::encodePath($file)
        );
    }
}

final class RdsClient
{
    public function __construct(private readonly HttpTransport $http) {}

    public function getInstances(): RdsInstancesResponse
    {
        return RdsInstancesResponse::fromArray(
            $this->http->get('/_fakecloud/rds/instances')
        );
    }

    /**
     * Bridge endpoint the PostgreSQL `aws_lambda` extension calls into
     * from inside an RDS DB container. Wire format is snake_case; the
     * SDK preserves it verbatim rather than auto-converting.
     */
    public function lambdaInvoke(RdsLambdaInvokeRequest $req): RdsLambdaInvokeResponse
    {
        return RdsLambdaInvokeResponse::fromArray(
            $this->http->postJson('/_fakecloud/rds/lambda-invoke', $req->toArray())
        );
    }

    /**
     * Bridge endpoint for the PostgreSQL `aws_s3` extension: fetch a
     * fakecloud S3 object. Body is base64-encoded so the JSON transport
     * stays text-only. snake_case on the wire.
     */
    public function s3Import(RdsS3ImportRequest $req): RdsS3ImportResponse
    {
        return RdsS3ImportResponse::fromArray(
            $this->http->postJson('/_fakecloud/rds/s3-import', $req->toArray())
        );
    }

    /**
     * Bridge equivalent of an S3 PutObject driven from inside the DB
     * container. snake_case on the wire.
     */
    public function s3Export(RdsS3ExportRequest $req): RdsS3ExportResponse
    {
        return RdsS3ExportResponse::fromArray(
            $this->http->postJson('/_fakecloud/rds/s3-export', $req->toArray())
        );
    }
}

final class ElastiCacheClient
{
    public function __construct(private readonly HttpTransport $http) {}

    public function getClusters(): ElastiCacheClustersResponse
    {
        return ElastiCacheClustersResponse::fromArray(
            $this->http->get('/_fakecloud/elasticache/clusters')
        );
    }

    public function getReplicationGroups(): ElastiCacheReplicationGroupsResponse
    {
        return ElastiCacheReplicationGroupsResponse::fromArray(
            $this->http->get('/_fakecloud/elasticache/replication-groups')
        );
    }

    public function getServerlessCaches(): ElastiCacheServerlessCachesResponse
    {
        return ElastiCacheServerlessCachesResponse::fromArray(
            $this->http->get('/_fakecloud/elasticache/serverless-caches')
        );
    }

    public function getElastiCacheAcls(): ElastiCacheAclsResponse
    {
        return ElastiCacheAclsResponse::fromArray(
            $this->http->get('/_fakecloud/elasticache/acls')
        );
    }
}

final class EcrClient
{
    public function __construct(private readonly HttpTransport $http) {}

    public function getRepositories(): EcrRepositoriesResponse
    {
        return EcrRepositoriesResponse::fromArray(
            $this->http->get('/_fakecloud/ecr/repositories')
        );
    }

    public function getImages(?string $repositoryName = null): EcrImagesResponse
    {
        $path = '/_fakecloud/ecr/images';
        if ($repositoryName !== null) {
            $path .= '?repo=' . rawurlencode($repositoryName);
        }
        return EcrImagesResponse::fromArray($this->http->get($path));
    }

    public function getPullThroughRules(): EcrPullThroughRulesResponse
    {
        return EcrPullThroughRulesResponse::fromArray(
            $this->http->get('/_fakecloud/ecr/pull-through-rules')
        );
    }
}

final class LogsClient
{
    public function __construct(private readonly HttpTransport $http) {}

    public function injectAnomaly(LogsAnomalyInjectRequest $req): LogsAnomalyInjectResponse
    {
        return LogsAnomalyInjectResponse::fromArray(
            $this->http->postJson('/_fakecloud/logs/anomalies/inject', $req->toArray())
        );
    }

    public function getDeliveryConfig(): LogsDeliveryConfigResponse
    {
        return LogsDeliveryConfigResponse::fromArray(
            $this->http->get('/_fakecloud/logs/delivery-config')
        );
    }

    public function getFieldIndexes(string $logGroupName): LogsFieldIndexesResponse
    {
        return LogsFieldIndexesResponse::fromArray(
            $this->http->get('/_fakecloud/logs/field-indexes/' . rawurlencode($logGroupName))
        );
    }
}

final class SesClient
{
    public function __construct(private readonly HttpTransport $http) {}

    public function getEmails(): SesEmailsResponse
    {
        return SesEmailsResponse::fromArray(
            $this->http->get('/_fakecloud/ses/emails')
        );
    }

    public function simulateInbound(InboundEmailRequest $req): InboundEmailResponse
    {
        return InboundEmailResponse::fromArray(
            $this->http->postJson('/_fakecloud/ses/inbound', $req->toArray())
        );
    }

    public function getMetrics(): SesMetrics
    {
        return SesMetrics::fromArray(
            $this->http->get('/_fakecloud/ses/metrics')
        );
    }

    public function setMailFromStatus(string $identity, string $status): SesMailFromStatusResponse
    {
        return SesMailFromStatusResponse::fromArray(
            $this->http->postJson(
                '/_fakecloud/ses/identities/' . rawurlencode($identity) . '/mail-from-status',
                ['status' => $status],
            )
        );
    }

    public function getDkimPublicKey(string $identity): SesDkimPublicKey
    {
        return SesDkimPublicKey::fromArray(
            $this->http->get(
                '/_fakecloud/ses/identities/' . rawurlencode($identity) . '/dkim-public-key'
            )
        );
    }

    public function setSandbox(bool $sandbox): SesSandboxResponse
    {
        return SesSandboxResponse::fromArray(
            $this->http->postJson('/_fakecloud/ses/account/sandbox', ['sandbox' => $sandbox])
        );
    }

    public function getBounces(): SesBouncesResponse
    {
        return SesBouncesResponse::fromArray(
            $this->http->get('/_fakecloud/ses/bounces')
        );
    }

    public function getMessageInsights(string $messageId): SesMessageInsightsResponse
    {
        return SesMessageInsightsResponse::fromArray(
            $this->http->get(
                '/_fakecloud/ses/messages/' . rawurlencode($messageId) . '/insights'
            )
        );
    }

    public function getSmtpSubmissions(): SesSmtpSubmissionsResponse
    {
        return SesSmtpSubmissionsResponse::fromArray(
            $this->http->get('/_fakecloud/ses/smtp/submissions')
        );
    }

    public function getEventDestinationDeliveries(): SesEventDestinationDeliveriesResponse
    {
        return SesEventDestinationDeliveriesResponse::fromArray(
            $this->http->get('/_fakecloud/ses/event-destinations/deliveries')
        );
    }
}

final class SnsClient
{
    public function __construct(private readonly HttpTransport $http) {}

    public function getMessages(): SnsMessagesResponse
    {
        return SnsMessagesResponse::fromArray(
            $this->http->get('/_fakecloud/sns/messages')
        );
    }

    public function getPendingConfirmations(): PendingConfirmationsResponse
    {
        return PendingConfirmationsResponse::fromArray(
            $this->http->get('/_fakecloud/sns/pending-confirmations')
        );
    }

    public function confirmSubscription(ConfirmSubscriptionRequest $req): ConfirmSubscriptionResponse
    {
        return ConfirmSubscriptionResponse::fromArray(
            $this->http->postJson('/_fakecloud/sns/confirm-subscription', $req->toArray())
        );
    }

    /**
     * Fetch the PEM-encoded signing certificate fakecloud uses to sign
     * SNS HTTP delivery payloads. Returned as a raw PEM string —
     * subscribers fetch this via `SigningCertURL` on real SNS.
     */
    public function getSigningCertPem(): string
    {
        return $this->http->getText('/_fakecloud/sns/cert.pem');
    }

    /**
     * List captured SMS messages. SNS SMS Publish calls don't hit a real
     * carrier under fakecloud; this returns the destination phone number
     * and message body so tests can assert delivery.
     */
    public function getSmsMessages(): SnsSmsResponse
    {
        return SnsSmsResponse::fromArray(
            $this->http->get('/_fakecloud/sns/sms')
        );
    }
}

final class SqsClient
{
    public function __construct(private readonly HttpTransport $http) {}

    public function getMessages(): SqsMessagesResponse
    {
        return SqsMessagesResponse::fromArray(
            $this->http->get('/_fakecloud/sqs/messages')
        );
    }

    public function tickExpiration(): ExpirationTickResponse
    {
        return ExpirationTickResponse::fromArray(
            $this->http->postEmpty('/_fakecloud/sqs/expiration-processor/tick')
        );
    }

    public function forceDlq(string $queueName): ForceDlqResponse
    {
        return ForceDlqResponse::fromArray(
            $this->http->postEmpty('/_fakecloud/sqs/' . HttpTransport::encodePath($queueName) . '/force-dlq')
        );
    }
}

final class ApplicationAutoScalingClient
{
    public function __construct(private readonly HttpTransport $http) {}

    public function tick(): AppAsTickResponse
    {
        return AppAsTickResponse::fromArray(
            $this->http->postEmpty('/_fakecloud/application-autoscaling/tick')
        );
    }

    public function scheduledTick(): AppAsScheduledTickResponse
    {
        return AppAsScheduledTickResponse::fromArray(
            $this->http->postEmpty('/_fakecloud/application-autoscaling/scheduled-tick')
        );
    }
}

final class AthenaClient
{
    public function __construct(private readonly HttpTransport $http) {}

    /**
     * List every named query stored in the Athena registry across all
     * workgroups for the default account. The response includes a
     * `lastUsedAt` timestamp the server bumps each time
     * `StartQueryExecution` resolves the query string by id.
     */
    public function getNamedQueries(): AthenaNamedQueriesResponse
    {
        return AthenaNamedQueriesResponse::fromArray(
            $this->http->get('/_fakecloud/athena/named-queries')
        );
    }
}

final class EventsClient
{
    public function __construct(private readonly HttpTransport $http) {}

    public function getHistory(): EventHistoryResponse
    {
        return EventHistoryResponse::fromArray(
            $this->http->get('/_fakecloud/events/history')
        );
    }

    public function fireRule(FireRuleRequest $req): FireRuleResponse
    {
        return FireRuleResponse::fromArray(
            $this->http->postJson('/_fakecloud/events/fire-rule', $req->toArray())
        );
    }
}

final class SchedulerClient
{
    public function __construct(private readonly HttpTransport $http) {}

    public function getSchedules(): SchedulerSchedulesResponse
    {
        return SchedulerSchedulesResponse::fromArray(
            $this->http->get('/_fakecloud/scheduler/schedules')
        );
    }

    public function fireSchedule(string $group, string $name): FireScheduleResponse
    {
        return FireScheduleResponse::fromArray(
            $this->http->postEmpty("/_fakecloud/scheduler/fire/{$group}/{$name}")
        );
    }
}

final class GlueClient
{
    public function __construct(private readonly HttpTransport $http) {}

    public function getJobs(): GlueJobsResponse
    {
        return GlueJobsResponse::fromArray(
            $this->http->get('/_fakecloud/glue/jobs')
        );
    }

    public function getJobRuns(?string $jobName = null): GlueJobRunsResponse
    {
        $path = '/_fakecloud/glue/job-runs';
        if ($jobName !== null && $jobName !== '') {
            $path .= '?job_name=' . rawurlencode($jobName);
        }
        return GlueJobRunsResponse::fromArray($this->http->get($path));
    }
}

final class S3Client
{
    public function __construct(private readonly HttpTransport $http) {}

    public function getNotifications(): S3NotificationsResponse
    {
        return S3NotificationsResponse::fromArray(
            $this->http->get('/_fakecloud/s3/notifications')
        );
    }

    public function tickLifecycle(): LifecycleTickResponse
    {
        return LifecycleTickResponse::fromArray(
            $this->http->postEmpty('/_fakecloud/s3/lifecycle-processor/tick')
        );
    }

    public function getAccessPoints(): S3AccessPointsResponse
    {
        return S3AccessPointsResponse::fromArray(
            $this->http->get('/_fakecloud/s3/access-points')
        );
    }

    public function getObjectLambdaResponses(): S3ObjectLambdaResponsesResponse
    {
        return S3ObjectLambdaResponsesResponse::fromArray(
            $this->http->get('/_fakecloud/s3/object-lambda-responses')
        );
    }
}

final class DynamoDbClient
{
    public function __construct(private readonly HttpTransport $http) {}

    public function tickTtl(): TtlTickResponse
    {
        return TtlTickResponse::fromArray(
            $this->http->postEmpty('/_fakecloud/dynamodb/ttl-processor/tick')
        );
    }
}

final class SecretsManagerClient
{
    public function __construct(private readonly HttpTransport $http) {}

    public function tickRotation(): RotationTickResponse
    {
        return RotationTickResponse::fromArray(
            $this->http->postEmpty('/_fakecloud/secretsmanager/rotation-scheduler/tick')
        );
    }
}

final class CognitoClient
{
    public function __construct(private readonly HttpTransport $http) {}

    public function getUserCodes(string $poolId, string $username): UserConfirmationCodes
    {
        return UserConfirmationCodes::fromArray(
            $this->http->get(
                '/_fakecloud/cognito/confirmation-codes/'
                . HttpTransport::encodePath($poolId)
                . '/'
                . HttpTransport::encodePath($username)
            )
        );
    }

    public function getConfirmationCodes(): ConfirmationCodesResponse
    {
        return ConfirmationCodesResponse::fromArray(
            $this->http->get('/_fakecloud/cognito/confirmation-codes')
        );
    }

    /**
     * Force-confirm a user, bypassing the confirmation code flow.
     *
     * fakecloud returns a JSON body with an `error` field on 404 for unknown users,
     * so we decode the body and surface it as a FakeCloudError.
     */
    public function confirmUser(ConfirmUserRequest $req): ConfirmUserResponse
    {
        $payload = json_encode($req->toArray(), JSON_THROW_ON_ERROR);
        $response = $this->http->execute('POST', '/_fakecloud/cognito/confirm-user', $payload, 'application/json');

        if ($response['status'] === 404) {
            $parsed = ConfirmUserResponse::fromArray(json_decode($response['body'], true, 512, JSON_THROW_ON_ERROR));
            throw new FakeCloudError(404, $parsed->error ?? 'user not found');
        }
        if ($response['status'] < 200 || $response['status'] >= 300) {
            throw new FakeCloudError($response['status'], $response['body']);
        }

        return ConfirmUserResponse::fromArray(json_decode($response['body'], true, 512, JSON_THROW_ON_ERROR));
    }

    public function getTokens(): TokensResponse
    {
        return TokensResponse::fromArray(
            $this->http->get('/_fakecloud/cognito/tokens')
        );
    }

    public function expireTokens(ExpireTokensRequest $req): ExpireTokensResponse
    {
        return ExpireTokensResponse::fromArray(
            $this->http->postJson('/_fakecloud/cognito/expire-tokens', $req->toArray())
        );
    }

    public function getAuthEvents(): AuthEventsResponse
    {
        return AuthEventsResponse::fromArray(
            $this->http->get('/_fakecloud/cognito/auth-events')
        );
    }

    /**
     * Returns the PreTokenGeneration Lambda trigger invocation log
     * recorded by `InitiateAuth`. Each entry has the full request /
     * response payloads plus pre-parsed `claims_added`,
     * `claims_overridden`, and `group_overrides` so tests can assert
     * claim mutation flows without inspecting the issued JWT.
     */
    public function getPreTokenGenInvocations(): PreTokenGenInvocationsResponse
    {
        return PreTokenGenInvocationsResponse::fromArray(
            $this->http->get('/_fakecloud/cognito/pretokengen/invocations')
        );
    }

    public function mintAuthorizationCode(
        MintAuthorizationCodeRequest $req
    ): MintAuthorizationCodeResponse {
        return MintAuthorizationCodeResponse::fromArray(
            $this->http->postJson(
                '/_fakecloud/cognito/authorization-codes',
                $req->toArray()
            )
        );
    }

    public function setCompromisedPasswords(
        CompromisedPasswordsRequest $req
    ): CompromisedPasswordsResponse {
        return CompromisedPasswordsResponse::fromArray(
            $this->http->postJson(
                '/_fakecloud/cognito/compromised-passwords',
                $req->toArray()
            )
        );
    }

    public function getWebAuthnCredentials(): WebAuthnCredentialsResponse
    {
        return WebAuthnCredentialsResponse::fromArray(
            $this->http->get('/_fakecloud/cognito/webauthn-credentials')
        );
    }
}

final class ApiGatewayV2Client
{
    public function __construct(private readonly HttpTransport $http) {}

    public function getRequests(): ApiGatewayV2RequestsResponse
    {
        return ApiGatewayV2RequestsResponse::fromArray(
            $this->http->get('/_fakecloud/apigatewayv2/requests')
        );
    }

    /**
     * List currently-active WebSocket connections across every
     * WebSocket API the server has seen.
     */
    public function getConnections(): ApiGatewayV2ConnectionsResponse
    {
        return ApiGatewayV2ConnectionsResponse::fromArray(
            $this->http->get('/_fakecloud/apigatewayv2/connections')
        );
    }

    /**
     * Fetch the mTLS configuration recorded for a custom domain name
     * (truststore bundle, version, validity). Pass-through JSON — the
     * server-side shape can evolve, so this returns the decoded array
     * unmodified.
     */
    public function mtlsInfo(string $domainName): array
    {
        return $this->http->get(
            '/_fakecloud/apigatewayv2/domain-names/'
            . HttpTransport::encodePath($domainName)
            . '/mtls-info'
        );
    }

    /**
     * Build the WebSocket upgrade URL for `apiId`. The SDK doesn't open
     * the socket itself — pair this with any PHP WebSocket client
     * (e.g. Ratchet, Workerman) in test code. `$stage` is accepted for
     * symmetry with other SDKs but is not part of the path today.
     */
    public function wsUrl(string $apiId, ?string $stage = null): string
    {
        $base = $this->http->baseUrl();
        $base = preg_replace('#^https://#', 'wss://', $base, 1);
        $base = preg_replace('#^http://#', 'ws://', $base, 1);
        $path = $base . '/_fakecloud/apigatewayv2/ws/' . HttpTransport::encodePath($apiId);
        if ($stage === null) {
            return $path;
        }
        return $path . '?stage=' . rawurlencode($stage);
    }
}

final class StepFunctionsClient
{
    public function __construct(private readonly HttpTransport $http) {}

    public function getExecutions(): StepFunctionsExecutionsResponse
    {
        return StepFunctionsExecutionsResponse::fromArray(
            $this->http->get('/_fakecloud/stepfunctions/executions')
        );
    }

    public function getSyncExecutions(): StepFunctionsSyncExecutionsResponse
    {
        return StepFunctionsSyncExecutionsResponse::fromArray(
            $this->http->get('/_fakecloud/stepfunctions/sync-executions')
        );
    }

    public function getExecutionTree(string $arn): StepFunctionsExecutionTreeResponse
    {
        return StepFunctionsExecutionTreeResponse::fromArray(
            $this->http->get('/_fakecloud/stepfunctions/execution-tree/' . rawurlencode($arn))
        );
    }

    public function enqueueActivityTask(SfnEnqueueActivityTaskRequest $req): SfnEnqueueActivityTaskResponse
    {
        return SfnEnqueueActivityTaskResponse::fromArray(
            $this->http->postJson('/_fakecloud/stepfunctions/enqueue-activity-task', $req->toArray())
        );
    }
}

final class BedrockClient
{
    public function __construct(private readonly HttpTransport $http) {}

    public function getInvocations(): BedrockInvocationsResponse
    {
        return BedrockInvocationsResponse::fromArray(
            $this->http->get('/_fakecloud/bedrock/invocations')
        );
    }

    public function setModelResponse(string $modelId, string $response): BedrockModelResponseConfig
    {
        return BedrockModelResponseConfig::fromArray(
            $this->http->postText(
                '/_fakecloud/bedrock/models/' . HttpTransport::encodePath($modelId) . '/response',
                $response
            )
        );
    }

    /**
     * @param BedrockResponseRule[] $rules
     */
    public function setResponseRules(string $modelId, array $rules): BedrockModelResponseConfig
    {
        return BedrockModelResponseConfig::fromArray(
            $this->http->postJson(
                '/_fakecloud/bedrock/models/' . HttpTransport::encodePath($modelId) . '/responses',
                ['rules' => array_map(fn(BedrockResponseRule $r) => $r->toArray(), $rules)]
            )
        );
    }

    public function clearResponseRules(string $modelId): BedrockModelResponseConfig
    {
        return BedrockModelResponseConfig::fromArray(
            $this->http->delete('/_fakecloud/bedrock/models/' . HttpTransport::encodePath($modelId) . '/responses')
        );
    }

    public function queueFault(BedrockFaultRule $rule): BedrockStatusResponse
    {
        return BedrockStatusResponse::fromArray(
            $this->http->postJson('/_fakecloud/bedrock/faults', $rule->toArray())
        );
    }

    public function getFaults(): BedrockFaultsResponse
    {
        return BedrockFaultsResponse::fromArray(
            $this->http->get('/_fakecloud/bedrock/faults')
        );
    }

    public function clearFaults(): BedrockStatusResponse
    {
        return BedrockStatusResponse::fromArray(
            $this->http->delete('/_fakecloud/bedrock/faults')
        );
    }
}

/** Bedrock Agent (control plane) introspection sub-client. */
final class BedrockAgentClient
{
    public function __construct(private readonly HttpTransport $http) {}

    public function getAgents(): BedrockAgentAgentsResponse
    {
        return BedrockAgentAgentsResponse::fromArray(
            $this->http->get('/_fakecloud/bedrock-agent/agents')
        );
    }
}

/** Bedrock Agent Runtime (data plane) introspection sub-client. */
final class BedrockAgentRuntimeClient
{
    public function __construct(private readonly HttpTransport $http) {}

    public function getInvocations(): BedrockAgentRuntimeInvocationsResponse
    {
        return BedrockAgentRuntimeInvocationsResponse::fromArray(
            $this->http->get('/_fakecloud/bedrock-agent-runtime/invocations')
        );
    }
}

final class EcsClient
{
    public function __construct(private readonly HttpTransport $http) {}

    public function getClusters(): EcsClustersResponse
    {
        return EcsClustersResponse::fromArray(
            $this->http->get('/_fakecloud/ecs/clusters')
        );
    }

    /**
     * List every task fakecloud is tracking. Pass null to skip a filter;
     * both parameters map to the server's `cluster` and `status` query args.
     */
    public function getTasks(?string $cluster = null, ?string $status = null): EcsTasksResponse
    {
        $path = '/_fakecloud/ecs/tasks';
        $params = [];
        if ($cluster !== null && $cluster !== '') {
            $params['cluster'] = $cluster;
        }
        if ($status !== null && $status !== '') {
            $params['status'] = $status;
        }
        if ($params !== []) {
            $path .= '?' . http_build_query($params);
        }
        return EcsTasksResponse::fromArray($this->http->get($path));
    }

    /** Fetch a single task snapshot by task ID. */
    public function getTask(string $taskId): EcsTask
    {
        return EcsTask::fromArray(
            $this->http->get('/_fakecloud/ecs/tasks/' . HttpTransport::encodePath($taskId))
        );
    }

    /** Captured docker stdout/stderr for a task plus its exit code if known. */
    public function getTaskLogs(string $taskId): EcsTaskLogsResponse
    {
        return EcsTaskLogsResponse::fromArray(
            $this->http->get('/_fakecloud/ecs/tasks/' . HttpTransport::encodePath($taskId) . '/logs')
        );
    }

    /**
     * SIGTERM (then SIGKILL after 10s) the task's running container via the
     * runtime. Returns the updated task snapshot.
     */
    public function forceStopTask(string $taskId): EcsTask
    {
        return EcsTask::fromArray(
            $this->http->postEmpty('/_fakecloud/ecs/tasks/' . HttpTransport::encodePath($taskId) . '/force-stop')
        );
    }

    /**
     * Flip a task to STOPPED without killing the container — useful for
     * simulating failed tasks deterministically in tests.
     */
    public function markTaskFailed(string $taskId, EcsMarkFailedRequest $req): EcsTask
    {
        return EcsTask::fromArray(
            $this->http->postJson(
                '/_fakecloud/ecs/tasks/' . HttpTransport::encodePath($taskId) . '/mark-failed',
                $req->toArray()
            )
        );
    }

    /** Replay the lifecycle event log. */
    public function getEvents(): EcsEventsResponse
    {
        return EcsEventsResponse::fromArray(
            $this->http->get('/_fakecloud/ecs/events')
        );
    }

    /**
     * Return the aggregated v4 metadata dump (the same shape
     * `ECS_CONTAINER_METADATA_URI_V4` exposes to a container) for the
     * task with the given full ARN. The ARN is URL-encoded into the
     * path before the request is issued.
     */
    public function getTaskMetadata(string $taskArn): EcsTaskMetadataResponse
    {
        return EcsTaskMetadataResponse::fromArray(
            $this->http->get(
                '/_fakecloud/ecs/metadata/' . HttpTransport::encodePath($taskArn)
            )
        );
    }

    /**
     * Fetch the IAM credentials fakecloud minted for a task. Mirrors
     * what `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI` exposes inside a
     * real ECS task container. Field casing is PascalCase on the wire;
     * the returned object exposes camelCase properties.
     */
    public function getTaskCredentials(string $taskId): EcsTaskCredentialsResponse
    {
        return EcsTaskCredentialsResponse::fromArray(
            $this->http->get('/_fakecloud/ecs/creds/' . HttpTransport::encodePath($taskId))
        );
    }

    /**
     * Fetch the v3 task metadata dump (the legacy endpoint exposed via
     * `ECS_CONTAINER_METADATA_URI`). Pass-through JSON.
     */
    public function getTaskMetadataV3(string $taskId): array
    {
        return $this->http->get('/_fakecloud/ecs/v3/' . HttpTransport::encodePath($taskId));
    }

    /**
     * Fetch the v4 task metadata dump (the endpoint exposed via
     * `ECS_CONTAINER_METADATA_URI_V4`). Pass-through JSON.
     */
    public function getTaskMetadataV4(string $taskId): array
    {
        return $this->http->get('/_fakecloud/ecs/v4/' . HttpTransport::encodePath($taskId));
    }
}

final class Elbv2Client
{
    public function __construct(private readonly HttpTransport $http) {}

    public function getLoadBalancers(): Elbv2LoadBalancersResponse
    {
        return Elbv2LoadBalancersResponse::fromArray(
            $this->http->get('/_fakecloud/elbv2/load-balancers')
        );
    }

    public function getTargetGroups(): Elbv2TargetGroupsResponse
    {
        return Elbv2TargetGroupsResponse::fromArray(
            $this->http->get('/_fakecloud/elbv2/target-groups')
        );
    }

    public function getListeners(): Elbv2ListenersResponse
    {
        return Elbv2ListenersResponse::fromArray(
            $this->http->get('/_fakecloud/elbv2/listeners')
        );
    }

    public function getRules(): Elbv2RulesResponse
    {
        return Elbv2RulesResponse::fromArray(
            $this->http->get('/_fakecloud/elbv2/rules')
        );
    }

    /**
     * Force every buffered access-log + connection-log line to flush
     * to S3 right now, bypassing the periodic 60-second timer.
     */
    public function flushAccessLogs(): Elbv2FlushAccessLogsResponse
    {
        return Elbv2FlushAccessLogsResponse::fromArray(
            $this->http->postEmpty('/_fakecloud/elbv2/access-logs/flush')
        );
    }

    /**
     * Snapshot of per-association WAF evaluation counters as recorded
     * by the ELBv2 -> WAFv2 bridge. The `counts` payload shape is
     * server-defined; it's exposed as-is so tests can drill into it
     * with array access.
     */
    public function getWafCounts(): Elbv2WafCountsResponse
    {
        return Elbv2WafCountsResponse::fromArray(
            $this->http->get('/_fakecloud/elbv2/waf-counts')
        );
    }
}

/**
 * Route 53 admin client.
 *
 * Wraps the per-health-check status admin endpoint that lets tests flip
 * a stored health check between healthy and unhealthy without a live
 * prober, so failover and multi-value routing can be exercised
 * end-to-end.
 */
final class Route53Client
{
    public function __construct(private readonly HttpTransport $http) {}

    /**
     * Flip a Route 53 health check's reported status. $status is one of
     * "Success", "Failure", "Timeout", "DnsError",
     * "InsufficientDataPoints", "Unknown". $reason is appended to the
     * <Status> element for failure-flavoured statuses (Failure, Timeout,
     * DnsError); ignored otherwise. Pass null to omit reason.
     */
    public function setHealthCheckStatus(string $healthCheckId, string $status, ?string $reason = null): void
    {
        $body = ['status' => $status];
        if ($reason !== null) {
            $body['reason'] = $reason;
        }
        $this->http->postJsonNoContent(
            '/_fakecloud/route53/health-checks/' . HttpTransport::encodePath($healthCheckId) . '/status',
            $body
        );
    }

    /**
     * Fetch the DNSSEC key material fakecloud derived for a hosted
     * zone's first ACTIVE Key Signing Key. Throws {@see FakeCloudError}
     * with status 404 if the zone has no active KSK.
     */
    public function getDnssecMaterial(string $zoneId): Route53DnssecMaterialResponse
    {
        return Route53DnssecMaterialResponse::fromArray(
            $this->http->get(
                '/_fakecloud/route53/zones/' . HttpTransport::encodePath($zoneId) . '/dnssec'
            )
        );
    }

    /**
     * Sign an RRset under the hosted zone's first ACTIVE KSK and return
     * the raw RRSIG fields so tests can verify the signature against
     * the material returned by {@see getDnssecMaterial()}.
     */
    public function signDnssec(string $zoneId, Route53DnssecSignRequest $req): Route53DnssecSignResponse
    {
        return Route53DnssecSignResponse::fromArray(
            $this->http->postJson(
                '/_fakecloud/route53/zones/' . HttpTransport::encodePath($zoneId) . '/dnssec/sign',
                $req->toArray()
            )
        );
    }
}

final class AcmClient
{
    public function __construct(private readonly HttpTransport $http) {}

    /**
     * Flip an ACM certificate's status synchronously. $status is one of
     * "ISSUED", "FAILED", "VALIDATION_TIMED_OUT"; $reason is recorded
     * as FailureReason on DescribeCertificate for non-ISSUED statuses
     * (pass null to omit). $arnOrId accepts the full ACM ARN or just
     * the trailing UUID.
     */
    public function setCertificateStatus(string $arnOrId, string $status, ?string $reason = null): void
    {
        $idx = strrpos($arnOrId, 'certificate/');
        $id = $idx === false ? $arnOrId : substr($arnOrId, $idx + strlen('certificate/'));
        $body = ['status' => $status];
        if ($reason !== null) {
            $body['reason'] = $reason;
        }
        $this->http->postJsonNoContent(
            '/_fakecloud/acm/certificates/' . HttpTransport::encodePath($id) . '/status',
            $body
        );
    }

    /**
     * Approve a PENDING_VALIDATION certificate. Synchronous equivalent
     * of "the user clicked the validation link in the email" — flips
     * the cert to ISSUED and refreshes its renewal eligibility /
     * RenewalSummary. EMAIL-validated certs do not auto-issue, so
     * tests drive their issuance through this endpoint. $arnOrId
     * accepts the full ACM ARN or just the trailing UUID.
     */
    public function approveCertificate(string $arnOrId): void
    {
        $idx = strrpos($arnOrId, 'certificate/');
        $id = $idx === false ? $arnOrId : substr($arnOrId, $idx + strlen('certificate/'));
        $this->http->postNoContent(
            '/_fakecloud/acm/certificates/' . HttpTransport::encodePath($id) . '/approve'
        );
    }

    /**
     * Inspect a stored certificate's PEM block counts and byte sizes.
     *
     * Returns the PEM block / byte counts for the certificate and chain
     * plus a constant `external_ca_validated=false` marker — fakecloud
     * does not run a real X.509 verifier, so the field documents the
     * emulator gap rather than reporting a verification result. Use
     * this to confirm uploaded chains round-trip intact, especially
     * for ImportCertificate flows. $arnOrId accepts the full ACM ARN
     * or just the trailing UUID.
     *
     * @return array{
     *     certificate_arn:string,
     *     certificate_pem_bytes:int,
     *     certificate_pem_blocks:int,
     *     chain_pem_bytes:int,
     *     chain_pem_blocks:int,
     *     external_ca_validated:bool,
     *     status:string,
     *     cert_type:string,
     * }
     */
    public function getCertificateChainInfo(string $arnOrId): array
    {
        $idx = strrpos($arnOrId, 'certificate/');
        $id = $idx === false ? $arnOrId : substr($arnOrId, $idx + strlen('certificate/'));
        /** @var array{
         *     certificate_arn:string,
         *     certificate_pem_bytes:int,
         *     certificate_pem_blocks:int,
         *     chain_pem_bytes:int,
         *     chain_pem_blocks:int,
         *     external_ca_validated:bool,
         *     status:string,
         *     cert_type:string,
         * } $resp
         */
        $resp = $this->http->get(
            '/_fakecloud/acm/certificates/' . HttpTransport::encodePath($id) . '/chain-info'
        );
        return $resp;
    }
}

/**
 * AWS Organizations admin/introspection sub-client. Bypasses IAM so
 * tests can assert on org shape without management-account credentials.
 */
final class OrganizationsClient
{
    public function __construct(private readonly HttpTransport $http) {}

    /**
     * List every member account in the org with lifecycle state,
     * parent OU, tags, and directly-attached SCPs. Returns an empty
     * accounts list (and null management/master ids) when no
     * organization has been created yet.
     */
    public function getAccounts(): OrganizationsAccountsResponse
    {
        return OrganizationsAccountsResponse::fromArray(
            $this->http->get('/_fakecloud/organizations/accounts')
        );
    }
}

/**
 * SSM admin / introspection sub-client. Lets tests drive RunCommand
 * invocations, parameter-policy events, and Session Manager records
 * synchronously without going through the real SSM agent flow.
 */
final class SsmClient
{
    public function __construct(private readonly HttpTransport $http) {}

    /**
     * Flip a stored `SendCommand` to a terminal status (`Success`,
     * `Failed`, `Cancelled`, `TimedOut`, etc). $accountId is optional;
     * when omitted the server resolves against the default account.
     */
    public function setCommandStatus(string $commandId, string $status, ?string $accountId = null): SetSsmCommandStatusResponse
    {
        $body = ['status' => $status];
        if ($accountId !== null) {
            $body['accountId'] = $accountId;
        }
        return SetSsmCommandStatusResponse::fromArray(
            $this->http->postJson(
                '/_fakecloud/ssm/commands/' . HttpTransport::encodePath($commandId) . '/status',
                $body
            )
        );
    }

    /**
     * Force a command (or a single invocation when $instanceId is set)
     * into the `Failed` terminal state. Optional $statusDetails /
     * $standardErrorContent surface through `GetCommandInvocation`.
     */
    public function failCommand(
        string $commandId,
        ?string $accountId = null,
        ?string $instanceId = null,
        ?string $statusDetails = null,
        ?string $standardErrorContent = null,
    ): FailSsmCommandResponse {
        $body = [];
        if ($accountId !== null) {
            $body['accountId'] = $accountId;
        }
        if ($instanceId !== null) {
            $body['instanceId'] = $instanceId;
        }
        if ($statusDetails !== null) {
            $body['statusDetails'] = $statusDetails;
        }
        if ($standardErrorContent !== null) {
            $body['standardErrorContent'] = $standardErrorContent;
        }
        return FailSsmCommandResponse::fromArray(
            $this->http->postJson(
                '/_fakecloud/ssm/commands/' . HttpTransport::encodePath($commandId) . '/fail',
                $body
            )
        );
    }

    /**
     * Snapshot of recorded SSM Parameter Store policy events
     * (registrations, expirations, no-change notifications).
     */
    public function getParameterPolicyEvents(?string $accountId = null): SsmParameterPolicyEventsResponse
    {
        $path = '/_fakecloud/ssm/parameter-policy-events';
        if ($accountId !== null) {
            $path .= '?accountId=' . rawurlencode($accountId);
        }
        return SsmParameterPolicyEventsResponse::fromArray($this->http->get($path));
    }

    /**
     * Inject a fake Session Manager session record so tests can assert
     * on `DescribeSessions`/`TerminateSession` without a live agent.
     * Defaults: status=Connected, owner=account-root IAM ARN, autogen
     * sessionId.
     */
    public function injectSession(
        string $target,
        ?string $accountId = null,
        ?string $status = null,
        ?string $owner = null,
        ?string $reason = null,
        ?string $sessionId = null,
    ): InjectSsmSessionResponse {
        $body = ['target' => $target];
        if ($accountId !== null) {
            $body['accountId'] = $accountId;
        }
        if ($status !== null) {
            $body['status'] = $status;
        }
        if ($owner !== null) {
            $body['owner'] = $owner;
        }
        if ($reason !== null) {
            $body['reason'] = $reason;
        }
        if ($sessionId !== null) {
            $body['sessionId'] = $sessionId;
        }
        return InjectSsmSessionResponse::fromArray(
            $this->http->postJson('/_fakecloud/ssm/sessions/inject', $body)
        );
    }
}

/**
 * KMS admin / introspection sub-client. Exposes the recorded
 * data-plane usage trail so tests can assert which keys/operations
 * the system under test invoked.
 */
final class KmsClient
{
    public function __construct(private readonly HttpTransport $http) {}

    /**
     * Snapshot every recorded KMS data-plane invocation (Encrypt,
     * Decrypt, GenerateDataKey, Sign, Verify, etc.) with timestamps,
     * caller service principal, and encryption context.
     */
    public function getUsage(): KmsUsageResponse
    {
        return KmsUsageResponse::fromArray(
            $this->http->get('/_fakecloud/kms/usage')
        );
    }
}

/**
 * WAFv2 admin / simulation sub-client. The `/evaluate` endpoint is a
 * pass-through that runs a rule-set evaluation harness; payload shape
 * is server-defined and exposed as a raw array.
 */
final class WafV2Client
{
    public function __construct(private readonly HttpTransport $http) {}

    /**
     * Run the WAFv2 evaluation harness against an arbitrary request.
     * Both the request body and response are forwarded as plain
     * decoded JSON arrays so tests can pin to the exact server shape.
     *
     * @param array<array-key, mixed> $body
     * @return array<array-key, mixed>
     */
    public function evaluate(array $body): array
    {
        return $this->http->postJson('/_fakecloud/wafv2/evaluate', $body);
    }
}

/**
 * CloudFront admin sub-client. Wraps the per-distribution status
 * admin endpoint so tests can synchronously flip a distribution
 * between `InProgress` and `Deployed` without waiting on the
 * propagation tick.
 */
final class CloudFrontClient
{
    public function __construct(private readonly HttpTransport $http) {}

    /**
     * Force a CloudFront distribution into the given status (typically
     * `"Deployed"` or `"InProgress"`). Throws {@see FakeCloudError}
     * with HTTP 404 when the distribution id is unknown.
     */
    public function setDistributionStatus(string $distributionId, string $status): void
    {
        $this->http->postJsonNoContent(
            '/_fakecloud/cloudfront/distributions/' . HttpTransport::encodePath($distributionId) . '/status',
            ['status' => $status]
        );
    }
}
