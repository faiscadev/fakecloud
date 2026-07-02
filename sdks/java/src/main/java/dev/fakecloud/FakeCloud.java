package dev.fakecloud;

import static dev.fakecloud.HttpTransport.encodePath;

import dev.fakecloud.Types.ApiGatewayV2RequestsResponse;
import dev.fakecloud.Types.AthenaNamedQueriesResponse;
import dev.fakecloud.Types.AppAsScheduledTickResponse;
import dev.fakecloud.Types.AppAsTickResponse;
import dev.fakecloud.Types.AuthEventsResponse;
import dev.fakecloud.Types.PreTokenGenInvocationsResponse;
import dev.fakecloud.Types.MintAuthorizationCodeRequest;
import dev.fakecloud.Types.MintAuthorizationCodeResponse;
import dev.fakecloud.Types.CompromisedPasswordsRequest;
import dev.fakecloud.Types.CompromisedPasswordsResponse;
import dev.fakecloud.Types.WebAuthnCredentialsResponse;
import dev.fakecloud.Types.BedrockFaultRule;
import dev.fakecloud.Types.BedrockFaultsResponse;
import dev.fakecloud.Types.BedrockInvocationsResponse;
import dev.fakecloud.Types.BedrockModelResponseConfig;
import dev.fakecloud.Types.BedrockResponseRule;
import dev.fakecloud.Types.BedrockStatusResponse;
import dev.fakecloud.Types.CloudFrontDistributionStatusRequest;
import dev.fakecloud.Types.CreateAdminRequest;
import dev.fakecloud.Types.CreateAdminResponse;
import dev.fakecloud.Types.ConfirmSubscriptionRequest;
import dev.fakecloud.Types.ConfirmSubscriptionResponse;
import dev.fakecloud.Types.ConfirmUserRequest;
import dev.fakecloud.Types.ConfirmUserResponse;
import dev.fakecloud.Types.ConfirmationCodesResponse;
import dev.fakecloud.Types.Ec2InstanceNetworksResponse;
import dev.fakecloud.Types.Ec2InstancesResponse;
import dev.fakecloud.Types.EcrImagesResponse;
import dev.fakecloud.Types.EcrPullThroughRulesResponse;
import dev.fakecloud.Types.EcrRepositoriesResponse;
import dev.fakecloud.Types.EcsClustersResponse;
import dev.fakecloud.Types.EcsEventsResponse;
import dev.fakecloud.Types.EcsTaskMetadataResponse;
import dev.fakecloud.Types.EcsMarkFailedRequest;
import dev.fakecloud.Types.EcsTask;
import dev.fakecloud.Types.EcsTaskLogsResponse;
import dev.fakecloud.Types.EcsTasksResponse;
import dev.fakecloud.Types.Elbv2FlushAccessLogsResponse;
import dev.fakecloud.Types.Elbv2ListenersResponse;
import dev.fakecloud.Types.Elbv2LoadBalancersResponse;
import dev.fakecloud.Types.Elbv2RulesResponse;
import dev.fakecloud.Types.Elbv2TargetGroupsResponse;
import dev.fakecloud.Types.Elbv2WafCountsResponse;
import dev.fakecloud.Types.FailSsmCommandRequest;
import dev.fakecloud.Types.FailSsmCommandResponse;
import dev.fakecloud.Types.InjectSsmSessionRequest;
import dev.fakecloud.Types.InjectSsmSessionResponse;
import dev.fakecloud.Types.KmsUsageResponse;
import dev.fakecloud.Types.SetSsmCommandStatusRequest;
import dev.fakecloud.Types.SetSsmCommandStatusResponse;
import dev.fakecloud.Types.SsmParameterPolicyEventsResponse;
import dev.fakecloud.Types.ElastiCacheAclsResponse;
import dev.fakecloud.Types.ElastiCacheClustersResponse;
import dev.fakecloud.Types.ElastiCacheReplicationGroupsResponse;
import dev.fakecloud.Types.ElastiCacheServerlessCachesResponse;
import dev.fakecloud.Types.EventHistoryResponse;
import dev.fakecloud.Types.EvictContainerResponse;
import dev.fakecloud.Types.ExpirationTickResponse;
import dev.fakecloud.Types.ExpireTokensRequest;
import dev.fakecloud.Types.ExpireTokensResponse;
import dev.fakecloud.Types.FireRuleRequest;
import dev.fakecloud.Types.FireRuleResponse;
import dev.fakecloud.Types.ForceDlqResponse;
import dev.fakecloud.Types.HealthResponse;
import dev.fakecloud.Types.InboundEmailRequest;
import dev.fakecloud.Types.InboundEmailResponse;
import dev.fakecloud.Types.LambdaInvocationsResponse;
import dev.fakecloud.Types.LifecycleTickResponse;
import dev.fakecloud.Types.LogsAnomalyInjectRequest;
import dev.fakecloud.Types.LogsAnomalyInjectResponse;
import dev.fakecloud.Types.LogsDeliveryConfigResponse;
import dev.fakecloud.Types.LogsFieldIndexesResponse;
import dev.fakecloud.Types.PendingConfirmationsResponse;
import dev.fakecloud.Types.RdsInstancesResponse;
import dev.fakecloud.Types.ResetResponse;
import dev.fakecloud.Types.ResetServiceResponse;
import dev.fakecloud.Types.RotationTickResponse;
import dev.fakecloud.Types.S3NotificationsResponse;
import dev.fakecloud.Types.SesDkimPublicKey;
import dev.fakecloud.Types.SesEmailsResponse;
import dev.fakecloud.Types.SesMailFromStatusRequest;
import dev.fakecloud.Types.SesMailFromStatusResponse;
import dev.fakecloud.Types.SesMetrics;
import dev.fakecloud.Types.SesBouncesResponse;
import dev.fakecloud.Types.SesEventDestinationDeliveriesResponse;
import dev.fakecloud.Types.SesMessageInsightsResponse;
import dev.fakecloud.Types.SesSandboxRequest;
import dev.fakecloud.Types.SesSandboxResponse;
import dev.fakecloud.Types.SesSmtpSubmissionsResponse;
import dev.fakecloud.Types.SetCertificateStatusRequest;
import dev.fakecloud.Types.SetHealthCheckStatusRequest;
import dev.fakecloud.Types.SnsMessagesResponse;
import dev.fakecloud.Types.SqsMessagesResponse;
import dev.fakecloud.Types.StepFunctionsExecutionsResponse;
import dev.fakecloud.Types.StepFunctionsSyncExecutionsResponse;
import dev.fakecloud.Types.StepFunctionsExecutionTreeResponse;
import dev.fakecloud.Types.TokensResponse;
import dev.fakecloud.Types.TtlTickResponse;
import dev.fakecloud.Types.DynamoDbSnapshotSaveRequest;
import dev.fakecloud.Types.DynamoDbSnapshotSaveResponse;
import dev.fakecloud.Types.UserConfirmationCodes;
import dev.fakecloud.Types.WarmContainersResponse;

import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.Map;

/**
 * Top-level client for the fakecloud introspection and simulation API.
 *
 * <pre>{@code
 * FakeCloud fc = new FakeCloud("http://localhost:4566");
 * fc.reset();
 * var emails = fc.ses().getEmails().emails();
 * }</pre>
 */
public final class FakeCloud {
    private static final String DEFAULT_BASE_URL = "http://localhost:4566";

    private final HttpTransport http;

    private final LambdaClient lambda;
    private final Ec2Client ec2;
    private final RdsClient rds;
    private final ElastiCacheClient elasticache;
    private final EcrClient ecr;
    private final LogsClient logs;
    private final SesClient ses;
    private final SnsClient sns;
    private final SqsClient sqs;
    private final EventsClient events;
    private final SchedulerClient scheduler;
    private final GlueClient glue;
    private final CloudWatchClient cloudwatch;
    private final FirehoseClient firehose;
    private final S3Client s3;
    private final DynamoDbClient dynamodb;
    private final SecretsManagerClient secretsmanager;
    private final CognitoClient cognito;
    private final ApiGatewayV2Client apigatewayv2;
    private final StepFunctionsClient stepfunctions;
    private final BedrockClient bedrock;
    private final BedrockAgentClient bedrockAgent;
    private final BedrockAgentRuntimeClient bedrockAgentRuntime;
    private final EcsClient ecs;
    private final Elbv2Client elbv2;
    private final Route53Client route53;
    private final AcmClient acm;
    private final ApplicationAutoScalingClient applicationAutoscaling;
    private final AthenaClient athena;
    private final OrganizationsClient organizations;
    private final SsmClient ssm;
    private final KmsClient kms;
    private final WafV2Client wafv2;
    private final CloudFrontClient cloudfront;

    public FakeCloud() {
        this(DEFAULT_BASE_URL);
    }

    public FakeCloud(String baseUrl) {
        this.http = new HttpTransport(trimTrailingSlashes(baseUrl));
        this.lambda = new LambdaClient(http);
        this.ec2 = new Ec2Client(http);
        this.rds = new RdsClient(http);
        this.elasticache = new ElastiCacheClient(http);
        this.ecr = new EcrClient(http);
        this.logs = new LogsClient(http);
        this.ses = new SesClient(http);
        this.sns = new SnsClient(http);
        this.sqs = new SqsClient(http);
        this.events = new EventsClient(http);
        this.scheduler = new SchedulerClient(http);
        this.glue = new GlueClient(http);
        this.cloudwatch = new CloudWatchClient(http);
        this.firehose = new FirehoseClient(http);
        this.s3 = new S3Client(http);
        this.dynamodb = new DynamoDbClient(http);
        this.secretsmanager = new SecretsManagerClient(http);
        this.cognito = new CognitoClient(http);
        this.apigatewayv2 = new ApiGatewayV2Client(http);
        this.stepfunctions = new StepFunctionsClient(http);
        this.bedrock = new BedrockClient(http);
        this.bedrockAgent = new BedrockAgentClient(http);
        this.bedrockAgentRuntime = new BedrockAgentRuntimeClient(http);
        this.ecs = new EcsClient(http);
        this.elbv2 = new Elbv2Client(http);
        this.route53 = new Route53Client(http);
        this.acm = new AcmClient(http);
        this.applicationAutoscaling = new ApplicationAutoScalingClient(http);
        this.athena = new AthenaClient(http);
        this.organizations = new OrganizationsClient(http);
        this.ssm = new SsmClient(http);
        this.kms = new KmsClient(http);
        this.wafv2 = new WafV2Client(http);
        this.cloudfront = new CloudFrontClient(http);
    }

    static String trimTrailingSlashes(String url) {
        int end = url.length();
        while (end > 0 && url.charAt(end - 1) == '/') {
            end--;
        }
        return url.substring(0, end);
    }

    public String baseUrl() {
        return http.baseUrl();
    }

    // ── Health & Reset ─────────────────────────────────────────────

    public HealthResponse health() {
        return http.get("/_fakecloud/health", HealthResponse.class);
    }

    public ResetResponse reset() {
        return http.postEmpty("/_reset", ResetResponse.class);
    }

    public ResetServiceResponse resetService(String service) {
        return http.postEmpty("/_fakecloud/reset/" + encodePath(service), ResetServiceResponse.class);
    }

    // ── IAM ───────────────────────────────────────────────────────

    public CreateAdminResponse createAdmin(String accountId, String userName) {
        return http.postJson(
                "/_fakecloud/iam/create-admin",
                new CreateAdminRequest(accountId, userName),
                CreateAdminResponse.class);
    }

    // ── Sub-client accessors ───────────────────────────────────────

    public LambdaClient lambda() { return lambda; }
    public Ec2Client ec2() { return ec2; }
    public RdsClient rds() { return rds; }
    public ElastiCacheClient elasticache() { return elasticache; }
    public EcrClient ecr() { return ecr; }
    public LogsClient logs() { return logs; }
    public SesClient ses() { return ses; }
    public SnsClient sns() { return sns; }
    public SqsClient sqs() { return sqs; }
    public EventsClient events() { return events; }
    public SchedulerClient scheduler() { return scheduler; }
    public GlueClient glue() { return glue; }

    public CloudWatchClient cloudwatch() { return cloudwatch; }
    public FirehoseClient firehose() { return firehose; }
    public S3Client s3() { return s3; }
    public DynamoDbClient dynamodb() { return dynamodb; }
    public SecretsManagerClient secretsmanager() { return secretsmanager; }
    public CognitoClient cognito() { return cognito; }
    public ApiGatewayV2Client apigatewayv2() { return apigatewayv2; }
    public StepFunctionsClient stepfunctions() { return stepfunctions; }
    public BedrockClient bedrock() { return bedrock; }
    public BedrockAgentClient bedrockAgent() { return bedrockAgent; }
    public BedrockAgentRuntimeClient bedrockAgentRuntime() { return bedrockAgentRuntime; }
    public EcsClient ecs() { return ecs; }
    public Elbv2Client elbv2() { return elbv2; }
    public Route53Client route53() { return route53; }
    public AcmClient acm() { return acm; }
    public ApplicationAutoScalingClient applicationAutoscaling() { return applicationAutoscaling; }
    public AthenaClient athena() { return athena; }
    public OrganizationsClient organizations() { return organizations; }
    public SsmClient ssm() { return ssm; }
    public KmsClient kms() { return kms; }
    public WafV2Client wafv2() { return wafv2; }
    public CloudFrontClient cloudfront() { return cloudfront; }

    // ── Sub-clients ────────────────────────────────────────────────

    public static final class LambdaClient {
        private final HttpTransport http;
        LambdaClient(HttpTransport http) { this.http = http; }

        public LambdaInvocationsResponse getInvocations() {
            return http.get("/_fakecloud/lambda/invocations", LambdaInvocationsResponse.class);
        }

        public WarmContainersResponse getWarmContainers() {
            return http.get("/_fakecloud/lambda/warm-containers", WarmContainersResponse.class);
        }

        public EvictContainerResponse evictContainer(String functionName) {
            return http.postEmpty(
                    "/_fakecloud/lambda/" + encodePath(functionName) + "/evict-container",
                    EvictContainerResponse.class);
        }

        /**
         * Download the stored zip archive for a Lambda function's deployment
         * package. {@code qualifierOrLatest} is either {@code "latest"} or a
         * concrete version (e.g. {@code "1"}); the corresponding file
         * ({@code latest.zip} / {@code <version>.zip}) is fetched verbatim.
         */
        public byte[] downloadFunctionCode(
                String accountId, String functionName, String qualifierOrLatest) {
            String file =
                    "latest".equals(qualifierOrLatest)
                            ? "latest.zip"
                            : qualifierOrLatest + ".zip";
            return http.getBytes(
                    "/_fakecloud/lambda/function-code/"
                            + encodePath(accountId)
                            + "/"
                            + encodePath(functionName)
                            + "/"
                            + encodePath(file));
        }

        /**
         * Download the stored zip archive for a specific Lambda layer
         * version.
         */
        public byte[] downloadLayerContent(
                String accountId, String layerName, long version) {
            return http.getBytes(
                    "/_fakecloud/lambda/layer-content/"
                            + encodePath(accountId)
                            + "/"
                            + encodePath(layerName)
                            + "/"
                            + version
                            + ".zip");
        }
    }

    public static final class Ec2Client {
        private final HttpTransport http;
        Ec2Client(HttpTransport http) { this.http = http; }

        public Ec2InstancesResponse getInstances() {
            return http.get("/_fakecloud/ec2/instances", Ec2InstancesResponse.class);
        }

        /**
         * Inspect the real backing network of each EC2 instance — which
         * Docker/Podman network or k8s NetworkPolicy backs it, its container IP,
         * and whether security-group enforcement is active or degraded. A
         * debugging aid for "why can't X reach Y" (issue #1745).
         */
        public Ec2InstanceNetworksResponse getInstanceNetworks() {
            return http.get("/_fakecloud/ec2/instance-networks", Ec2InstanceNetworksResponse.class);
        }
    }

    public static final class RdsClient {
        private final HttpTransport http;
        RdsClient(HttpTransport http) { this.http = http; }

        public RdsInstancesResponse getInstances() {
            return http.get("/_fakecloud/rds/instances", RdsInstancesResponse.class);
        }

        /**
         * Bridge endpoint the PostgreSQL {@code aws_lambda} extension calls
         * into from inside an RDS DB instance container. Normally not driven
         * by user code directly.
         */
        public Types.RdsLambdaInvokeResponse lambdaInvoke(Types.RdsLambdaInvokeRequest req) {
            return http.postJson(
                    "/_fakecloud/rds/lambda-invoke", req, Types.RdsLambdaInvokeResponse.class);
        }

        /**
         * Bridge endpoint the PostgreSQL {@code aws_s3} extension calls into
         * to fetch an object from a fakecloud bucket. Body is returned base64
         * encoded so JSON transport stays text-only.
         */
        public Types.RdsS3ImportResponse s3Import(Types.RdsS3ImportRequest req) {
            return http.postJson(
                    "/_fakecloud/rds/s3-import", req, Types.RdsS3ImportResponse.class);
        }

        /**
         * Bridge equivalent of an S3 PutObject driven from inside the DB
         * container.
         */
        public Types.RdsS3ExportResponse s3Export(Types.RdsS3ExportRequest req) {
            return http.postJson(
                    "/_fakecloud/rds/s3-export", req, Types.RdsS3ExportResponse.class);
        }
    }

    public static final class ElastiCacheClient {
        private final HttpTransport http;
        ElastiCacheClient(HttpTransport http) { this.http = http; }

        public ElastiCacheClustersResponse getClusters() {
            return http.get("/_fakecloud/elasticache/clusters", ElastiCacheClustersResponse.class);
        }

        public ElastiCacheReplicationGroupsResponse getReplicationGroups() {
            return http.get(
                    "/_fakecloud/elasticache/replication-groups",
                    ElastiCacheReplicationGroupsResponse.class);
        }

        public ElastiCacheServerlessCachesResponse getServerlessCaches() {
            return http.get(
                    "/_fakecloud/elasticache/serverless-caches",
                    ElastiCacheServerlessCachesResponse.class);
        }

        public ElastiCacheAclsResponse getElastiCacheAcls() {
            return http.get(
                    "/_fakecloud/elasticache/acls",
                    ElastiCacheAclsResponse.class);
        }
    }

    public static final class EcrClient {
        private final HttpTransport http;
        EcrClient(HttpTransport http) { this.http = http; }

        public EcrRepositoriesResponse getRepositories() {
            return http.get("/_fakecloud/ecr/repositories", EcrRepositoriesResponse.class);
        }

        public EcrImagesResponse getImages() {
            return http.get("/_fakecloud/ecr/images", EcrImagesResponse.class);
        }

        public EcrImagesResponse getImagesForRepository(String repositoryName) {
            return http.get(
                    "/_fakecloud/ecr/images?repo="
                            + java.net.URLEncoder.encode(
                                    repositoryName, java.nio.charset.StandardCharsets.UTF_8),
                    EcrImagesResponse.class);
        }

        public EcrPullThroughRulesResponse getPullThroughRules() {
            return http.get(
                    "/_fakecloud/ecr/pull-through-rules", EcrPullThroughRulesResponse.class);
        }
    }

    public static final class LogsClient {
        private final HttpTransport http;
        LogsClient(HttpTransport http) { this.http = http; }

        public LogsAnomalyInjectResponse injectAnomaly(LogsAnomalyInjectRequest req) {
            return http.postJson(
                    "/_fakecloud/logs/anomalies/inject", req, LogsAnomalyInjectResponse.class);
        }

        /** Persisted CloudWatch Logs delivery configurations. */
        public LogsDeliveryConfigResponse getDeliveryConfig() {
            return http.get(
                    "/_fakecloud/logs/delivery-config", LogsDeliveryConfigResponse.class);
        }

        /** Parsed {@code Fields} from index policies on the given log group. */
        public LogsFieldIndexesResponse getFieldIndexes(String logGroupName) {
            return http.get(
                    "/_fakecloud/logs/field-indexes/" + encodePath(logGroupName),
                    LogsFieldIndexesResponse.class);
        }
    }

    public static final class SesClient {
        private final HttpTransport http;
        SesClient(HttpTransport http) { this.http = http; }

        public SesEmailsResponse getEmails() {
            return http.get("/_fakecloud/ses/emails", SesEmailsResponse.class);
        }

        public InboundEmailResponse simulateInbound(InboundEmailRequest req) {
            return http.postJson("/_fakecloud/ses/inbound", req, InboundEmailResponse.class);
        }

        public SesMetrics getMetrics() {
            return http.get("/_fakecloud/ses/metrics", SesMetrics.class);
        }

        public SesMailFromStatusResponse setMailFromStatus(String identity, String status) {
            return http.postJson(
                    "/_fakecloud/ses/identities/" + identity + "/mail-from-status",
                    new SesMailFromStatusRequest(status),
                    SesMailFromStatusResponse.class);
        }

        public SesDkimPublicKey getDkimPublicKey(String identity) {
            return http.get(
                    "/_fakecloud/ses/identities/" + identity + "/dkim-public-key",
                    SesDkimPublicKey.class);
        }

        public SesSandboxResponse setSandbox(boolean sandbox) {
            return http.postJson(
                    "/_fakecloud/ses/account/sandbox",
                    new SesSandboxRequest(sandbox),
                    SesSandboxResponse.class);
        }

        public SesBouncesResponse getBounces() {
            return http.get("/_fakecloud/ses/bounces", SesBouncesResponse.class);
        }

        public SesMessageInsightsResponse getMessageInsights(String messageId) {
            return http.get(
                    "/_fakecloud/ses/messages/" + messageId + "/insights",
                    SesMessageInsightsResponse.class);
        }

        public SesSmtpSubmissionsResponse getSmtpSubmissions() {
            return http.get(
                    "/_fakecloud/ses/smtp/submissions", SesSmtpSubmissionsResponse.class);
        }

        public SesEventDestinationDeliveriesResponse getEventDestinationDeliveries() {
            return http.get(
                    "/_fakecloud/ses/event-destinations/deliveries",
                    SesEventDestinationDeliveriesResponse.class);
        }
    }

    public static final class SnsClient {
        private final HttpTransport http;
        SnsClient(HttpTransport http) { this.http = http; }

        public SnsMessagesResponse getMessages() {
            return http.get("/_fakecloud/sns/messages", SnsMessagesResponse.class);
        }

        public PendingConfirmationsResponse getPendingConfirmations() {
            return http.get(
                    "/_fakecloud/sns/pending-confirmations", PendingConfirmationsResponse.class);
        }

        public ConfirmSubscriptionResponse confirmSubscription(ConfirmSubscriptionRequest req) {
            return http.postJson(
                    "/_fakecloud/sns/confirm-subscription", req, ConfirmSubscriptionResponse.class);
        }

        /**
         * Returns the PEM-encoded SNS signing certificate used by message
         * signature validators (e.g. {@code aws-sns-validator}).
         */
        public String getCertPem() {
            return http.getText("/_fakecloud/sns/cert.pem");
        }

        /** List captured SMS messages SNS has "delivered". */
        public Types.SnsSmsResponse getSmsMessages() {
            return http.get("/_fakecloud/sns/sms", Types.SnsSmsResponse.class);
        }
    }

    public static final class SqsClient {
        private final HttpTransport http;
        SqsClient(HttpTransport http) { this.http = http; }

        public SqsMessagesResponse getMessages() {
            return http.get("/_fakecloud/sqs/messages", SqsMessagesResponse.class);
        }

        public ExpirationTickResponse tickExpiration() {
            return http.postEmpty(
                    "/_fakecloud/sqs/expiration-processor/tick", ExpirationTickResponse.class);
        }

        public ForceDlqResponse forceDlq(String queueName) {
            return http.postEmpty(
                    "/_fakecloud/sqs/" + encodePath(queueName) + "/force-dlq",
                    ForceDlqResponse.class);
        }
    }

    public static final class ApplicationAutoScalingClient {
        private final HttpTransport http;
        ApplicationAutoScalingClient(HttpTransport http) { this.http = http; }

        public AppAsTickResponse tick() {
            return http.postEmpty(
                    "/_fakecloud/application-autoscaling/tick",
                    AppAsTickResponse.class);
        }

        public AppAsScheduledTickResponse scheduledTick() {
            return http.postEmpty(
                    "/_fakecloud/application-autoscaling/scheduled-tick",
                    AppAsScheduledTickResponse.class);
        }
    }

    public static final class AthenaClient {
        private final HttpTransport http;
        AthenaClient(HttpTransport http) { this.http = http; }

        /**
         * List every named query stored in the Athena registry across all
         * workgroups for the default account. The response includes a
         * {@code lastUsedAt} timestamp the server bumps each time
         * {@code StartQueryExecution} resolves the query by id.
         */
        public AthenaNamedQueriesResponse getNamedQueries() {
            return http.get(
                    "/_fakecloud/athena/named-queries",
                    AthenaNamedQueriesResponse.class);
        }
    }

    public static final class EventsClient {
        private final HttpTransport http;
        EventsClient(HttpTransport http) { this.http = http; }

        public EventHistoryResponse getHistory() {
            return http.get("/_fakecloud/events/history", EventHistoryResponse.class);
        }

        public FireRuleResponse fireRule(FireRuleRequest req) {
            return http.postJson("/_fakecloud/events/fire-rule", req, FireRuleResponse.class);
        }
    }

    public static final class SchedulerClient {
        private final HttpTransport http;
        SchedulerClient(HttpTransport http) { this.http = http; }

        public Types.SchedulerSchedulesResponse getSchedules() {
            return http.get(
                    "/_fakecloud/scheduler/schedules",
                    Types.SchedulerSchedulesResponse.class);
        }

        public Types.FireScheduleResponse fireSchedule(String group, String name) {
            return http.postEmpty(
                    "/_fakecloud/scheduler/fire/" + group + "/" + name,
                    Types.FireScheduleResponse.class);
        }
    }

    public static final class GlueClient {
        private final HttpTransport http;
        GlueClient(HttpTransport http) { this.http = http; }

        public Types.GlueJobsResponse getJobs() {
            return http.get("/_fakecloud/glue/jobs", Types.GlueJobsResponse.class);
        }

        public Types.GlueJobRunsResponse getJobRuns() {
            return getJobRuns(null);
        }

        public Types.GlueJobRunsResponse getJobRuns(String jobName) {
            String path = "/_fakecloud/glue/job-runs";
            if (jobName != null && !jobName.isEmpty()) {
                path += "?job_name=" + encodePath(jobName);
            }
            return http.get(path, Types.GlueJobRunsResponse.class);
        }

        public Types.GlueCrawlersResponse getCrawlers() {
            return http.get("/_fakecloud/glue/crawlers", Types.GlueCrawlersResponse.class);
        }
    }

    public static final class CloudWatchClient {
        private final HttpTransport http;
        CloudWatchClient(HttpTransport http) { this.http = http; }

        public Types.CloudWatchAlarmsResponse getAlarms() {
            return http.get("/_fakecloud/cloudwatch/alarms", Types.CloudWatchAlarmsResponse.class);
        }

        public Types.CloudWatchMetricsResponse getMetrics() {
            return http.get("/_fakecloud/cloudwatch/metrics", Types.CloudWatchMetricsResponse.class);
        }
    }

    public static final class FirehoseClient {
        private final HttpTransport http;
        FirehoseClient(HttpTransport http) { this.http = http; }

        public Types.FirehoseDeliveryStreamsResponse getDeliveryStreams() {
            return http.get(
                "/_fakecloud/firehose/delivery-streams",
                Types.FirehoseDeliveryStreamsResponse.class);
        }
    }

    public static final class S3Client {
        private final HttpTransport http;
        S3Client(HttpTransport http) { this.http = http; }

        public S3NotificationsResponse getNotifications() {
            return http.get("/_fakecloud/s3/notifications", S3NotificationsResponse.class);
        }

        public LifecycleTickResponse tickLifecycle() {
            return http.postEmpty(
                    "/_fakecloud/s3/lifecycle-processor/tick", LifecycleTickResponse.class);
        }

        public Types.S3AccessPointsResponse getAccessPoints() {
            return http.get("/_fakecloud/s3/access-points", Types.S3AccessPointsResponse.class);
        }

        public Types.S3ObjectLambdaResponsesResponse getObjectLambdaResponses() {
            return http.get(
                    "/_fakecloud/s3/object-lambda-responses",
                    Types.S3ObjectLambdaResponsesResponse.class);
        }
    }

    public static final class DynamoDbClient {
        private final HttpTransport http;
        DynamoDbClient(HttpTransport http) { this.http = http; }

        public TtlTickResponse tickTtl() {
            return http.postEmpty("/_fakecloud/dynamodb/ttl-processor/tick", TtlTickResponse.class);
        }

        /**
         * Write the current DynamoDB state as a canonical snapshot on demand.
         *
         * <p>When {@code dataPath} is non-null the snapshot is written to
         * {@code <dataPath>/dynamodb/snapshot.json}; when null it is written to
         * the server's configured persistent store (an error if none is
         * configured).
         */
        public DynamoDbSnapshotSaveResponse saveSnapshot(String dataPath) {
            return http.postJson(
                    "/_fakecloud/dynamodb/snapshot/save",
                    new DynamoDbSnapshotSaveRequest(dataPath),
                    DynamoDbSnapshotSaveResponse.class);
        }
    }

    public static final class SecretsManagerClient {
        private final HttpTransport http;
        SecretsManagerClient(HttpTransport http) { this.http = http; }

        public RotationTickResponse tickRotation() {
            return http.postEmpty(
                    "/_fakecloud/secretsmanager/rotation-scheduler/tick",
                    RotationTickResponse.class);
        }
    }

    public static final class CognitoClient {
        private final HttpTransport http;
        CognitoClient(HttpTransport http) { this.http = http; }

        public UserConfirmationCodes getUserCodes(String poolId, String username) {
            return http.get(
                    "/_fakecloud/cognito/confirmation-codes/"
                            + encodePath(poolId)
                            + "/"
                            + encodePath(username),
                    UserConfirmationCodes.class);
        }

        public ConfirmationCodesResponse getConfirmationCodes() {
            return http.get(
                    "/_fakecloud/cognito/confirmation-codes", ConfirmationCodesResponse.class);
        }

        /**
         * Force-confirm a user, bypassing the confirmation code flow.
         *
         * <p>Mirrors the TypeScript SDK's special-case: fakecloud returns a JSON body with an
         * {@code error} field on 404 for unknown users, so we decode the body and surface it
         * as a {@link FakeCloudError}.
         */
        public ConfirmUserResponse confirmUser(ConfirmUserRequest req) {
            HttpRequest.Builder builder = http.builder("/_fakecloud/cognito/confirm-user")
                    .header("Content-Type", "application/json");
            try {
                byte[] payload = new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsBytes(req);
                builder.POST(HttpRequest.BodyPublishers.ofByteArray(payload));
            } catch (Exception e) {
                throw new FakeCloudError(-1, "failed to encode request: " + e.getMessage());
            }
            HttpResponse<byte[]> resp = http.execute(builder);
            ConfirmUserResponse parsed;
            try {
                parsed = new com.fasterxml.jackson.databind.ObjectMapper()
                        .readValue(resp.body(), ConfirmUserResponse.class);
            } catch (Exception e) {
                throw new FakeCloudError(
                        resp.statusCode(),
                        new String(resp.body(), java.nio.charset.StandardCharsets.UTF_8));
            }
            if (resp.statusCode() == 404) {
                throw new FakeCloudError(
                        404, parsed.error() != null ? parsed.error() : "user not found");
            }
            if (resp.statusCode() < 200 || resp.statusCode() >= 300) {
                throw new FakeCloudError(
                        resp.statusCode(),
                        new String(resp.body(), java.nio.charset.StandardCharsets.UTF_8));
            }
            return parsed;
        }

        public TokensResponse getTokens() {
            return http.get("/_fakecloud/cognito/tokens", TokensResponse.class);
        }

        public ExpireTokensResponse expireTokens(ExpireTokensRequest req) {
            return http.postJson(
                    "/_fakecloud/cognito/expire-tokens", req, ExpireTokensResponse.class);
        }

        public AuthEventsResponse getAuthEvents() {
            return http.get("/_fakecloud/cognito/auth-events", AuthEventsResponse.class);
        }

        /**
         * Returns the PreTokenGeneration Lambda trigger invocation log
         * recorded by {@code InitiateAuth}. Each entry has the full
         * request/response payloads plus pre-parsed claim additions,
         * suppressions, and group overrides.
         */
        public PreTokenGenInvocationsResponse getPreTokenGenInvocations() {
            return http.get(
                    "/_fakecloud/cognito/pretokengen/invocations",
                    PreTokenGenInvocationsResponse.class);
        }

        public MintAuthorizationCodeResponse mintAuthorizationCode(
                MintAuthorizationCodeRequest req) {
            return http.postJson(
                    "/_fakecloud/cognito/authorization-codes",
                    req,
                    MintAuthorizationCodeResponse.class);
        }

        public CompromisedPasswordsResponse setCompromisedPasswords(
                CompromisedPasswordsRequest req) {
            return http.postJson(
                    "/_fakecloud/cognito/compromised-passwords",
                    req,
                    CompromisedPasswordsResponse.class);
        }

        public WebAuthnCredentialsResponse getWebAuthnCredentials() {
            return http.get(
                    "/_fakecloud/cognito/webauthn-credentials",
                    WebAuthnCredentialsResponse.class);
        }
    }

    public static final class ApiGatewayV2Client {
        private final HttpTransport http;
        ApiGatewayV2Client(HttpTransport http) { this.http = http; }

        public ApiGatewayV2RequestsResponse getRequests() {
            return http.get(
                    "/_fakecloud/apigatewayv2/requests", ApiGatewayV2RequestsResponse.class);
        }

        /** List every active WebSocket connection tracked by API Gateway v2. */
        public Types.ApiGatewayV2ConnectionsResponse getConnections() {
            return http.get(
                    "/_fakecloud/apigatewayv2/connections",
                    Types.ApiGatewayV2ConnectionsResponse.class);
        }

        /**
         * Fetch the mTLS truststore info for a custom domain name. Returns
         * a raw JSON map so the surface stays forward-compatible with
         * server-side additions.
         */
        @SuppressWarnings("unchecked")
        public Map<String, Object> getDomainNameMtlsInfo(String domainName) {
            return http.get(
                    "/_fakecloud/apigatewayv2/domain-names/"
                            + encodePath(domainName)
                            + "/mtls-info",
                    Map.class);
        }

        /**
         * Build the WebSocket URL fakecloud serves for the given API id on
         * the default {@code "$default"} stage.
         */
        public String wsUrl(String apiId) {
            return wsUrl(apiId, null);
        }

        /** Build the WebSocket URL for the given API id and stage. */
        public String wsUrl(String apiId, String stage) {
            String base = http.baseUrl();
            String wsBase;
            if (base.startsWith("https://")) {
                wsBase = "wss://" + base.substring("https://".length());
            } else if (base.startsWith("http://")) {
                wsBase = "ws://" + base.substring("http://".length());
            } else {
                wsBase = base;
            }
            String path = wsBase + "/_fakecloud/apigatewayv2/ws/" + encodePath(apiId);
            if (stage == null) {
                return path;
            }
            try {
                return path + "?stage=" + java.net.URLEncoder.encode(stage, java.nio.charset.StandardCharsets.UTF_8);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static final class StepFunctionsClient {
        private final HttpTransport http;
        StepFunctionsClient(HttpTransport http) { this.http = http; }

        public StepFunctionsExecutionsResponse getExecutions() {
            return http.get(
                    "/_fakecloud/stepfunctions/executions",
                    StepFunctionsExecutionsResponse.class);
        }

        public StepFunctionsSyncExecutionsResponse getSyncExecutions() {
            return http.get(
                    "/_fakecloud/stepfunctions/sync-executions",
                    StepFunctionsSyncExecutionsResponse.class);
        }

        public StepFunctionsExecutionTreeResponse getExecutionTree(String arn) {
            return http.get(
                    "/_fakecloud/stepfunctions/execution-tree/" + encodePath(arn),
                    StepFunctionsExecutionTreeResponse.class);
        }

        public Types.SfnEnqueueActivityTaskResponse enqueueActivityTask(
                Types.SfnEnqueueActivityTaskRequest req) {
            return http.postJson(
                    "/_fakecloud/stepfunctions/enqueue-activity-task",
                    req,
                    Types.SfnEnqueueActivityTaskResponse.class);
        }
    }

    public static final class BedrockClient {
        private final HttpTransport http;
        BedrockClient(HttpTransport http) { this.http = http; }

        public BedrockInvocationsResponse getInvocations() {
            return http.get("/_fakecloud/bedrock/invocations", BedrockInvocationsResponse.class);
        }

        public BedrockModelResponseConfig setModelResponse(String modelId, String response) {
            return http.postText(
                    "/_fakecloud/bedrock/models/" + encodePath(modelId) + "/response",
                    response,
                    BedrockModelResponseConfig.class);
        }

        public BedrockModelResponseConfig setResponseRules(
                String modelId, List<BedrockResponseRule> rules) {
            return http.postJson(
                    "/_fakecloud/bedrock/models/" + encodePath(modelId) + "/responses",
                    Map.of("rules", rules),
                    BedrockModelResponseConfig.class);
        }

        public BedrockModelResponseConfig clearResponseRules(String modelId) {
            return http.delete(
                    "/_fakecloud/bedrock/models/" + encodePath(modelId) + "/responses",
                    BedrockModelResponseConfig.class);
        }

        public BedrockStatusResponse queueFault(BedrockFaultRule rule) {
            return http.postJson("/_fakecloud/bedrock/faults", rule, BedrockStatusResponse.class);
        }

        public BedrockFaultsResponse getFaults() {
            return http.get("/_fakecloud/bedrock/faults", BedrockFaultsResponse.class);
        }

        public BedrockStatusResponse clearFaults() {
            return http.delete("/_fakecloud/bedrock/faults", BedrockStatusResponse.class);
        }
    }

    /** Bedrock Agent (control plane) introspection sub-client. */
    public static final class BedrockAgentClient {
        private final HttpTransport http;
        BedrockAgentClient(HttpTransport http) { this.http = http; }

        public Types.BedrockAgentAgentsResponse getAgents() {
            return http.get("/_fakecloud/bedrock-agent/agents", Types.BedrockAgentAgentsResponse.class);
        }
    }

    /** Bedrock Agent Runtime (data plane) introspection sub-client. */
    public static final class BedrockAgentRuntimeClient {
        private final HttpTransport http;
        BedrockAgentRuntimeClient(HttpTransport http) { this.http = http; }

        public Types.BedrockAgentRuntimeInvocationsResponse getInvocations() {
            return http.get(
                "/_fakecloud/bedrock-agent-runtime/invocations",
                Types.BedrockAgentRuntimeInvocationsResponse.class);
        }
    }

    public static final class EcsClient {
        private final HttpTransport http;
        EcsClient(HttpTransport http) { this.http = http; }

        public EcsClustersResponse getClusters() {
            return http.get("/_fakecloud/ecs/clusters", EcsClustersResponse.class);
        }

        /** List every task fakecloud is tracking, optionally filtered by cluster and status. */
        public EcsTasksResponse getTasks(String cluster, String status) {
            StringBuilder path = new StringBuilder("/_fakecloud/ecs/tasks");
            StringBuilder qs = new StringBuilder();
            if (cluster != null && !cluster.isEmpty()) {
                qs.append("cluster=").append(encodePath(cluster));
            }
            if (status != null && !status.isEmpty()) {
                if (qs.length() > 0) qs.append('&');
                qs.append("status=").append(encodePath(status));
            }
            if (qs.length() > 0) {
                path.append('?').append(qs);
            }
            return http.get(path.toString(), EcsTasksResponse.class);
        }

        /** Fetch a single task snapshot by task ID. */
        public EcsTask getTask(String taskId) {
            return http.get("/_fakecloud/ecs/tasks/" + encodePath(taskId), EcsTask.class);
        }

        /** Captured docker stdout/stderr for a task plus its exit code if known. */
        public EcsTaskLogsResponse getTaskLogs(String taskId) {
            return http.get(
                    "/_fakecloud/ecs/tasks/" + encodePath(taskId) + "/logs",
                    EcsTaskLogsResponse.class);
        }

        /**
         * SIGTERM (then SIGKILL after 10s) the task's running container via
         * the runtime. Returns the updated task snapshot.
         */
        public EcsTask forceStopTask(String taskId) {
            return http.postEmpty(
                    "/_fakecloud/ecs/tasks/" + encodePath(taskId) + "/force-stop",
                    EcsTask.class);
        }

        /**
         * Flip a task to STOPPED without killing the container — useful for
         * simulating failed tasks deterministically in tests.
         */
        public EcsTask markTaskFailed(String taskId, EcsMarkFailedRequest req) {
            return http.postJson(
                    "/_fakecloud/ecs/tasks/" + encodePath(taskId) + "/mark-failed",
                    req,
                    EcsTask.class);
        }

        /** Replay the lifecycle event log. */
        public EcsEventsResponse getEvents() {
            return http.get("/_fakecloud/ecs/events", EcsEventsResponse.class);
        }

        /**
         * Return the aggregated v4 metadata dump (the same shape
         * {@code ECS_CONTAINER_METADATA_URI_V4} exposes to a container) for
         * the task with the given full ARN. The ARN is URL-encoded into the
         * path before the request is issued.
         */
        public EcsTaskMetadataResponse getTaskMetadata(String taskArn) {
            return http.get(
                    "/_fakecloud/ecs/metadata/" + encodePath(taskArn),
                    EcsTaskMetadataResponse.class);
        }

        /**
         * Return short-lived IAM credentials for a task. Matches the wire
         * shape ECS exposes via the task metadata credentials endpoint
         * (PascalCase keys).
         */
        public Types.EcsTaskCredentialsResponse getCredentials(String taskId) {
            return http.get(
                    "/_fakecloud/ecs/creds/" + encodePath(taskId),
                    Types.EcsTaskCredentialsResponse.class);
        }

        /**
         * Return the raw v3 task metadata document for the task. The
         * server response is a free-form JSON object that mirrors what
         * {@code ECS_CONTAINER_METADATA_URI} would expose; returned as a
         * {@code Map<String, Object>} pass-through to stay
         * forward-compatible.
         */
        @SuppressWarnings("unchecked")
        public Map<String, Object> getMetadataV3(String taskId) {
            return http.get(
                    "/_fakecloud/ecs/v3/" + encodePath(taskId), Map.class);
        }

        /**
         * Return the raw v4 task metadata document for the task. Returned
         * as a {@code Map<String, Object>} pass-through to stay
         * forward-compatible.
         */
        @SuppressWarnings("unchecked")
        public Map<String, Object> getMetadataV4(String taskId) {
            return http.get(
                    "/_fakecloud/ecs/v4/" + encodePath(taskId), Map.class);
        }
    }

    public static final class Elbv2Client {
        private final HttpTransport http;
        Elbv2Client(HttpTransport http) { this.http = http; }

        public Elbv2LoadBalancersResponse getLoadBalancers() {
            return http.get("/_fakecloud/elbv2/load-balancers", Elbv2LoadBalancersResponse.class);
        }

        public Elbv2TargetGroupsResponse getTargetGroups() {
            return http.get("/_fakecloud/elbv2/target-groups", Elbv2TargetGroupsResponse.class);
        }

        public Elbv2ListenersResponse getListeners() {
            return http.get("/_fakecloud/elbv2/listeners", Elbv2ListenersResponse.class);
        }

        public Elbv2RulesResponse getRules() {
            return http.get("/_fakecloud/elbv2/rules", Elbv2RulesResponse.class);
        }

        /**
         * Force every buffered access-log + connection-log line to flush
         * to S3 right now, bypassing the periodic 60-second timer.
         */
        public Elbv2FlushAccessLogsResponse flushAccessLogs() {
            return http.postEmpty(
                    "/_fakecloud/elbv2/access-logs/flush", Elbv2FlushAccessLogsResponse.class);
        }

        /**
         * Returns the WAFv2 association/evaluation counts the ELBv2 service
         * has accumulated. The exact shape of {@code counts} is
         * service-internal and intentionally returned as free-form JSON.
         */
        public Elbv2WafCountsResponse getWafCounts() {
            return http.get("/_fakecloud/elbv2/waf-counts", Elbv2WafCountsResponse.class);
        }
    }

    /**
     * Route 53 admin client.
     *
     * Wraps the per-health-check status admin endpoint that lets tests
     * flip a stored health check between healthy and unhealthy without a
     * live prober, so failover and multi-value routing can be exercised
     * end-to-end.
     */
    public static final class Route53Client {
        private final HttpTransport http;
        Route53Client(HttpTransport http) { this.http = http; }

        /**
         * Flip a Route 53 health check's reported status. {@code status}
         * is {@code "Success"} or {@code "Failure"}; {@code reason} is
         * appended to the {@code <Status>} element when status is
         * Failure (pass {@code null} to omit).
         */
        public void setHealthCheckStatus(String healthCheckId, String status, String reason) {
            http.postJsonNoContent(
                    "/_fakecloud/route53/health-checks/" + encodePath(healthCheckId) + "/status",
                    new SetHealthCheckStatusRequest(status, reason));
        }

        /**
         * Fetch the deterministic DNSSEC material (DNSKEY + DS digest) for
         * a hosted zone with at least one ACTIVE Key Signing Key. Throws
         * {@link FakeCloudError} with status 404 when the zone has no
         * active KSK.
         */
        public Types.Route53DnssecMaterialResponse getDnssecMaterial(String zoneId) {
            return http.get(
                    "/_fakecloud/route53/zones/" + encodePath(zoneId) + "/dnssec",
                    Types.Route53DnssecMaterialResponse.class);
        }

        /**
         * Sign an RRset under the zone's first ACTIVE KSK. Returns raw
         * RRSIG fields so tests can verify the signature against the
         * DNSKEY public key from {@link #getDnssecMaterial(String)}.
         */
        public Types.Route53DnssecSignResponse signDnssec(
                String zoneId, Types.Route53DnssecSignRequest req) {
            return http.postJson(
                    "/_fakecloud/route53/zones/" + encodePath(zoneId) + "/dnssec/sign",
                    req,
                    Types.Route53DnssecSignResponse.class);
        }
    }

    /**
     * ACM admin client.
     *
     * Wraps the per-certificate status admin endpoint that lets tests
     * flip a stored certificate between PENDING_VALIDATION, ISSUED,
     * FAILED, and VALIDATION_TIMED_OUT without waiting on the
     * auto-issue tick, so validation-failure flows can be exercised
     * end-to-end.
     */
    public static final class AcmClient {
        private final HttpTransport http;
        AcmClient(HttpTransport http) { this.http = http; }

        /**
         * Flip an ACM certificate's status synchronously. {@code status}
         * is one of {@code "ISSUED"}, {@code "FAILED"},
         * {@code "VALIDATION_TIMED_OUT"}; {@code reason} is recorded as
         * {@code FailureReason} on {@code DescribeCertificate} for
         * non-ISSUED statuses (pass {@code null} to omit).
         * {@code arnOrId} accepts either the full ACM ARN or the
         * trailing UUID portion.
         */
        public void setCertificateStatus(String arnOrId, String status, String reason) {
            String id = arnOrId;
            int idx = arnOrId.lastIndexOf("certificate/");
            if (idx >= 0) {
                id = arnOrId.substring(idx + "certificate/".length());
            }
            http.postJsonNoContent(
                    "/_fakecloud/acm/certificates/" + encodePath(id) + "/status",
                    new SetCertificateStatusRequest(status, reason));
        }

        /**
         * Approve a {@code PENDING_VALIDATION} certificate. Synchronous
         * equivalent of "the user clicked the validation link in the
         * email" — flips the cert to {@code ISSUED} and refreshes its
         * renewal eligibility / RenewalSummary. EMAIL-validated certs
         * do not auto-issue, so tests drive their issuance through this
         * endpoint. {@code arnOrId} accepts either the full ACM ARN or
         * the trailing UUID portion.
         */
        public void approveCertificate(String arnOrId) {
            String id = arnOrId;
            int idx = arnOrId.lastIndexOf("certificate/");
            if (idx >= 0) {
                id = arnOrId.substring(idx + "certificate/".length());
            }
            http.postNoContent(
                    "/_fakecloud/acm/certificates/" + encodePath(id) + "/approve");
        }

        /**
         * Inspect a stored certificate's PEM block counts and byte sizes.
         * Returns {@code externalCaValidated=false} to document that
         * fakecloud does not run real X.509 verification — use the
         * byte/block counts to confirm uploaded chains round-trip intact,
         * especially for {@code ImportCertificate} flows. {@code arnOrId}
         * accepts either the full ACM ARN or the trailing UUID portion.
         */
        public Types.AcmCertificateChainInfo getCertificateChainInfo(String arnOrId) {
            String id = arnOrId;
            int idx = arnOrId.lastIndexOf("certificate/");
            if (idx >= 0) {
                id = arnOrId.substring(idx + "certificate/".length());
            }
            return http.get(
                    "/_fakecloud/acm/certificates/" + encodePath(id) + "/chain-info",
                    Types.AcmCertificateChainInfo.class);
        }
    }

    /**
     * AWS Organizations admin/introspection sub-client. Bypasses IAM
     * so tests can assert on org shape without management-account
     * credentials.
     */
    public static final class OrganizationsClient {
        private final HttpTransport http;
        OrganizationsClient(HttpTransport http) { this.http = http; }

        /**
         * List every member account in the org with lifecycle state,
         * parent OU, tags, and directly-attached SCPs. Returns an
         * empty list (and {@code null} management/master ids) when
         * no organization has been created yet.
         */
        public Types.OrganizationsAccountsResponse getAccounts() {
            return http.get(
                    "/_fakecloud/organizations/accounts",
                    Types.OrganizationsAccountsResponse.class);
        }

        /**
         * List every billing responsibility transfer in the org, with
         * direction (INBOUND/OUTBOUND), lifecycle status, and the active
         * handshake. Returns an empty list when no organization has been
         * created.
         */
        public Types.OrganizationsResponsibilityTransfersResponse getResponsibilityTransfers() {
            return http.get(
                    "/_fakecloud/organizations/responsibility-transfers",
                    Types.OrganizationsResponsibilityTransfersResponse.class);
        }
    }

    /**
     * Systems Manager admin sub-client. Wraps the {@code /_fakecloud/ssm/*}
     * endpoints that let tests force command/invocation lifecycle
     * transitions and seed sessions without going through the real
     * agent-driven path.
     */
    public static final class SsmClient {
        private final HttpTransport http;
        SsmClient(HttpTransport http) { this.http = http; }

        /**
         * Flip the status of every invocation under a SendCommand
         * command id. {@code accountId} may be {@code null} to target
         * the default account.
         */
        public SetSsmCommandStatusResponse setCommandStatus(
                String commandId, String accountId, String status) {
            return http.postJson(
                    "/_fakecloud/ssm/commands/" + encodePath(commandId) + "/status",
                    new SetSsmCommandStatusRequest(accountId, status),
                    SetSsmCommandStatusResponse.class);
        }

        /**
         * Force a command (or a specific invocation under it) into
         * {@code Failed}. Pass {@code null} for the request to flip every
         * invocation on the command with default status detail.
         */
        public FailSsmCommandResponse failCommand(
                String commandId, FailSsmCommandRequest req) {
            FailSsmCommandRequest body =
                    req != null ? req : new FailSsmCommandRequest(null, null, null, null);
            return http.postJson(
                    "/_fakecloud/ssm/commands/" + encodePath(commandId) + "/fail",
                    body,
                    FailSsmCommandResponse.class);
        }

        /**
         * Return every parameter-policy event recorded for the given
         * account (default account when {@code accountId} is null).
         */
        public SsmParameterPolicyEventsResponse getParameterPolicyEvents(String accountId) {
            String path = "/_fakecloud/ssm/parameter-policy-events";
            if (accountId != null && !accountId.isEmpty()) {
                path += "?accountId=" + encodePath(accountId);
            }
            return http.get(path, SsmParameterPolicyEventsResponse.class);
        }

        /**
         * Drop a fake Session Manager session into state without going
         * through {@code StartSession}, so {@code DescribeSessions} /
         * {@code TerminateSession} can be exercised end-to-end.
         */
        public InjectSsmSessionResponse injectSession(InjectSsmSessionRequest req) {
            return http.postJson(
                    "/_fakecloud/ssm/sessions/inject",
                    req,
                    InjectSsmSessionResponse.class);
        }
    }

    /**
     * KMS admin sub-client. Exposes the data-plane usage recorder so
     * tests can assert on which keys were touched, with what
     * encryption context, by which service.
     */
    public static final class KmsClient {
        private final HttpTransport http;
        KmsClient(HttpTransport http) { this.http = http; }

        /** Return every recorded KMS data-plane invocation. */
        public KmsUsageResponse getUsage() {
            return http.get("/_fakecloud/kms/usage", KmsUsageResponse.class);
        }
    }

    /**
     * WAFv2 admin sub-client. Wraps the {@code /_fakecloud/wafv2/evaluate}
     * endpoint that runs an arbitrary evaluation payload through the
     * stored web ACL rule set. Request and response are intentionally
     * free-form JSON.
     */
    public static final class WafV2Client {
        private final HttpTransport http;
        WafV2Client(HttpTransport http) { this.http = http; }

        /**
         * Evaluate an arbitrary request payload against the stored
         * WAFv2 rule set. Both the body and the response are free-form
         * JSON — the exact shape is service-internal.
         */
        @SuppressWarnings("unchecked")
        public Map<String, Object> evaluate(Map<String, Object> request) {
            return http.postJson(
                    "/_fakecloud/wafv2/evaluate", request, Map.class);
        }
    }

    /**
     * CloudFront admin sub-client. Wraps the per-distribution status
     * admin endpoint that lets tests synchronously flip a stored
     * Distribution between {@code Deployed} and {@code InProgress}
     * without waiting on the propagation tick.
     */
    public static final class CloudFrontClient {
        private final HttpTransport http;
        CloudFrontClient(HttpTransport http) { this.http = http; }

        /**
         * Flip a stored CloudFront Distribution's status (e.g.
         * {@code "Deployed"} or {@code "InProgress"}). Throws
         * {@link FakeCloudError} with status 404 when the distribution
         * does not exist.
         */
        public void setDistributionStatus(String distributionId, String status) {
            http.postJsonNoContent(
                    "/_fakecloud/cloudfront/distributions/"
                            + encodePath(distributionId)
                            + "/status",
                    new CloudFrontDistributionStatusRequest(status));
        }
    }
}
