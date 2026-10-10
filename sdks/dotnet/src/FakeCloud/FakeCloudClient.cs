using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace FakeCloud;

/// <summary>
/// Top-level client for the fakecloud introspection and simulation API.
/// <code>
/// var fc = new FakeCloudClient("http://localhost:4566");
/// await fc.ResetAsync();
/// var emails = (await fc.Ses.GetEmailsAsync()).Emails;
/// </code>
/// </summary>
public sealed class FakeCloudClient : IDisposable
{
    private const string DefaultBaseUrl = "http://localhost:4566";

    private readonly HttpTransport _http;

    public FakeCloudClient() : this(DefaultBaseUrl) { }

    public FakeCloudClient(string baseUrl)
    {
        _http = new HttpTransport(TrimTrailingSlashes(baseUrl));
        Lambda = new LambdaClient(_http);
        Ec2 = new Ec2Client(_http);
        Rds = new RdsClient(_http);
        ElastiCache = new ElastiCacheClient(_http);
        Ecr = new EcrClient(_http);
        Logs = new LogsClient(_http);
        Ses = new SesClient(_http);
        Sns = new SnsClient(_http);
        Sqs = new SqsClient(_http);
        Events = new EventsClient(_http);
        Scheduler = new SchedulerClient(_http);
        Glue = new GlueClient(_http);
        CloudWatch = new CloudWatchClient(_http);
        Firehose = new FirehoseClient(_http);
        S3 = new S3Client(_http);
        DynamoDb = new DynamoDbClient(_http);
        SecretsManager = new SecretsManagerClient(_http);
        Cognito = new CognitoClient(_http);
        ApiGatewayV2 = new ApiGatewayV2Client(_http);
        StepFunctions = new StepFunctionsClient(_http);
        Bedrock = new BedrockClient(_http);
        BedrockAgent = new BedrockAgentClient(_http);
        BedrockAgentRuntime = new BedrockAgentRuntimeClient(_http);
        Ecs = new EcsClient(_http);
        Elbv2 = new Elbv2Client(_http);
        Route53 = new Route53Client(_http);
        Acm = new AcmClient(_http);
        ApplicationAutoScaling = new ApplicationAutoScalingClient(_http);
        Athena = new AthenaClient(_http);
        Organizations = new OrganizationsClient(_http);
        Ssm = new SsmClient(_http);
        Kms = new KmsClient(_http);
        WafV2 = new WafV2Client(_http);
        CloudFront = new CloudFrontClient(_http);
        ServiceQuotas = new ServiceQuotasClient(_http);
    }

    internal static string TrimTrailingSlashes(string url)
    {
        return url.TrimEnd('/');
    }

    /// <summary>
    /// Releases the underlying <see cref="System.Net.Http.HttpClient"/> and its
    /// connection pool. Call this when a client is created per-test or
    /// per-scope; a single long-lived client shared across a suite does not
    /// need explicit disposal.
    /// </summary>
    public void Dispose() => _http.Dispose();

    public string BaseUrl => _http.BaseUrl;

    // ── Health & Reset ─────────────────────────────────────────────

    public Task<HealthResponse> HealthAsync(CancellationToken ct = default) =>
        _http.GetAsync<HealthResponse>("/_fakecloud/health", ct);

    public Task<ResetResponse> ResetAsync(CancellationToken ct = default) =>
        _http.PostEmptyAsync<ResetResponse>("/_reset", ct);

    public Task<ResetServiceResponse> ResetServiceAsync(string service, CancellationToken ct = default) =>
        _http.PostEmptyAsync<ResetServiceResponse>(
            "/_fakecloud/reset/" + HttpTransport.EncodePath(service), ct);

    /// <summary>
    /// Fetch temporary credentials from the general-purpose container/instance
    /// credential endpoint (<c>GET /_fakecloud/credentials</c>). This is the
    /// same JSON an app's AWS SDK fetches when
    /// <c>AWS_CONTAINER_CREDENTIALS_FULL_URI</c> points at fakecloud, letting
    /// a real binary that expects an instance/task role resolve the default
    /// credential chain locally with no code change.
    /// </summary>
    public Task<ContainerCredentialsResponse> CredentialsAsync(CancellationToken ct = default) =>
        _http.GetAsync<ContainerCredentialsResponse>("/_fakecloud/credentials", ct);

    /// <summary>
    /// Fetch the EC2 instance identity document from the IMDS surface
    /// (<c>GET /latest/dynamic/instance-identity/document</c>). Returned as a
    /// raw <see cref="JsonElement"/> pass-through so callers can assert on
    /// the fields they care about.
    /// </summary>
    public Task<JsonElement> InstanceIdentityDocumentAsync(CancellationToken ct = default) =>
        _http.GetAsync<JsonElement>("/latest/dynamic/instance-identity/document", ct);

    /// <summary>
    /// Resolve a name against the Route 53 records fakecloud holds, exactly
    /// as the built-in DNS resolver (<c>--dns</c>) would answer it
    /// (<c>GET /_fakecloud/dns/resolve?name=&lt;n&gt;&amp;type=&lt;A|AAAA|CNAME|MX|TXT|...&gt;</c>).
    /// Lets a test assert what a container pointed at fakecloud for DNS would
    /// see, without opening a UDP socket. Status is one of <c>ANSWERED</c>,
    /// <c>NODATA</c>, <c>NXDOMAIN</c>, <c>NOT_AUTHORITATIVE</c>.
    /// </summary>
    public Task<DnsResolution> DnsResolveAsync(string name, string type = "A", CancellationToken ct = default) =>
        _http.GetAsync<DnsResolution>(
            "/_fakecloud/dns/resolve?name=" + HttpTransport.EncodePath(name)
                + "&type=" + HttpTransport.EncodePath(type),
            ct);

    // ── IAM ───────────────────────────────────────────────────────

    /// <summary>
    /// Create an IAM admin user in a specific account. The account is
    /// standalone: it joins no organization, matching AWS, where a freshly
    /// vended account belongs to no organization until it is invited and
    /// accepts. Use <see cref="CreateAdminInOrgAsync"/> to enroll it into an
    /// organization you already created.
    /// </summary>
    public Task<CreateAdminResponse> CreateAdminAsync(
        string accountId, string userName, CancellationToken ct = default) =>
        _http.PostJsonAsync<CreateAdminResponse>(
            "/_fakecloud/iam/create-admin",
            new CreateAdminRequest(accountId, userName), ct);

    /// <summary>
    /// <see cref="CreateAdminAsync"/> plus enrollment of the account into the
    /// organization <paramref name="organizationId"/> as a member of its root
    /// OU -- the shortcut equivalent of an invite/accept handshake.
    /// </summary>
    public Task<CreateAdminResponse> CreateAdminInOrgAsync(
        string accountId, string userName, string organizationId,
        CancellationToken ct = default) =>
        _http.PostJsonAsync<CreateAdminResponse>(
            "/_fakecloud/iam/create-admin",
            new CreateAdminRequest(accountId, userName, organizationId), ct);

    // ── Sub-client accessors ───────────────────────────────────────

    public LambdaClient Lambda { get; }
    public Ec2Client Ec2 { get; }
    public RdsClient Rds { get; }
    public ElastiCacheClient ElastiCache { get; }
    public EcrClient Ecr { get; }
    public LogsClient Logs { get; }
    public SesClient Ses { get; }
    public SnsClient Sns { get; }
    public SqsClient Sqs { get; }
    public EventsClient Events { get; }
    public SchedulerClient Scheduler { get; }
    public GlueClient Glue { get; }
    public CloudWatchClient CloudWatch { get; }
    public FirehoseClient Firehose { get; }
    public S3Client S3 { get; }
    public DynamoDbClient DynamoDb { get; }
    public SecretsManagerClient SecretsManager { get; }
    public CognitoClient Cognito { get; }
    public ApiGatewayV2Client ApiGatewayV2 { get; }
    public StepFunctionsClient StepFunctions { get; }
    public BedrockClient Bedrock { get; }
    public BedrockAgentClient BedrockAgent { get; }
    public BedrockAgentRuntimeClient BedrockAgentRuntime { get; }
    public EcsClient Ecs { get; }
    public Elbv2Client Elbv2 { get; }
    public Route53Client Route53 { get; }
    public AcmClient Acm { get; }
    public ApplicationAutoScalingClient ApplicationAutoScaling { get; }
    public AthenaClient Athena { get; }
    public OrganizationsClient Organizations { get; }
    public SsmClient Ssm { get; }
    public KmsClient Kms { get; }
    public WafV2Client WafV2 { get; }
    public CloudFrontClient CloudFront { get; }
    public ServiceQuotasClient ServiceQuotas { get; }

    // ── Sub-clients ────────────────────────────────────────────────

    public sealed class LambdaClient
    {
        private readonly HttpTransport _http;
        internal LambdaClient(HttpTransport http) => _http = http;

        public Task<LambdaInvocationsResponse> GetInvocationsAsync(CancellationToken ct = default) =>
            _http.GetAsync<LambdaInvocationsResponse>("/_fakecloud/lambda/invocations", ct);

        public Task<WarmContainersResponse> GetWarmContainersAsync(CancellationToken ct = default) =>
            _http.GetAsync<WarmContainersResponse>("/_fakecloud/lambda/warm-containers", ct);

        public Task<EvictContainerResponse> EvictContainerAsync(
            string functionName, CancellationToken ct = default) =>
            _http.PostEmptyAsync<EvictContainerResponse>(
                "/_fakecloud/lambda/" + HttpTransport.EncodePath(functionName) + "/evict-container", ct);

        /// <summary>
        /// Download the stored zip archive for a Lambda function's deployment
        /// package. <paramref name="qualifierOrLatest"/> is either
        /// <c>"latest"</c> or a concrete version (e.g. <c>"1"</c>); the
        /// corresponding file (<c>latest.zip</c> / <c>&lt;version&gt;.zip</c>)
        /// is fetched verbatim.
        /// </summary>
        public Task<byte[]> DownloadFunctionCodeAsync(
            string accountId, string functionName, string qualifierOrLatest,
            CancellationToken ct = default)
        {
            var file = qualifierOrLatest == "latest" ? "latest.zip" : qualifierOrLatest + ".zip";
            return _http.GetBytesAsync(
                "/_fakecloud/lambda/function-code/"
                    + HttpTransport.EncodePath(accountId) + "/"
                    + HttpTransport.EncodePath(functionName) + "/"
                    + HttpTransport.EncodePath(file),
                ct);
        }

        /// <summary>Download the stored zip archive for a specific Lambda layer version.</summary>
        public Task<byte[]> DownloadLayerContentAsync(
            string accountId, string layerName, long version, CancellationToken ct = default) =>
            _http.GetBytesAsync(
                "/_fakecloud/lambda/layer-content/"
                    + HttpTransport.EncodePath(accountId) + "/"
                    + HttpTransport.EncodePath(layerName) + "/"
                    + version + ".zip",
                ct);
    }

    public sealed class Ec2Client
    {
        private readonly HttpTransport _http;
        internal Ec2Client(HttpTransport http) => _http = http;

        public Task<Ec2InstancesResponse> GetInstancesAsync(CancellationToken ct = default) =>
            _http.GetAsync<Ec2InstancesResponse>("/_fakecloud/ec2/instances", ct);

        /// <summary>
        /// Inspect the real backing network of each EC2 instance — which
        /// Docker/Podman network or k8s NetworkPolicy backs it, its container
        /// IP, and whether security-group enforcement is active or degraded.
        /// A debugging aid for "why can't X reach Y" (issue #1745).
        /// </summary>
        public Task<Ec2InstanceNetworksResponse> GetInstanceNetworksAsync(CancellationToken ct = default) =>
            _http.GetAsync<Ec2InstanceNetworksResponse>("/_fakecloud/ec2/instance-networks", ct);
    }

    public sealed class RdsClient
    {
        private readonly HttpTransport _http;
        internal RdsClient(HttpTransport http) => _http = http;

        public Task<RdsInstancesResponse> GetInstancesAsync(CancellationToken ct = default) =>
            _http.GetAsync<RdsInstancesResponse>("/_fakecloud/rds/instances", ct);

        /// <summary>
        /// Bridge endpoint the PostgreSQL <c>aws_lambda</c> extension calls
        /// into from inside an RDS DB instance container. Normally not driven
        /// by user code directly.
        /// </summary>
        public Task<RdsLambdaInvokeResponse> LambdaInvokeAsync(
            RdsLambdaInvokeRequest req, CancellationToken ct = default) =>
            _http.PostJsonAsync<RdsLambdaInvokeResponse>("/_fakecloud/rds/lambda-invoke", req, ct);

        /// <summary>
        /// Bridge endpoint the PostgreSQL <c>aws_s3</c> extension calls into
        /// to fetch an object from a fakecloud bucket. Body is returned
        /// base64 encoded so JSON transport stays text-only.
        /// </summary>
        public Task<RdsS3ImportResponse> S3ImportAsync(
            RdsS3ImportRequest req, CancellationToken ct = default) =>
            _http.PostJsonAsync<RdsS3ImportResponse>("/_fakecloud/rds/s3-import", req, ct);

        /// <summary>Bridge equivalent of an S3 PutObject driven from inside the DB container.</summary>
        public Task<RdsS3ExportResponse> S3ExportAsync(
            RdsS3ExportRequest req, CancellationToken ct = default) =>
            _http.PostJsonAsync<RdsS3ExportResponse>("/_fakecloud/rds/s3-export", req, ct);
    }

    public sealed class ElastiCacheClient
    {
        private readonly HttpTransport _http;
        internal ElastiCacheClient(HttpTransport http) => _http = http;

        public Task<ElastiCacheClustersResponse> GetClustersAsync(CancellationToken ct = default) =>
            _http.GetAsync<ElastiCacheClustersResponse>("/_fakecloud/elasticache/clusters", ct);

        public Task<ElastiCacheReplicationGroupsResponse> GetReplicationGroupsAsync(
            CancellationToken ct = default) =>
            _http.GetAsync<ElastiCacheReplicationGroupsResponse>(
                "/_fakecloud/elasticache/replication-groups", ct);

        public Task<ElastiCacheServerlessCachesResponse> GetServerlessCachesAsync(
            CancellationToken ct = default) =>
            _http.GetAsync<ElastiCacheServerlessCachesResponse>(
                "/_fakecloud/elasticache/serverless-caches", ct);

        public Task<ElastiCacheAclsResponse> GetAclsAsync(CancellationToken ct = default) =>
            _http.GetAsync<ElastiCacheAclsResponse>("/_fakecloud/elasticache/acls", ct);
    }

    public sealed class EcrClient
    {
        private readonly HttpTransport _http;
        internal EcrClient(HttpTransport http) => _http = http;

        public Task<EcrRepositoriesResponse> GetRepositoriesAsync(CancellationToken ct = default) =>
            _http.GetAsync<EcrRepositoriesResponse>("/_fakecloud/ecr/repositories", ct);

        public Task<EcrImagesResponse> GetImagesAsync(CancellationToken ct = default) =>
            _http.GetAsync<EcrImagesResponse>("/_fakecloud/ecr/images", ct);

        public Task<EcrImagesResponse> GetImagesForRepositoryAsync(
            string repositoryName, CancellationToken ct = default) =>
            _http.GetAsync<EcrImagesResponse>(
                "/_fakecloud/ecr/images?repo=" + Uri.EscapeDataString(repositoryName), ct);

        public Task<EcrPullThroughRulesResponse> GetPullThroughRulesAsync(CancellationToken ct = default) =>
            _http.GetAsync<EcrPullThroughRulesResponse>("/_fakecloud/ecr/pull-through-rules", ct);
    }

    public sealed class LogsClient
    {
        private readonly HttpTransport _http;
        internal LogsClient(HttpTransport http) => _http = http;

        public Task<LogsAnomalyInjectResponse> InjectAnomalyAsync(
            LogsAnomalyInjectRequest req, CancellationToken ct = default) =>
            _http.PostJsonAsync<LogsAnomalyInjectResponse>("/_fakecloud/logs/anomalies/inject", req, ct);

        /// <summary>Persisted CloudWatch Logs delivery configurations.</summary>
        public Task<LogsDeliveryConfigResponse> GetDeliveryConfigAsync(CancellationToken ct = default) =>
            _http.GetAsync<LogsDeliveryConfigResponse>("/_fakecloud/logs/delivery-config", ct);

        /// <summary>Parsed <c>Fields</c> from index policies on the given log group.</summary>
        public Task<LogsFieldIndexesResponse> GetFieldIndexesAsync(
            string logGroupName, CancellationToken ct = default) =>
            _http.GetAsync<LogsFieldIndexesResponse>(
                "/_fakecloud/logs/field-indexes/" + HttpTransport.EncodePath(logGroupName), ct);
    }

    public sealed class SesClient
    {
        private readonly HttpTransport _http;
        internal SesClient(HttpTransport http) => _http = http;

        public Task<SesEmailsResponse> GetEmailsAsync(CancellationToken ct = default) =>
            _http.GetAsync<SesEmailsResponse>("/_fakecloud/ses/emails", ct);

        public Task<InboundEmailResponse> SimulateInboundAsync(
            InboundEmailRequest req, CancellationToken ct = default) =>
            _http.PostJsonAsync<InboundEmailResponse>("/_fakecloud/ses/inbound", req, ct);

        public Task<SesMetrics> GetMetricsAsync(CancellationToken ct = default) =>
            _http.GetAsync<SesMetrics>("/_fakecloud/ses/metrics", ct);

        public Task<SesMailFromStatusResponse> SetMailFromStatusAsync(
            string identity, string status, CancellationToken ct = default) =>
            _http.PostJsonAsync<SesMailFromStatusResponse>(
                "/_fakecloud/ses/identities/" + identity + "/mail-from-status",
                new SesMailFromStatusRequest(status), ct);

        public Task<SesDkimPublicKey> GetDkimPublicKeyAsync(
            string identity, CancellationToken ct = default) =>
            _http.GetAsync<SesDkimPublicKey>(
                "/_fakecloud/ses/identities/" + identity + "/dkim-public-key", ct);

        public Task<SesSandboxResponse> SetSandboxAsync(bool sandbox, CancellationToken ct = default) =>
            _http.PostJsonAsync<SesSandboxResponse>(
                "/_fakecloud/ses/account/sandbox", new SesSandboxRequest(sandbox), ct);

        public Task<SesBouncesResponse> GetBouncesAsync(CancellationToken ct = default) =>
            _http.GetAsync<SesBouncesResponse>("/_fakecloud/ses/bounces", ct);

        public Task<SesMessageInsightsResponse> GetMessageInsightsAsync(
            string messageId, CancellationToken ct = default) =>
            _http.GetAsync<SesMessageInsightsResponse>(
                "/_fakecloud/ses/messages/" + messageId + "/insights", ct);

        public Task<SesSmtpSubmissionsResponse> GetSmtpSubmissionsAsync(CancellationToken ct = default) =>
            _http.GetAsync<SesSmtpSubmissionsResponse>("/_fakecloud/ses/smtp/submissions", ct);

        public Task<SesEventDestinationDeliveriesResponse> GetEventDestinationDeliveriesAsync(
            CancellationToken ct = default) =>
            _http.GetAsync<SesEventDestinationDeliveriesResponse>(
                "/_fakecloud/ses/event-destinations/deliveries", ct);
    }

    public sealed class SnsClient
    {
        private readonly HttpTransport _http;
        internal SnsClient(HttpTransport http) => _http = http;

        public Task<SnsMessagesResponse> GetMessagesAsync(CancellationToken ct = default) =>
            _http.GetAsync<SnsMessagesResponse>("/_fakecloud/sns/messages", ct);

        public Task<PendingConfirmationsResponse> GetPendingConfirmationsAsync(
            CancellationToken ct = default) =>
            _http.GetAsync<PendingConfirmationsResponse>("/_fakecloud/sns/pending-confirmations", ct);

        public Task<ConfirmSubscriptionResponse> ConfirmSubscriptionAsync(
            ConfirmSubscriptionRequest req, CancellationToken ct = default) =>
            _http.PostJsonAsync<ConfirmSubscriptionResponse>("/_fakecloud/sns/confirm-subscription", req, ct);

        /// <summary>
        /// Returns the PEM-encoded SNS signing certificate used by message
        /// signature validators (e.g. <c>aws-sns-validator</c>).
        /// </summary>
        public Task<string> GetCertPemAsync(CancellationToken ct = default) =>
            _http.GetTextAsync("/_fakecloud/sns/cert.pem", ct);

        /// <summary>List captured SMS messages SNS has "delivered".</summary>
        public Task<SnsSmsResponse> GetSmsMessagesAsync(CancellationToken ct = default) =>
            _http.GetAsync<SnsSmsResponse>("/_fakecloud/sns/sms", ct);
    }

    public sealed class SqsClient
    {
        private readonly HttpTransport _http;
        internal SqsClient(HttpTransport http) => _http = http;

        public Task<SqsMessagesResponse> GetMessagesAsync(CancellationToken ct = default) =>
            _http.GetAsync<SqsMessagesResponse>("/_fakecloud/sqs/messages", ct);

        public Task<ExpirationTickResponse> TickExpirationAsync(CancellationToken ct = default) =>
            _http.PostEmptyAsync<ExpirationTickResponse>("/_fakecloud/sqs/expiration-processor/tick", ct);

        public Task<ForceDlqResponse> ForceDlqAsync(string queueName, CancellationToken ct = default) =>
            ForceDlqAsync(queueName, null, null, ct);

        /// <summary>Force a queue's messages to its DLQ, for the queue of that name in
        /// <paramref name="accountId"/> / <paramref name="region"/> (null: the server default).</summary>
        public Task<ForceDlqResponse> ForceDlqAsync(
            string queueName, string? accountId, string? region, CancellationToken ct = default)
        {
            var path = "/_fakecloud/sqs/" + HttpTransport.EncodePath(queueName) + "/force-dlq";
            var sep = "?";
            if (!string.IsNullOrEmpty(accountId))
            {
                path += sep + "accountId=" + HttpTransport.EncodePath(accountId);
                sep = "&";
            }
            if (!string.IsNullOrEmpty(region))
            {
                path += sep + "region=" + HttpTransport.EncodePath(region);
            }
            return _http.PostEmptyAsync<ForceDlqResponse>(path, ct);
        }
    }

    public sealed class ApplicationAutoScalingClient
    {
        private readonly HttpTransport _http;
        internal ApplicationAutoScalingClient(HttpTransport http) => _http = http;

        public Task<AppAsTickResponse> TickAsync(CancellationToken ct = default) =>
            _http.PostEmptyAsync<AppAsTickResponse>("/_fakecloud/application-autoscaling/tick", ct);

        public Task<AppAsScheduledTickResponse> ScheduledTickAsync(CancellationToken ct = default) =>
            _http.PostEmptyAsync<AppAsScheduledTickResponse>(
                "/_fakecloud/application-autoscaling/scheduled-tick", ct);
    }

    public sealed class AthenaClient
    {
        private readonly HttpTransport _http;
        internal AthenaClient(HttpTransport http) => _http = http;

        /// <summary>
        /// List every named query stored in the Athena registry across all
        /// workgroups for the default account. The response includes a
        /// <c>LastUsedAt</c> timestamp the server bumps each time
        /// <c>StartQueryExecution</c> resolves the query by id.
        /// </summary>
        public Task<AthenaNamedQueriesResponse> GetNamedQueriesAsync(CancellationToken ct = default) =>
            _http.GetAsync<AthenaNamedQueriesResponse>("/_fakecloud/athena/named-queries", ct);
    }

    public sealed class EventsClient
    {
        private readonly HttpTransport _http;
        internal EventsClient(HttpTransport http) => _http = http;

        public Task<EventHistoryResponse> GetHistoryAsync(CancellationToken ct = default) =>
            _http.GetAsync<EventHistoryResponse>("/_fakecloud/events/history", ct);

        public Task<FireRuleResponse> FireRuleAsync(FireRuleRequest req, CancellationToken ct = default) =>
            _http.PostJsonAsync<FireRuleResponse>("/_fakecloud/events/fire-rule", req, ct);
    }

    public sealed class SchedulerClient
    {
        private readonly HttpTransport _http;
        internal SchedulerClient(HttpTransport http) => _http = http;

        public Task<SchedulerSchedulesResponse> GetSchedulesAsync(CancellationToken ct = default) =>
            _http.GetAsync<SchedulerSchedulesResponse>("/_fakecloud/scheduler/schedules", ct);

        public Task<FireScheduleResponse> FireScheduleAsync(
            string group, string name, CancellationToken ct = default) =>
            FireScheduleAsync(group, name, null, null, ct);

        /// <summary>Fire the schedule of that group and name in <paramref name="accountId"/> /
        /// <paramref name="region"/> (null: the server default).</summary>
        public Task<FireScheduleResponse> FireScheduleAsync(
            string group, string name, string? accountId, string? region, CancellationToken ct = default)
        {
            var path = "/_fakecloud/scheduler/fire/" + HttpTransport.EncodePath(group) + "/"
                + HttpTransport.EncodePath(name);
            var sep = "?";
            if (!string.IsNullOrEmpty(accountId))
            {
                path += sep + "accountId=" + HttpTransport.EncodePath(accountId);
                sep = "&";
            }
            if (!string.IsNullOrEmpty(region))
            {
                path += sep + "region=" + HttpTransport.EncodePath(region);
            }
            return _http.PostEmptyAsync<FireScheduleResponse>(path, ct);
        }
    }

    public sealed class GlueClient
    {
        private readonly HttpTransport _http;
        internal GlueClient(HttpTransport http) => _http = http;

        public Task<GlueJobsResponse> GetJobsAsync(CancellationToken ct = default) =>
            _http.GetAsync<GlueJobsResponse>("/_fakecloud/glue/jobs", ct);

        public Task<GlueJobRunsResponse> GetJobRunsAsync(
            string? jobName = null, CancellationToken ct = default)
        {
            var path = "/_fakecloud/glue/job-runs";
            if (!string.IsNullOrEmpty(jobName))
            {
                path += "?job_name=" + HttpTransport.EncodePath(jobName);
            }
            return _http.GetAsync<GlueJobRunsResponse>(path, ct);
        }

        public Task<GlueCrawlersResponse> GetCrawlersAsync(CancellationToken ct = default) =>
            _http.GetAsync<GlueCrawlersResponse>("/_fakecloud/glue/crawlers", ct);
    }

    public sealed class CloudWatchClient
    {
        private readonly HttpTransport _http;
        internal CloudWatchClient(HttpTransport http) => _http = http;

        public Task<CloudWatchAlarmsResponse> GetAlarmsAsync(CancellationToken ct = default) =>
            _http.GetAsync<CloudWatchAlarmsResponse>("/_fakecloud/cloudwatch/alarms", ct);

        public Task<CloudWatchMetricsResponse> GetMetricsAsync(CancellationToken ct = default) =>
            _http.GetAsync<CloudWatchMetricsResponse>("/_fakecloud/cloudwatch/metrics", ct);
    }

    public sealed class FirehoseClient
    {
        private readonly HttpTransport _http;
        internal FirehoseClient(HttpTransport http) => _http = http;

        public Task<FirehoseDeliveryStreamsResponse> GetDeliveryStreamsAsync(
            CancellationToken ct = default) =>
            _http.GetAsync<FirehoseDeliveryStreamsResponse>("/_fakecloud/firehose/delivery-streams", ct);
    }

    public sealed class S3Client
    {
        private readonly HttpTransport _http;
        internal S3Client(HttpTransport http) => _http = http;

        public Task<S3NotificationsResponse> GetNotificationsAsync(CancellationToken ct = default) =>
            _http.GetAsync<S3NotificationsResponse>("/_fakecloud/s3/notifications", ct);

        public Task<LifecycleTickResponse> TickLifecycleAsync(CancellationToken ct = default) =>
            _http.PostEmptyAsync<LifecycleTickResponse>("/_fakecloud/s3/lifecycle-processor/tick", ct);

        public Task<S3AccessPointsResponse> GetAccessPointsAsync(CancellationToken ct = default) =>
            _http.GetAsync<S3AccessPointsResponse>("/_fakecloud/s3/access-points", ct);

        public Task<S3ObjectLambdaResponsesResponse> GetObjectLambdaResponsesAsync(
            CancellationToken ct = default) =>
            _http.GetAsync<S3ObjectLambdaResponsesResponse>("/_fakecloud/s3/object-lambda-responses", ct);
    }

    public sealed class DynamoDbClient
    {
        private readonly HttpTransport _http;
        internal DynamoDbClient(HttpTransport http) => _http = http;

        public Task<TtlTickResponse> TickTtlAsync(CancellationToken ct = default) =>
            _http.PostEmptyAsync<TtlTickResponse>("/_fakecloud/dynamodb/ttl-processor/tick", ct);

        /// <summary>
        /// Write the current DynamoDB state as a canonical snapshot on demand.
        /// When <paramref name="dataPath"/> is non-null the snapshot is
        /// written to <c>&lt;dataPath&gt;/dynamodb/snapshot.json</c>; when
        /// null it is written to the server's configured persistent store (an
        /// error if none is configured).
        /// </summary>
        public Task<DynamoDbSnapshotSaveResponse> SaveSnapshotAsync(
            string? dataPath, CancellationToken ct = default) =>
            _http.PostJsonAsync<DynamoDbSnapshotSaveResponse>(
                "/_fakecloud/dynamodb/snapshot/save", new DynamoDbSnapshotSaveRequest(dataPath), ct);
    }

    public sealed class SecretsManagerClient
    {
        private readonly HttpTransport _http;
        internal SecretsManagerClient(HttpTransport http) => _http = http;

        public Task<RotationTickResponse> TickRotationAsync(CancellationToken ct = default) =>
            _http.PostEmptyAsync<RotationTickResponse>(
                "/_fakecloud/secretsmanager/rotation-scheduler/tick", ct);
    }

    public sealed class CognitoClient
    {
        private readonly HttpTransport _http;
        internal CognitoClient(HttpTransport http) => _http = http;

        public Task<UserConfirmationCodes> GetUserCodesAsync(
            string poolId, string username, CancellationToken ct = default) =>
            _http.GetAsync<UserConfirmationCodes>(
                "/_fakecloud/cognito/confirmation-codes/"
                    + HttpTransport.EncodePath(poolId) + "/"
                    + HttpTransport.EncodePath(username),
                ct);

        public Task<ConfirmationCodesResponse> GetConfirmationCodesAsync(CancellationToken ct = default) =>
            _http.GetAsync<ConfirmationCodesResponse>("/_fakecloud/cognito/confirmation-codes", ct);

        /// <summary>
        /// Force-confirm a user, bypassing the confirmation code flow.
        /// fakecloud returns a JSON body with an <c>error</c> field on 404
        /// for unknown users, so the body is decoded and surfaced as a
        /// <see cref="FakeCloudException"/>.
        /// </summary>
        public async Task<ConfirmUserResponse> ConfirmUserAsync(
            ConfirmUserRequest req, CancellationToken ct = default)
        {
            var (status, parsed, raw) = await _http
                .PostJsonAllowingErrorAsync<ConfirmUserResponse>("/_fakecloud/cognito/confirm-user", req, ct)
                .ConfigureAwait(false);
            if (parsed is null)
            {
                throw new FakeCloudException(status, raw);
            }
            if (status == 404)
            {
                throw new FakeCloudException(404, parsed.Error ?? "user not found");
            }
            if (status is < 200 or >= 300)
            {
                throw new FakeCloudException(status, raw);
            }
            return parsed;
        }

        public Task<TokensResponse> GetTokensAsync(CancellationToken ct = default) =>
            _http.GetAsync<TokensResponse>("/_fakecloud/cognito/tokens", ct);

        public Task<ExpireTokensResponse> ExpireTokensAsync(
            ExpireTokensRequest req, CancellationToken ct = default) =>
            _http.PostJsonAsync<ExpireTokensResponse>("/_fakecloud/cognito/expire-tokens", req, ct);

        public Task<AuthEventsResponse> GetAuthEventsAsync(CancellationToken ct = default) =>
            _http.GetAsync<AuthEventsResponse>("/_fakecloud/cognito/auth-events", ct);

        /// <summary>
        /// Returns the PreTokenGeneration Lambda trigger invocation log
        /// recorded by <c>InitiateAuth</c>. Each entry has the full
        /// request/response payloads plus pre-parsed claim additions,
        /// suppressions, and group overrides.
        /// </summary>
        public Task<PreTokenGenInvocationsResponse> GetPreTokenGenInvocationsAsync(
            CancellationToken ct = default) =>
            _http.GetAsync<PreTokenGenInvocationsResponse>(
                "/_fakecloud/cognito/pretokengen/invocations", ct);

        public Task<MintAuthorizationCodeResponse> MintAuthorizationCodeAsync(
            MintAuthorizationCodeRequest req, CancellationToken ct = default) =>
            _http.PostJsonAsync<MintAuthorizationCodeResponse>(
                "/_fakecloud/cognito/authorization-codes", req, ct);

        /// <summary>
        /// Give a user a verified authenticator-app (TOTP) secret chosen by
        /// the caller, so its codes can be generated outside fakecloud.
        /// </summary>
        public Task<SetSoftwareTokenResponse> SetSoftwareTokenAsync(
            SetSoftwareTokenRequest req, CancellationToken ct = default) =>
            _http.PostJsonAsync<SetSoftwareTokenResponse>(
                "/_fakecloud/cognito/software-token", req, ct);

        public Task<CompromisedPasswordsResponse> SetCompromisedPasswordsAsync(
            CompromisedPasswordsRequest req, CancellationToken ct = default) =>
            _http.PostJsonAsync<CompromisedPasswordsResponse>(
                "/_fakecloud/cognito/compromised-passwords", req, ct);

        public Task<WebAuthnCredentialsResponse> GetWebAuthnCredentialsAsync(
            CancellationToken ct = default) =>
            _http.GetAsync<WebAuthnCredentialsResponse>("/_fakecloud/cognito/webauthn-credentials", ct);
    }

    public sealed class ApiGatewayV2Client
    {
        private readonly HttpTransport _http;
        internal ApiGatewayV2Client(HttpTransport http) => _http = http;

        public Task<ApiGatewayV2RequestsResponse> GetRequestsAsync(CancellationToken ct = default) =>
            _http.GetAsync<ApiGatewayV2RequestsResponse>("/_fakecloud/apigatewayv2/requests", ct);

        /// <summary>List every active WebSocket connection tracked by API Gateway v2.</summary>
        public Task<ApiGatewayV2ConnectionsResponse> GetConnectionsAsync(CancellationToken ct = default) =>
            _http.GetAsync<ApiGatewayV2ConnectionsResponse>("/_fakecloud/apigatewayv2/connections", ct);

        /// <summary>
        /// Fetch the mTLS truststore info for a custom domain name. Returns a
        /// raw <see cref="JsonElement"/> so the surface stays
        /// forward-compatible with server-side additions.
        /// </summary>
        public Task<JsonElement> GetDomainNameMtlsInfoAsync(
            string domainName, CancellationToken ct = default) =>
            _http.GetAsync<JsonElement>(
                "/_fakecloud/apigatewayv2/domain-names/"
                    + HttpTransport.EncodePath(domainName) + "/mtls-info",
                ct);

        /// <summary>
        /// Build the WebSocket URL fakecloud serves for the given API id.
        /// When <paramref name="stage"/> is null, the default
        /// <c>"$default"</c> stage is used.
        /// </summary>
        public string WsUrl(string apiId, string? stage = null)
        {
            var baseUrl = _http.BaseUrl;
            string wsBase;
            if (baseUrl.StartsWith("https://", StringComparison.Ordinal))
            {
                wsBase = "wss://" + baseUrl["https://".Length..];
            }
            else if (baseUrl.StartsWith("http://", StringComparison.Ordinal))
            {
                wsBase = "ws://" + baseUrl["http://".Length..];
            }
            else
            {
                wsBase = baseUrl;
            }
            var path = wsBase + "/_fakecloud/apigatewayv2/ws/" + HttpTransport.EncodePath(apiId);
            return stage is null ? path : path + "?stage=" + Uri.EscapeDataString(stage);
        }
    }

    public sealed class StepFunctionsClient
    {
        private readonly HttpTransport _http;
        internal StepFunctionsClient(HttpTransport http) => _http = http;

        public Task<StepFunctionsExecutionsResponse> GetExecutionsAsync(CancellationToken ct = default) =>
            _http.GetAsync<StepFunctionsExecutionsResponse>("/_fakecloud/stepfunctions/executions", ct);

        public Task<StepFunctionsSyncExecutionsResponse> GetSyncExecutionsAsync(
            CancellationToken ct = default) =>
            _http.GetAsync<StepFunctionsSyncExecutionsResponse>(
                "/_fakecloud/stepfunctions/sync-executions", ct);

        public Task<StepFunctionsExecutionTreeResponse> GetExecutionTreeAsync(
            string arn, CancellationToken ct = default) =>
            _http.GetAsync<StepFunctionsExecutionTreeResponse>(
                "/_fakecloud/stepfunctions/execution-tree/" + HttpTransport.EncodePath(arn), ct);

        public Task<SfnEnqueueActivityTaskResponse> EnqueueActivityTaskAsync(
            SfnEnqueueActivityTaskRequest req, CancellationToken ct = default) =>
            _http.PostJsonAsync<SfnEnqueueActivityTaskResponse>(
                "/_fakecloud/stepfunctions/enqueue-activity-task", req, ct);
    }

    public sealed class BedrockClient
    {
        private readonly HttpTransport _http;
        internal BedrockClient(HttpTransport http) => _http = http;

        public Task<BedrockInvocationsResponse> GetInvocationsAsync(CancellationToken ct = default) =>
            _http.GetAsync<BedrockInvocationsResponse>("/_fakecloud/bedrock/invocations", ct);

        public Task<BedrockModelResponseConfig> SetModelResponseAsync(
            string modelId, string response, CancellationToken ct = default) =>
            _http.PostTextAsync<BedrockModelResponseConfig>(
                "/_fakecloud/bedrock/models/" + HttpTransport.EncodePath(modelId) + "/response",
                response, ct);

        public Task<BedrockModelResponseConfig> SetResponseRulesAsync(
            string modelId, IReadOnlyList<BedrockResponseRule> rules, CancellationToken ct = default) =>
            _http.PostJsonAsync<BedrockModelResponseConfig>(
                "/_fakecloud/bedrock/models/" + HttpTransport.EncodePath(modelId) + "/responses",
                new Dictionary<string, IReadOnlyList<BedrockResponseRule>> { ["rules"] = rules }, ct);

        public Task<BedrockModelResponseConfig> ClearResponseRulesAsync(
            string modelId, CancellationToken ct = default) =>
            _http.DeleteAsync<BedrockModelResponseConfig>(
                "/_fakecloud/bedrock/models/" + HttpTransport.EncodePath(modelId) + "/responses", ct);

        public Task<BedrockStatusResponse> QueueFaultAsync(
            BedrockFaultRule rule, CancellationToken ct = default) =>
            _http.PostJsonAsync<BedrockStatusResponse>("/_fakecloud/bedrock/faults", rule, ct);

        public Task<BedrockFaultsResponse> GetFaultsAsync(CancellationToken ct = default) =>
            _http.GetAsync<BedrockFaultsResponse>("/_fakecloud/bedrock/faults", ct);

        public Task<BedrockStatusResponse> ClearFaultsAsync(CancellationToken ct = default) =>
            _http.DeleteAsync<BedrockStatusResponse>("/_fakecloud/bedrock/faults", ct);
    }

    /// <summary>Bedrock Agent (control plane) introspection sub-client.</summary>
    public sealed class BedrockAgentClient
    {
        private readonly HttpTransport _http;
        internal BedrockAgentClient(HttpTransport http) => _http = http;

        public Task<BedrockAgentAgentsResponse> GetAgentsAsync(CancellationToken ct = default) =>
            _http.GetAsync<BedrockAgentAgentsResponse>("/_fakecloud/bedrock-agent/agents", ct);
    }

    /// <summary>Bedrock Agent Runtime (data plane) introspection sub-client.</summary>
    public sealed class BedrockAgentRuntimeClient
    {
        private readonly HttpTransport _http;
        internal BedrockAgentRuntimeClient(HttpTransport http) => _http = http;

        public Task<BedrockAgentRuntimeInvocationsResponse> GetInvocationsAsync(
            CancellationToken ct = default) =>
            _http.GetAsync<BedrockAgentRuntimeInvocationsResponse>(
                "/_fakecloud/bedrock-agent-runtime/invocations", ct);
    }

    public sealed class EcsClient
    {
        private readonly HttpTransport _http;
        internal EcsClient(HttpTransport http) => _http = http;

        public Task<EcsClustersResponse> GetClustersAsync(CancellationToken ct = default) =>
            _http.GetAsync<EcsClustersResponse>("/_fakecloud/ecs/clusters", ct);

        /// <summary>List every task fakecloud is tracking, optionally filtered by cluster and status.</summary>
        public Task<EcsTasksResponse> GetTasksAsync(
            string? cluster = null, string? status = null, CancellationToken ct = default)
        {
            var qs = new List<string>(2);
            if (!string.IsNullOrEmpty(cluster))
            {
                qs.Add("cluster=" + HttpTransport.EncodePath(cluster));
            }
            if (!string.IsNullOrEmpty(status))
            {
                qs.Add("status=" + HttpTransport.EncodePath(status));
            }
            var path = "/_fakecloud/ecs/tasks";
            if (qs.Count > 0)
            {
                path += "?" + string.Join('&', qs);
            }
            return _http.GetAsync<EcsTasksResponse>(path, ct);
        }

        /// <summary>Fetch a single task snapshot by task ID.</summary>
        public Task<EcsTask> GetTaskAsync(string taskId, CancellationToken ct = default) =>
            _http.GetAsync<EcsTask>("/_fakecloud/ecs/tasks/" + HttpTransport.EncodePath(taskId), ct);

        /// <summary>Captured docker stdout/stderr for a task plus its exit code if known.</summary>
        public Task<EcsTaskLogsResponse> GetTaskLogsAsync(string taskId, CancellationToken ct = default) =>
            _http.GetAsync<EcsTaskLogsResponse>(
                "/_fakecloud/ecs/tasks/" + HttpTransport.EncodePath(taskId) + "/logs", ct);

        /// <summary>
        /// SIGTERM (then SIGKILL after 10s) the task's running container via
        /// the runtime. Returns the updated task snapshot.
        /// </summary>
        public Task<EcsTask> ForceStopTaskAsync(string taskId, CancellationToken ct = default) =>
            _http.PostEmptyAsync<EcsTask>(
                "/_fakecloud/ecs/tasks/" + HttpTransport.EncodePath(taskId) + "/force-stop", ct);

        /// <summary>
        /// Flip a task to STOPPED without killing the container — useful for
        /// simulating failed tasks deterministically in tests.
        /// </summary>
        public Task<EcsTask> MarkTaskFailedAsync(
            string taskId, EcsMarkFailedRequest req, CancellationToken ct = default) =>
            _http.PostJsonAsync<EcsTask>(
                "/_fakecloud/ecs/tasks/" + HttpTransport.EncodePath(taskId) + "/mark-failed", req, ct);

        /// <summary>Replay the lifecycle event log.</summary>
        public Task<EcsEventsResponse> GetEventsAsync(CancellationToken ct = default) =>
            _http.GetAsync<EcsEventsResponse>("/_fakecloud/ecs/events", ct);

        /// <summary>
        /// Return the aggregated v4 metadata dump (the same shape
        /// <c>ECS_CONTAINER_METADATA_URI_V4</c> exposes to a container) for
        /// the task with the given full ARN. The ARN is URL-encoded into the
        /// path before the request is issued.
        /// </summary>
        public Task<EcsTaskMetadataResponse> GetTaskMetadataAsync(
            string taskArn, CancellationToken ct = default) =>
            _http.GetAsync<EcsTaskMetadataResponse>(
                "/_fakecloud/ecs/metadata/" + HttpTransport.EncodePath(taskArn), ct);

        /// <summary>
        /// Return the task-role credentials a running task's containers get
        /// from <c>http://169.254.170.2</c> plus the path in
        /// <c>AWS_CONTAINER_CREDENTIALS_RELATIVE_URI</c> (or from
        /// <c>AWS_CONTAINER_CREDENTIALS_FULL_URI</c> when the task's network
        /// could not be routed): a session for the
        /// task role named after the task ID, in the ECS credentials wire
        /// shape (PascalCase keys). Throws once the task has stopped, or for
        /// an unknown task or one without a task role (HTTP 400
        /// <c>InvalidIdInRequest</c>, as the ECS agent answers).
        /// </summary>
        public Task<EcsTaskCredentialsResponse> GetCredentialsAsync(
            string taskId, CancellationToken ct = default) =>
            _http.GetAsync<EcsTaskCredentialsResponse>(
                "/_fakecloud/ecs/creds/" + HttpTransport.EncodePath(taskId), ct);

        /// <summary>
        /// Return the raw v3 task metadata document for the task — a
        /// free-form JSON object mirroring what
        /// <c>ECS_CONTAINER_METADATA_URI</c> would expose; returned as a raw
        /// <see cref="JsonElement"/> pass-through to stay forward-compatible.
        /// </summary>
        public Task<JsonElement> GetMetadataV3Async(string taskId, CancellationToken ct = default) =>
            _http.GetAsync<JsonElement>("/_fakecloud/ecs/v3/" + HttpTransport.EncodePath(taskId), ct);

        /// <summary>
        /// Return the raw v4 task metadata document for the task. Returned as
        /// a raw <see cref="JsonElement"/> pass-through to stay
        /// forward-compatible.
        /// </summary>
        public Task<JsonElement> GetMetadataV4Async(string taskId, CancellationToken ct = default) =>
            _http.GetAsync<JsonElement>("/_fakecloud/ecs/v4/" + HttpTransport.EncodePath(taskId), ct);
    }

    public sealed class Elbv2Client
    {
        private readonly HttpTransport _http;
        internal Elbv2Client(HttpTransport http) => _http = http;

        public Task<Elbv2LoadBalancersResponse> GetLoadBalancersAsync(CancellationToken ct = default) =>
            _http.GetAsync<Elbv2LoadBalancersResponse>("/_fakecloud/elbv2/load-balancers", ct);

        public Task<Elbv2TargetGroupsResponse> GetTargetGroupsAsync(CancellationToken ct = default) =>
            _http.GetAsync<Elbv2TargetGroupsResponse>("/_fakecloud/elbv2/target-groups", ct);

        public Task<Elbv2ListenersResponse> GetListenersAsync(CancellationToken ct = default) =>
            _http.GetAsync<Elbv2ListenersResponse>("/_fakecloud/elbv2/listeners", ct);

        public Task<Elbv2RulesResponse> GetRulesAsync(CancellationToken ct = default) =>
            _http.GetAsync<Elbv2RulesResponse>("/_fakecloud/elbv2/rules", ct);

        /// <summary>
        /// Force every buffered access-log + connection-log line to flush to
        /// S3 right now, bypassing the periodic 60-second timer.
        /// </summary>
        public Task<Elbv2FlushAccessLogsResponse> FlushAccessLogsAsync(CancellationToken ct = default) =>
            _http.PostEmptyAsync<Elbv2FlushAccessLogsResponse>("/_fakecloud/elbv2/access-logs/flush", ct);

        /// <summary>
        /// Returns the WAFv2 association/evaluation counts the ELBv2 service
        /// has accumulated. The exact shape of <c>Counts</c> is
        /// service-internal and intentionally returned as free-form JSON.
        /// </summary>
        public Task<Elbv2WafCountsResponse> GetWafCountsAsync(CancellationToken ct = default) =>
            _http.GetAsync<Elbv2WafCountsResponse>("/_fakecloud/elbv2/waf-counts", ct);
    }

    /// <summary>
    /// Route 53 admin client. Wraps the per-health-check status admin
    /// endpoint that lets tests flip a stored health check between healthy
    /// and unhealthy without a live prober, so failover and multi-value
    /// routing can be exercised end-to-end.
    /// </summary>
    public sealed class Route53Client
    {
        private readonly HttpTransport _http;
        internal Route53Client(HttpTransport http) => _http = http;

        /// <summary>
        /// Flip a Route 53 health check's reported status.
        /// <paramref name="status"/> is <c>"Success"</c> or <c>"Failure"</c>;
        /// <paramref name="reason"/> is appended to the <c>&lt;Status&gt;</c>
        /// element when status is Failure (pass null to omit).
        /// </summary>
        public Task SetHealthCheckStatusAsync(
            string healthCheckId, string status, string? reason = null, CancellationToken ct = default) =>
            _http.PostJsonNoContentAsync(
                "/_fakecloud/route53/health-checks/" + HttpTransport.EncodePath(healthCheckId) + "/status",
                new SetHealthCheckStatusRequest(status, reason), ct);

        /// <summary>
        /// Fetch the deterministic DNSSEC material (DNSKEY + DS digest) for a
        /// hosted zone with at least one ACTIVE Key Signing Key. Throws
        /// <see cref="FakeCloudException"/> with status 404 when the zone has
        /// no active KSK.
        /// </summary>
        public Task<Route53DnssecMaterialResponse> GetDnssecMaterialAsync(
            string zoneId, CancellationToken ct = default) =>
            _http.GetAsync<Route53DnssecMaterialResponse>(
                "/_fakecloud/route53/zones/" + HttpTransport.EncodePath(zoneId) + "/dnssec", ct);

        /// <summary>
        /// Sign an RRset under the zone's first ACTIVE KSK. Returns raw RRSIG
        /// fields so tests can verify the signature against the DNSKEY public
        /// key from <see cref="GetDnssecMaterialAsync"/>.
        /// </summary>
        public Task<Route53DnssecSignResponse> SignDnssecAsync(
            string zoneId, Route53DnssecSignRequest req, CancellationToken ct = default) =>
            _http.PostJsonAsync<Route53DnssecSignResponse>(
                "/_fakecloud/route53/zones/" + HttpTransport.EncodePath(zoneId) + "/dnssec/sign", req, ct);
    }

    /// <summary>
    /// ACM admin client. Wraps the per-certificate status admin endpoint
    /// that lets tests flip a stored certificate between PENDING_VALIDATION,
    /// ISSUED, FAILED, and VALIDATION_TIMED_OUT without waiting on the
    /// auto-issue tick, so validation-failure flows can be exercised
    /// end-to-end.
    /// </summary>
    public sealed class AcmClient
    {
        private readonly HttpTransport _http;
        internal AcmClient(HttpTransport http) => _http = http;

        private static string ExtractId(string arnOrId)
        {
            var idx = arnOrId.LastIndexOf("certificate/", StringComparison.Ordinal);
            return idx >= 0 ? arnOrId[(idx + "certificate/".Length)..] : arnOrId;
        }

        /// <summary>
        /// Flip an ACM certificate's status synchronously.
        /// <paramref name="status"/> is one of <c>"ISSUED"</c>,
        /// <c>"FAILED"</c>, <c>"VALIDATION_TIMED_OUT"</c>;
        /// <paramref name="reason"/> is recorded as <c>FailureReason</c> on
        /// <c>DescribeCertificate</c> for non-ISSUED statuses (pass null to
        /// omit). <paramref name="arnOrId"/> accepts either the full ACM ARN
        /// or the trailing UUID portion.
        /// </summary>
        public Task SetCertificateStatusAsync(
            string arnOrId, string status, string? reason = null, CancellationToken ct = default) =>
            _http.PostJsonNoContentAsync(
                "/_fakecloud/acm/certificates/" + HttpTransport.EncodePath(ExtractId(arnOrId)) + "/status",
                new SetCertificateStatusRequest(status, reason), ct);

        /// <summary>
        /// Approve a <c>PENDING_VALIDATION</c> certificate. Synchronous
        /// equivalent of "the user clicked the validation link in the email"
        /// — flips the cert to <c>ISSUED</c> and refreshes its renewal
        /// eligibility / RenewalSummary. EMAIL-validated certs do not
        /// auto-issue, so tests drive their issuance through this endpoint.
        /// <paramref name="arnOrId"/> accepts either the full ACM ARN or the
        /// trailing UUID portion.
        /// </summary>
        public Task ApproveCertificateAsync(string arnOrId, CancellationToken ct = default) =>
            _http.PostNoContentAsync(
                "/_fakecloud/acm/certificates/" + HttpTransport.EncodePath(ExtractId(arnOrId)) + "/approve",
                ct);

        /// <summary>
        /// Inspect a stored certificate's PEM block counts and byte sizes.
        /// Returns <c>ExternalCaValidated=false</c> to document that
        /// fakecloud does not run real X.509 verification — use the
        /// byte/block counts to confirm uploaded chains round-trip intact,
        /// especially for <c>ImportCertificate</c> flows.
        /// <paramref name="arnOrId"/> accepts either the full ACM ARN or the
        /// trailing UUID portion.
        /// </summary>
        public Task<AcmCertificateChainInfo> GetCertificateChainInfoAsync(
            string arnOrId, CancellationToken ct = default) =>
            _http.GetAsync<AcmCertificateChainInfo>(
                "/_fakecloud/acm/certificates/" + HttpTransport.EncodePath(ExtractId(arnOrId))
                    + "/chain-info",
                ct);
    }

    /// <summary>
    /// AWS Organizations admin/introspection sub-client. Bypasses IAM so
    /// tests can assert on org shape without management-account credentials.
    /// </summary>
    public sealed class OrganizationsClient
    {
        private readonly HttpTransport _http;
        internal OrganizationsClient(HttpTransport http) => _http = http;

        /// <summary>
        /// List every member account in the org with lifecycle state, parent
        /// OU, tags, and directly-attached SCPs. Returns an empty list (and
        /// null management/master ids) when no organization has been created
        /// yet.
        /// </summary>
        public Task<OrganizationsAccountsResponse> GetAccountsAsync(CancellationToken ct = default) =>
            _http.GetAsync<OrganizationsAccountsResponse>("/_fakecloud/organizations/accounts", ct);

        /// <summary>
        /// List every billing responsibility transfer in the org, with
        /// direction (INBOUND/OUTBOUND), lifecycle status, and the active
        /// handshake. Returns an empty list when no organization has been
        /// created.
        /// </summary>
        public Task<OrganizationsResponsibilityTransfersResponse> GetResponsibilityTransfersAsync(
            CancellationToken ct = default) =>
            _http.GetAsync<OrganizationsResponsibilityTransfersResponse>(
                "/_fakecloud/organizations/responsibility-transfers", ct);
    }

    /// <summary>
    /// Systems Manager admin sub-client. Wraps the <c>/_fakecloud/ssm/*</c>
    /// endpoints that let tests force command/invocation lifecycle
    /// transitions and seed sessions without going through the real
    /// agent-driven path.
    /// </summary>
    public sealed class SsmClient
    {
        private readonly HttpTransport _http;
        internal SsmClient(HttpTransport http) => _http = http;

        /// <summary>
        /// Flip the status of every invocation under a SendCommand command
        /// id. <paramref name="accountId"/> may be null to target the default
        /// account.
        /// </summary>
        public Task<SetSsmCommandStatusResponse> SetCommandStatusAsync(
            string commandId, string? accountId, string status, CancellationToken ct = default) =>
            _http.PostJsonAsync<SetSsmCommandStatusResponse>(
                "/_fakecloud/ssm/commands/" + HttpTransport.EncodePath(commandId) + "/status",
                new SetSsmCommandStatusRequest(accountId, status), ct);

        /// <summary>
        /// Force a command (or a specific invocation under it) into
        /// <c>Failed</c>. Pass null for the request to flip every invocation
        /// on the command with default status detail.
        /// </summary>
        public Task<FailSsmCommandResponse> FailCommandAsync(
            string commandId, FailSsmCommandRequest? req = null, CancellationToken ct = default) =>
            _http.PostJsonAsync<FailSsmCommandResponse>(
                "/_fakecloud/ssm/commands/" + HttpTransport.EncodePath(commandId) + "/fail",
                req ?? new FailSsmCommandRequest(null, null, null, null), ct);

        /// <summary>
        /// Return every parameter-policy event recorded for the given
        /// account (default account when <paramref name="accountId"/> is null).
        /// </summary>
        public Task<SsmParameterPolicyEventsResponse> GetParameterPolicyEventsAsync(
            string? accountId = null, CancellationToken ct = default)
        {
            var path = "/_fakecloud/ssm/parameter-policy-events";
            if (!string.IsNullOrEmpty(accountId))
            {
                path += "?accountId=" + HttpTransport.EncodePath(accountId);
            }
            return _http.GetAsync<SsmParameterPolicyEventsResponse>(path, ct);
        }

        /// <summary>
        /// Drop a fake Session Manager session into state without going
        /// through <c>StartSession</c>, so <c>DescribeSessions</c> /
        /// <c>TerminateSession</c> can be exercised end-to-end.
        /// </summary>
        public Task<InjectSsmSessionResponse> InjectSessionAsync(
            InjectSsmSessionRequest req, CancellationToken ct = default) =>
            _http.PostJsonAsync<InjectSsmSessionResponse>("/_fakecloud/ssm/sessions/inject", req, ct);
    }

    /// <summary>
    /// KMS admin sub-client. Exposes the data-plane usage recorder so tests
    /// can assert on which keys were touched, with what encryption context,
    /// by which service.
    /// </summary>
    public sealed class KmsClient
    {
        private readonly HttpTransport _http;
        internal KmsClient(HttpTransport http) => _http = http;

        /// <summary>Return every recorded KMS data-plane invocation.</summary>
        public Task<KmsUsageResponse> GetUsageAsync(CancellationToken ct = default) =>
            _http.GetAsync<KmsUsageResponse>("/_fakecloud/kms/usage", ct);
    }

    /// <summary>
    /// WAFv2 admin sub-client. Wraps the <c>/_fakecloud/wafv2/evaluate</c>
    /// endpoint that runs an arbitrary evaluation payload through the stored
    /// web ACL rule set. Request and response are intentionally free-form
    /// JSON.
    /// </summary>
    public sealed class WafV2Client
    {
        private readonly HttpTransport _http;
        internal WafV2Client(HttpTransport http) => _http = http;

        /// <summary>
        /// Evaluate an arbitrary request payload against the stored WAFv2
        /// rule set. Both the body and the response are free-form JSON — the
        /// exact shape is service-internal.
        /// </summary>
        public Task<JsonElement> EvaluateAsync(object request, CancellationToken ct = default) =>
            _http.PostJsonAsync<JsonElement>("/_fakecloud/wafv2/evaluate", request, ct);
    }

    /// <summary>
    /// CloudFront admin sub-client. Wraps the per-distribution status admin
    /// endpoint that lets tests synchronously flip a stored Distribution
    /// between <c>Deployed</c> and <c>InProgress</c> without waiting on the
    /// propagation tick.
    /// </summary>
    public sealed class CloudFrontClient
    {
        private readonly HttpTransport _http;
        internal CloudFrontClient(HttpTransport http) => _http = http;

        /// <summary>
        /// List every CloudFront Distribution fakecloud is tracking. Each
        /// entry carries its <c>&lt;id&gt;.cloudfront.net</c> domain name
        /// (send it as the <c>Host</c> header to fakecloud's main endpoint to
        /// reach the data plane), its <c>Enabled</c> flag, and whether the
        /// in-process data plane currently <c>Served</c> it.
        /// </summary>
        public Task<CloudFrontDistributionsResponse> GetDistributionsAsync(
            CancellationToken ct = default) =>
            _http.GetAsync<CloudFrontDistributionsResponse>("/_fakecloud/cloudfront/distributions", ct);

        /// <summary>
        /// Flip a stored CloudFront Distribution's status (e.g.
        /// <c>"Deployed"</c> or <c>"InProgress"</c>). Throws
        /// <see cref="FakeCloudException"/> with status 404 when the
        /// distribution does not exist.
        /// </summary>
        public Task SetDistributionStatusAsync(
            string distributionId, string status, CancellationToken ct = default) =>
            _http.PostJsonNoContentAsync(
                "/_fakecloud/cloudfront/distributions/"
                    + HttpTransport.EncodePath(distributionId) + "/status",
                new CloudFrontDistributionStatusRequest(status), ct);
    }

    /// <summary>
    /// Service Quotas admin sub-client. Wraps the
    /// <c>/_fakecloud/service-quotas/*</c> endpoints that read every quota's
    /// applied value, usage and enforcement state; set applied values
    /// directly (including below the AWS default, so a test can hit a limit
    /// without creating the default number of resources); switch enforcement
    /// on or off globally, per quota or per account; and decide increase
    /// requests held <c>PENDING</c> under manual approval.
    /// </summary>
    public sealed class ServiceQuotasClient
    {
        private const string Base = "/_fakecloud/service-quotas";

        private readonly HttpTransport _http;
        internal ServiceQuotasClient(HttpTransport http) => _http = http;

        private static string QuotaPath(string serviceCode, string quotaCode) =>
            Base + "/quotas/" + HttpTransport.EncodePath(serviceCode) + "/"
                + HttpTransport.EncodePath(quotaCode);

        /// <summary>Append the non-null <paramref name="pairs"/> as a query string.</summary>
        private static string WithQuery(string path, params (string Key, string? Value)[] pairs)
        {
            var sb = new StringBuilder(path);
            var sep = '?';
            foreach (var (key, value) in pairs)
            {
                if (value is null)
                {
                    continue;
                }
                sb.Append(sep).Append(key).Append('=').Append(HttpTransport.EncodePath(value));
                sep = '&';
            }
            return sb.ToString();
        }

        /// <summary>
        /// The JSON value of an enforcement setting: <c>true</c>,
        /// <c>false</c>, or an explicit <c>null</c> that clears the override.
        /// </summary>
        private static JsonNode? EnforceNode(QuotaEnforcement enforcement) => enforcement switch
        {
            QuotaEnforcement.Enforce => JsonValue.Create(true),
            QuotaEnforcement.Ignore => JsonValue.Create(false),
            _ => null,
        };

        /// <summary>
        /// List quotas with their applied value, usage and enforcement state
        /// for an account and region. Without a service code it returns the
        /// quotas that are enforceable, measured, or changed (an applied
        /// value, an override, or an open request); pass a service code for
        /// a whole service. The account and region default server-side when
        /// null. Throws
        /// <see cref="FakeCloudException"/> with status 404 for an unknown
        /// service code.
        /// </summary>
        public Task<ServiceQuotasResponse> GetQuotasAsync(
            string? accountId = null, string? region = null, string? serviceCode = null,
            CancellationToken ct = default) =>
            _http.GetAsync<ServiceQuotasResponse>(
                WithQuery(Base + "/quotas",
                    ("accountId", accountId), ("region", region), ("serviceCode", serviceCode)),
                ct);

        /// <summary>
        /// Set one quota's applied value and/or enforcement override. The
        /// value may be below the AWS default. A null
        /// <see cref="PutServiceQuotaRequest.Enforcement"/> leaves enforcement
        /// unchanged (the <c>enforce</c> key is omitted);
        /// <see cref="QuotaEnforcement.Default"/> clears the override. When
        /// <see cref="PutServiceQuotaRequest.AccountId"/> is set the override
        /// applies to that account only, otherwise to every account.
        /// </summary>
        public Task<ServiceQuota> PutQuotaAsync(
            string serviceCode, string quotaCode, PutServiceQuotaRequest req,
            CancellationToken ct = default)
        {
            var body = new JsonObject();
            if (req.AccountId is not null)
            {
                body["accountId"] = req.AccountId;
            }
            if (req.Region is not null)
            {
                body["region"] = req.Region;
            }
            if (req.Value is { } value)
            {
                body["value"] = value;
            }
            if (req.Enforcement is { } enforcement)
            {
                body["enforce"] = EnforceNode(enforcement);
            }
            return _http.PutJsonAsync<ServiceQuota>(QuotaPath(serviceCode, quotaCode), body, ct);
        }

        /// <summary>
        /// Put a quota back to its AWS default value and drop its enforcement
        /// override: the account's when <paramref name="accountId"/> is
        /// given, else the server-wide one.
        /// </summary>
        public Task<ServiceQuota> DeleteQuotaAsync(
            string serviceCode, string quotaCode, string? accountId = null, string? region = null,
            CancellationToken ct = default) =>
            _http.DeleteAsync<ServiceQuota>(
                WithQuery(QuotaPath(serviceCode, quotaCode), ("accountId", accountId), ("region", region)),
                ct);

        /// <summary>Read the global enforcement switch and every override.</summary>
        public Task<ServiceQuotaEnforcementResponse> GetEnforcementAsync(CancellationToken ct = default) =>
            _http.GetAsync<ServiceQuotaEnforcementResponse>(Base + "/enforcement", ct);

        /// <summary>
        /// Change the global enforcement switch and/or a batch of overrides.
        /// Every change is validated before any is applied; an override with
        /// <see cref="QuotaEnforcement.Default"/> clears it.
        /// </summary>
        public Task<ServiceQuotaEnforcementResponse> PutEnforcementAsync(
            PutServiceQuotaEnforcementRequest req, CancellationToken ct = default)
        {
            var body = new JsonObject();
            if (req.EnforceAll is { } enforceAll)
            {
                body["enforceAll"] = enforceAll;
            }
            if (req.Overrides is { } overrides)
            {
                var list = new JsonArray();
                foreach (var o in overrides)
                {
                    var entry = new JsonObject
                    {
                        ["serviceCode"] = o.ServiceCode,
                        ["quotaCode"] = o.QuotaCode,
                    };
                    if (o.AccountId is not null)
                    {
                        entry["accountId"] = o.AccountId;
                    }
                    entry["enforce"] = EnforceNode(o.Enforcement);
                    list.Add(entry);
                }
                body["overrides"] = list;
            }
            return _http.PutJsonAsync<ServiceQuotaEnforcementResponse>(Base + "/enforcement", body, ct);
        }

        /// <summary>Read how increase requests are decided (<c>auto</c> or <c>manual</c>).</summary>
        public Task<ServiceQuotaRequestApprovalResponse> GetRequestApprovalAsync(
            CancellationToken ct = default) =>
            _http.GetAsync<ServiceQuotaRequestApprovalResponse>(Base + "/request-approval", ct);

        /// <summary>
        /// Set how increase requests are decided: <c>"auto"</c> decides them
        /// on submission (approved unless AWS would refuse the value),
        /// <c>"manual"</c> holds them
        /// <c>PENDING</c> until <see cref="ApproveRequestAsync"/> or
        /// <see cref="DenyRequestAsync"/> decides them.
        /// </summary>
        public Task<ServiceQuotaRequestApprovalResponse> SetRequestApprovalAsync(
            string mode, CancellationToken ct = default) =>
            _http.PutJsonAsync<ServiceQuotaRequestApprovalResponse>(
                Base + "/request-approval", new JsonObject { ["mode"] = mode }, ct);

        /// <summary>
        /// List increase requests across accounts (or one), newest first,
        /// optionally filtered by status.
        /// </summary>
        public Task<ServiceQuotaRequestsResponse> GetRequestsAsync(
            string? accountId = null, string? status = null, CancellationToken ct = default) =>
            _http.GetAsync<ServiceQuotaRequestsResponse>(
                WithQuery(Base + "/requests", ("accountId", accountId), ("status", status)), ct);

        /// <summary>
        /// Approve a <c>PENDING</c> or <c>CASE_OPENED</c> increase request,
        /// raising the account's applied value to the requested one. Throws
        /// <see cref="FakeCloudException"/> with status 404 for an unknown
        /// request and 409 for one already decided.
        /// </summary>
        public Task<ServiceQuotaRequest> ApproveRequestAsync(string requestId, CancellationToken ct = default) =>
            _http.PostEmptyAsync<ServiceQuotaRequest>(
                Base + "/requests/" + HttpTransport.EncodePath(requestId) + "/approve", ct);

        /// <summary>
        /// Close a <c>PENDING</c> or <c>CASE_OPENED</c> increase request
        /// without raising the quota. <paramref name="status"/> is one of
        /// <c>DENIED</c> (the default when null), <c>NOT_APPROVED</c>,
        /// <c>CASE_CLOSED</c>, <c>INVALID_REQUEST</c>.
        /// </summary>
        public Task<ServiceQuotaRequest> DenyRequestAsync(
            string requestId, string? status = null, CancellationToken ct = default)
        {
            var path = Base + "/requests/" + HttpTransport.EncodePath(requestId) + "/deny";
            return status is null
                ? _http.PostEmptyAsync<ServiceQuotaRequest>(path, ct)
                : _http.PostJsonAsync<ServiceQuotaRequest>(path, new JsonObject { ["status"] = status }, ct);
        }
    }
}
