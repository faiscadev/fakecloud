using System.Text.Json;
using System.Text.Json.Serialization;

namespace FakeCloud;

// Response and request payload records for the fakecloud introspection API.
//
// All records are deserialized with System.Text.Json using a camelCase naming
// policy; extra fields from newer fakecloud versions are ignored so older SDK
// builds keep working against newer servers. Fields whose wire names are not
// camelCase carry explicit [JsonPropertyName] attributes.

// ── Health & Reset ─────────────────────────────────────────────
public sealed record HealthResponse(string? Status, string? Version, IReadOnlyList<string>? Services);

public sealed record ResetResponse(string? Status);

public sealed record ResetServiceResponse(string? Reset);

// ── RDS ────────────────────────────────────────────────────────
public sealed record RdsTag(string? Key, string? Value);

public sealed record RdsInstance(
    string? DbInstanceIdentifier,
    string? DbInstanceArn,
    string? DbInstanceClass,
    string? Engine,
    string? EngineVersion,
    string? DbInstanceStatus,
    string? MasterUsername,
    string? DbName,
    string? EndpointAddress,
    int Port,
    int AllocatedStorage,
    bool PubliclyAccessible,
    bool DeletionProtection,
    string? CreatedAt,
    string? DbiResourceId,
    string? ContainerId,
    int HostPort,
    IReadOnlyList<RdsTag>? Tags);

public sealed record RdsInstancesResponse(IReadOnlyList<RdsInstance>? Instances);

// ── EC2 ────────────────────────────────────────────────────────
public sealed record Ec2Instance(
    string? InstanceId,
    string? ImageId,
    string? InstanceType,
    string? State,
    string? PrivateIp,
    string? PublicIp,
    string? SubnetId,
    string? VpcId,
    string? KeyName,
    IReadOnlyList<string>? SecurityGroupIds,
    string? AvailabilityZone,
    string? LaunchTime,
    string? ContainerId);

public sealed record Ec2InstancesResponse(IReadOnlyList<Ec2Instance>? Instances);

/// <summary>
/// The real backing network of an EC2 instance — which Docker/Podman network
/// or k8s NetworkPolicy backs it, its container IP, and whether
/// security-group enforcement is active or degraded. A debugging aid for
/// "why can't X reach Y" (issue #1745).
/// </summary>
public sealed record Ec2InstanceNetwork(
    string? InstanceId,
    string? VpcId,
    string? SubnetId,
    string? PrivateIp,
    string? BackingNetwork,
    string? IsolationBackend,
    string? SecurityGroupEnforcement,
    bool EnforcementActive);

public sealed record Ec2InstanceNetworksResponse(IReadOnlyList<Ec2InstanceNetwork>? InstanceNetworks);

// ── ElastiCache ────────────────────────────────────────────────
public sealed record ElastiCacheCluster(
    string? CacheClusterId,
    string? CacheClusterStatus,
    string? Engine,
    string? EngineVersion,
    string? CacheNodeType,
    int NumCacheNodes,
    string? ReplicationGroupId,
    int? Port,
    int? HostPort,
    string? ContainerId);

public sealed record ElastiCacheClustersResponse(IReadOnlyList<ElastiCacheCluster>? Clusters);

public sealed record ElastiCacheReplicationGroupIntrospection(
    string? ReplicationGroupId,
    string? Status,
    string? Description,
    IReadOnlyList<string>? MemberClusters,
    bool AutomaticFailover,
    bool MultiAz,
    string? Engine,
    string? EngineVersion,
    string? CacheNodeType,
    int NumCacheClusters);

public sealed record ElastiCacheReplicationGroupsResponse(
    IReadOnlyList<ElastiCacheReplicationGroupIntrospection>? ReplicationGroups);

public sealed record ElastiCacheServerlessCacheIntrospection(
    string? ServerlessCacheName,
    string? Status,
    string? Engine,
    string? EngineVersion,
    string? CacheNodeType);

public sealed record ElastiCacheServerlessCachesResponse(
    IReadOnlyList<ElastiCacheServerlessCacheIntrospection>? ServerlessCaches);

public sealed record ElastiCacheAclUser(
    string? Name,
    string? Status,
    string? AccessString,
    bool NoPasswordRequired,
    int PasswordCount);

public sealed record ElastiCacheAclGroup(string? Name, IReadOnlyList<string>? Members);

public sealed record ElastiCacheAclCluster(
    string? ClusterId,
    string? Engine,
    IReadOnlyList<ElastiCacheAclUser>? Users,
    IReadOnlyList<ElastiCacheAclGroup>? Groups);

public sealed record ElastiCacheAclsResponse(IReadOnlyList<ElastiCacheAclCluster>? Acls);

// ── Lambda ─────────────────────────────────────────────────────
public sealed record LambdaInvocation(
    string? FunctionArn, string? Payload, string? Source, string? Timestamp);

public sealed record LambdaInvocationsResponse(IReadOnlyList<LambdaInvocation>? Invocations);

public sealed record WarmContainer(
    string? FunctionName, string? Runtime, string? ContainerId, long LastUsedSecsAgo);

public sealed record WarmContainersResponse(IReadOnlyList<WarmContainer>? Containers);

public sealed record EvictContainerResponse(bool Evicted);

// ── SES ────────────────────────────────────────────────────────
public sealed record SentEmail(
    string? MessageId,
    string? From,
    IReadOnlyList<string>? To,
    IReadOnlyList<string>? Cc,
    IReadOnlyList<string>? Bcc,
    string? Subject,
    string? HtmlBody,
    string? TextBody,
    string? RawData,
    string? TemplateName,
    string? TemplateData,
    string? DkimSignature,
    IReadOnlyList<IReadOnlyList<string>>? Headers,
    string? Timestamp);

public sealed record SesEmailsResponse(IReadOnlyList<SentEmail>? Emails);

public sealed record InboundEmailRequest(
    string? From, IReadOnlyList<string>? To, string? Subject, string? Body);

public sealed record InboundActionExecuted(string? Rule, string? ActionType);

public sealed record InboundEmailResponse(
    string? MessageId,
    IReadOnlyList<string>? MatchedRules,
    IReadOnlyList<InboundActionExecuted>? ActionsExecuted);

public sealed record SesMetrics(long SuppressedDropsTotal);

public sealed record SesMailFromStatusRequest(string? Status);

public sealed record SesMailFromStatusResponse(string? Identity, string? MailFromDomainStatus);

public sealed record SesDkimPublicKey(
    string? Identity,
    string? Selector,
    string? PublicKeyBase64,
    bool SigningEnabled);

public sealed record SesSandboxRequest(bool Sandbox);

public sealed record SesSandboxResponse(bool Sandbox, bool ProductionAccessEnabled);

public sealed record SesBouncedRecipientInfo(
    string? Recipient,
    string? BounceType,
    string? Action,
    string? Status,
    string? DiagnosticCode);

public sealed record SesBounce(
    string? MessageId,
    string? BounceType,
    string? BounceSubType,
    IReadOnlyList<SesBouncedRecipientInfo>? BouncedRecipientInfo,
    string? Explanation,
    string? Timestamp,
    string? OriginalMessageId,
    string? BounceSender);

public sealed record SesBouncesResponse(IReadOnlyList<SesBounce>? Bounces);

public sealed record SesMessageInsightEvent(
    string? Destination,
    string? Timestamp,
    string? BounceType,
    string? BounceSubType,
    string? DiagnosticCode,
    string? ComplaintFeedbackType);

public sealed record SesMessageInsightsResponse(
    string? MessageId,
    IReadOnlyList<SesMessageInsightEvent>? Sends,
    IReadOnlyList<SesMessageInsightEvent>? Deliveries,
    IReadOnlyList<SesMessageInsightEvent>? Opens,
    IReadOnlyList<SesMessageInsightEvent>? Clicks,
    IReadOnlyList<SesMessageInsightEvent>? Bounces,
    IReadOnlyList<SesMessageInsightEvent>? Complaints,
    IReadOnlyList<SesMessageInsightEvent>? Rejects);

public sealed record SesSmtpSubmission(
    string? MessageId,
    string? From,
    IReadOnlyList<string>? To,
    string? Subject,
    long RawSizeBytes,
    string? ReceivedAt,
    string? AuthUser);

public sealed record SesSmtpSubmissionsResponse(IReadOnlyList<SesSmtpSubmission>? Submissions);

public sealed record SesEventDestinationDelivery(
    string? DestinationName,
    string? DestinationType,
    string? EventType,
    string? MessageId,
    string? DispatchedAt,
    string? TargetArn);

public sealed record SesEventDestinationDeliveriesResponse(
    IReadOnlyList<SesEventDestinationDelivery>? Deliveries);

// ── SNS ────────────────────────────────────────────────────────
public sealed record SnsMessage(
    string? MessageId,
    string? TopicArn,
    string? Message,
    string? Subject,
    string? Timestamp);

public sealed record SnsMessagesResponse(IReadOnlyList<SnsMessage>? Messages);

public sealed record PendingConfirmation(
    string? SubscriptionArn,
    string? TopicArn,
    string? Protocol,
    string? Endpoint,
    string? Token);

public sealed record PendingConfirmationsResponse(IReadOnlyList<PendingConfirmation>? PendingConfirmations);

public sealed record ConfirmSubscriptionRequest(string? SubscriptionArn);

public sealed record ConfirmSubscriptionResponse(bool Confirmed);

// ── SQS ────────────────────────────────────────────────────────
public sealed record SqsMessageInfo(
    string? MessageId,
    string? Body,
    int ReceiveCount,
    bool InFlight,
    string? CreatedAt);

public sealed record SqsQueueMessages(
    string? QueueUrl, string? QueueName, IReadOnlyList<SqsMessageInfo>? Messages);

public sealed record SqsMessagesResponse(IReadOnlyList<SqsQueueMessages>? Queues);

public sealed record ExpirationTickResponse(int ExpiredMessages);

public sealed record ForceDlqResponse(int MovedMessages);

public sealed record AppAsTickResponse(int Applied);

public sealed record AppAsScheduledTickResponse(int Fired);

// ── EventBridge ────────────────────────────────────────────────
public sealed record EventBridgeEvent(
    string? EventId,
    string? Source,
    string? DetailType,
    string? Detail,
    string? BusName,
    string? Timestamp);

public sealed record EventBridgeLambdaDelivery(
    string? FunctionArn, string? Payload, string? Timestamp);

public sealed record EventBridgeLogDelivery(
    string? LogGroupArn, string? Payload, string? Timestamp);

public sealed record EventBridgeDeliveries(
    IReadOnlyList<EventBridgeLambdaDelivery>? Lambda,
    IReadOnlyList<EventBridgeLogDelivery>? Logs);

public sealed record EventHistoryResponse(
    IReadOnlyList<EventBridgeEvent>? Events, EventBridgeDeliveries? Deliveries);

public sealed record FireRuleRequest(string? BusName, string? RuleName)
{
    public FireRuleRequest(string? ruleName) : this(null, ruleName) { }
}

public sealed record FireRuleTarget(string? Type, string? Arn);

public sealed record FireRuleResponse(IReadOnlyList<FireRuleTarget>? Targets);

// ── Glue ───────────────────────────────────────────────────────
public sealed record GlueJob(
    string? AccountId,
    string? Name,
    string? Role,
    JsonElement? Command,
    IReadOnlyDictionary<string, string>? DefaultArguments,
    double? MaxCapacity,
    long MaxRetries,
    long? Timeout,
    string? GlueVersion,
    string? WorkerType,
    long? NumberOfWorkers,
    string? CreatedOn,
    string? LastModifiedOn);

public sealed record GlueJobsResponse(IReadOnlyList<GlueJob>? Jobs);

public sealed record GlueJobRun(
    string? AccountId,
    string? Id,
    string? JobName,
    long Attempt,
    string? StartedOn,
    string? CompletedOn,
    string? JobRunState,
    IReadOnlyDictionary<string, string>? Arguments,
    string? ErrorMessage,
    long ExecutionTime);

public sealed record GlueJobRunsResponse(IReadOnlyList<GlueJobRun>? Runs);

public sealed record GlueCrawler(
    string? AccountId,
    string? Name,
    string? Role,
    string? DatabaseName,
    string? State,
    string? TargetSummary,
    string? Schedule,
    string? CreationTime,
    string? LastUpdated);

public sealed record GlueCrawlersResponse(IReadOnlyList<GlueCrawler>? Crawlers);

// ── CloudWatch ─────────────────────────────────────────────────
public sealed record CloudWatchDimension(string? Name, string? Value);

public sealed record CloudWatchAlarm(
    string? AccountId,
    string? Region,
    string? Name,
    string? Type,
    string? State,
    string? StateReason,
    string? StateUpdatedTimestamp,
    bool ActionsEnabled,
    IReadOnlyList<string>? AlarmActions,
    IReadOnlyList<string>? OkActions,
    IReadOnlyList<string>? InsufficientDataActions,
    string? Namespace,
    string? MetricName,
    double? Threshold,
    string? ComparisonOperator,
    string? AlarmRule);

public sealed record CloudWatchAlarmsResponse(IReadOnlyList<CloudWatchAlarm>? Alarms);

public sealed record CloudWatchLatestDatapoint(string? Timestamp, double? Value, string? Unit);

public sealed record CloudWatchMetric(
    string? AccountId,
    string? Region,
    string? Namespace,
    string? MetricName,
    IReadOnlyList<CloudWatchDimension>? Dimensions,
    int DatapointCount,
    CloudWatchLatestDatapoint? Latest);

public sealed record CloudWatchMetricsResponse(IReadOnlyList<CloudWatchMetric>? Metrics);

// ── Firehose ───────────────────────────────────────────────────
public sealed record FirehoseEncryption(string? Status, string? KeyType, string? KeyArn);

public sealed record FirehoseDeliveryStream(
    string? AccountId,
    string? Name,
    string? Arn,
    string? StreamType,
    string? Status,
    FirehoseEncryption? Encryption,
    int DestinationCount,
    string? CreateTimestamp,
    string? LastUpdateTimestamp);

public sealed record FirehoseDeliveryStreamsResponse(IReadOnlyList<FirehoseDeliveryStream>? DeliveryStreams);

// ── Scheduler (EventBridge Scheduler) ──────────────────────────
public sealed record SchedulerSchedule(
    string? AccountId,
    string? GroupName,
    string? Name,
    string? Arn,
    string? State,
    string? ScheduleExpression,
    string? TargetArn,
    string? LastFired);

public sealed record SchedulerSchedulesResponse(IReadOnlyList<SchedulerSchedule>? Schedules);

public sealed record FireScheduleResponse(string? ScheduleArn, string? TargetArn);

// ── S3 ─────────────────────────────────────────────────────────
public sealed record S3Notification(
    string? Bucket, string? Key, string? EventType, string? Timestamp);

public sealed record S3NotificationsResponse(IReadOnlyList<S3Notification>? Notifications);

public sealed record LifecycleTickResponse(
    int ProcessedBuckets, int ExpiredObjects, int TransitionedObjects);

public sealed record S3AccessPointEntry(
    string? Name,
    string? Alias,
    string? Bucket,
    string? AccountId,
    string? NetworkOrigin,
    string? VpcConfiguration,
    string? PublicAccessBlock,
    string? CreatedAt);

public sealed record S3AccessPointsResponse(IReadOnlyList<S3AccessPointEntry>? AccessPoints);

public sealed record S3ObjectLambdaResponse(
    string? RequestToken,
    string? RequestRoute,
    int? StatusCode,
    string? BodyBase64,
    long BodySize,
    string? ContentType,
    string? ErrorMessage,
    IReadOnlyDictionary<string, string>? Metadata);

public sealed record S3ObjectLambdaResponsesResponse(IReadOnlyList<S3ObjectLambdaResponse>? Responses);

// ── DynamoDB ───────────────────────────────────────────────────
public sealed record TtlTickResponse(int ExpiredItems);

public sealed record DynamoDbSnapshotSaveRequest(string? DataPath);

public sealed record DynamoDbSnapshotSaveResponse(bool Saved);

// ── SecretsManager ─────────────────────────────────────────────
public sealed record RotationTickResponse(IReadOnlyList<string>? RotatedSecrets);

// ── Cognito ────────────────────────────────────────────────────
public sealed record UserConfirmationCodes(
    string? ConfirmationCode, IReadOnlyDictionary<string, JsonElement>? AttributeVerificationCodes);

public sealed record ConfirmationCode(
    string? PoolId, string? Username, string? Code, string? Type, string? Attribute);

public sealed record ConfirmationCodesResponse(IReadOnlyList<ConfirmationCode>? Codes);

public sealed record ConfirmUserRequest(string? UserPoolId, string? Username);

public sealed record ConfirmUserResponse(bool Confirmed, string? Error);

public sealed record TokenInfo(
    string? Type,
    string? Username,
    string? PoolId,
    string? ClientId,
    long IssuedAt);

public sealed record TokensResponse(IReadOnlyList<TokenInfo>? Tokens);

public sealed record ExpireTokensRequest(string? UserPoolId, string? Username);

public sealed record ExpireTokensResponse(int ExpiredTokens);

public sealed record AuthEvent(
    string? EventType,
    string? Username,
    string? UserPoolId,
    string? ClientId,
    long Timestamp,
    bool Success);

public sealed record AuthEventsResponse(IReadOnlyList<AuthEvent>? Events);

/// <summary>
/// One PreTokenGeneration Lambda trigger invocation captured by
/// <c>InitiateAuth</c>. <c>ClaimsAdded</c> / <c>ClaimsOverridden</c> /
/// <c>GroupOverrides</c> are pre-parsed from the Lambda response so tests
/// don't have to walk the raw <c>claimsAndScopeOverrideDetails</c> shape
/// themselves.
/// </summary>
public sealed record PreTokenGenInvocation(
    string? PoolId,
    string? UserPoolArn,
    string? Username,
    string? TriggerSource,
    string? LambdaArn,
    IReadOnlyDictionary<string, JsonElement>? RequestPayload,
    IReadOnlyDictionary<string, JsonElement>? ResponsePayload,
    IReadOnlyList<string>? ClaimsAdded,
    IReadOnlyList<string>? ClaimsOverridden,
    IReadOnlyList<string>? GroupOverrides,
    string? InvokedAt,
    long DurationMs);

public sealed record PreTokenGenInvocationsResponse(IReadOnlyList<PreTokenGenInvocation>? Invocations);

/// <summary>
/// Payload for <c>POST /_fakecloud/cognito/authorization-codes</c>. Lets test
/// harnesses pre-allocate a single-use OAuth2 authorization code that the
/// <c>/oauth2/token</c> <c>authorization_code</c> grant later consumes.
/// </summary>
public sealed record MintAuthorizationCodeRequest(
    string? UserPoolId,
    string? ClientId,
    string? Username,
    string? RedirectUri,
    IReadOnlyList<string>? Scopes,
    string? CodeChallenge,
    string? CodeChallengeMethod,
    string? Nonce);

public sealed record MintAuthorizationCodeResponse(string? Code);

/// <summary>
/// Payload for <c>POST /_fakecloud/cognito/compromised-passwords</c>. Each
/// plaintext is SHA-256 hashed server-side and added to the per-account
/// compromised-password set; subsequent <c>SignUp</c> /
/// <c>AdminInitiateAuth</c> fail with <c>InvalidPasswordException</c> on any
/// pool whose
/// <c>CompromisedCredentialsRiskConfiguration.Actions.EventAction</c> is
/// <c>BLOCK</c>.
/// </summary>
public sealed record CompromisedPasswordsRequest(IReadOnlyList<string>? Passwords);

public sealed record CompromisedPasswordsResponse(long Added);

/// <summary>
/// Registered WebAuthn credential from
/// <c>GET /_fakecloud/cognito/webauthn-credentials</c>. The
/// <c>AttestationInfo</c> field is the parsed-attestation JSON (packed format
/// details, AAGUID, certificate chain summary, signature counter); its shape
/// depends on the attestation format so it is surfaced as a raw
/// <see cref="JsonElement"/>.
/// </summary>
public sealed record WebAuthnCredential(
    [property: JsonPropertyName("account_id")] string? AccountId,
    [property: JsonPropertyName("pool_user")] string? PoolUser,
    [property: JsonPropertyName("credential_id")] string? CredentialId,
    [property: JsonPropertyName("relying_party_id")] string? RelyingPartyId,
    [property: JsonPropertyName("attestation_info")] JsonElement? AttestationInfo);

public sealed record WebAuthnCredentialsResponse(IReadOnlyList<WebAuthnCredential>? Credentials);

// ── Step Functions ─────────────────────────────────────────────
public sealed record StepFunctionsExecution(
    string? ExecutionArn,
    string? StateMachineArn,
    string? Name,
    string? Status,
    string? StartDate,
    string? Input,
    string? Output,
    string? StopDate);

public sealed record StepFunctionsExecutionsResponse(IReadOnlyList<StepFunctionsExecution>? Executions);

public sealed record StepFunctionsSyncBillingDetails(
    long BilledDurationInMilliseconds,
    long BilledMemoryUsedInMb);

public sealed record StepFunctionsSyncExecution(
    string? ExecutionArn,
    string? StateMachineArn,
    string? Name,
    string? Status,
    string? Input,
    string? Output,
    string? StartedAt,
    string? StoppedAt,
    long DurationMs,
    StepFunctionsSyncBillingDetails? BillingDetails);

public sealed record StepFunctionsSyncExecutionsResponse(IReadOnlyList<StepFunctionsSyncExecution>? Executions);

public sealed record StepFunctionsExecutionTreeNode(
    string? Arn,
    string? StateMachineArn,
    string? Status,
    string? StartedAt,
    string? StoppedAt,
    IReadOnlyList<StepFunctionsExecutionTreeNode>? Children);

public sealed record StepFunctionsExecutionTreeResponse(
    string? RootArn,
    StepFunctionsExecutionTreeNode? Tree);

public sealed record SfnEnqueueActivityTaskRequest(
    string? ActivityArn,
    string? Input,
    long? HeartbeatSeconds,
    long? TimeoutSeconds);

public sealed record SfnEnqueueActivityTaskResponse(string? TaskToken);

// ── Bedrock ────────────────────────────────────────────────────
public sealed record BedrockInvocation(
    string? ModelId, string? Input, string? Output, string? Timestamp, string? Error);

public sealed record BedrockInvocationsResponse(IReadOnlyList<BedrockInvocation>? Invocations);

public sealed record BedrockModelResponseConfig(string? Status, string? ModelId);

public sealed record BedrockResponseRule(string? PromptContains, string? Response);

public sealed record BedrockFaultRule(
    string? ErrorType,
    string? Message,
    int? HttpStatus,
    int? Count,
    string? ModelId,
    string? Operation)
{
    public BedrockFaultRule(string? errorType) : this(errorType, null, null, null, null, null) { }
}

public sealed record BedrockFaultRuleState(
    string? ErrorType,
    string? Message,
    int HttpStatus,
    int Remaining,
    string? ModelId,
    string? Operation);

public sealed record BedrockFaultsResponse(IReadOnlyList<BedrockFaultRuleState>? Faults);

public sealed record BedrockStatusResponse(string? Status);

// ── Bedrock Agent (control plane) ──────────────────────────────
public sealed record BedrockAgentAliasSummary(
    string? AliasId,
    string? AliasName,
    string? AgentVersion,
    string? AliasArn,
    string? Status,
    string? CreatedAt,
    string? UpdatedAt);

public sealed record BedrockAgentVersionSummary(
    string? AgentVersion,
    string? CreatedAt,
    string? Instruction,
    string? FoundationModel);

public sealed record BedrockAgentKnowledgeBaseSummary(
    string? KnowledgeBaseId,
    string? State,
    string? Description);

public sealed record BedrockAgentCollaboratorSummary(
    string? CollaboratorId,
    string? CollaboratorName,
    string? CollaboratorAliasArn,
    string? RelayConversationHistory);

public sealed record BedrockAgentRow(
    string? AgentId,
    string? AgentName,
    string? AgentArn,
    string? AgentStatus,
    string? FoundationModel,
    string? Instruction,
    IReadOnlyList<BedrockAgentKnowledgeBaseSummary>? KnowledgeBases,
    IReadOnlyList<JsonElement>? ActionGroups,
    IReadOnlyList<BedrockAgentCollaboratorSummary>? Collaborators,
    IReadOnlyList<BedrockAgentAliasSummary>? Aliases,
    IReadOnlyList<BedrockAgentVersionSummary>? Versions,
    JsonElement? PromptOverrides,
    string? CreatedAt,
    string? UpdatedAt);

public sealed record BedrockAgentAgentsResponse(IReadOnlyList<BedrockAgentRow>? Agents);

// ── Bedrock Agent Runtime (data plane) ────────────────────────
public sealed record BedrockAgentRuntimeInvocation(
    string? InvocationId,
    string? Op,
    string? AgentId,
    string? FlowId,
    string? SessionId,
    string? Input,
    string? Output,
    long OutputChunks,
    JsonElement? Trace,
    IReadOnlyList<JsonElement>? Citations,
    string? InvokedAt,
    long DurationMs);

public sealed record BedrockAgentRuntimeInvocationsResponse(
    IReadOnlyList<BedrockAgentRuntimeInvocation>? Invocations);

// ── API Gateway v2 ─────────────────────────────────────────────
public sealed record ApiGatewayV2Request(
    string? RequestId,
    string? ApiId,
    string? Stage,
    string? Method,
    string? Path,
    IReadOnlyDictionary<string, string>? Headers,
    IReadOnlyDictionary<string, string>? QueryParams,
    string? Body,
    string? Timestamp,
    int StatusCode);

public sealed record ApiGatewayV2RequestsResponse(IReadOnlyList<ApiGatewayV2Request>? Requests);

// ── IAM ───────────────────────────────────────────────────────
public sealed record CreateAdminRequest(string? AccountId, string? UserName);

public sealed record CreateAdminResponse(
    string? AccessKeyId,
    string? SecretAccessKey,
    string? AccountId,
    string? Arn);

// ── ECR ────────────────────────────────────────────────────────
public sealed record EcrTag(string? Key, string? Value);

public sealed record EcrRepository(
    string? RepositoryName,
    string? RepositoryArn,
    string? RegistryId,
    string? RepositoryUri,
    string? ImageTagMutability,
    bool ScanOnPush,
    string? CreatedAt,
    IReadOnlyList<EcrTag>? Tags,
    bool HasPolicy,
    bool HasLifecyclePolicy,
    long ImageCount,
    long LayerCount);

public sealed record EcrRepositoriesResponse(IReadOnlyList<EcrRepository>? Repositories);

public sealed record EcrImage(
    string? RepositoryName,
    string? ImageDigest,
    IReadOnlyList<string>? ImageTags,
    long ImageSizeInBytes,
    string? ImageManifestMediaType,
    string? ImagePushedAt);

public sealed record EcrImagesResponse(IReadOnlyList<EcrImage>? Images);

public sealed record EcrPullThroughRule(
    string? EcrRepositoryPrefix,
    string? UpstreamRegistryUrl,
    string? UpstreamRegistry,
    string? CredentialArn,
    string? CustomRoleArn,
    string? CreatedAt,
    string? UpdatedAt);

public sealed record EcrPullThroughRulesResponse(IReadOnlyList<EcrPullThroughRule>? Rules);

// ── ECS ───────────────────────────────────────────────────────
public sealed record EcsTag(string? Key, string? Value);

public sealed record EcsCluster(
    string? ClusterName,
    string? ClusterArn,
    string? Status,
    int RunningTasksCount,
    int PendingTasksCount,
    int ActiveServicesCount,
    int RegisteredContainerInstancesCount,
    IReadOnlyList<string>? CapacityProviders,
    IReadOnlyList<EcsTag>? Tags,
    string? CreatedAt);

public sealed record EcsClustersResponse(IReadOnlyList<EcsCluster>? Clusters);

public sealed record EcsTaskContainer(
    string? Name,
    string? Image,
    string? LastStatus,
    long? ExitCode,
    string? RuntimeId,
    bool Essential);

public sealed record EcsTask(
    string? TaskArn,
    string? TaskId,
    string? ClusterArn,
    string? ClusterName,
    string? TaskDefinitionArn,
    string? Family,
    int Revision,
    string? LastStatus,
    string? DesiredStatus,
    string? LaunchType,
    string? CreatedAt,
    string? StartedAt,
    string? StoppingAt,
    string? StoppedAt,
    string? StopCode,
    string? StoppedReason,
    IReadOnlyList<EcsTaskContainer>? Containers,
    long CapturedLogBytes);

public sealed record EcsTasksResponse(IReadOnlyList<EcsTask>? Tasks);

public sealed record EcsTaskLogsResponse(
    string? TaskArn,
    string? Logs,
    string? LastStatus,
    long? ExitCode);

public sealed record EcsMarkFailedRequest(long? ExitCode, string? Reason);

public sealed record EcsLifecycleEvent(
    string? At,
    string? EventType,
    string? TaskArn,
    string? ClusterArn,
    string? LastStatus,
    JsonElement? Detail);

public sealed record EcsEventsResponse(IReadOnlyList<EcsLifecycleEvent>? Events);

public sealed record EcsTaskMetadataLimits(double? Cpu, long? Memory);

public sealed record EcsTaskMetadataPort(long? ContainerPort, long? HostPort, string? Protocol);

public sealed record EcsTaskMetadataContainer(
    string? Name,
    string? Image,
    string? ImageId,
    IReadOnlyList<EcsTaskMetadataPort>? Ports,
    IReadOnlyDictionary<string, string>? Labels,
    string? DesiredStatus,
    string? KnownStatus,
    EcsTaskMetadataLimits? Limits,
    string? CreatedAt,
    string? StartedAt,
    long? ExitCode);

public sealed record EcsTaskMetadata(
    string? Cluster,
    string? TaskArn,
    string? Family,
    int? Revision,
    string? DesiredStatus,
    string? KnownStatus,
    IReadOnlyList<EcsTaskMetadataContainer>? Containers,
    string? PullStartedAt,
    string? PullStoppedAt,
    string? AvailabilityZone,
    string? LaunchType,
    string? VpcId,
    string? EniId);

public sealed record EcsTaskMetadataResponse(EcsTaskMetadata? Task);

// ── ELBv2 ─────────────────────────────────────────────────────
public sealed record Elbv2Tag(string? Key, string? Value);

public sealed record Elbv2AvailabilityZone(string? ZoneName, string? SubnetId);

public sealed record Elbv2LoadBalancer(
    string? Arn,
    string? Name,
    string? DnsName,
    string? Scheme,
    string? VpcId,
    string? StateCode,
    string? StateReason,
    string? LbType,
    string? IpAddressType,
    IReadOnlyList<Elbv2AvailabilityZone>? AvailabilityZones,
    IReadOnlyList<string>? SecurityGroups,
    string? CreatedTime,
    IReadOnlyList<Elbv2Tag>? Tags);

public sealed record Elbv2LoadBalancersResponse(IReadOnlyList<Elbv2LoadBalancer>? LoadBalancers);

public sealed record Elbv2Target(
    string? Id,
    int? Port,
    string? AvailabilityZone,
    string? HealthState,
    string? HealthReason,
    string? HealthDescription);

public sealed record Elbv2TargetGroup(
    string? Arn,
    string? Name,
    string? Protocol,
    int? Port,
    string? VpcId,
    string? TargetType,
    IReadOnlyList<string>? LoadBalancerArns,
    IReadOnlyList<Elbv2Target>? Targets,
    string? HealthCheckProtocol,
    string? HealthCheckPort,
    string? HealthCheckPath,
    int HealthyThresholdCount,
    int UnhealthyThresholdCount,
    string? CreatedTime,
    IReadOnlyList<Elbv2Tag>? Tags);

public sealed record Elbv2TargetGroupsResponse(IReadOnlyList<Elbv2TargetGroup>? TargetGroups);

public sealed record Elbv2Listener(
    string? Arn,
    string? LoadBalancerArn,
    int? Port,
    string? Protocol,
    string? SslPolicy,
    IReadOnlyList<string>? CertificateArns,
    string? DefaultActionType,
    string? DefaultTargetGroupArn);

public sealed record Elbv2ListenersResponse(IReadOnlyList<Elbv2Listener>? Listeners);

public sealed record Elbv2Rule(
    string? Arn,
    string? ListenerArn,
    string? Priority,
    bool IsDefault,
    IReadOnlyList<string>? ConditionFields,
    string? ActionType);

public sealed record Elbv2RulesResponse(IReadOnlyList<Elbv2Rule>? Rules);

/// <summary>
/// Response from <c>POST /_fakecloud/elbv2/access-logs/flush</c>.
/// <c>Flushed</c> is true when an access-log buffer was wired and the
/// synchronous flush ran; false when no logger was configured.
/// </summary>
public sealed record Elbv2FlushAccessLogsResponse(bool Flushed);

// ── Route 53 ─────────────────────────────────────────────────

/// <summary>
/// Body for the Route 53 admin endpoint
/// <c>POST /_fakecloud/route53/health-checks/{id}/status</c>.
/// <c>Status</c> is one of <c>"Success"</c>, <c>"Failure"</c>,
/// <c>"Timeout"</c>, <c>"DnsError"</c>, <c>"InsufficientDataPoints"</c>,
/// <c>"Unknown"</c>; <c>Reason</c> is omitted from the JSON when null.
/// </summary>
public sealed record SetHealthCheckStatusRequest(string? Status, string? Reason);

// ── ACM ──────────────────────────────────────────────────────

/// <summary>
/// Body for the ACM admin endpoint
/// <c>POST /_fakecloud/acm/certificates/{arn-or-id}/status</c>.
/// <c>Status</c> is one of <c>"ISSUED"</c>, <c>"FAILED"</c>, or
/// <c>"VALIDATION_TIMED_OUT"</c>; <c>Reason</c> is recorded as
/// <c>FailureReason</c> on subsequent <c>DescribeCertificate</c> calls when
/// status is non-ISSUED, and is omitted from the JSON when null.
/// </summary>
public sealed record SetCertificateStatusRequest(string? Status, string? Reason);

// ── CloudWatch Logs ───────────────────────────────────────────

/// <summary>
/// Admin payload for <c>POST /_fakecloud/logs/anomalies/inject</c>. Lets
/// tests seed synthetic CloudWatch Logs anomalies so they can exercise
/// <c>ListAnomalies</c>/<c>UpdateAnomaly</c> deterministically.
/// </summary>
public sealed record LogsAnomalyInjectRequest(
    string? AnomalyDetectorArn,
    string? PatternString,
    IReadOnlyList<string>? LogGroupArns,
    string? Priority);

public sealed record LogsAnomalyInjectResponse(string? AnomalyId);

/// <summary>
/// One entry of <c>GET /_fakecloud/logs/delivery-config</c>. Joins a
/// delivery with the <c>LogType</c> from its delivery source so test code
/// does not have to re-query the AWS-shaped APIs.
/// </summary>
public sealed record LogsDeliveryConfiguration(
    string? Id,
    string? Name,
    string? DeliveryDestinationArn,
    string? DeliverySourceName,
    string? LogType,
    IReadOnlyList<string>? RecordFields,
    string? FieldDelimiter,
    JsonElement? S3DeliveryConfiguration,
    long CreatedAt);

public sealed record LogsDeliveryConfigResponse(
    IReadOnlyList<LogsDeliveryConfiguration>? Configurations);

/// <summary>One parsed <c>IndexPolicy</c> on a log group.</summary>
public sealed record LogsFieldIndex(
    IReadOnlyList<string>? Fields, long CreatedAt, long LastUsedAt);

public sealed record LogsFieldIndexesResponse(
    string? LogGroupName, IReadOnlyList<LogsFieldIndex>? Indexes);

/// <summary>
/// Response from <c>GET /_fakecloud/acm/certificates/{arn-or-id}/chain-info</c>.
/// fakecloud is not a PKI: <c>ExternalCaValidated</c> is always false,
/// documenting that imported chains are stored verbatim rather than verified
/// against a real trust store. The byte/block counts let callers confirm the
/// PEM they uploaded round-trips intact.
/// </summary>
public sealed record AcmCertificateChainInfo(
    [property: JsonPropertyName("certificate_arn")] string? CertificateArn,
    [property: JsonPropertyName("certificate_pem_bytes")] int CertificatePemBytes,
    [property: JsonPropertyName("certificate_pem_blocks")] int CertificatePemBlocks,
    [property: JsonPropertyName("chain_pem_bytes")] int ChainPemBytes,
    [property: JsonPropertyName("chain_pem_blocks")] int ChainPemBlocks,
    [property: JsonPropertyName("external_ca_validated")] bool ExternalCaValidated,
    [property: JsonPropertyName("status")] string? Status,
    [property: JsonPropertyName("cert_type")] string? CertType);

// ── Athena ────────────────────────────────────────────────────
public sealed record AthenaNamedQuery(
    string? NamedQueryId,
    string? Name,
    string? Description,
    string? Database,
    string? QueryString,
    string? Workgroup,
    string? LastUsedAt);

public sealed record AthenaNamedQueriesResponse(IReadOnlyList<AthenaNamedQuery>? Queries);

public sealed record OrganizationsTag(string? Key, string? Value);

/// <summary>
/// A single member account from
/// <c>GET /_fakecloud/organizations/accounts</c>. Mirrors the AWS
/// Organizations <c>Account</c> shape plus <c>ParentOuId</c> and
/// <c>ScpAttached</c> — the latter contains SCP ids directly attached to
/// this account only (no inherited policies).
/// </summary>
public sealed record OrganizationsAccount(
    string? Id,
    string? Arn,
    string? Email,
    string? Name,
    string? Status,
    string? JoinedMethod,
    string? JoinedTimestamp,
    string? ParentOuId,
    IReadOnlyList<OrganizationsTag>? Tags,
    IReadOnlyList<string>? ScpAttached);

public sealed record OrganizationsAccountsResponse(
    IReadOnlyList<OrganizationsAccount>? Accounts,
    string? ManagementAccountId,
    string? MasterAccountId);

public sealed record OrganizationsResponsibilityTransfer(
    string? Id,
    string? Arn,
    string? Name,
    string? Type,
    string? Status,
    string? Direction,
    string? SourceManagementAccountId,
    string? SourceManagementAccountEmail,
    string? TargetManagementAccountId,
    string? TargetManagementAccountEmail,
    string? StartTimestamp,
    string? EndTimestamp,
    string? ActiveHandshakeId);

public sealed record OrganizationsResponsibilityTransfersResponse(
    IReadOnlyList<OrganizationsResponsibilityTransfer>? ResponsibilityTransfers);

// ── API Gateway v2 WebSocket connections ────────────────────────
public sealed record ApiGatewayV2Connection(
    string? ConnectionId,
    string? ApiId,
    string? Stage,
    string? ConnectedAt,
    string? LastActiveAt,
    string? SourceIp);

public sealed record ApiGatewayV2ConnectionsResponse(IReadOnlyList<ApiGatewayV2Connection>? Connections);

// ── RDS aws_lambda + aws_s3 extension bridges ───────────────────
public sealed record RdsLambdaInvokeRequest(
    [property: JsonPropertyName("function_name")] string? FunctionName,
    [property: JsonPropertyName("payload")] JsonElement? Payload,
    [property: JsonPropertyName("invocation_type")] string? InvocationType,
    [property: JsonPropertyName("region")] string? Region);

public sealed record RdsLambdaInvokeResponse(
    [property: JsonPropertyName("status_code")] int StatusCode,
    [property: JsonPropertyName("payload")] JsonElement? Payload,
    [property: JsonPropertyName("executed_version")] string? ExecutedVersion,
    [property: JsonPropertyName("log_result")] string? LogResult);

public sealed record RdsS3ImportRequest(
    [property: JsonPropertyName("bucket")] string? Bucket,
    [property: JsonPropertyName("key")] string? Key,
    [property: JsonPropertyName("region")] string? Region);

public sealed record RdsS3ImportResponse(
    [property: JsonPropertyName("bucket")] string? Bucket,
    [property: JsonPropertyName("key")] string? Key,
    [property: JsonPropertyName("body_b64")] string? BodyB64,
    [property: JsonPropertyName("bytes_processed")] long BytesProcessed);

public sealed record RdsS3ExportRequest(
    [property: JsonPropertyName("bucket")] string? Bucket,
    [property: JsonPropertyName("key")] string? Key,
    [property: JsonPropertyName("region")] string? Region,
    [property: JsonPropertyName("body_b64")] string? BodyB64);

public sealed record RdsS3ExportResponse(
    [property: JsonPropertyName("bucket")] string? Bucket,
    [property: JsonPropertyName("key")] string? Key,
    [property: JsonPropertyName("bytes_uploaded")] long BytesUploaded);

// ── Route 53 DNSSEC ─────────────────────────────────────────────
public sealed record Route53DnssecMaterialResponse(
    string? HostedZoneId,
    string? KeySigningKeyName,
    int Algorithm,
    int Flags,
    int KeyTag,
    string? DnskeyPublicKeyB64,
    string? DsDigestSha256Hex);

public sealed record Route53DnssecSignRequest(
    string? Name,
    [property: JsonPropertyName("type")] string? RecordType,
    long Ttl,
    IReadOnlyList<string>? Rdatas);

public sealed record Route53DnssecSignResponse(
    string? SignatureB64,
    int Algorithm,
    int KeyTag,
    string? SignerName,
    long Inception,
    long Expiration,
    int Labels,
    long OriginalTtl,
    [property: JsonPropertyName("type")] string? RecordType);

// ── SNS SMS ─────────────────────────────────────────────────────
public sealed record SnsSmsMessage(string? PhoneNumber, string? Message);

public sealed record SnsSmsResponse(IReadOnlyList<SnsSmsMessage>? Messages);

// ── ECS task IAM credentials (PascalCase wire) ──────────────────
public sealed record EcsTaskCredentialsResponse(
    [property: JsonPropertyName("AccessKeyId")] string? AccessKeyId,
    [property: JsonPropertyName("SecretAccessKey")] string? SecretAccessKey,
    [property: JsonPropertyName("Token")] string? Token,
    [property: JsonPropertyName("Expiration")] string? Expiration,
    [property: JsonPropertyName("RoleArn")] string? RoleArn);

/// <summary>
/// Response for <c>GET /_fakecloud/credentials</c> — the temporary
/// credentials the AWS SDK's container-credentials provider fetches when
/// <c>AWS_CONTAINER_CREDENTIALS_FULL_URI</c> points at fakecloud.
/// </summary>
public sealed record ContainerCredentialsResponse(
    [property: JsonPropertyName("AccessKeyId")] string? AccessKeyId,
    [property: JsonPropertyName("SecretAccessKey")] string? SecretAccessKey,
    [property: JsonPropertyName("Token")] string? Token,
    [property: JsonPropertyName("Expiration")] string? Expiration,
    [property: JsonPropertyName("RoleArn")] string? RoleArn);

// ── DNS resolver ─────────────────────────────────────────────

/// <summary>One record from <c>GET /_fakecloud/dns/resolve</c>.</summary>
public sealed record DnsRecord(
    string? Name,
    string? Type,
    long Ttl,
    string? Value);

/// <summary>
/// Response for <c>GET /_fakecloud/dns/resolve</c> — what the built-in DNS
/// resolver (<c>--dns</c>) would answer for a name + type from the Route 53
/// records. <c>Status</c> is one of <c>ANSWERED</c>, <c>NODATA</c>,
/// <c>NXDOMAIN</c>, <c>NOT_AUTHORITATIVE</c>. <c>ExternalCname</c> is set
/// for an A/AAAA query whose CNAME chain exits every local zone — the
/// external target the resolver would forward-resolve upstream (this
/// endpoint does no upstream I/O).
/// </summary>
public sealed record DnsResolution(
    string? Name,
    string? Type,
    string? Status,
    bool Authoritative,
    IReadOnlyList<DnsRecord>? Records,
    [property: JsonPropertyName("external_cname")] string? ExternalCname);

// ── SSM admin ────────────────────────────────────────────────

/// <summary>
/// Body for <c>POST /_fakecloud/ssm/commands/{commandId}/status</c>.
/// <c>AccountId</c> is omitted from the JSON when null and falls back to the
/// default account on the server side.
/// </summary>
public sealed record SetSsmCommandStatusRequest(string? AccountId, string? Status);

public sealed record SetSsmCommandStatusResponse(bool Updated);

/// <summary>
/// Body for <c>POST /_fakecloud/ssm/commands/{commandId}/fail</c>. All
/// fields are optional: when <c>InstanceId</c> is null every invocation on
/// the command is flipped to <c>Failed</c>.
/// </summary>
public sealed record FailSsmCommandRequest(
    string? AccountId,
    string? InstanceId,
    string? StatusDetails,
    string? StandardErrorContent);

public sealed record FailSsmCommandResponse(int UpdatedInvocations);

/// <summary>One entry from <c>GET /_fakecloud/ssm/parameter-policy-events</c>.</summary>
public sealed record SsmParameterPolicyEvent(
    string? ParameterName,
    string? ParameterArn,
    string? EventType,
    string? Message,
    string? CreatedAt);

public sealed record SsmParameterPolicyEventsResponse(
    IReadOnlyList<SsmParameterPolicyEvent>? Events);

/// <summary>
/// Body for <c>POST /_fakecloud/ssm/sessions/inject</c>. Drops a fake
/// session record into state without going through <c>StartSession</c>.
/// </summary>
public sealed record InjectSsmSessionRequest(
    string? AccountId,
    string? Target,
    string? Status,
    string? Owner,
    string? Reason,
    string? SessionId);

public sealed record InjectSsmSessionResponse(string? SessionId);

// ── KMS usage (admin) ────────────────────────────────────────

/// <summary>One recorded KMS data-plane invocation.</summary>
public sealed record KmsUsageRecord(
    string? Timestamp,
    string? Operation,
    string? ServicePrincipal,
    string? AccountId,
    string? KeyArn,
    IReadOnlyDictionary<string, string>? EncryptionContext);

public sealed record KmsUsageResponse(IReadOnlyList<KmsUsageRecord>? Records);

// ── ELBv2 WAF counts (admin) ─────────────────────────────────

/// <summary>
/// Response from <c>GET /_fakecloud/elbv2/waf-counts</c>. The exact shape of
/// <c>Counts</c> is service-internal and intentionally left as free-form JSON.
/// </summary>
public sealed record Elbv2WafCountsResponse(JsonElement? Counts);

// ── CloudFront ───────────────────────────────────────────────

/// <summary>
/// One distribution from <c>GET /_fakecloud/cloudfront/distributions</c>.
/// <c>DomainName</c> is the <c>&lt;id&gt;.cloudfront.net</c> domain; send it
/// as the <c>Host</c> header to fakecloud's main endpoint to reach the
/// in-process data plane. <c>Served</c> is whether the data plane currently
/// serves this distribution (true when it is enabled and the data plane has
/// not been disabled via <c>FAKECLOUD_CLOUDFRONT_DISABLE_DATAPLANE</c>).
/// </summary>
public sealed record CloudFrontDistribution(
    string? Id,
    string? DomainName,
    bool Enabled,
    bool Served);

public sealed record CloudFrontDistributionsResponse(IReadOnlyList<CloudFrontDistribution>? Distributions);

// ── CloudFront admin ─────────────────────────────────────────

/// <summary>
/// Body for <c>POST /_fakecloud/cloudfront/distributions/{id}/status</c>.
/// Flips a stored CloudFront Distribution's status to e.g. <c>"Deployed"</c>
/// or <c>"InProgress"</c> synchronously.
/// </summary>
public sealed record CloudFrontDistributionStatusRequest(string? Status);
