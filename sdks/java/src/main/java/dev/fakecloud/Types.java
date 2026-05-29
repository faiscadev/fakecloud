package dev.fakecloud;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;

/**
 * Response and request payload records for the fakecloud introspection API.
 *
 * <p>Grouped here as nested records so a single {@code import dev.fakecloud.Types;}
 * gives access to every shape. All records are deserialized by Jackson; extra
 * fields from newer fakecloud versions are ignored so older SDK builds keep
 * working against newer servers.
 */
public final class Types {
    private Types() {}

    // ── Health & Reset ─────────────────────────────────────────────
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record HealthResponse(String status, String version, List<String> services) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record ResetResponse(String status) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record ResetServiceResponse(String reset) {}

    // ── RDS ────────────────────────────────────────────────────────
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record RdsTag(String key, String value) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record RdsInstance(
            String dbInstanceIdentifier,
            String dbInstanceArn,
            String dbInstanceClass,
            String engine,
            String engineVersion,
            String dbInstanceStatus,
            String masterUsername,
            String dbName,
            String endpointAddress,
            int port,
            int allocatedStorage,
            boolean publiclyAccessible,
            boolean deletionProtection,
            String createdAt,
            String dbiResourceId,
            String containerId,
            int hostPort,
            List<RdsTag> tags) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record RdsInstancesResponse(List<RdsInstance> instances) {}

    // ── ElastiCache ────────────────────────────────────────────────
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record ElastiCacheCluster(
            String cacheClusterId,
            String cacheClusterStatus,
            String engine,
            String engineVersion,
            String cacheNodeType,
            int numCacheNodes,
            String replicationGroupId,
            Integer port,
            Integer hostPort,
            String containerId) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record ElastiCacheClustersResponse(List<ElastiCacheCluster> clusters) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record ElastiCacheReplicationGroupIntrospection(
            String replicationGroupId,
            String status,
            String description,
            List<String> memberClusters,
            boolean automaticFailover,
            boolean multiAz,
            String engine,
            String engineVersion,
            String cacheNodeType,
            int numCacheClusters) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record ElastiCacheReplicationGroupsResponse(
            List<ElastiCacheReplicationGroupIntrospection> replicationGroups) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record ElastiCacheServerlessCacheIntrospection(
            String serverlessCacheName,
            String status,
            String engine,
            String engineVersion,
            String cacheNodeType) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record ElastiCacheServerlessCachesResponse(
            List<ElastiCacheServerlessCacheIntrospection> serverlessCaches) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record ElastiCacheAclUser(
            String name,
            String status,
            String accessString,
            boolean noPasswordRequired,
            int passwordCount) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record ElastiCacheAclGroup(String name, List<String> members) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record ElastiCacheAclCluster(
            String clusterId,
            String engine,
            List<ElastiCacheAclUser> users,
            List<ElastiCacheAclGroup> groups) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record ElastiCacheAclsResponse(List<ElastiCacheAclCluster> acls) {}

    // ── Lambda ─────────────────────────────────────────────────────
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record LambdaInvocation(
            String functionArn, String payload, String source, String timestamp) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record LambdaInvocationsResponse(List<LambdaInvocation> invocations) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record WarmContainer(
            String functionName, String runtime, String containerId, long lastUsedSecsAgo) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record WarmContainersResponse(List<WarmContainer> containers) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record EvictContainerResponse(boolean evicted) {}

    // ── SES ────────────────────────────────────────────────────────
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record SentEmail(
            String messageId,
            String from,
            List<String> to,
            List<String> cc,
            List<String> bcc,
            String subject,
            String htmlBody,
            String textBody,
            String rawData,
            String templateName,
            String templateData,
            String dkimSignature,
            List<List<String>> headers,
            String timestamp) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record SesEmailsResponse(List<SentEmail> emails) {}

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record InboundEmailRequest(
            String from, List<String> to, String subject, String body) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record InboundActionExecuted(String rule, String actionType) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record InboundEmailResponse(
            String messageId,
            List<String> matchedRules,
            List<InboundActionExecuted> actionsExecuted) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record SesMetrics(long suppressedDropsTotal) {}

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record SesMailFromStatusRequest(String status) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record SesMailFromStatusResponse(String identity, String mailFromDomainStatus) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record SesDkimPublicKey(
            String identity,
            String selector,
            String publicKeyBase64,
            boolean signingEnabled) {}

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record SesSandboxRequest(boolean sandbox) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record SesSandboxResponse(boolean sandbox, boolean productionAccessEnabled) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record SesBouncedRecipientInfo(
            String recipient,
            String bounceType,
            String action,
            String status,
            String diagnosticCode) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record SesBounce(
            String messageId,
            String bounceType,
            String bounceSubType,
            List<SesBouncedRecipientInfo> bouncedRecipientInfo,
            String explanation,
            String timestamp,
            String originalMessageId,
            String bounceSender) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record SesBouncesResponse(List<SesBounce> bounces) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record SesMessageInsightEvent(
            String destination,
            String timestamp,
            String bounceType,
            String bounceSubType,
            String diagnosticCode,
            String complaintFeedbackType) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record SesMessageInsightsResponse(
            String messageId,
            List<SesMessageInsightEvent> sends,
            List<SesMessageInsightEvent> deliveries,
            List<SesMessageInsightEvent> opens,
            List<SesMessageInsightEvent> clicks,
            List<SesMessageInsightEvent> bounces,
            List<SesMessageInsightEvent> complaints,
            List<SesMessageInsightEvent> rejects) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record SesSmtpSubmission(
            String messageId,
            String from,
            List<String> to,
            String subject,
            long rawSizeBytes,
            String receivedAt,
            String authUser) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record SesSmtpSubmissionsResponse(List<SesSmtpSubmission> submissions) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record SesEventDestinationDelivery(
            String destinationName,
            String destinationType,
            String eventType,
            String messageId,
            String dispatchedAt,
            String targetArn) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record SesEventDestinationDeliveriesResponse(
            List<SesEventDestinationDelivery> deliveries) {}

    // ── SNS ────────────────────────────────────────────────────────
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record SnsMessage(
            String messageId,
            String topicArn,
            String message,
            String subject,
            String timestamp) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record SnsMessagesResponse(List<SnsMessage> messages) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record PendingConfirmation(
            String subscriptionArn,
            String topicArn,
            String protocol,
            String endpoint,
            String token) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record PendingConfirmationsResponse(List<PendingConfirmation> pendingConfirmations) {}

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record ConfirmSubscriptionRequest(String subscriptionArn) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record ConfirmSubscriptionResponse(boolean confirmed) {}

    // ── SQS ────────────────────────────────────────────────────────
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record SqsMessageInfo(
            String messageId,
            String body,
            int receiveCount,
            boolean inFlight,
            String createdAt) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record SqsQueueMessages(
            String queueUrl, String queueName, List<SqsMessageInfo> messages) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record SqsMessagesResponse(List<SqsQueueMessages> queues) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record ExpirationTickResponse(int expiredMessages) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record ForceDlqResponse(int movedMessages) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record AppAsTickResponse(int applied) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record AppAsScheduledTickResponse(int fired) {}

    // ── EventBridge ────────────────────────────────────────────────
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record EventBridgeEvent(
            String eventId,
            String source,
            String detailType,
            String detail,
            String busName,
            String timestamp) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record EventBridgeLambdaDelivery(
            String functionArn, String payload, String timestamp) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record EventBridgeLogDelivery(
            String logGroupArn, String payload, String timestamp) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record EventBridgeDeliveries(
            List<EventBridgeLambdaDelivery> lambda,
            List<EventBridgeLogDelivery> logs) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record EventHistoryResponse(
            List<EventBridgeEvent> events, EventBridgeDeliveries deliveries) {}

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record FireRuleRequest(String busName, String ruleName) {
        public FireRuleRequest(String ruleName) {
            this(null, ruleName);
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record FireRuleTarget(String type, String arn) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record FireRuleResponse(List<FireRuleTarget> targets) {}

    // ── Glue ───────────────────────────────────────────────────────
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record GlueJob(
            String accountId,
            String name,
            String role,
            Object command,
            Map<String, String> defaultArguments,
            Double maxCapacity,
            long maxRetries,
            Long timeout,
            String glueVersion,
            String workerType,
            Long numberOfWorkers,
            String createdOn,
            String lastModifiedOn) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record GlueJobsResponse(List<GlueJob> jobs) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record GlueJobRun(
            String accountId,
            String id,
            String jobName,
            long attempt,
            String startedOn,
            String completedOn,
            String jobRunState,
            Map<String, String> arguments,
            String errorMessage,
            long executionTime) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record GlueJobRunsResponse(List<GlueJobRun> runs) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record GlueCrawler(
            String accountId,
            String name,
            String role,
            String databaseName,
            String state,
            String targetSummary,
            String schedule,
            String creationTime,
            String lastUpdated) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record GlueCrawlersResponse(List<GlueCrawler> crawlers) {}

    // ── CloudWatch ─────────────────────────────────────────────────
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record CloudWatchDimension(String name, String value) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record CloudWatchAlarm(
            String accountId,
            String region,
            String name,
            String type,
            String state,
            String stateReason,
            String stateUpdatedTimestamp,
            boolean actionsEnabled,
            List<String> alarmActions,
            List<String> okActions,
            List<String> insufficientDataActions,
            String namespace,
            String metricName,
            Double threshold,
            String comparisonOperator,
            String alarmRule) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record CloudWatchAlarmsResponse(List<CloudWatchAlarm> alarms) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record CloudWatchLatestDatapoint(String timestamp, Double value, String unit) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record CloudWatchMetric(
            String accountId,
            String region,
            String namespace,
            String metricName,
            List<CloudWatchDimension> dimensions,
            int datapointCount,
            CloudWatchLatestDatapoint latest) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record CloudWatchMetricsResponse(List<CloudWatchMetric> metrics) {}

    // ── Scheduler (EventBridge Scheduler) ──────────────────────────
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record SchedulerSchedule(
            String accountId,
            String groupName,
            String name,
            String arn,
            String state,
            String scheduleExpression,
            String targetArn,
            String lastFired) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record SchedulerSchedulesResponse(List<SchedulerSchedule> schedules) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record FireScheduleResponse(String scheduleArn, String targetArn) {}

    // ── S3 ─────────────────────────────────────────────────────────
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record S3Notification(
            String bucket, String key, String eventType, String timestamp) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record S3NotificationsResponse(List<S3Notification> notifications) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record LifecycleTickResponse(
            int processedBuckets, int expiredObjects, int transitionedObjects) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record S3AccessPointEntry(
            String name,
            String alias,
            String bucket,
            String accountId,
            String networkOrigin,
            String vpcConfiguration,
            String publicAccessBlock,
            String createdAt) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record S3AccessPointsResponse(List<S3AccessPointEntry> accessPoints) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record S3ObjectLambdaResponse(
            String requestToken,
            String requestRoute,
            Integer statusCode,
            String bodyBase64,
            long bodySize,
            String contentType,
            String errorMessage,
            Map<String, String> metadata) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record S3ObjectLambdaResponsesResponse(List<S3ObjectLambdaResponse> responses) {}

    // ── DynamoDB ───────────────────────────────────────────────────
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record TtlTickResponse(int expiredItems) {}

    // ── SecretsManager ─────────────────────────────────────────────
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record RotationTickResponse(List<String> rotatedSecrets) {}

    // ── Cognito ────────────────────────────────────────────────────
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record UserConfirmationCodes(
            String confirmationCode, Map<String, Object> attributeVerificationCodes) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record ConfirmationCode(
            String poolId, String username, String code, String type, String attribute) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record ConfirmationCodesResponse(List<ConfirmationCode> codes) {}

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record ConfirmUserRequest(String userPoolId, String username) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record ConfirmUserResponse(boolean confirmed, String error) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record TokenInfo(
            String type,
            String username,
            String poolId,
            String clientId,
            long issuedAt) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record TokensResponse(List<TokenInfo> tokens) {}

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record ExpireTokensRequest(String userPoolId, String username) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record ExpireTokensResponse(int expiredTokens) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record AuthEvent(
            String eventType,
            String username,
            String userPoolId,
            String clientId,
            long timestamp,
            boolean success) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record AuthEventsResponse(List<AuthEvent> events) {}

    /**
     * One PreTokenGeneration Lambda trigger invocation captured by
     * {@code InitiateAuth}. {@code claimsAdded} / {@code claimsOverridden} /
     * {@code groupOverrides} are pre-parsed from the Lambda response so
     * tests don't have to walk the raw
     * {@code claimsAndScopeOverrideDetails} shape themselves.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record PreTokenGenInvocation(
            String poolId,
            String userPoolArn,
            String username,
            String triggerSource,
            String lambdaArn,
            Map<String, Object> requestPayload,
            Map<String, Object> responsePayload,
            List<String> claimsAdded,
            List<String> claimsOverridden,
            List<String> groupOverrides,
            String invokedAt,
            long durationMs) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record PreTokenGenInvocationsResponse(List<PreTokenGenInvocation> invocations) {}

    /**
     * Payload for {@code POST /_fakecloud/cognito/authorization-codes}.
     * Lets test harnesses pre-allocate a single-use OAuth2 authorization
     * code that the {@code /oauth2/token} {@code authorization_code}
     * grant later consumes.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record MintAuthorizationCodeRequest(
            String userPoolId,
            String clientId,
            String username,
            String redirectUri,
            List<String> scopes,
            String codeChallenge,
            String codeChallengeMethod,
            String nonce) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record MintAuthorizationCodeResponse(String code) {}

    /**
     * Payload for {@code POST /_fakecloud/cognito/compromised-passwords}.
     * Each plaintext is SHA-256 hashed server-side and added to the
     * per-account compromised-password set; subsequent {@code SignUp}
     * / {@code AdminInitiateAuth} fail with
     * {@code InvalidPasswordException} on any pool whose
     * {@code CompromisedCredentialsRiskConfiguration.Actions.EventAction}
     * is {@code BLOCK}.
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record CompromisedPasswordsRequest(List<String> passwords) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record CompromisedPasswordsResponse(long added) {}

    /**
     * Registered WebAuthn credential from
     * {@code GET /_fakecloud/cognito/webauthn-credentials}. The
     * {@code attestationInfo} field is the parsed-attestation JSON
     * (packed format details, AAGUID, certificate chain summary,
     * signature counter); its shape depends on the attestation format
     * so it is surfaced as a generic {@link Object}.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record WebAuthnCredential(
            @com.fasterxml.jackson.annotation.JsonProperty("account_id") String accountId,
            @com.fasterxml.jackson.annotation.JsonProperty("pool_user") String poolUser,
            @com.fasterxml.jackson.annotation.JsonProperty("credential_id") String credentialId,
            @com.fasterxml.jackson.annotation.JsonProperty("relying_party_id") String relyingPartyId,
            @com.fasterxml.jackson.annotation.JsonProperty("attestation_info") Object attestationInfo) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record WebAuthnCredentialsResponse(List<WebAuthnCredential> credentials) {}

    // ── Step Functions ─────────────────────────────────────────────
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record StepFunctionsExecution(
            String executionArn,
            String stateMachineArn,
            String name,
            String status,
            String startDate,
            String input,
            String output,
            String stopDate) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record StepFunctionsExecutionsResponse(List<StepFunctionsExecution> executions) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record StepFunctionsSyncBillingDetails(
            long billedDurationInMilliseconds,
            long billedMemoryUsedInMb) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record StepFunctionsSyncExecution(
            String executionArn,
            String stateMachineArn,
            String name,
            String status,
            String input,
            String output,
            String startedAt,
            String stoppedAt,
            long durationMs,
            StepFunctionsSyncBillingDetails billingDetails) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record StepFunctionsSyncExecutionsResponse(List<StepFunctionsSyncExecution> executions) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record StepFunctionsExecutionTreeNode(
            String arn,
            String stateMachineArn,
            String status,
            String startedAt,
            String stoppedAt,
            List<StepFunctionsExecutionTreeNode> children) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record StepFunctionsExecutionTreeResponse(
            String rootArn,
            StepFunctionsExecutionTreeNode tree) {}

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record SfnEnqueueActivityTaskRequest(
            String activityArn,
            String input,
            Long heartbeatSeconds,
            Long timeoutSeconds) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record SfnEnqueueActivityTaskResponse(String taskToken) {}

    // ── Bedrock ────────────────────────────────────────────────────
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record BedrockInvocation(
            String modelId, String input, String output, String timestamp, String error) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record BedrockInvocationsResponse(List<BedrockInvocation> invocations) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record BedrockModelResponseConfig(String status, String modelId) {}

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record BedrockResponseRule(String promptContains, String response) {}

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record BedrockFaultRule(
            String errorType,
            String message,
            Integer httpStatus,
            Integer count,
            String modelId,
            String operation) {
        public BedrockFaultRule(String errorType) {
            this(errorType, null, null, null, null, null);
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record BedrockFaultRuleState(
            String errorType,
            String message,
            int httpStatus,
            int remaining,
            String modelId,
            String operation) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record BedrockFaultsResponse(List<BedrockFaultRuleState> faults) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record BedrockStatusResponse(String status) {}

    // ── Bedrock Agent (control plane) ──────────────────────────────
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record BedrockAgentAliasSummary(
            String aliasId,
            String aliasName,
            String agentVersion,
            String aliasArn,
            String status,
            String createdAt,
            String updatedAt) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record BedrockAgentVersionSummary(
            String agentVersion,
            String createdAt,
            String instruction,
            String foundationModel) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record BedrockAgentKnowledgeBaseSummary(
            String knowledgeBaseId,
            String state,
            String description) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record BedrockAgentCollaboratorSummary(
            String collaboratorId,
            String collaboratorName,
            String collaboratorAliasArn,
            String relayConversationHistory) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record BedrockAgentRow(
            String agentId,
            String agentName,
            String agentArn,
            String agentStatus,
            String foundationModel,
            String instruction,
            List<BedrockAgentKnowledgeBaseSummary> knowledgeBases,
            List<Object> actionGroups,
            List<BedrockAgentCollaboratorSummary> collaborators,
            List<BedrockAgentAliasSummary> aliases,
            List<BedrockAgentVersionSummary> versions,
            Object promptOverrides,
            String createdAt,
            String updatedAt) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record BedrockAgentAgentsResponse(List<BedrockAgentRow> agents) {}

    // ── Bedrock Agent Runtime (data plane) ────────────────────────
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record BedrockAgentRuntimeInvocation(
            String invocationId,
            String op,
            String agentId,
            String flowId,
            String sessionId,
            String input,
            String output,
            long outputChunks,
            Object trace,
            List<Object> citations,
            String invokedAt,
            long durationMs) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record BedrockAgentRuntimeInvocationsResponse(
            List<BedrockAgentRuntimeInvocation> invocations) {}

    // ── API Gateway v2 ─────────────────────────────────────────────
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record ApiGatewayV2Request(
            String requestId,
            String apiId,
            String stage,
            String method,
            String path,
            Map<String, String> headers,
            Map<String, String> queryParams,
            String body,
            String timestamp,
            int statusCode) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record ApiGatewayV2RequestsResponse(List<ApiGatewayV2Request> requests) {}

    // ── IAM ───────────────────────────────────────────────────────

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record CreateAdminRequest(String accountId, String userName) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record CreateAdminResponse(
            String accessKeyId,
            String secretAccessKey,
            String accountId,
            String arn) {}

    // ── ECR ────────────────────────────────────────────────────────

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record EcrTag(String key, String value) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record EcrRepository(
            String repositoryName,
            String repositoryArn,
            String registryId,
            String repositoryUri,
            String imageTagMutability,
            boolean scanOnPush,
            String createdAt,
            List<EcrTag> tags,
            boolean hasPolicy,
            boolean hasLifecyclePolicy,
            long imageCount,
            long layerCount) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record EcrRepositoriesResponse(List<EcrRepository> repositories) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record EcrImage(
            String repositoryName,
            String imageDigest,
            List<String> imageTags,
            long imageSizeInBytes,
            String imageManifestMediaType,
            String imagePushedAt) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record EcrImagesResponse(List<EcrImage> images) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record EcrPullThroughRule(
            String ecrRepositoryPrefix,
            String upstreamRegistryUrl,
            String upstreamRegistry,
            String credentialArn,
            String customRoleArn,
            String createdAt,
            String updatedAt) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record EcrPullThroughRulesResponse(List<EcrPullThroughRule> rules) {}

    // ── ECS ───────────────────────────────────────────────────────

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record EcsTag(String key, String value) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record EcsCluster(
            String clusterName,
            String clusterArn,
            String status,
            int runningTasksCount,
            int pendingTasksCount,
            int activeServicesCount,
            int registeredContainerInstancesCount,
            List<String> capacityProviders,
            List<EcsTag> tags,
            String createdAt) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record EcsClustersResponse(List<EcsCluster> clusters) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record EcsTaskContainer(
            String name,
            String image,
            String lastStatus,
            Long exitCode,
            String runtimeId,
            boolean essential) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record EcsTask(
            String taskArn,
            String taskId,
            String clusterArn,
            String clusterName,
            String taskDefinitionArn,
            String family,
            int revision,
            String lastStatus,
            String desiredStatus,
            String launchType,
            String createdAt,
            String startedAt,
            String stoppingAt,
            String stoppedAt,
            String stopCode,
            String stoppedReason,
            List<EcsTaskContainer> containers,
            long capturedLogBytes) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record EcsTasksResponse(List<EcsTask> tasks) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record EcsTaskLogsResponse(
            String taskArn,
            String logs,
            String lastStatus,
            Long exitCode) {}

    public record EcsMarkFailedRequest(Long exitCode, String reason) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record EcsLifecycleEvent(
            String at,
            String eventType,
            String taskArn,
            String clusterArn,
            String lastStatus,
            Object detail) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record EcsEventsResponse(List<EcsLifecycleEvent> events) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record EcsTaskMetadataLimits(Double cpu, Long memory) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record EcsTaskMetadataPort(Long containerPort, Long hostPort, String protocol) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record EcsTaskMetadataContainer(
            String name,
            String image,
            String imageId,
            List<EcsTaskMetadataPort> ports,
            java.util.Map<String, String> labels,
            String desiredStatus,
            String knownStatus,
            EcsTaskMetadataLimits limits,
            String createdAt,
            String startedAt,
            Long exitCode) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record EcsTaskMetadata(
            String cluster,
            String taskArn,
            String family,
            Integer revision,
            String desiredStatus,
            String knownStatus,
            List<EcsTaskMetadataContainer> containers,
            String pullStartedAt,
            String pullStoppedAt,
            String availabilityZone,
            String launchType,
            String vpcId,
            String eniId) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record EcsTaskMetadataResponse(EcsTaskMetadata task) {}

    // ── ELBv2 ─────────────────────────────────────────────────────

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Elbv2Tag(String key, String value) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Elbv2AvailabilityZone(String zoneName, String subnetId) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Elbv2LoadBalancer(
            String arn,
            String name,
            String dnsName,
            String scheme,
            String vpcId,
            String stateCode,
            String stateReason,
            String lbType,
            String ipAddressType,
            List<Elbv2AvailabilityZone> availabilityZones,
            List<String> securityGroups,
            String createdTime,
            List<Elbv2Tag> tags) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Elbv2LoadBalancersResponse(List<Elbv2LoadBalancer> loadBalancers) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Elbv2Target(
            String id,
            Integer port,
            String availabilityZone,
            String healthState,
            String healthReason,
            String healthDescription) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Elbv2TargetGroup(
            String arn,
            String name,
            String protocol,
            Integer port,
            String vpcId,
            String targetType,
            List<String> loadBalancerArns,
            List<Elbv2Target> targets,
            String healthCheckProtocol,
            String healthCheckPort,
            String healthCheckPath,
            int healthyThresholdCount,
            int unhealthyThresholdCount,
            String createdTime,
            List<Elbv2Tag> tags) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Elbv2TargetGroupsResponse(List<Elbv2TargetGroup> targetGroups) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Elbv2Listener(
            String arn,
            String loadBalancerArn,
            Integer port,
            String protocol,
            String sslPolicy,
            List<String> certificateArns,
            String defaultActionType,
            String defaultTargetGroupArn) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Elbv2ListenersResponse(List<Elbv2Listener> listeners) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Elbv2Rule(
            String arn,
            String listenerArn,
            String priority,
            boolean isDefault,
            List<String> conditionFields,
            String actionType) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Elbv2RulesResponse(List<Elbv2Rule> rules) {}

    /**
     * Response from {@code POST /_fakecloud/elbv2/access-logs/flush}.
     * {@code flushed} is true when an access-log buffer was wired and the
     * synchronous flush ran; false when no logger was configured.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Elbv2FlushAccessLogsResponse(boolean flushed) {}

    // ── Route 53 ─────────────────────────────────────────────────

    /**
     * Body for the Route 53 admin endpoint
     * {@code POST /_fakecloud/route53/health-checks/{id}/status}.
     * {@code status} is one of {@code "Success"}, {@code "Failure"},
     * {@code "Timeout"}, {@code "DnsError"},
     * {@code "InsufficientDataPoints"}, {@code "Unknown"};
     * {@code reason} is omitted from the JSON when null.
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record SetHealthCheckStatusRequest(String status, String reason) {}

    // ── ACM ──────────────────────────────────────────────────────

    /**
     * Body for the ACM admin endpoint
     * {@code POST /_fakecloud/acm/certificates/{arn-or-id}/status}.
     * {@code status} is one of {@code "ISSUED"}, {@code "FAILED"},
     * or {@code "VALIDATION_TIMED_OUT"}; {@code reason} is recorded
     * as {@code FailureReason} on subsequent {@code DescribeCertificate}
     * calls when status is non-ISSUED, and is omitted from the JSON
     * when null.
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record SetCertificateStatusRequest(String status, String reason) {}

    // ── CloudWatch Logs ───────────────────────────────────────────

    /**
     * Admin payload for {@code POST /_fakecloud/logs/anomalies/inject}.
     * Lets tests seed synthetic CloudWatch Logs anomalies so they
     * can exercise {@code ListAnomalies}/{@code UpdateAnomaly}
     * deterministically.
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record LogsAnomalyInjectRequest(
            String anomalyDetectorArn,
            String patternString,
            List<String> logGroupArns,
            String priority) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record LogsAnomalyInjectResponse(String anomalyId) {}

    /**
     * One entry of {@code GET /_fakecloud/logs/delivery-config}. Joins a
     * delivery with the {@code logType} from its delivery source so test
     * code does not have to re-query the AWS-shaped APIs.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record LogsDeliveryConfiguration(
            String id,
            String name,
            String deliveryDestinationArn,
            String deliverySourceName,
            String logType,
            List<String> recordFields,
            String fieldDelimiter,
            Object s3DeliveryConfiguration,
            long createdAt) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record LogsDeliveryConfigResponse(
            List<LogsDeliveryConfiguration> configurations) {}

    /** One parsed {@code IndexPolicy} on a log group. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record LogsFieldIndex(
            List<String> fields, long createdAt, long lastUsedAt) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record LogsFieldIndexesResponse(
            String logGroupName, List<LogsFieldIndex> indexes) {}

    /**
     * Response from {@code GET /_fakecloud/acm/certificates/{arn-or-id}/chain-info}.
     *
     * <p>fakecloud is not a PKI: {@code externalCaValidated} is always
     * {@code false}, documenting that imported chains are stored verbatim
     * rather than verified against a real trust store. The byte/block
     * counts let callers confirm the PEM they uploaded round-trips intact.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record AcmCertificateChainInfo(
            @JsonProperty("certificate_arn") String certificateArn,
            @JsonProperty("certificate_pem_bytes") int certificatePemBytes,
            @JsonProperty("certificate_pem_blocks") int certificatePemBlocks,
            @JsonProperty("chain_pem_bytes") int chainPemBytes,
            @JsonProperty("chain_pem_blocks") int chainPemBlocks,
            @JsonProperty("external_ca_validated") boolean externalCaValidated,
            @JsonProperty("status") String status,
            @JsonProperty("cert_type") String certType) {}

    // ── Athena ────────────────────────────────────────────────────
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record AthenaNamedQuery(
            String namedQueryId,
            String name,
            String description,
            String database,
            String queryString,
            String workgroup,
            String lastUsedAt) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record AthenaNamedQueriesResponse(List<AthenaNamedQuery> queries) {}
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record OrganizationsTag(String key, String value) {}

    /**
     * A single member account from
     * {@code GET /_fakecloud/organizations/accounts}. Mirrors the AWS
     * Organizations {@code Account} shape plus {@code parentOuId} and
     * {@code scpAttached} — the latter contains SCP ids directly
     * attached to this account only (no inherited policies).
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record OrganizationsAccount(
            String id,
            String arn,
            String email,
            String name,
            String status,
            String joinedMethod,
            String joinedTimestamp,
            String parentOuId,
            List<OrganizationsTag> tags,
            List<String> scpAttached) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record OrganizationsAccountsResponse(
            List<OrganizationsAccount> accounts,
            String managementAccountId,
            String masterAccountId) {}

    // ── API Gateway v2 WebSocket connections ────────────────────────

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record ApiGatewayV2Connection(
            String connectionId,
            String apiId,
            String stage,
            String connectedAt,
            String lastActiveAt,
            String sourceIp) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record ApiGatewayV2ConnectionsResponse(List<ApiGatewayV2Connection> connections) {}

    // ── RDS aws_lambda + aws_s3 extension bridges ───────────────────

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record RdsLambdaInvokeRequest(
            @JsonProperty("function_name") String functionName,
            @JsonProperty("payload") Object payload,
            @JsonProperty("invocation_type") String invocationType,
            @JsonProperty("region") String region) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record RdsLambdaInvokeResponse(
            @JsonProperty("status_code") int statusCode,
            @JsonProperty("payload") Object payload,
            @JsonProperty("executed_version") String executedVersion,
            @JsonProperty("log_result") String logResult) {}

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record RdsS3ImportRequest(
            @JsonProperty("bucket") String bucket,
            @JsonProperty("key") String key,
            @JsonProperty("region") String region) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record RdsS3ImportResponse(
            @JsonProperty("bucket") String bucket,
            @JsonProperty("key") String key,
            @JsonProperty("body_b64") String bodyB64,
            @JsonProperty("bytes_processed") long bytesProcessed) {}

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record RdsS3ExportRequest(
            @JsonProperty("bucket") String bucket,
            @JsonProperty("key") String key,
            @JsonProperty("region") String region,
            @JsonProperty("body_b64") String bodyB64) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record RdsS3ExportResponse(
            @JsonProperty("bucket") String bucket,
            @JsonProperty("key") String key,
            @JsonProperty("bytes_uploaded") long bytesUploaded) {}

    // ── Route 53 DNSSEC ─────────────────────────────────────────────

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Route53DnssecMaterialResponse(
            String hostedZoneId,
            String keySigningKeyName,
            int algorithm,
            int flags,
            int keyTag,
            String dnskeyPublicKeyB64,
            String dsDigestSha256Hex) {}

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Route53DnssecSignRequest(
            String name,
            @JsonProperty("type") String recordType,
            long ttl,
            List<String> rdatas) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Route53DnssecSignResponse(
            String signatureB64,
            int algorithm,
            int keyTag,
            String signerName,
            long inception,
            long expiration,
            int labels,
            long originalTtl,
            @JsonProperty("type") String recordType) {}

    // ── SNS SMS ─────────────────────────────────────────────────────

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record SnsSmsMessage(String phoneNumber, String message) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record SnsSmsResponse(List<SnsSmsMessage> messages) {}

    // ── ECS task IAM credentials (PascalCase wire) ──────────────────

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record EcsTaskCredentialsResponse(
            @JsonProperty("AccessKeyId") String accessKeyId,
            @JsonProperty("SecretAccessKey") String secretAccessKey,
            @JsonProperty("Token") String token,
            @JsonProperty("Expiration") String expiration,
            @JsonProperty("RoleArn") String roleArn) {}

    // ── SSM admin ────────────────────────────────────────────────

    /**
     * Body for {@code POST /_fakecloud/ssm/commands/{commandId}/status}.
     * {@code accountId} is omitted from the JSON when null and falls back
     * to the default account on the server side.
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record SetSsmCommandStatusRequest(String accountId, String status) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record SetSsmCommandStatusResponse(boolean updated) {}

    /**
     * Body for {@code POST /_fakecloud/ssm/commands/{commandId}/fail}. All
     * fields are optional: when {@code instanceId} is null every
     * invocation on the command is flipped to {@code Failed}.
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record FailSsmCommandRequest(
            String accountId,
            String instanceId,
            String statusDetails,
            String standardErrorContent) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record FailSsmCommandResponse(int updatedInvocations) {}

    /** One entry from {@code GET /_fakecloud/ssm/parameter-policy-events}. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record SsmParameterPolicyEvent(
            String parameterName,
            String parameterArn,
            String eventType,
            String message,
            String createdAt) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record SsmParameterPolicyEventsResponse(
            List<SsmParameterPolicyEvent> events) {}

    /**
     * Body for {@code POST /_fakecloud/ssm/sessions/inject}. Drops a fake
     * session record into state without going through {@code StartSession}.
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record InjectSsmSessionRequest(
            String accountId,
            String target,
            String status,
            String owner,
            String reason,
            String sessionId) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record InjectSsmSessionResponse(String sessionId) {}

    // ── KMS usage (admin) ────────────────────────────────────────

    /** One recorded KMS data-plane invocation. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record KmsUsageRecord(
            String timestamp,
            String operation,
            String servicePrincipal,
            String accountId,
            String keyArn,
            Map<String, String> encryptionContext) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record KmsUsageResponse(List<KmsUsageRecord> records) {}

    // ── ELBv2 WAF counts (admin) ─────────────────────────────────

    /**
     * Response from {@code GET /_fakecloud/elbv2/waf-counts}. The exact
     * shape of {@code counts} is service-internal and intentionally left
     * as free-form JSON.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Elbv2WafCountsResponse(Object counts) {}

    // ── CloudFront admin ─────────────────────────────────────────

    /**
     * Body for {@code POST /_fakecloud/cloudfront/distributions/{id}/status}.
     * Flips a stored CloudFront Distribution's status to e.g.
     * {@code "Deployed"} or {@code "InProgress"} synchronously.
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record CloudFrontDistributionStatusRequest(String status) {}
}
