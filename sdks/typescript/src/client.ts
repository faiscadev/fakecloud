import type {
  AthenaNamedQueriesResponse,
  CreateAdminResponse,
  ApiGatewayV2RequestsResponse,
  ApiGatewayV2ConnectionsResponse,
  RdsLambdaInvokeRequest,
  RdsLambdaInvokeResponse,
  RdsS3ImportRequest,
  RdsS3ImportResponse,
  RdsS3ExportRequest,
  RdsS3ExportResponse,
  Route53DnssecMaterialResponse,
  Route53DnssecSignRequest,
  Route53DnssecSignResponse,
  SnsSmsResponse,
  EcsTaskCredentials,
  BedrockFaultRule,
  BedrockFaultsResponse,
  BedrockInvocationsResponse,
  BedrockModelResponseConfig,
  BedrockResponseRule,
  BedrockStatusResponse,
  BedrockAgentAgentsResponse,
  BedrockAgentRuntimeInvocationsResponse,
  HealthResponse,
  ResetResponse,
  ResetServiceResponse,
  RdsInstancesResponse,
  ElastiCacheAclsResponse,
  ElastiCacheClustersResponse,
  ElastiCacheReplicationGroupsResponse,
  ElastiCacheServerlessCachesResponse,
  EcrRepositoriesResponse,
  EcrImagesResponse,
  EcrPullThroughRulesResponse,
  LambdaInvocationsResponse,
  LogsAnomalyInjectRequest,
  LogsAnomalyInjectResponse,
  LogsDeliveryConfigResponse,
  LogsFieldIndexesResponse,
  WarmContainersResponse,
  EvictContainerResponse,
  SesBouncesResponse,
  SesEmailsResponse,
  SesEventDestinationDeliveriesResponse,
  SesMessageInsightsResponse,
  SesSmtpSubmissionsResponse,
  InboundEmailRequest,
  InboundEmailResponse,
  SesMetrics,
  SesMailFromStatus,
  SesMailFromStatusResponse,
  SesDkimPublicKey,
  SesSandboxResponse,
  SnsMessagesResponse,
  PendingConfirmationsResponse,
  ConfirmSubscriptionRequest,
  ConfirmSubscriptionResponse,
  SqsMessagesResponse,
  ExpirationTickResponse,
  ForceDlqResponse,
  AppAsScheduledTickResponse,
  AppAsTickResponse,
  EventHistoryResponse,
  FireRuleRequest,
  FireRuleResponse,
  FireScheduleResponse,
  GlueJobRunsResponse,
  GlueJobsResponse,
  SchedulerSchedulesResponse,
  S3AccessPointsResponse,
  S3NotificationsResponse,
  S3ObjectLambdaResponsesResponse,
  LifecycleTickResponse,
  TtlTickResponse,
  RotationTickResponse,
  UserConfirmationCodes,
  ConfirmationCodesResponse,
  ConfirmUserRequest,
  ConfirmUserResponse,
  TokensResponse,
  ExpireTokensRequest,
  ExpireTokensResponse,
  AuthEventsResponse,
  PreTokenGenInvocationsResponse,
  MintAuthorizationCodeRequest,
  MintAuthorizationCodeResponse,
  CompromisedPasswordsRequest,
  CompromisedPasswordsResponse,
  WebAuthnCredentialsResponse,
  StepFunctionsExecutionsResponse,
  SfnEnqueueActivityTaskRequest,
  SfnEnqueueActivityTaskResponse,
  EcsClustersResponse,
  EcsTask,
  EcsTasksResponse,
  EcsTaskLogsResponse,
  EcsMarkFailedRequest,
  EcsEventsResponse,
  EcsTaskMetadataResponse,
  Elbv2LoadBalancersResponse,
  Elbv2TargetGroupsResponse,
  Elbv2ListenersResponse,
  Elbv2RulesResponse,
  OrganizationsAccountsResponse,
} from "./types.js";

export class FakeCloudError extends Error {
  constructor(
    public readonly status: number,
    public readonly body: string,
  ) {
    super(`fakecloud API error (${status}): ${body}`);
    this.name = "FakeCloudError";
  }
}

async function parse<T>(resp: Response): Promise<T> {
  if (!resp.ok) {
    const body = await resp.text().catch(() => "");
    throw new FakeCloudError(resp.status, body);
  }
  return resp.json() as Promise<T>;
}

// ── Sub-clients ────────────────────────────────────────────────────

export class LambdaClient {
  constructor(private baseUrl: string) {}

  async getInvocations(): Promise<LambdaInvocationsResponse> {
    const resp = await fetch(`${this.baseUrl}/_fakecloud/lambda/invocations`);
    return parse(resp);
  }

  async getWarmContainers(): Promise<WarmContainersResponse> {
    const resp = await fetch(
      `${this.baseUrl}/_fakecloud/lambda/warm-containers`,
    );
    return parse(resp);
  }

  async evictContainer(functionName: string): Promise<EvictContainerResponse> {
    const resp = await fetch(
      `${this.baseUrl}/_fakecloud/lambda/${encodeURIComponent(functionName)}/evict-container`,
      { method: "POST" },
    );
    return parse(resp);
  }
}

export class RdsClient {
  constructor(private baseUrl: string) {}

  async getInstances(): Promise<RdsInstancesResponse> {
    const resp = await fetch(`${this.baseUrl}/_fakecloud/rds/instances`);
    return parse(resp);
  }

  /**
   * Bridge endpoint the PostgreSQL `aws_lambda` extension dispatches
   * into from inside an RDS DB instance. Body and response use
   * snake_case to match the Rust types on the wire.
   */
  async lambdaInvoke(
    req: RdsLambdaInvokeRequest,
  ): Promise<RdsLambdaInvokeResponse> {
    const resp = await fetch(`${this.baseUrl}/_fakecloud/rds/lambda-invoke`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(req),
    });
    return parse(resp);
  }

  /** Bridge for the PostgreSQL `aws_s3` extension's import path. */
  async s3Import(req: RdsS3ImportRequest): Promise<RdsS3ImportResponse> {
    const resp = await fetch(`${this.baseUrl}/_fakecloud/rds/s3-import`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(req),
    });
    return parse(resp);
  }

  /** Bridge for the PostgreSQL `aws_s3` extension's export path. */
  async s3Export(req: RdsS3ExportRequest): Promise<RdsS3ExportResponse> {
    const resp = await fetch(`${this.baseUrl}/_fakecloud/rds/s3-export`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(req),
    });
    return parse(resp);
  }
}

export class ElastiCacheClient {
  constructor(private baseUrl: string) {}

  async getClusters(): Promise<ElastiCacheClustersResponse> {
    const resp = await fetch(`${this.baseUrl}/_fakecloud/elasticache/clusters`);
    return parse(resp);
  }

  async getReplicationGroups(): Promise<ElastiCacheReplicationGroupsResponse> {
    const resp = await fetch(
      `${this.baseUrl}/_fakecloud/elasticache/replication-groups`,
    );
    return parse(resp);
  }

  async getServerlessCaches(): Promise<ElastiCacheServerlessCachesResponse> {
    const resp = await fetch(
      `${this.baseUrl}/_fakecloud/elasticache/serverless-caches`,
    );
    return parse(resp);
  }

  async getElastiCacheAcls(): Promise<ElastiCacheAclsResponse> {
    const resp = await fetch(`${this.baseUrl}/_fakecloud/elasticache/acls`);
    return parse(resp);
  }
}

export class EcrClient {
  constructor(private baseUrl: string) {}

  async getRepositories(): Promise<EcrRepositoriesResponse> {
    const resp = await fetch(`${this.baseUrl}/_fakecloud/ecr/repositories`);
    return parse(resp);
  }

  async getImages(repositoryName?: string): Promise<EcrImagesResponse> {
    const url = repositoryName
      ? `${this.baseUrl}/_fakecloud/ecr/images?repo=${encodeURIComponent(repositoryName)}`
      : `${this.baseUrl}/_fakecloud/ecr/images`;
    const resp = await fetch(url);
    return parse(resp);
  }

  async getPullThroughRules(): Promise<EcrPullThroughRulesResponse> {
    const resp = await fetch(
      `${this.baseUrl}/_fakecloud/ecr/pull-through-rules`,
    );
    return parse(resp);
  }
}

export class SesClient {
  constructor(private baseUrl: string) {}

  async getEmails(): Promise<SesEmailsResponse> {
    const resp = await fetch(`${this.baseUrl}/_fakecloud/ses/emails`);
    return parse(resp);
  }

  async simulateInbound(
    req: InboundEmailRequest,
  ): Promise<InboundEmailResponse> {
    const resp = await fetch(`${this.baseUrl}/_fakecloud/ses/inbound`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(req),
    });
    return parse(resp);
  }

  async getMetrics(): Promise<SesMetrics> {
    const resp = await fetch(`${this.baseUrl}/_fakecloud/ses/metrics`);
    return parse(resp);
  }

  async setMailFromStatus(
    identity: string,
    status: SesMailFromStatus,
  ): Promise<SesMailFromStatusResponse> {
    const resp = await fetch(
      `${this.baseUrl}/_fakecloud/ses/identities/${encodeURIComponent(identity)}/mail-from-status`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ status }),
      },
    );
    return parse(resp);
  }

  async getDkimPublicKey(identity: string): Promise<SesDkimPublicKey> {
    const resp = await fetch(
      `${this.baseUrl}/_fakecloud/ses/identities/${encodeURIComponent(identity)}/dkim-public-key`,
    );
    return parse(resp);
  }

  async setSandbox(sandbox: boolean): Promise<SesSandboxResponse> {
    const resp = await fetch(`${this.baseUrl}/_fakecloud/ses/account/sandbox`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ sandbox }),
    });
    return parse(resp);
  }

  async getBounces(): Promise<SesBouncesResponse> {
    const resp = await fetch(`${this.baseUrl}/_fakecloud/ses/bounces`);
    return parse(resp);
  }

  async getMessageInsights(
    messageId: string,
  ): Promise<SesMessageInsightsResponse> {
    const resp = await fetch(
      `${this.baseUrl}/_fakecloud/ses/messages/${encodeURIComponent(messageId)}/insights`,
    );
    return parse(resp);
  }

  async getSmtpSubmissions(): Promise<SesSmtpSubmissionsResponse> {
    const resp = await fetch(`${this.baseUrl}/_fakecloud/ses/smtp/submissions`);
    return parse(resp);
  }

  async getEventDestinationDeliveries(): Promise<SesEventDestinationDeliveriesResponse> {
    const resp = await fetch(
      `${this.baseUrl}/_fakecloud/ses/event-destinations/deliveries`,
    );
    return parse(resp);
  }
}

export class LogsClient {
  constructor(private baseUrl: string) {}

  async injectAnomaly(
    req: LogsAnomalyInjectRequest,
  ): Promise<LogsAnomalyInjectResponse> {
    const resp = await fetch(
      `${this.baseUrl}/_fakecloud/logs/anomalies/inject`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(req),
      },
    );
    return parse(resp);
  }

  /** Persisted CloudWatch Logs delivery configurations. */
  async getDeliveryConfig(): Promise<LogsDeliveryConfigResponse> {
    const resp = await fetch(`${this.baseUrl}/_fakecloud/logs/delivery-config`);
    return parse(resp);
  }

  /** Parsed `Fields` from index policies on a log group. 404 on unknown group. */
  async getFieldIndexes(
    logGroupName: string,
  ): Promise<LogsFieldIndexesResponse> {
    const resp = await fetch(
      `${this.baseUrl}/_fakecloud/logs/field-indexes/${encodeURIComponent(logGroupName)}`,
    );
    return parse(resp);
  }
}

export class SnsClient {
  constructor(private baseUrl: string) {}

  async getMessages(): Promise<SnsMessagesResponse> {
    const resp = await fetch(`${this.baseUrl}/_fakecloud/sns/messages`);
    return parse(resp);
  }

  async getPendingConfirmations(): Promise<PendingConfirmationsResponse> {
    const resp = await fetch(
      `${this.baseUrl}/_fakecloud/sns/pending-confirmations`,
    );
    return parse(resp);
  }

  async confirmSubscription(
    req: ConfirmSubscriptionRequest,
  ): Promise<ConfirmSubscriptionResponse> {
    const resp = await fetch(
      `${this.baseUrl}/_fakecloud/sns/confirm-subscription`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(req),
      },
    );
    return parse(resp);
  }

  /**
   * Returns the PEM-encoded SNS signing certificate used to sign
   * outbound HTTP/HTTPS notifications. The endpoint returns raw PEM
   * text, not JSON.
   */
  async getCertPem(): Promise<string> {
    const resp = await fetch(`${this.baseUrl}/_fakecloud/sns/cert.pem`);
    if (!resp.ok) {
      const body = await resp.text().catch(() => "");
      throw new FakeCloudError(resp.status, body);
    }
    return resp.text();
  }

  /** Recorded SMS messages dispatched via `SNS Publish`. */
  async getSms(): Promise<SnsSmsResponse> {
    const resp = await fetch(`${this.baseUrl}/_fakecloud/sns/sms`);
    return parse(resp);
  }
}

export class SqsClient {
  constructor(private baseUrl: string) {}

  async getMessages(): Promise<SqsMessagesResponse> {
    const resp = await fetch(`${this.baseUrl}/_fakecloud/sqs/messages`);
    return parse(resp);
  }

  async tickExpiration(): Promise<ExpirationTickResponse> {
    const resp = await fetch(
      `${this.baseUrl}/_fakecloud/sqs/expiration-processor/tick`,
      { method: "POST" },
    );
    return parse(resp);
  }

  async forceDlq(queueName: string): Promise<ForceDlqResponse> {
    const resp = await fetch(
      `${this.baseUrl}/_fakecloud/sqs/${encodeURIComponent(queueName)}/force-dlq`,
      { method: "POST" },
    );
    return parse(resp);
  }
}

/**
 * Application Auto Scaling watcher introspection client. The watcher
 * periodically reads CloudWatch metrics, evaluates each scaling
 * policy, and applies capacity changes on registered scalable
 * targets. The `tick` endpoint forces an immediate evaluation.
 */
export class ApplicationAutoScalingClient {
  constructor(private baseUrl: string) {}

  async tick(): Promise<AppAsTickResponse> {
    const resp = await fetch(
      `${this.baseUrl}/_fakecloud/application-autoscaling/tick`,
      { method: "POST" },
    );
    return parse(resp);
  }

  async scheduledTick(): Promise<AppAsScheduledTickResponse> {
    const resp = await fetch(
      `${this.baseUrl}/_fakecloud/application-autoscaling/scheduled-tick`,
      { method: "POST" },
    );
    return parse(resp);
  }
}

export class EventsClient {
  constructor(private baseUrl: string) {}

  async getHistory(): Promise<EventHistoryResponse> {
    const resp = await fetch(`${this.baseUrl}/_fakecloud/events/history`);
    return parse(resp);
  }

  async fireRule(req: FireRuleRequest): Promise<FireRuleResponse> {
    const resp = await fetch(`${this.baseUrl}/_fakecloud/events/fire-rule`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(req),
    });
    return parse(resp);
  }
}

export class SchedulerClient {
  constructor(private baseUrl: string) {}

  async getSchedules(): Promise<SchedulerSchedulesResponse> {
    const resp = await fetch(`${this.baseUrl}/_fakecloud/scheduler/schedules`);
    return parse(resp);
  }

  async fireSchedule(
    group: string,
    name: string,
  ): Promise<FireScheduleResponse> {
    const resp = await fetch(
      `${this.baseUrl}/_fakecloud/scheduler/fire/${encodeURIComponent(group)}/${encodeURIComponent(name)}`,
      { method: "POST" },
    );
    return parse(resp);
  }
}

export class GlueClient {
  constructor(private baseUrl: string) {}

  async getJobs(): Promise<GlueJobsResponse> {
    const resp = await fetch(`${this.baseUrl}/_fakecloud/glue/jobs`);
    return parse(resp);
  }

  async getJobRuns(jobName?: string): Promise<GlueJobRunsResponse> {
    const url = jobName
      ? `${this.baseUrl}/_fakecloud/glue/job-runs?job_name=${encodeURIComponent(jobName)}`
      : `${this.baseUrl}/_fakecloud/glue/job-runs`;
    const resp = await fetch(url);
    return parse(resp);
  }
}

export class S3Client {
  constructor(private baseUrl: string) {}

  async getNotifications(): Promise<S3NotificationsResponse> {
    const resp = await fetch(`${this.baseUrl}/_fakecloud/s3/notifications`);
    return parse(resp);
  }

  async tickLifecycle(): Promise<LifecycleTickResponse> {
    const resp = await fetch(
      `${this.baseUrl}/_fakecloud/s3/lifecycle-processor/tick`,
      { method: "POST" },
    );
    return parse(resp);
  }

  async getAccessPoints(): Promise<S3AccessPointsResponse> {
    const resp = await fetch(`${this.baseUrl}/_fakecloud/s3/access-points`);
    return parse(resp);
  }

  async getObjectLambdaResponses(): Promise<S3ObjectLambdaResponsesResponse> {
    const resp = await fetch(
      `${this.baseUrl}/_fakecloud/s3/object-lambda-responses`,
    );
    return parse(resp);
  }
}

export class DynamoDbClient {
  constructor(private baseUrl: string) {}

  async tickTtl(): Promise<TtlTickResponse> {
    const resp = await fetch(
      `${this.baseUrl}/_fakecloud/dynamodb/ttl-processor/tick`,
      { method: "POST" },
    );
    return parse(resp);
  }
}

export class SecretsManagerClient {
  constructor(private baseUrl: string) {}

  async tickRotation(): Promise<RotationTickResponse> {
    const resp = await fetch(
      `${this.baseUrl}/_fakecloud/secretsmanager/rotation-scheduler/tick`,
      { method: "POST" },
    );
    return parse(resp);
  }
}

export class CognitoClient {
  constructor(private baseUrl: string) {}

  async getUserCodes(
    poolId: string,
    username: string,
  ): Promise<UserConfirmationCodes> {
    const resp = await fetch(
      `${this.baseUrl}/_fakecloud/cognito/confirmation-codes/${encodeURIComponent(poolId)}/${encodeURIComponent(username)}`,
    );
    return parse(resp);
  }

  async getConfirmationCodes(): Promise<ConfirmationCodesResponse> {
    const resp = await fetch(
      `${this.baseUrl}/_fakecloud/cognito/confirmation-codes`,
    );
    return parse(resp);
  }

  async confirmUser(req: ConfirmUserRequest): Promise<ConfirmUserResponse> {
    const resp = await fetch(
      `${this.baseUrl}/_fakecloud/cognito/confirm-user`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(req),
      },
    );
    // This endpoint returns 404 for missing users but still has a JSON body
    if (resp.status === 404) {
      const body: ConfirmUserResponse = await resp.json();
      throw new FakeCloudError(404, body.error ?? "user not found");
    }
    return parse(resp);
  }

  async getTokens(): Promise<TokensResponse> {
    const resp = await fetch(`${this.baseUrl}/_fakecloud/cognito/tokens`);
    return parse(resp);
  }

  async expireTokens(req: ExpireTokensRequest): Promise<ExpireTokensResponse> {
    const resp = await fetch(
      `${this.baseUrl}/_fakecloud/cognito/expire-tokens`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(req),
      },
    );
    return parse(resp);
  }

  async getAuthEvents(): Promise<AuthEventsResponse> {
    const resp = await fetch(`${this.baseUrl}/_fakecloud/cognito/auth-events`);
    return parse(resp);
  }

  /**
   * Returns the PreTokenGeneration Lambda trigger invocation log
   * recorded by `InitiateAuth`. Each entry includes the full request /
   * response payloads plus pre-parsed `claimsAdded`,
   * `claimsOverridden`, and `groupOverrides` so tests can assert claim
   * mutation flows without inspecting the issued JWT.
   */
  async getPreTokenGenInvocations(): Promise<PreTokenGenInvocationsResponse> {
    const resp = await fetch(
      `${this.baseUrl}/_fakecloud/cognito/pretokengen/invocations`,
    );
    return parse(resp);
  }

  async mintAuthorizationCode(
    req: MintAuthorizationCodeRequest,
  ): Promise<MintAuthorizationCodeResponse> {
    const resp = await fetch(
      `${this.baseUrl}/_fakecloud/cognito/authorization-codes`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(req),
      },
    );
    return parse(resp);
  }

  async setCompromisedPasswords(
    req: CompromisedPasswordsRequest,
  ): Promise<CompromisedPasswordsResponse> {
    const resp = await fetch(
      `${this.baseUrl}/_fakecloud/cognito/compromised-passwords`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(req),
      },
    );
    return parse(resp);
  }

  async getWebAuthnCredentials(): Promise<WebAuthnCredentialsResponse> {
    const resp = await fetch(
      `${this.baseUrl}/_fakecloud/cognito/webauthn-credentials`,
    );
    return parse(resp);
  }
}

export class ApiGatewayV2Client {
  constructor(private readonly baseUrl: string) {}

  async getRequests(): Promise<ApiGatewayV2RequestsResponse> {
    const resp = await fetch(
      `${this.baseUrl}/_fakecloud/apigatewayv2/requests`,
    );
    return parse(resp);
  }

  /** List every live WebSocket connection currently tracked. */
  async getConnections(): Promise<ApiGatewayV2ConnectionsResponse> {
    const resp = await fetch(
      `${this.baseUrl}/_fakecloud/apigatewayv2/connections`,
    );
    return parse(resp);
  }

  /**
   * Inspect the mTLS trust store / chain configuration for a custom
   * domain name. The response is free JSON — fakecloud returns
   * whichever fields the server has wired up.
   */
  async getMtlsInfo(domainName: string): Promise<Record<string, unknown>> {
    const resp = await fetch(
      `${this.baseUrl}/_fakecloud/apigatewayv2/domain-names/${encodeURIComponent(
        domainName,
      )}/mtls-info`,
    );
    return parse(resp);
  }

  /**
   * Build the `ws://` URL that connects to a WebSocket API stage,
   * derived from the configured fakecloud base URL. The HTTP scheme is
   * swapped for `ws://` (or `wss://` when the base URL is `https://`).
   */
  wsUrl(apiId: string, stage: string = "$default"): string {
    const wsBase = this.baseUrl.replace(/^http/, "ws");
    return `${wsBase}/_fakecloud/apigatewayv2/ws/${encodeURIComponent(
      apiId,
    )}/${encodeURIComponent(stage)}`;
  }
}

export class StepFunctionsClient {
  constructor(private readonly baseUrl: string) {}

  async getExecutions(): Promise<StepFunctionsExecutionsResponse> {
    const resp = await fetch(
      `${this.baseUrl}/_fakecloud/stepfunctions/executions`,
    );
    return parse(resp);
  }

  async enqueueActivityTask(
    req: SfnEnqueueActivityTaskRequest,
  ): Promise<SfnEnqueueActivityTaskResponse> {
    const resp = await fetch(
      `${this.baseUrl}/_fakecloud/stepfunctions/enqueue-activity-task`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(req),
      },
    );
    return parse(resp);
  }
}

export class BedrockClient {
  constructor(private readonly baseUrl: string) {}

  async getInvocations(): Promise<BedrockInvocationsResponse> {
    const resp = await fetch(`${this.baseUrl}/_fakecloud/bedrock/invocations`);
    return parse(resp);
  }

  async setModelResponse(
    modelId: string,
    response: string,
  ): Promise<BedrockModelResponseConfig> {
    const resp = await fetch(
      `${this.baseUrl}/_fakecloud/bedrock/models/${encodeURIComponent(modelId)}/response`,
      {
        method: "POST",
        headers: { "Content-Type": "text/plain" },
        body: response,
      },
    );
    return parse(resp);
  }

  /** Replace the prompt-conditional response rules for a given model. */
  async setResponseRules(
    modelId: string,
    rules: BedrockResponseRule[],
  ): Promise<BedrockModelResponseConfig> {
    const resp = await fetch(
      `${this.baseUrl}/_fakecloud/bedrock/models/${encodeURIComponent(modelId)}/responses`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ rules }),
      },
    );
    return parse(resp);
  }

  /** Clear all prompt-conditional response rules for a given model. */
  async clearResponseRules(
    modelId: string,
  ): Promise<BedrockModelResponseConfig> {
    const resp = await fetch(
      `${this.baseUrl}/_fakecloud/bedrock/models/${encodeURIComponent(modelId)}/responses`,
      { method: "DELETE" },
    );
    return parse(resp);
  }

  /** Queue a fault rule that will cause the next matching Bedrock runtime call(s) to fail. */
  async queueFault(rule: BedrockFaultRule): Promise<BedrockStatusResponse> {
    const resp = await fetch(`${this.baseUrl}/_fakecloud/bedrock/faults`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(rule),
    });
    return parse(resp);
  }

  /** List currently queued fault rules. */
  async getFaults(): Promise<BedrockFaultsResponse> {
    const resp = await fetch(`${this.baseUrl}/_fakecloud/bedrock/faults`);
    return parse(resp);
  }

  /** Clear all queued fault rules. */
  async clearFaults(): Promise<BedrockStatusResponse> {
    const resp = await fetch(`${this.baseUrl}/_fakecloud/bedrock/faults`, {
      method: "DELETE",
    });
    return parse(resp);
  }
}

/**
 * Bedrock Agent (control plane) sub-client. Reads `/_fakecloud/bedrock-agent/*`
 * introspection endpoints for test assertions.
 */
export class BedrockAgentClient {
  constructor(private readonly baseUrl: string) {}

  /** List every Bedrock Agent with its aliases, versions, knowledge bases, and collaborators flattened. */
  async getAgents(): Promise<BedrockAgentAgentsResponse> {
    const resp = await fetch(`${this.baseUrl}/_fakecloud/bedrock-agent/agents`);
    return parse(resp);
  }
}

/**
 * Bedrock Agent Runtime (data plane) sub-client. Reads
 * `/_fakecloud/bedrock-agent-runtime/*` introspection endpoints.
 */
export class BedrockAgentRuntimeClient {
  constructor(private readonly baseUrl: string) {}

  /** List recorded InvokeAgent / InvokeInlineAgent / InvokeFlow / Retrieve / RetrieveAndGenerate calls. */
  async getInvocations(): Promise<BedrockAgentRuntimeInvocationsResponse> {
    const resp = await fetch(
      `${this.baseUrl}/_fakecloud/bedrock-agent-runtime/invocations`,
    );
    return parse(resp);
  }
}

// ── Main client ────────────────────────────────────────────────────

export class FakeCloud {
  private readonly baseUrl: string;

  private readonly _lambda: LambdaClient;
  private readonly _rds: RdsClient;
  private readonly _elasticache: ElastiCacheClient;
  private readonly _ecr: EcrClient;
  private readonly _logs: LogsClient;
  private readonly _ses: SesClient;
  private readonly _sns: SnsClient;
  private readonly _sqs: SqsClient;
  private readonly _events: EventsClient;
  private readonly _scheduler: SchedulerClient;
  private readonly _glue: GlueClient;
  private readonly _s3: S3Client;
  private readonly _dynamodb: DynamoDbClient;
  private readonly _secretsmanager: SecretsManagerClient;
  private readonly _cognito: CognitoClient;
  private readonly _apigatewayv2: ApiGatewayV2Client;
  private readonly _stepfunctions: StepFunctionsClient;
  private readonly _bedrock: BedrockClient;
  private readonly _bedrockAgent: BedrockAgentClient;
  private readonly _bedrockAgentRuntime: BedrockAgentRuntimeClient;
  private readonly _ecs: EcsClient;
  private readonly _elbv2: Elbv2Client;
  private readonly _route53: Route53Client;
  private readonly _acm: AcmClient;
  private readonly _applicationAutoscaling: ApplicationAutoScalingClient;
  private readonly _athena: AthenaClient;
  private readonly _organizations: OrganizationsClient;

  constructor(baseUrl: string = "http://localhost:4566") {
    this.baseUrl = baseUrl.replace(/\/+$/, "");

    this._lambda = new LambdaClient(this.baseUrl);
    this._rds = new RdsClient(this.baseUrl);
    this._elasticache = new ElastiCacheClient(this.baseUrl);
    this._ecr = new EcrClient(this.baseUrl);
    this._logs = new LogsClient(this.baseUrl);
    this._ses = new SesClient(this.baseUrl);
    this._sns = new SnsClient(this.baseUrl);
    this._sqs = new SqsClient(this.baseUrl);
    this._events = new EventsClient(this.baseUrl);
    this._scheduler = new SchedulerClient(this.baseUrl);
    this._glue = new GlueClient(this.baseUrl);
    this._s3 = new S3Client(this.baseUrl);
    this._dynamodb = new DynamoDbClient(this.baseUrl);
    this._secretsmanager = new SecretsManagerClient(this.baseUrl);
    this._cognito = new CognitoClient(this.baseUrl);
    this._apigatewayv2 = new ApiGatewayV2Client(this.baseUrl);
    this._stepfunctions = new StepFunctionsClient(this.baseUrl);
    this._bedrock = new BedrockClient(this.baseUrl);
    this._bedrockAgent = new BedrockAgentClient(this.baseUrl);
    this._bedrockAgentRuntime = new BedrockAgentRuntimeClient(this.baseUrl);
    this._ecs = new EcsClient(this.baseUrl);
    this._elbv2 = new Elbv2Client(this.baseUrl);
    this._route53 = new Route53Client(this.baseUrl);
    this._acm = new AcmClient(this.baseUrl);
    this._applicationAutoscaling = new ApplicationAutoScalingClient(
      this.baseUrl,
    );
    this._athena = new AthenaClient(this.baseUrl);
    this._organizations = new OrganizationsClient(this.baseUrl);
  }

  // ── Health & Reset ─────────────────────────────────────────────

  async health(): Promise<HealthResponse> {
    const resp = await fetch(`${this.baseUrl}/_fakecloud/health`);
    return parse(resp);
  }

  async reset(): Promise<ResetResponse> {
    const resp = await fetch(`${this.baseUrl}/_reset`, { method: "POST" });
    return parse(resp);
  }

  async resetService(service: string): Promise<ResetServiceResponse> {
    const resp = await fetch(
      `${this.baseUrl}/_fakecloud/reset/${encodeURIComponent(service)}`,
      { method: "POST" },
    );
    return parse(resp);
  }

  // ── IAM ────────────────────────────────────────────────────────

  async createAdmin(
    accountId: string,
    userName: string,
  ): Promise<CreateAdminResponse> {
    const resp = await fetch(`${this.baseUrl}/_fakecloud/iam/create-admin`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ accountId, userName }),
    });
    return parse(resp);
  }

  // ── Sub-clients ────────────────────────────────────────────────

  get lambda(): LambdaClient {
    return this._lambda;
  }

  get rds(): RdsClient {
    return this._rds;
  }

  get elasticache(): ElastiCacheClient {
    return this._elasticache;
  }

  get ecr(): EcrClient {
    return this._ecr;
  }

  get logs(): LogsClient {
    return this._logs;
  }

  get ses(): SesClient {
    return this._ses;
  }

  get sns(): SnsClient {
    return this._sns;
  }

  get sqs(): SqsClient {
    return this._sqs;
  }

  get events(): EventsClient {
    return this._events;
  }

  get scheduler(): SchedulerClient {
    return this._scheduler;
  }

  get glue(): GlueClient {
    return this._glue;
  }

  get s3(): S3Client {
    return this._s3;
  }

  get dynamodb(): DynamoDbClient {
    return this._dynamodb;
  }

  get secretsmanager(): SecretsManagerClient {
    return this._secretsmanager;
  }

  get cognito(): CognitoClient {
    return this._cognito;
  }

  get apigatewayv2(): ApiGatewayV2Client {
    return this._apigatewayv2;
  }

  get stepfunctions(): StepFunctionsClient {
    return this._stepfunctions;
  }

  get bedrock(): BedrockClient {
    return this._bedrock;
  }

  get bedrockAgent(): BedrockAgentClient {
    return this._bedrockAgent;
  }

  get bedrockAgentRuntime(): BedrockAgentRuntimeClient {
    return this._bedrockAgentRuntime;
  }

  get ecs(): EcsClient {
    return this._ecs;
  }

  get elbv2(): Elbv2Client {
    return this._elbv2;
  }

  get route53(): Route53Client {
    return this._route53;
  }

  get acm(): AcmClient {
    return this._acm;
  }

  get applicationAutoscaling(): ApplicationAutoScalingClient {
    return this._applicationAutoscaling;
  }

  get athena(): AthenaClient {
    return this._athena;
  }

  get organizations(): OrganizationsClient {
    return this._organizations;
  }
}

export class AthenaClient {
  constructor(private baseUrl: string) {}

  /**
   * List every named query stored in the Athena registry across all
   * workgroups for the default account. The response includes a
   * `lastUsedAt` timestamp the server bumps each time
   * `StartQueryExecution` resolves the query string by id.
   */
  async getNamedQueries(): Promise<AthenaNamedQueriesResponse> {
    const resp = await fetch(`${this.baseUrl}/_fakecloud/athena/named-queries`);
    return parse(resp);
  }
}

/**
 * AWS Organizations admin/introspection client. Bypasses IAM so tests
 * can assert on org shape without management-account credentials.
 */
export class OrganizationsClient {
  constructor(private baseUrl: string) {}

  /**
   * List every member account in the org with lifecycle state, parent
   * OU, tags, and directly-attached SCPs. Returns an empty accounts
   * list (and nullable management/master ids) when no organization
   * has been created yet.
   */
  async getAccounts(): Promise<OrganizationsAccountsResponse> {
    const resp = await fetch(
      `${this.baseUrl}/_fakecloud/organizations/accounts`,
    );
    return parse(resp);
  }
}

export class EcsClient {
  constructor(private baseUrl: string) {}

  /** List every ECS cluster fakecloud has seen, across every account. */
  async getClusters(): Promise<EcsClustersResponse> {
    const resp = await fetch(`${this.baseUrl}/_fakecloud/ecs/clusters`);
    return parse(resp);
  }

  /**
   * List every task fakecloud is tracking. Pass `cluster` and/or `status`
   * to narrow the result set; both filters match the server's query params.
   */
  async getTasks(opts?: {
    cluster?: string;
    status?: string;
  }): Promise<EcsTasksResponse> {
    const params = new URLSearchParams();
    if (opts?.cluster) params.set("cluster", opts.cluster);
    if (opts?.status) params.set("status", opts.status);
    const qs = params.toString();
    const resp = await fetch(
      `${this.baseUrl}/_fakecloud/ecs/tasks${qs ? `?${qs}` : ""}`,
    );
    return parse(resp);
  }

  /** Fetch a single task snapshot by task ID. */
  async getTask(taskId: string): Promise<EcsTask> {
    const resp = await fetch(
      `${this.baseUrl}/_fakecloud/ecs/tasks/${encodeURIComponent(taskId)}`,
    );
    return parse(resp);
  }

  /** Captured docker stdout/stderr for a task plus its exit code if known. */
  async getTaskLogs(taskId: string): Promise<EcsTaskLogsResponse> {
    const resp = await fetch(
      `${this.baseUrl}/_fakecloud/ecs/tasks/${encodeURIComponent(taskId)}/logs`,
    );
    return parse(resp);
  }

  /**
   * SIGTERM (then SIGKILL after 10s) the task's running container via the
   * runtime. Returns the updated task snapshot.
   */
  async forceStopTask(taskId: string): Promise<EcsTask> {
    const resp = await fetch(
      `${this.baseUrl}/_fakecloud/ecs/tasks/${encodeURIComponent(taskId)}/force-stop`,
      { method: "POST" },
    );
    return parse(resp);
  }

  /**
   * Flip a task to STOPPED without killing the container — useful for
   * simulating failed tasks deterministically in tests.
   */
  async markTaskFailed(
    taskId: string,
    req: EcsMarkFailedRequest,
  ): Promise<EcsTask> {
    const resp = await fetch(
      `${this.baseUrl}/_fakecloud/ecs/tasks/${encodeURIComponent(taskId)}/mark-failed`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(req),
      },
    );
    return parse(resp);
  }

  /** Replay the lifecycle event log. */
  async getEvents(): Promise<EcsEventsResponse> {
    const resp = await fetch(`${this.baseUrl}/_fakecloud/ecs/events`);
    return parse(resp);
  }

  /**
   * Return the aggregated v4 metadata dump (the same shape
   * `ECS_CONTAINER_METADATA_URI_V4` exposes to a container) for the task
   * with the given full ARN. The ARN is URL-encoded into the path.
   */
  async getTaskMetadata(taskArn: string): Promise<EcsTaskMetadataResponse> {
    const resp = await fetch(
      `${this.baseUrl}/_fakecloud/ecs/metadata/${encodeURIComponent(taskArn)}`,
    );
    return parse(resp);
  }

  /**
   * Return the IMDS-style temporary credentials fakecloud minted for an
   * ECS task — the same payload a container reads from the credentials
   * provider URL when running under Fargate.
   */
  async getTaskCredentials(taskId: string): Promise<EcsTaskCredentials> {
    const resp = await fetch(
      `${this.baseUrl}/_fakecloud/ecs/creds/${encodeURIComponent(taskId)}`,
    );
    return parse(resp);
  }

  /**
   * v3 task metadata dump (`ECS_CONTAINER_METADATA_URI`). Returns the
   * raw JSON payload the container would receive — fakecloud does not
   * impose a typed schema on this surface.
   */
  async getTaskMetadataV3(taskId: string): Promise<Record<string, unknown>> {
    const resp = await fetch(
      `${this.baseUrl}/_fakecloud/ecs/v3/${encodeURIComponent(taskId)}`,
    );
    return parse(resp);
  }

  /** v4 task metadata dump (`ECS_CONTAINER_METADATA_URI_V4`). */
  async getTaskMetadataV4(taskId: string): Promise<Record<string, unknown>> {
    const resp = await fetch(
      `${this.baseUrl}/_fakecloud/ecs/v4/${encodeURIComponent(taskId)}`,
    );
    return parse(resp);
  }
}

export class Elbv2Client {
  constructor(private baseUrl: string) {}

  /** List every ELBv2 load balancer fakecloud has seen, across every account. */
  async getLoadBalancers(): Promise<Elbv2LoadBalancersResponse> {
    const resp = await fetch(`${this.baseUrl}/_fakecloud/elbv2/load-balancers`);
    return parse(resp);
  }

  async getTargetGroups(): Promise<Elbv2TargetGroupsResponse> {
    const resp = await fetch(`${this.baseUrl}/_fakecloud/elbv2/target-groups`);
    return parse(resp);
  }

  async getListeners(): Promise<Elbv2ListenersResponse> {
    const resp = await fetch(`${this.baseUrl}/_fakecloud/elbv2/listeners`);
    return parse(resp);
  }

  async getRules(): Promise<Elbv2RulesResponse> {
    const resp = await fetch(`${this.baseUrl}/_fakecloud/elbv2/rules`);
    return parse(resp);
  }

  /**
   * Force every buffered ALB access-log + connection-log line to flush
   * to S3 right now, bypassing the periodic 60-second timer. Useful in
   * tests that need to assert log delivery without waiting.
   */
  async flushAccessLogs(): Promise<{ flushed: boolean }> {
    const resp = await fetch(
      `${this.baseUrl}/_fakecloud/elbv2/access-logs/flush`,
      {
        method: "POST",
      },
    );
    return parse(resp);
  }
}

/** Body for `route53.setHealthCheckStatus`. */
export interface SetHealthCheckStatusRequest {
  /**
   * One of `"Success"`, `"Failure"`, `"Timeout"`, `"DnsError"`,
   * `"InsufficientDataPoints"`, `"Unknown"`.
   */
  status:
    | "Success"
    | "Failure"
    | "Timeout"
    | "DnsError"
    | "InsufficientDataPoints"
    | "Unknown";
  /**
   * Optional reason appended to the `<Status>` element for
   * failure-flavoured statuses (`Failure`, `Timeout`, `DnsError`).
   * Ignored for `Success`, `InsufficientDataPoints`, `Unknown`.
   */
  reason?: string;
}

/**
 * Route 53 admin client.
 *
 * Wraps the per-health-check status admin endpoint that lets tests flip a
 * stored health check between healthy and unhealthy without a live prober,
 * so failover and multi-value routing can be exercised end-to-end.
 */
export class Route53Client {
  constructor(private baseUrl: string) {}

  async setHealthCheckStatus(
    healthCheckId: string,
    req: SetHealthCheckStatusRequest,
  ): Promise<void> {
    const resp = await fetch(
      `${this.baseUrl}/_fakecloud/route53/health-checks/${encodeURIComponent(
        healthCheckId,
      )}/status`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(req),
      },
    );
    if (!resp.ok) {
      const body = await resp.text().catch(() => "");
      throw new FakeCloudError(resp.status, body);
    }
  }

  /**
   * Fetch the stable DNSSEC material (DNSKEY public key + DS digest)
   * derived from the hosted zone's first ACTIVE KSK. 404 when the zone
   * has no active KSK.
   */
  async getDnssecMaterial(
    zoneId: string,
  ): Promise<Route53DnssecMaterialResponse> {
    const resp = await fetch(
      `${this.baseUrl}/_fakecloud/route53/zones/${encodeURIComponent(
        zoneId,
      )}/dnssec`,
    );
    return parse(resp);
  }

  /**
   * Sign an RRset under the zone's first ACTIVE KSK and return the raw
   * RRSIG fields so tests can verify against `dnskeyPublicKeyB64`.
   */
  async signDnssecRrset(
    zoneId: string,
    req: Route53DnssecSignRequest,
  ): Promise<Route53DnssecSignResponse> {
    const resp = await fetch(
      `${this.baseUrl}/_fakecloud/route53/zones/${encodeURIComponent(
        zoneId,
      )}/dnssec/sign`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(req),
      },
    );
    return parse(resp);
  }
}

/** Body for `acm.setCertificateStatus`. */
export interface SetCertificateStatusRequest {
  /** New certificate status. */
  status: "ISSUED" | "FAILED" | "VALIDATION_TIMED_OUT" | string;
  /** Optional reason recorded as `FailureReason` for non-ISSUED states. */
  reason?: string;
}

/**
 * Response shape for `acm.getCertificateChainInfo`.
 *
 * fakecloud isn't a PKI — `externalCaValidated` is always `false`,
 * documenting that imported chains are stored verbatim rather than
 * verified against a real trust store. The byte/block counts let
 * callers confirm the PEM they uploaded round-trips intact.
 */
export interface AcmCertificateChainInfo {
  certificateArn: string;
  certificatePemBytes: number;
  certificatePemBlocks: number;
  chainPemBytes: number;
  chainPemBlocks: number;
  externalCaValidated: boolean;
  status: string;
  certType: string;
}

/**
 * ACM admin client.
 *
 * Wraps the per-certificate status admin endpoint that lets tests flip
 * a stored certificate between PENDING_VALIDATION, ISSUED, FAILED, and
 * VALIDATION_TIMED_OUT without waiting on the auto-issue tick, so
 * validation-failure flows can be exercised end-to-end.
 */
export class AcmClient {
  constructor(private baseUrl: string) {}

  /**
   * Flip an ACM certificate's status synchronously. `arnOrId` accepts
   * either the full ACM ARN or the trailing UUID portion; full ARNs
   * are reduced to their UUID before being embedded in the URL.
   */
  async setCertificateStatus(
    arnOrId: string,
    req: SetCertificateStatusRequest,
  ): Promise<void> {
    const idx = arnOrId.lastIndexOf("certificate/");
    const id =
      idx >= 0 ? arnOrId.substring(idx + "certificate/".length) : arnOrId;
    const resp = await fetch(
      `${this.baseUrl}/_fakecloud/acm/certificates/${encodeURIComponent(id)}/status`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(req),
      },
    );
    if (!resp.ok) {
      const body = await resp.text().catch(() => "");
      throw new FakeCloudError(resp.status, body);
    }
  }

  /**
   * Approve a `PENDING_VALIDATION` certificate. Synchronous equivalent
   * of "the user clicked the validation link in the email" — flips the
   * cert to `ISSUED` and refreshes its renewal eligibility /
   * RenewalSummary. EMAIL-validated certs do not auto-issue, so tests
   * drive their issuance through this endpoint.
   */
  async approveCertificate(arnOrId: string): Promise<void> {
    const idx = arnOrId.lastIndexOf("certificate/");
    const id =
      idx >= 0 ? arnOrId.substring(idx + "certificate/".length) : arnOrId;
    const resp = await fetch(
      `${this.baseUrl}/_fakecloud/acm/certificates/${encodeURIComponent(id)}/approve`,
      { method: "POST" },
    );
    if (!resp.ok) {
      const body = await resp.text().catch(() => "");
      throw new FakeCloudError(resp.status, body);
    }
  }

  /**
   * Inspect a stored certificate's PEM block counts and byte sizes.
   * Returns `externalCaValidated: false` to document that fakecloud
   * does not run real X.509 verification — use the byte/block counts
   * to confirm uploaded chains round-trip intact, especially for
   * `ImportCertificate` flows. `arnOrId` accepts the full ACM ARN or
   * just the trailing UUID.
   */
  async getCertificateChainInfo(
    arnOrId: string,
  ): Promise<AcmCertificateChainInfo> {
    const idx = arnOrId.lastIndexOf("certificate/");
    const id =
      idx >= 0 ? arnOrId.substring(idx + "certificate/".length) : arnOrId;
    const resp = await fetch(
      `${this.baseUrl}/_fakecloud/acm/certificates/${encodeURIComponent(id)}/chain-info`,
    );
    if (!resp.ok) {
      const body = await resp.text().catch(() => "");
      throw new FakeCloudError(resp.status, body);
    }
    const data = (await resp.json()) as Record<string, unknown>;
    return {
      certificateArn: String(data["certificate_arn"] ?? ""),
      certificatePemBytes: Number(data["certificate_pem_bytes"] ?? 0),
      certificatePemBlocks: Number(data["certificate_pem_blocks"] ?? 0),
      chainPemBytes: Number(data["chain_pem_bytes"] ?? 0),
      chainPemBlocks: Number(data["chain_pem_blocks"] ?? 0),
      externalCaValidated: Boolean(data["external_ca_validated"] ?? false),
      status: String(data["status"] ?? ""),
      certType: String(data["cert_type"] ?? ""),
    };
  }
}
