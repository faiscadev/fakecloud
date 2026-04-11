import type {
  ApiGatewayV2RequestsResponse,
  HealthResponse,
  ResetResponse,
  ResetServiceResponse,
  RdsInstancesResponse,
  ElastiCacheClustersResponse,
  ElastiCacheReplicationGroupsResponse,
  ElastiCacheServerlessCachesResponse,
  LambdaInvocationsResponse,
  WarmContainersResponse,
  EvictContainerResponse,
  SesEmailsResponse,
  InboundEmailRequest,
  InboundEmailResponse,
  SnsMessagesResponse,
  PendingConfirmationsResponse,
  ConfirmSubscriptionRequest,
  ConfirmSubscriptionResponse,
  SqsMessagesResponse,
  ExpirationTickResponse,
  ForceDlqResponse,
  EventHistoryResponse,
  FireRuleRequest,
  FireRuleResponse,
  S3NotificationsResponse,
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
  StepFunctionsExecutionsResponse,
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
}

export class ApiGatewayV2Client {
  constructor(private readonly baseUrl: string) {}

  async getRequests(): Promise<ApiGatewayV2RequestsResponse> {
    const resp = await fetch(`${this.baseUrl}/_fakecloud/apigatewayv2/requests`);
    return parse(resp);
  }
}

export class StepFunctionsClient {
  constructor(private readonly baseUrl: string) {}

  async getExecutions(): Promise<StepFunctionsExecutionsResponse> {
    const resp = await fetch(`${this.baseUrl}/_fakecloud/stepfunctions/executions`);
    return parse(resp);
  }
}

// ── Main client ────────────────────────────────────────────────────

export class FakeCloud {
  private readonly baseUrl: string;

  private readonly _lambda: LambdaClient;
  private readonly _rds: RdsClient;
  private readonly _elasticache: ElastiCacheClient;
  private readonly _ses: SesClient;
  private readonly _sns: SnsClient;
  private readonly _sqs: SqsClient;
  private readonly _events: EventsClient;
  private readonly _s3: S3Client;
  private readonly _dynamodb: DynamoDbClient;
  private readonly _secretsmanager: SecretsManagerClient;
  private readonly _cognito: CognitoClient;
  private readonly _apigatewayv2: ApiGatewayV2Client;
  private readonly _stepfunctions: StepFunctionsClient;

  constructor(baseUrl: string = "http://localhost:4566") {
    this.baseUrl = baseUrl.replace(/\/+$/, "");

    this._lambda = new LambdaClient(this.baseUrl);
    this._rds = new RdsClient(this.baseUrl);
    this._elasticache = new ElastiCacheClient(this.baseUrl);
    this._ses = new SesClient(this.baseUrl);
    this._sns = new SnsClient(this.baseUrl);
    this._sqs = new SqsClient(this.baseUrl);
    this._events = new EventsClient(this.baseUrl);
    this._s3 = new S3Client(this.baseUrl);
    this._dynamodb = new DynamoDbClient(this.baseUrl);
    this._secretsmanager = new SecretsManagerClient(this.baseUrl);
    this._cognito = new CognitoClient(this.baseUrl);
    this._apigatewayv2 = new ApiGatewayV2Client(this.baseUrl);
    this._stepfunctions = new StepFunctionsClient(this.baseUrl);
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
}
