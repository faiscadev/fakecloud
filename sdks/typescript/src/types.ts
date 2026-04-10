// ── Health & Reset ─────────────────────────────────────────────────

export interface HealthResponse {
  status: string;
  version: string;
  services: string[];
}

export interface ResetResponse {
  status: string;
}

export interface ResetServiceResponse {
  reset: string;
}

// ── RDS ────────────────────────────────────────────────────────────

export interface RdsTag {
  key: string;
  value: string;
}

export interface RdsInstance {
  dbInstanceIdentifier: string;
  dbInstanceArn: string;
  dbInstanceClass: string;
  engine: string;
  engineVersion: string;
  dbInstanceStatus: string;
  masterUsername: string;
  dbName: string | null;
  endpointAddress: string;
  port: number;
  allocatedStorage: number;
  publiclyAccessible: boolean;
  deletionProtection: boolean;
  createdAt: string;
  dbiResourceId: string;
  containerId: string;
  hostPort: number;
  tags: RdsTag[];
}

export interface RdsInstancesResponse {
  instances: RdsInstance[];
}

// ── Lambda ─────────────────────────────────────────────────────────

export interface LambdaInvocation {
  functionArn: string;
  payload: string;
  source: string;
  timestamp: string;
}

export interface LambdaInvocationsResponse {
  invocations: LambdaInvocation[];
}

export interface WarmContainer {
  functionName: string;
  runtime: string;
  containerId: string;
  lastUsedSecsAgo: number;
}

export interface WarmContainersResponse {
  containers: WarmContainer[];
}

export interface EvictContainerResponse {
  evicted: boolean;
}

// ── SES ────────────────────────────────────────────────────────────

export interface SentEmail {
  messageId: string;
  from: string;
  to: string[];
  cc: string[];
  bcc: string[];
  subject: string | null;
  htmlBody: string | null;
  textBody: string | null;
  rawData: string | null;
  templateName: string | null;
  templateData: string | null;
  timestamp: string;
}

export interface SesEmailsResponse {
  emails: SentEmail[];
}

export interface InboundEmailRequest {
  from: string;
  to: string[];
  subject: string;
  body: string;
}

export interface InboundActionExecuted {
  rule: string;
  actionType: string;
}

export interface InboundEmailResponse {
  messageId: string;
  matchedRules: string[];
  actionsExecuted: InboundActionExecuted[];
}

// ── SNS ────────────────────────────────────────────────────────────

export interface SnsMessage {
  messageId: string;
  topicArn: string;
  message: string;
  subject: string | null;
  timestamp: string;
}

export interface SnsMessagesResponse {
  messages: SnsMessage[];
}

export interface PendingConfirmation {
  subscriptionArn: string;
  topicArn: string;
  protocol: string;
  endpoint: string;
  token: string | null;
}

export interface PendingConfirmationsResponse {
  pendingConfirmations: PendingConfirmation[];
}

export interface ConfirmSubscriptionRequest {
  subscriptionArn: string;
}

export interface ConfirmSubscriptionResponse {
  confirmed: boolean;
}

// ── SQS ────────────────────────────────────────────────────────────

export interface SqsMessageInfo {
  messageId: string;
  body: string;
  receiveCount: number;
  inFlight: boolean;
  createdAt: string;
}

export interface SqsQueueMessages {
  queueUrl: string;
  queueName: string;
  messages: SqsMessageInfo[];
}

export interface SqsMessagesResponse {
  queues: SqsQueueMessages[];
}

export interface ExpirationTickResponse {
  expiredMessages: number;
}

export interface ForceDlqResponse {
  movedMessages: number;
}

// ── EventBridge ────────────────────────────────────────────────────

export interface EventBridgeEvent {
  eventId: string;
  source: string;
  detailType: string;
  detail: string;
  busName: string;
  timestamp: string;
}

export interface EventBridgeLambdaDelivery {
  functionArn: string;
  payload: string;
  timestamp: string;
}

export interface EventBridgeLogDelivery {
  logGroupArn: string;
  payload: string;
  timestamp: string;
}

export interface EventBridgeDeliveries {
  lambda: EventBridgeLambdaDelivery[];
  logs: EventBridgeLogDelivery[];
}

export interface EventHistoryResponse {
  events: EventBridgeEvent[];
  deliveries: EventBridgeDeliveries;
}

export interface FireRuleRequest {
  busName?: string;
  ruleName: string;
}

export interface FireRuleTarget {
  type: string;
  arn: string;
}

export interface FireRuleResponse {
  targets: FireRuleTarget[];
}

// ── S3 ─────────────────────────────────────────────────────────────

export interface S3Notification {
  bucket: string;
  key: string;
  eventType: string;
  timestamp: string;
}

export interface S3NotificationsResponse {
  notifications: S3Notification[];
}

export interface LifecycleTickResponse {
  processedBuckets: number;
  expiredObjects: number;
  transitionedObjects: number;
}

// ── DynamoDB ───────────────────────────────────────────────────────

export interface TtlTickResponse {
  expiredItems: number;
}

// ── SecretsManager ─────────────────────────────────────────────────

export interface RotationTickResponse {
  rotatedSecrets: string[];
}

// ── Cognito ────────────────────────────────────────────────────────

export interface UserConfirmationCodes {
  confirmationCode: string | null;
  attributeVerificationCodes: Record<string, unknown>;
}

export interface ConfirmationCode {
  poolId: string;
  username: string;
  code: string;
  type: string;
  attribute?: string;
}

export interface ConfirmationCodesResponse {
  codes: ConfirmationCode[];
}

export interface ConfirmUserRequest {
  userPoolId: string;
  username: string;
}

export interface ConfirmUserResponse {
  confirmed: boolean;
  error?: string;
}

export interface TokenInfo {
  type: string;
  username: string;
  poolId: string;
  clientId: string;
  issuedAt: number;
}

export interface TokensResponse {
  tokens: TokenInfo[];
}

export interface ExpireTokensRequest {
  userPoolId?: string;
  username?: string;
}

export interface ExpireTokensResponse {
  expiredTokens: number;
}

export interface AuthEvent {
  eventType: string;
  username: string;
  userPoolId: string;
  clientId: string | null;
  timestamp: number;
  success: boolean;
}

export interface AuthEventsResponse {
  events: AuthEvent[];
}
