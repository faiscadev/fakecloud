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

// ── EC2 ────────────────────────────────────────────────────────────

export interface Ec2Instance {
  instanceId: string;
  imageId: string;
  instanceType: string;
  state: string;
  privateIp: string;
  publicIp: string | null;
  subnetId: string | null;
  vpcId: string | null;
  keyName: string | null;
  securityGroupIds: string[];
  availabilityZone: string;
  launchTime: string;
  /** Backing Docker container id or Kubernetes Pod name; null when metadata-only. */
  containerId: string | null;
}

export interface Ec2InstancesResponse {
  instances: Ec2Instance[];
}

/**
 * The real backing network of an EC2 instance — which Docker/Podman network or
 * k8s NetworkPolicy backs it, its container IP, and whether security-group
 * enforcement is active or degraded. A debugging aid for "why can't X reach Y"
 * (issue #1745).
 */
export interface Ec2InstanceNetwork {
  instanceId: string;
  vpcId: string | null;
  subnetId: string | null;
  privateIp: string;
  /** Docker/Podman network (`fakecloud-subnet-<id>`) or k8s NetworkPolicy
   * (`fakecloud-ec2-<id>`) backing the instance; null when metadata-only or not
   * yet running. */
  backingNetwork: string | null;
  /** "docker" | "podman" | "kubernetes" | "none". */
  isolationBackend: string;
  /** "nftables" | "networkpolicy" | "disabled". */
  securityGroupEnforcement: string;
  /** Whether security-group rules are actually enforced (vs tracked-only). */
  enforcementActive: boolean;
}

export interface Ec2InstanceNetworksResponse {
  instanceNetworks: Ec2InstanceNetwork[];
}

// ── ElastiCache ────────────────────────────────────────────────────

export interface ElastiCacheCluster {
  cacheClusterId: string;
  cacheClusterStatus: string;
  engine: string;
  engineVersion: string;
  cacheNodeType: string;
  numCacheNodes: number;
  replicationGroupId: string | null;
  port: number | null;
  hostPort: number | null;
  containerId: string | null;
}

export interface ElastiCacheClustersResponse {
  clusters: ElastiCacheCluster[];
}

export interface ElastiCacheReplicationGroupIntrospection {
  replicationGroupId: string;
  status: string;
  description: string;
  memberClusters: string[];
  automaticFailover: boolean;
  multiAz: boolean;
  engine: string;
  engineVersion: string;
  cacheNodeType: string;
  numCacheClusters: number;
}

export interface ElastiCacheReplicationGroupsResponse {
  replicationGroups: ElastiCacheReplicationGroupIntrospection[];
}

export interface ElastiCacheServerlessCacheIntrospection {
  serverlessCacheName: string;
  status: string;
  engine: string;
  engineVersion: string;
  cacheNodeType: string | null;
}

export interface ElastiCacheServerlessCachesResponse {
  serverlessCaches: ElastiCacheServerlessCacheIntrospection[];
}

export interface ElastiCacheAclUser {
  name: string;
  status: string;
  accessString: string;
  noPasswordRequired: boolean;
  passwordCount: number;
}

export interface ElastiCacheAclGroup {
  name: string;
  members: string[];
}

export interface ElastiCacheAclCluster {
  clusterId: string;
  engine: string;
  users: ElastiCacheAclUser[];
  groups: ElastiCacheAclGroup[];
}

export interface ElastiCacheAclsResponse {
  acls: ElastiCacheAclCluster[];
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
  /** DKIM-Signature header value when signing was active for this send. */
  dkimSignature?: string | null;
  /** Synthesized RFC 5322 headers (DKIM-Signature first when signing). */
  headers?: [string, string][];
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

export interface SesMetrics {
  suppressedDropsTotal: number;
}

export type SesMailFromStatus = "NotStarted" | "Pending" | "Success" | "Failed";

export interface SesMailFromStatusResponse {
  identity: string;
  mailFromDomainStatus: SesMailFromStatus;
}

export interface SesDkimPublicKey {
  identity: string;
  selector: string;
  publicKeyBase64: string;
  signingEnabled: boolean;
}

export interface SesSandboxResponse {
  sandbox: boolean;
  productionAccessEnabled: boolean;
}

export interface SesBouncedRecipientInfo {
  recipient: string;
  bounceType: string;
  action: string;
  status: string;
  diagnosticCode: string;
}

export interface SesBounce {
  messageId: string;
  bounceType: string;
  bounceSubType: string;
  bouncedRecipientInfo: SesBouncedRecipientInfo[];
  explanation: string | null;
  timestamp: string;
  originalMessageId: string;
  bounceSender: string;
}

export interface SesBouncesResponse {
  bounces: SesBounce[];
}

export interface SesMessageInsightEvent {
  destination: string;
  timestamp: string;
  bounceType?: string | null;
  bounceSubType?: string | null;
  diagnosticCode?: string | null;
  complaintFeedbackType?: string | null;
}

export interface SesMessageInsightsResponse {
  messageId: string;
  sends: SesMessageInsightEvent[];
  deliveries: SesMessageInsightEvent[];
  opens: SesMessageInsightEvent[];
  clicks: SesMessageInsightEvent[];
  bounces: SesMessageInsightEvent[];
  complaints: SesMessageInsightEvent[];
  rejects: SesMessageInsightEvent[];
}

export interface SesSmtpSubmission {
  messageId: string;
  from: string;
  to: string[];
  subject: string | null;
  rawSizeBytes: number;
  receivedAt: string;
  authUser: string;
}

export interface SesSmtpSubmissionsResponse {
  submissions: SesSmtpSubmission[];
}

export interface SesEventDestinationDelivery {
  destinationName: string;
  destinationType: string;
  eventType: string;
  messageId: string;
  dispatchedAt: string;
  targetArn: string;
}

export interface SesEventDestinationDeliveriesResponse {
  deliveries: SesEventDestinationDelivery[];
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

/**
 * Result of forcing one Application Auto Scaling watcher tick. The
 * `applied` field is the number of policies that successfully changed
 * a target's capacity on this tick.
 */
export interface AppAsTickResponse {
  applied: number;
}

/**
 * Result of forcing one Application Auto Scaling scheduled-action
 * tick. The `fired` field is the number of scheduled actions that
 * successfully fired on this tick.
 */
export interface AppAsScheduledTickResponse {
  fired: number;
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

export interface S3AccessPointEntry {
  name: string;
  alias: string;
  bucket: string;
  accountId: string;
  networkOrigin: string;
  vpcConfiguration?: string;
  publicAccessBlock?: string;
  createdAt: string;
}

export interface S3AccessPointsResponse {
  accessPoints: S3AccessPointEntry[];
}

export interface S3ObjectLambdaResponse {
  requestToken: string;
  requestRoute: string;
  statusCode?: number;
  bodyBase64: string;
  bodySize: number;
  contentType?: string;
  errorMessage?: string;
  metadata: Record<string, string>;
}

export interface S3ObjectLambdaResponsesResponse {
  responses: S3ObjectLambdaResponse[];
}

// ── DynamoDB ───────────────────────────────────────────────────────

export interface TtlTickResponse {
  expiredItems: number;
}

export interface DynamoDbSnapshotSaveRequest {
  dataPath?: string;
}

export interface DynamoDbSnapshotSaveResponse {
  saved: boolean;
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

/**
 * One PreTokenGeneration Lambda trigger invocation captured by
 * `InitiateAuth`. `claimsAdded` / `claimsOverridden` / `groupOverrides`
 * are pre-parsed from the Lambda response so tests don't have to walk
 * the raw `claimsAndScopeOverrideDetails` shape themselves.
 */
export interface PreTokenGenInvocation {
  poolId: string;
  userPoolArn: string;
  username: string;
  triggerSource: string;
  lambdaArn: string;
  requestPayload: Record<string, unknown>;
  responsePayload?: Record<string, unknown> | null;
  claimsAdded: string[];
  claimsOverridden: string[];
  groupOverrides: string[];
  /** RFC3339 timestamp. */
  invokedAt: string;
  durationMs: number;
}

export interface PreTokenGenInvocationsResponse {
  invocations: PreTokenGenInvocation[];
}

/**
 * Payload for `POST /_fakecloud/cognito/authorization-codes`. Lets
 * test harnesses pre-allocate a single-use OAuth2 authorization code
 * that the `/oauth2/token` `authorization_code` grant later consumes.
 */
export interface MintAuthorizationCodeRequest {
  userPoolId: string;
  clientId: string;
  username: string;
  redirectUri: string;
  scopes?: string[];
  codeChallenge?: string;
  codeChallengeMethod?: string;
  nonce?: string;
}

export interface MintAuthorizationCodeResponse {
  code: string;
}

/**
 * Payload for `POST /_fakecloud/cognito/compromised-passwords`. Each
 * plaintext is SHA-256 hashed server-side and added to the
 * compromised-password set; subsequent `SignUp` / `AdminInitiateAuth`
 * fail with `InvalidPasswordException` on pools whose
 * `CompromisedCredentialsRiskConfiguration.Actions.EventAction` is
 * `BLOCK`.
 */
export interface CompromisedPasswordsRequest {
  passwords: string[];
}

export interface CompromisedPasswordsResponse {
  added: number;
}

/**
 * Registered WebAuthn credential from
 * `GET /_fakecloud/cognito/webauthn-credentials`. `attestationInfo` is
 * the parsed-attestation JSON (packed format details, AAGUID,
 * certificate chain summary, signature counter); its shape depends on
 * the attestation format so it is left untyped.
 */
export interface WebAuthnCredential {
  account_id: string;
  pool_user: string;
  credential_id: string;
  relying_party_id: string;
  attestation_info: unknown;
}

export interface WebAuthnCredentialsResponse {
  credentials: WebAuthnCredential[];
}

// ── Step Functions ────────────────────────────────────────────────

export interface StepFunctionsExecution {
  executionArn: string;
  stateMachineArn: string;
  name: string;
  status: string;
  startDate: string;
  input?: string | null;
  output?: string | null;
  stopDate?: string | null;
}

export interface StepFunctionsExecutionsResponse {
  executions: StepFunctionsExecution[];
}

export interface StepFunctionsSyncBillingDetails {
  billedDurationInMilliseconds: number;
  billedMemoryUsedInMb: number;
}

export interface StepFunctionsSyncExecution {
  executionArn: string;
  stateMachineArn: string;
  name: string;
  status: string;
  input?: string | null;
  output?: string | null;
  startedAt: string;
  stoppedAt?: string | null;
  durationMs: number;
  billingDetails: StepFunctionsSyncBillingDetails;
}

export interface StepFunctionsSyncExecutionsResponse {
  executions: StepFunctionsSyncExecution[];
}

export interface StepFunctionsExecutionTreeNode {
  arn: string;
  stateMachineArn: string;
  status: string;
  startedAt: string;
  stoppedAt?: string | null;
  children: StepFunctionsExecutionTreeNode[];
}

export interface StepFunctionsExecutionTreeResponse {
  rootArn: string;
  tree: StepFunctionsExecutionTreeNode;
}

export interface SfnEnqueueActivityTaskRequest {
  activityArn: string;
  input?: string;
  heartbeatSeconds?: number;
  timeoutSeconds?: number;
}

export interface SfnEnqueueActivityTaskResponse {
  taskToken: string;
}

// ── Bedrock ───────────────────────────────────────────────────────

export interface BedrockInvocation {
  modelId: string;
  input: string;
  output: string;
  timestamp: string;
  /** Error detail for faulted calls, or null on success. */
  error: string | null;
}

export interface BedrockInvocationsResponse {
  invocations: BedrockInvocation[];
}

export interface BedrockModelResponseConfig {
  status: string;
  modelId: string;
}

export interface BedrockResponseRule {
  /** Substring that must appear in the prompt for this rule to match. Null/empty matches any prompt. */
  promptContains?: string | null;
  response: string;
}

export interface BedrockFaultRule {
  errorType: string;
  message?: string;
  httpStatus?: number;
  /** Number of calls this rule will fault before being consumed. Defaults to 1 server-side. */
  count?: number;
  /** Optional model filter. */
  modelId?: string;
  /** Optional operation filter: "InvokeModel" | "Converse" | "InvokeModelWithResponseStream" | "ConverseStream". */
  operation?: string;
}

export interface BedrockFaultRuleState {
  errorType: string;
  message: string;
  httpStatus: number;
  remaining: number;
  modelId: string | null;
  operation: string | null;
}

export interface BedrockFaultsResponse {
  faults: BedrockFaultRuleState[];
}

export interface BedrockStatusResponse {
  status: string;
}

// ── Bedrock Agent (control plane) ─────────────────────────────────────

export interface BedrockAgentAliasSummary {
  aliasId: string;
  aliasName: string;
  agentVersion: string;
  aliasArn: string;
  status: string;
  createdAt: string;
  updatedAt: string;
}

export interface BedrockAgentVersionSummary {
  agentVersion: string;
  createdAt: string;
  instruction: string | null;
  foundationModel: string | null;
}

export interface BedrockAgentKnowledgeBaseSummary {
  knowledgeBaseId: string;
  state: string;
  description: string | null;
}

export interface BedrockAgentCollaboratorSummary {
  collaboratorId: string;
  collaboratorName: string;
  collaboratorAliasArn: string;
  relayConversationHistory: string;
}

export interface BedrockAgentRow {
  agentId: string;
  agentName: string;
  agentArn: string;
  agentStatus: string;
  foundationModel: string | null;
  instruction: string | null;
  knowledgeBases: BedrockAgentKnowledgeBaseSummary[];
  actionGroups: unknown[];
  collaborators: BedrockAgentCollaboratorSummary[];
  aliases: BedrockAgentAliasSummary[];
  versions: BedrockAgentVersionSummary[];
  promptOverrides: unknown;
  createdAt: string;
  updatedAt: string;
}

export interface BedrockAgentAgentsResponse {
  agents: BedrockAgentRow[];
}

// ── Bedrock Agent Runtime (data plane) ────────────────────────────────

export interface BedrockAgentRuntimeInvocation {
  invocationId: string;
  /** invoke_agent | invoke_inline_agent | invoke_flow | retrieve | retrieve_and_generate | create_invocation */
  op: string;
  agentId: string | null;
  flowId: string | null;
  sessionId: string | null;
  input: string;
  output: string;
  outputChunks: number;
  trace: unknown;
  citations: unknown[];
  invokedAt: string;
  durationMs: number;
}

export interface BedrockAgentRuntimeInvocationsResponse {
  invocations: BedrockAgentRuntimeInvocation[];
}

// ── IAM ───────────────────────────────────────────────────────────────

export interface CreateAdminRequest {
  accountId: string;
  userName: string;
}

export interface CreateAdminResponse {
  accessKeyId: string;
  secretAccessKey: string;
  accountId: string;
  arn: string;
}

// ── API Gateway v2 ──────────────────────────────────────────────────

export interface ApiGatewayV2Request {
  requestId: string;
  apiId: string;
  stage: string;
  method: string;
  path: string;
  headers: Record<string, string>;
  queryParams: Record<string, string>;
  body?: string | null;
  timestamp: string;
  statusCode: number;
}

export interface ApiGatewayV2RequestsResponse {
  requests: ApiGatewayV2Request[];
}

// ── Glue ────────────────────────────────────────────────────────────

export interface GlueJob {
  accountId: string;
  name: string;
  role: string;
  command: unknown;
  defaultArguments: Record<string, string>;
  maxCapacity?: number | null;
  maxRetries: number;
  timeout?: number | null;
  glueVersion?: string | null;
  workerType?: string | null;
  numberOfWorkers?: number | null;
  createdOn: string;
  lastModifiedOn: string;
}

export interface GlueJobsResponse {
  jobs: GlueJob[];
}

export interface GlueJobRun {
  accountId: string;
  id: string;
  jobName: string;
  attempt: number;
  startedOn: string;
  completedOn?: string | null;
  jobRunState: string;
  arguments: Record<string, string>;
  errorMessage?: string | null;
  executionTime: number;
}

export interface GlueJobRunsResponse {
  runs: GlueJobRun[];
}

export interface GlueCrawler {
  accountId: string;
  name: string;
  role: string;
  databaseName?: string | null;
  state: string;
  targetSummary: string;
  schedule?: string | null;
  creationTime: string;
  lastUpdated: string;
}

export interface GlueCrawlersResponse {
  crawlers: GlueCrawler[];
}

// ── CloudWatch ──────────────────────────────────────────────────────

export interface CloudWatchDimension {
  name: string;
  value: string;
}

export interface CloudWatchAlarm {
  accountId: string;
  region: string;
  name: string;
  type: string;
  state: string;
  stateReason: string;
  stateUpdatedTimestamp?: string | null;
  actionsEnabled: boolean;
  alarmActions: string[];
  okActions: string[];
  insufficientDataActions: string[];
  namespace?: string | null;
  metricName?: string | null;
  threshold?: number | null;
  comparisonOperator?: string | null;
  alarmRule?: string | null;
}

export interface CloudWatchAlarmsResponse {
  alarms: CloudWatchAlarm[];
}

export interface CloudWatchLatestDatapoint {
  timestamp: string;
  value?: number | null;
  unit?: string | null;
}

export interface CloudWatchMetric {
  accountId: string;
  region: string;
  namespace: string;
  metricName: string;
  dimensions: CloudWatchDimension[];
  datapointCount: number;
  latest: CloudWatchLatestDatapoint | null;
}

export interface CloudWatchMetricsResponse {
  metrics: CloudWatchMetric[];
}

// ── Firehose ────────────────────────────────────────────────────────

export interface FirehoseEncryption {
  status: string;
  keyType?: string | null;
  keyArn?: string | null;
}

export interface FirehoseDeliveryStream {
  accountId: string;
  name: string;
  arn: string;
  streamType: string;
  status: string;
  encryption: FirehoseEncryption;
  destinationCount: number;
  createTimestamp: string;
  lastUpdateTimestamp?: string | null;
}

export interface FirehoseDeliveryStreamsResponse {
  deliveryStreams: FirehoseDeliveryStream[];
}

// ── Scheduler (EventBridge Scheduler) ───────────────────────────────

export interface SchedulerSchedule {
  accountId: string;
  groupName: string;
  name: string;
  arn: string;
  state: string;
  scheduleExpression: string;
  targetArn: string;
  lastFired?: string | null;
}

export interface SchedulerSchedulesResponse {
  schedules: SchedulerSchedule[];
}

export interface FireScheduleResponse {
  scheduleArn: string;
  targetArn: string;
}

// ── ECR ────────────────────────────────────────────────────────────

export interface EcrTag {
  key: string;
  value: string;
}

export interface EcrRepository {
  repositoryName: string;
  repositoryArn: string;
  registryId: string;
  repositoryUri: string;
  imageTagMutability: string;
  scanOnPush: boolean;
  createdAt: string;
  tags: EcrTag[];
  hasPolicy: boolean;
  hasLifecyclePolicy: boolean;
  imageCount: number;
  layerCount: number;
}

export interface EcrRepositoriesResponse {
  repositories: EcrRepository[];
}

export interface EcrImage {
  repositoryName: string;
  imageDigest: string;
  imageTags: string[];
  imageSizeInBytes: number;
  imageManifestMediaType: string;
  imagePushedAt: string;
}

export interface EcrImagesResponse {
  images: EcrImage[];
}

export interface EcrPullThroughRule {
  ecrRepositoryPrefix: string;
  upstreamRegistryUrl: string;
  upstreamRegistry?: string | null;
  credentialArn?: string | null;
  customRoleArn?: string | null;
  createdAt: string;
  updatedAt: string;
}

export interface EcrPullThroughRulesResponse {
  rules: EcrPullThroughRule[];
}

// ── ECS ─────────────────────────────────────────────────────────────

export interface EcsTag {
  key: string;
  value: string;
}

export interface EcsCluster {
  clusterName: string;
  clusterArn: string;
  status: string;
  runningTasksCount: number;
  pendingTasksCount: number;
  activeServicesCount: number;
  registeredContainerInstancesCount: number;
  capacityProviders: string[];
  tags: EcsTag[];
  createdAt: string;
}

export interface EcsClustersResponse {
  clusters: EcsCluster[];
}

export interface EcsTaskContainer {
  name: string;
  image: string;
  lastStatus: string;
  exitCode?: number | null;
  runtimeId?: string | null;
  essential: boolean;
}

export interface EcsTask {
  taskArn: string;
  taskId: string;
  clusterArn: string;
  clusterName: string;
  taskDefinitionArn: string;
  family: string;
  revision: number;
  lastStatus: string;
  desiredStatus: string;
  launchType: string;
  createdAt: string;
  startedAt?: string | null;
  stoppingAt?: string | null;
  stoppedAt?: string | null;
  stopCode?: string | null;
  stoppedReason?: string | null;
  containers: EcsTaskContainer[];
  capturedLogBytes: number;
}

export interface EcsTasksResponse {
  tasks: EcsTask[];
}

export interface EcsTaskLogsResponse {
  taskArn: string;
  logs: string;
  lastStatus: string;
  exitCode?: number | null;
}

export interface EcsMarkFailedRequest {
  exitCode?: number | null;
  reason?: string | null;
}

export interface EcsLifecycleEvent {
  at: string;
  eventType: string;
  taskArn?: string | null;
  clusterArn?: string | null;
  lastStatus?: string | null;
  detail: unknown;
}

export interface EcsEventsResponse {
  events: EcsLifecycleEvent[];
}

export interface EcsTaskMetadataLimits {
  cpu?: number;
  memory?: number;
}

export interface EcsTaskMetadataPort {
  containerPort?: number;
  hostPort?: number;
  protocol?: string;
}

export interface EcsTaskMetadataContainer {
  name: string;
  image: string;
  imageId?: string;
  ports: EcsTaskMetadataPort[];
  labels: Record<string, string>;
  desiredStatus: string;
  knownStatus: string;
  limits: EcsTaskMetadataLimits;
  createdAt?: string;
  startedAt?: string;
  exitCode?: number;
}

export interface EcsTaskMetadata {
  cluster: string;
  taskArn: string;
  family: string;
  revision: number;
  desiredStatus: string;
  knownStatus: string;
  containers: EcsTaskMetadataContainer[];
  pullStartedAt?: string;
  pullStoppedAt?: string;
  availabilityZone: string;
  launchType: string;
  vpcId?: string;
  eniId?: string;
}

export interface EcsTaskMetadataResponse {
  task: EcsTaskMetadata;
}

// ── ELBv2 ───────────────────────────────────────────────────────────

export interface Elbv2Tag {
  key: string;
  value: string;
}

export interface Elbv2AvailabilityZone {
  zoneName: string;
  subnetId: string;
}

export interface Elbv2LoadBalancer {
  arn: string;
  name: string;
  dnsName: string;
  scheme: string;
  vpcId: string;
  stateCode: string;
  stateReason?: string | null;
  lbType: string;
  ipAddressType: string;
  availabilityZones: Elbv2AvailabilityZone[];
  securityGroups: string[];
  createdTime: string;
  tags: Elbv2Tag[];
}

export interface Elbv2LoadBalancersResponse {
  loadBalancers: Elbv2LoadBalancer[];
}

export interface Elbv2Target {
  id: string;
  port?: number | null;
  availabilityZone?: string | null;
  healthState: string;
  healthReason?: string | null;
  healthDescription?: string | null;
}

export interface Elbv2TargetGroup {
  arn: string;
  name: string;
  protocol?: string | null;
  port?: number | null;
  vpcId?: string | null;
  targetType: string;
  loadBalancerArns: string[];
  targets: Elbv2Target[];
  healthCheckProtocol?: string | null;
  healthCheckPort?: string | null;
  healthCheckPath?: string | null;
  healthyThresholdCount: number;
  unhealthyThresholdCount: number;
  createdTime: string;
  tags: Elbv2Tag[];
}

export interface Elbv2TargetGroupsResponse {
  targetGroups: Elbv2TargetGroup[];
}

export interface Elbv2Listener {
  arn: string;
  loadBalancerArn: string;
  port?: number | null;
  protocol?: string | null;
  sslPolicy?: string | null;
  certificateArns: string[];
  defaultActionType?: string | null;
  defaultTargetGroupArn?: string | null;
}

export interface Elbv2ListenersResponse {
  listeners: Elbv2Listener[];
}

export interface Elbv2Rule {
  arn: string;
  listenerArn: string;
  priority: string;
  isDefault: boolean;
  conditionFields: string[];
  actionType?: string | null;
}

export interface Elbv2RulesResponse {
  rules: Elbv2Rule[];
}

// ── CloudWatch Logs ────────────────────────────────────────────────

/**
 * Admin payload for `/_fakecloud/logs/anomalies/inject`. Lets tests
 * seed synthetic CloudWatch Logs anomalies so they can exercise
 * `ListAnomalies`/`UpdateAnomaly` deterministically.
 */
export interface LogsAnomalyInjectRequest {
  anomalyDetectorArn: string;
  patternString: string;
  logGroupArns?: string[];
  priority?: string;
}

export interface LogsAnomalyInjectResponse {
  anomalyId: string;
}

/** One entry of `/_fakecloud/logs/delivery-config`. Joins a Delivery
 * with the `logType` from its DeliverySource for assertion-friendly
 * test shape. */
export interface LogsDeliveryConfiguration {
  id: string;
  /** Mirrors `id`; AWS deliveries are referenced by ID, not name. */
  name: string;
  deliveryDestinationArn: string;
  deliverySourceName: string;
  logType: string;
  recordFields?: string[];
  fieldDelimiter?: string;
  s3DeliveryConfiguration?: unknown;
  /** Unix-ms timestamp of CreateDelivery. */
  createdAt: number;
}

export interface LogsDeliveryConfigResponse {
  configurations: LogsDeliveryConfiguration[];
}

/** One parsed IndexPolicy on a log group. */
export interface LogsFieldIndex {
  fields: string[];
  /** Unix-ms when the policy was created (mirrors `last_updated_time`). */
  createdAt: number;
  lastUsedAt: number;
}

export interface LogsFieldIndexesResponse {
  logGroupName: string;
  indexes: LogsFieldIndex[];
}

// ── Organizations ───────────────────────────────────────────────────

export interface OrganizationsTag {
  key: string;
  value: string;
}

export interface OrganizationsAccount {
  id: string;
  arn: string;
  email: string;
  name: string;
  /** AWS lifecycle state: ACTIVE | SUSPENDED | PENDING_CLOSURE. */
  status: string;
  /** How the account joined the org: INVITED | CREATED. */
  joinedMethod: string;
  /** RFC3339. */
  joinedTimestamp: string;
  parentOuId?: string;
  tags: OrganizationsTag[];
  /** SCP ids directly attached to the account. Does not include
   *  policies inherited from the parent OU or root. */
  scpAttached: string[];
}

export interface OrganizationsAccountsResponse {
  accounts: OrganizationsAccount[];
  /** `null`/undefined when no organization has been created yet. */
  managementAccountId?: string;
  /** Duplicate of `managementAccountId`. AWS renamed Master to
   *  Management in 2020 but kept the old field for back-compat. */
  masterAccountId?: string;
}

export interface OrganizationsResponsibilityTransfer {
  id: string;
  arn: string;
  name: string;
  type: string;
  status: string;
  /** INBOUND | OUTBOUND. */
  direction: string;
  sourceManagementAccountId: string;
  sourceManagementAccountEmail: string;
  targetManagementAccountId: string;
  targetManagementAccountEmail: string;
  /** RFC3339. */
  startTimestamp: string;
  /** RFC3339, or `null`/undefined while the transfer is still open. */
  endTimestamp?: string | null;
  activeHandshakeId?: string | null;
}

export interface OrganizationsResponsibilityTransfersResponse {
  responsibilityTransfers: OrganizationsResponsibilityTransfer[];
}

// ── Athena ─────────────────────────────────────────────────────────

/**
 * One row in the Athena named-query introspection listing returned by
 * `GET /_fakecloud/athena/named-queries`. Mirrors the underlying named
 * query record plus a `lastUsedAt` timestamp the server bumps every
 * time `StartQueryExecution` resolves the query by id.
 */
export interface AthenaNamedQuery {
  namedQueryId: string;
  name: string;
  description?: string | null;
  database: string;
  queryString: string;
  workgroup: string;
  /**
   * RFC3339 timestamp of the most recent `StartQueryExecution` that
   * resolved its query string from this named query. `null` until the
   * first such invocation.
   */
  lastUsedAt?: string | null;
}

export interface AthenaNamedQueriesResponse {
  queries: AthenaNamedQuery[];
}

// ── API Gateway v2 WebSocket connections ───────────────────────────

export interface ApiGatewayV2Connection {
  connectionId: string;
  apiId: string;
  stage: string;
  connectedAt: string;
  lastActiveAt: string;
  sourceIp: string;
}

export interface ApiGatewayV2ConnectionsResponse {
  connections: ApiGatewayV2Connection[];
}

// ── RDS aws_lambda / aws_s3 extension bridge ───────────────────────

export interface RdsLambdaInvokeRequest {
  function_name: string;
  payload?: unknown;
  invocation_type?: string;
  region?: string;
}

export interface RdsLambdaInvokeResponse {
  status_code: number;
  payload?: unknown;
  executed_version?: string;
  log_result?: string;
}

export interface RdsS3ImportRequest {
  bucket: string;
  key: string;
  region?: string;
}

export interface RdsS3ImportResponse {
  bucket: string;
  key: string;
  body_b64: string;
  bytes_processed: number;
}

export interface RdsS3ExportRequest {
  bucket: string;
  key: string;
  region?: string;
  body_b64: string;
}

export interface RdsS3ExportResponse {
  bucket: string;
  key: string;
  bytes_uploaded: number;
}

// ── Route 53 DNSSEC introspection ──────────────────────────────────

export interface Route53DnssecMaterialResponse {
  hostedZoneId: string;
  keySigningKeyName: string;
  algorithm: number;
  flags: number;
  keyTag: number;
  dnskeyPublicKeyB64: string;
  dsDigestSha256Hex: string;
}

export interface Route53DnssecSignRequest {
  name: string;
  type: string;
  ttl: number;
  rdatas: string[];
}

export interface Route53DnssecSignResponse {
  signatureB64: string;
  algorithm: number;
  keyTag: number;
  signerName: string;
  inception: number;
  expiration: number;
  labels: number;
  originalTtl: number;
  type: string;
}

// ── SNS SMS introspection ──────────────────────────────────────────

export interface SnsSmsMessage {
  phoneNumber: string;
  message: string;
}

export interface SnsSmsResponse {
  messages: SnsSmsMessage[];
}

// ── ECS task credentials & metadata pass-through ───────────────────

export interface EcsTaskCredentials {
  AccessKeyId: string;
  SecretAccessKey: string;
  Token: string;
  Expiration: string;
  RoleArn: string;
}

// ── SSM ────────────────────────────────────────────────────────────

/** Body for `POST /_fakecloud/ssm/commands/{commandId}/status`. */
export interface SetSsmCommandStatusRequest {
  accountId?: string;
  /** New status value, e.g. `Success`, `Failed`, `InProgress`. */
  status: string;
}

export interface SetSsmCommandStatusResponse {
  updated: boolean;
}

/**
 * Body for `POST /_fakecloud/ssm/commands/{commandId}/fail`. All
 * fields are optional. When `instanceId` is omitted, every invocation
 * on the command is flipped to `Failed`.
 */
export interface FailSsmCommandRequest {
  accountId?: string;
  /** Target a single invocation; defaults to all invocations. */
  instanceId?: string;
  /** Friendly status detail, defaults to "Failed". */
  statusDetails?: string;
  /** Captured stderr exposed via `GetCommandInvocation`. */
  standardErrorContent?: string;
}

export interface FailSsmCommandResponse {
  updatedInvocations: number;
}

/** One emitted SSM parameter-policy event. */
export interface SsmParameterPolicyEvent {
  parameterName: string;
  parameterArn: string;
  /**
   * One of `ExpirationRegistered`, `ExpirationNotificationRegistered`,
   * `NoChangeNotificationRegistered`, `Expiration`,
   * `ExpirationNotification`, `NoChangeNotification`.
   */
  eventType: string;
  message: string;
  createdAt: string;
}

export interface SsmParameterPolicyEventsResponse {
  events: SsmParameterPolicyEvent[];
}

/**
 * Body for `POST /_fakecloud/ssm/sessions/inject`. Drops a fake
 * session record into state without going through `StartSession`.
 */
export interface InjectSsmSessionRequest {
  accountId?: string;
  target: string;
  /** Defaults to `Connected`; pass `Terminated` to seed a finished session. */
  status?: string;
  /** Defaults to the account-root IAM ARN. */
  owner?: string;
  reason?: string;
  /** Optional explicit session ID; falls back to autogenerated form. */
  sessionId?: string;
}

export interface InjectSsmSessionResponse {
  sessionId: string;
}

// ── KMS ────────────────────────────────────────────────────────────

/** One recorded KMS usage record. */
export interface KmsUsageRecord {
  timestamp: string;
  operation: string;
  servicePrincipal: string;
  accountId: string;
  keyArn: string;
  encryptionContext: Record<string, string>;
}

export interface KmsUsageResponse {
  records: KmsUsageRecord[];
}

// ── CloudFront ─────────────────────────────────────────────────────

/** Body for `POST /_fakecloud/cloudfront/distributions/{id}/status`. */
export interface CloudFrontDistributionStatusRequest {
  /** Typically `"Deployed"` or `"InProgress"`. */
  status: string;
}

// ── ELBv2 WAF counts ───────────────────────────────────────────────

/** Pass-through response from `GET /_fakecloud/elbv2/waf-counts`. */
export interface Elbv2WafCountsResponse {
  counts: unknown;
}
