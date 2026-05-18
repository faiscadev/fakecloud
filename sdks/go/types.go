package fakecloud

import "encoding/json"

// ── Health & Reset ─────────────────────────────────────────────────

// HealthResponse is returned by the health endpoint.
type HealthResponse struct {
	Status   string   `json:"status"`
	Version  string   `json:"version"`
	Services []string `json:"services"`
}

// ResetResponse is returned by the global reset endpoint.
type ResetResponse struct {
	Status string `json:"status"`
}

// ResetServiceResponse is returned when resetting a single service.
type ResetServiceResponse struct {
	Reset string `json:"reset"`
}

// ── RDS ───────────────────────────────────────────────────────────

type RDSTag struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type RDSInstance struct {
	DBInstanceIdentifier string   `json:"dbInstanceIdentifier"`
	DBInstanceARN        string   `json:"dbInstanceArn"`
	DBInstanceClass      string   `json:"dbInstanceClass"`
	Engine               string   `json:"engine"`
	EngineVersion        string   `json:"engineVersion"`
	DBInstanceStatus     string   `json:"dbInstanceStatus"`
	MasterUsername       string   `json:"masterUsername"`
	DBName               *string  `json:"dbName"`
	EndpointAddress      string   `json:"endpointAddress"`
	Port                 int32    `json:"port"`
	AllocatedStorage     int32    `json:"allocatedStorage"`
	PubliclyAccessible   bool     `json:"publiclyAccessible"`
	DeletionProtection   bool     `json:"deletionProtection"`
	CreatedAt            string   `json:"createdAt"`
	DBIResourceID        string   `json:"dbiResourceId"`
	ContainerID          string   `json:"containerId"`
	HostPort             uint16   `json:"hostPort"`
	Tags                 []RDSTag `json:"tags"`
}

type RDSInstancesResponse struct {
	Instances []RDSInstance `json:"instances"`
}

// ── ElastiCache ────────────────────────────────────────────────────

type ElastiCacheCluster struct {
	CacheClusterID     string  `json:"cacheClusterId"`
	CacheClusterStatus string  `json:"cacheClusterStatus"`
	Engine             string  `json:"engine"`
	EngineVersion      string  `json:"engineVersion"`
	CacheNodeType      string  `json:"cacheNodeType"`
	NumCacheNodes      int32   `json:"numCacheNodes"`
	ReplicationGroupID *string `json:"replicationGroupId"`
	Port               *int32  `json:"port"`
	HostPort           *uint16 `json:"hostPort"`
	ContainerID        *string `json:"containerId"`
}

type ElastiCacheClustersResponse struct {
	Clusters []ElastiCacheCluster `json:"clusters"`
}

type ElastiCacheReplicationGroupIntrospection struct {
	ReplicationGroupID string   `json:"replicationGroupId"`
	Status             string   `json:"status"`
	Description        string   `json:"description"`
	MemberClusters     []string `json:"memberClusters"`
	AutomaticFailover  bool     `json:"automaticFailover"`
	MultiAZ            bool     `json:"multiAz"`
	Engine             string   `json:"engine"`
	EngineVersion      string   `json:"engineVersion"`
	CacheNodeType      string   `json:"cacheNodeType"`
	NumCacheClusters   int32    `json:"numCacheClusters"`
}

type ElastiCacheReplicationGroupsResponse struct {
	ReplicationGroups []ElastiCacheReplicationGroupIntrospection `json:"replicationGroups"`
}

type ElastiCacheServerlessCacheIntrospection struct {
	ServerlessCacheName string  `json:"serverlessCacheName"`
	Status              string  `json:"status"`
	Engine              string  `json:"engine"`
	EngineVersion       string  `json:"engineVersion"`
	CacheNodeType       *string `json:"cacheNodeType"`
}

type ElastiCacheServerlessCachesResponse struct {
	ServerlessCaches []ElastiCacheServerlessCacheIntrospection `json:"serverlessCaches"`
}

type ElastiCacheAclUser struct {
	Name               string `json:"name"`
	Status             string `json:"status"`
	AccessString       string `json:"accessString"`
	NoPasswordRequired bool   `json:"noPasswordRequired"`
	PasswordCount      int32  `json:"passwordCount"`
}

type ElastiCacheAclGroup struct {
	Name    string   `json:"name"`
	Members []string `json:"members"`
}

type ElastiCacheAclCluster struct {
	ClusterID string                `json:"clusterId"`
	Engine    string                `json:"engine"`
	Users     []ElastiCacheAclUser  `json:"users"`
	Groups    []ElastiCacheAclGroup `json:"groups"`
}

type ElastiCacheAclsResponse struct {
	Acls []ElastiCacheAclCluster `json:"acls"`
}

// ── Lambda ─────────────────────────────────────────────────────────

// LambdaInvocation represents a recorded Lambda invocation.
type LambdaInvocation struct {
	FunctionArn string `json:"functionArn"`
	Payload     string `json:"payload"`
	Source      string `json:"source"`
	Timestamp   string `json:"timestamp"`
}

// LambdaInvocationsResponse contains recorded Lambda invocations.
type LambdaInvocationsResponse struct {
	Invocations []LambdaInvocation `json:"invocations"`
}

// WarmContainer represents a cached Lambda container.
type WarmContainer struct {
	FunctionName    string `json:"functionName"`
	Runtime         string `json:"runtime"`
	ContainerID     string `json:"containerId"`
	LastUsedSecsAgo uint64 `json:"lastUsedSecsAgo"`
}

// WarmContainersResponse contains warm Lambda containers.
type WarmContainersResponse struct {
	Containers []WarmContainer `json:"containers"`
}

// EvictContainerResponse is returned when evicting a warm container.
type EvictContainerResponse struct {
	Evicted bool `json:"evicted"`
}

// ── SES ────────────────────────────────────────────────────────────

// SentEmail represents an email captured by the SES emulator.
type SentEmail struct {
	MessageID     string      `json:"messageId"`
	From          string      `json:"from"`
	To            []string    `json:"to"`
	CC            []string    `json:"cc"`
	BCC           []string    `json:"bcc"`
	Subject       *string     `json:"subject,omitempty"`
	HTMLBody      *string     `json:"htmlBody,omitempty"`
	TextBody      *string     `json:"textBody,omitempty"`
	RawData       *string     `json:"rawData,omitempty"`
	TemplateName  *string     `json:"templateName,omitempty"`
	TemplateData  *string     `json:"templateData,omitempty"`
	DKIMSignature *string     `json:"dkimSignature,omitempty"`
	Headers       [][2]string `json:"headers,omitempty"`
	Timestamp     string      `json:"timestamp"`
}

// SESEmailsResponse contains all sent emails.
type SESEmailsResponse struct {
	Emails []SentEmail `json:"emails"`
}

// InboundEmailRequest is the payload for simulating an inbound email.
type InboundEmailRequest struct {
	From    string   `json:"from"`
	To      []string `json:"to"`
	Subject string   `json:"subject"`
	Body    string   `json:"body"`
}

// InboundActionExecuted describes an action triggered by a receipt rule.
type InboundActionExecuted struct {
	Rule       string `json:"rule"`
	ActionType string `json:"actionType"`
}

// InboundEmailResponse is returned after simulating an inbound email.
type InboundEmailResponse struct {
	MessageID       string                  `json:"messageId"`
	MatchedRules    []string                `json:"matchedRules"`
	ActionsExecuted []InboundActionExecuted `json:"actionsExecuted"`
}

// SESMetrics exposes counters tracked by the SES emulator.
type SESMetrics struct {
	SuppressedDropsTotal uint64 `json:"suppressedDropsTotal"`
}

// SESMailFromStatusRequest sets the MAIL FROM domain verification status.
type SESMailFromStatusRequest struct {
	Status string `json:"status"`
}

// SESMailFromStatusResponse is returned after updating MAIL FROM status.
type SESMailFromStatusResponse struct {
	Identity             string `json:"identity"`
	MailFromDomainStatus string `json:"mailFromDomainStatus"`
}

// SESDkimPublicKey describes the DKIM signing material for an identity.
type SESDkimPublicKey struct {
	Identity        string `json:"identity"`
	Selector        string `json:"selector"`
	PublicKeyBase64 string `json:"publicKeyBase64"`
	SigningEnabled  bool   `json:"signingEnabled"`
}

// SESSandboxRequest toggles sandbox / production access for the account.
type SESSandboxRequest struct {
	Sandbox bool `json:"sandbox"`
}

// SESSandboxResponse echoes the new sandbox state.
type SESSandboxResponse struct {
	Sandbox                 bool `json:"sandbox"`
	ProductionAccessEnabled bool `json:"productionAccessEnabled"`
}

// SESBouncedRecipientInfo describes one recipient inside a queued bounce.
type SESBouncedRecipientInfo struct {
	Recipient      string `json:"recipient"`
	BounceType     string `json:"bounceType"`
	Action         string `json:"action"`
	Status         string `json:"status"`
	DiagnosticCode string `json:"diagnosticCode"`
}

// SESBounce is one bounce queued via SES SendBounce.
type SESBounce struct {
	MessageID            string                    `json:"messageId"`
	BounceType           string                    `json:"bounceType"`
	BounceSubType        string                    `json:"bounceSubType"`
	BouncedRecipientInfo []SESBouncedRecipientInfo `json:"bouncedRecipientInfo"`
	Explanation          *string                   `json:"explanation,omitempty"`
	Timestamp            string                    `json:"timestamp"`
	OriginalMessageID    string                    `json:"originalMessageId"`
	BounceSender         string                    `json:"bounceSender"`
}

// SESBouncesResponse contains all queued bounces.
type SESBouncesResponse struct {
	Bounces []SESBounce `json:"bounces"`
}

// SESMessageInsightEvent is one delivery / bounce / complaint observation
// recorded against a sent SES message.
type SESMessageInsightEvent struct {
	Destination           string  `json:"destination"`
	Timestamp             string  `json:"timestamp"`
	BounceType            *string `json:"bounceType,omitempty"`
	BounceSubType         *string `json:"bounceSubType,omitempty"`
	DiagnosticCode        *string `json:"diagnosticCode,omitempty"`
	ComplaintFeedbackType *string `json:"complaintFeedbackType,omitempty"`
}

// SESMessageInsightsResponse is the per-message MessageInsights shape.
type SESMessageInsightsResponse struct {
	MessageID  string                   `json:"messageId"`
	Sends      []SESMessageInsightEvent `json:"sends"`
	Deliveries []SESMessageInsightEvent `json:"deliveries"`
	Opens      []SESMessageInsightEvent `json:"opens"`
	Clicks     []SESMessageInsightEvent `json:"clicks"`
	Bounces    []SESMessageInsightEvent `json:"bounces"`
	Complaints []SESMessageInsightEvent `json:"complaints"`
	Rejects    []SESMessageInsightEvent `json:"rejects"`
}

// SESSmtpSubmission is one message accepted via the SES SMTP listener.
type SESSmtpSubmission struct {
	MessageID    string   `json:"messageId"`
	From         string   `json:"from"`
	To           []string `json:"to"`
	Subject      *string  `json:"subject,omitempty"`
	RawSizeBytes int      `json:"rawSizeBytes"`
	ReceivedAt   string   `json:"receivedAt"`
	AuthUser     string   `json:"authUser"`
}

// SESSmtpSubmissionsResponse contains all SMTP submissions.
type SESSmtpSubmissionsResponse struct {
	Submissions []SESSmtpSubmission `json:"submissions"`
}

// SESEventDestinationDelivery is one event handed off by the SES fanout
// to a configured event destination.
type SESEventDestinationDelivery struct {
	DestinationName string `json:"destinationName"`
	DestinationType string `json:"destinationType"`
	EventType       string `json:"eventType"`
	MessageID       string `json:"messageId"`
	DispatchedAt    string `json:"dispatchedAt"`
	TargetArn       string `json:"targetArn"`
}

// SESEventDestinationDeliveriesResponse contains all event-destination dispatches.
type SESEventDestinationDeliveriesResponse struct {
	Deliveries []SESEventDestinationDelivery `json:"deliveries"`
}

// ── SNS ────────────────────────────────────────────────────────────

// SNSMessage represents a published SNS message.
type SNSMessage struct {
	MessageID string  `json:"messageId"`
	TopicArn  string  `json:"topicArn"`
	Message   string  `json:"message"`
	Subject   *string `json:"subject,omitempty"`
	Timestamp string  `json:"timestamp"`
}

// SNSMessagesResponse contains all published SNS messages.
type SNSMessagesResponse struct {
	Messages []SNSMessage `json:"messages"`
}

// PendingConfirmation represents a subscription awaiting confirmation.
type PendingConfirmation struct {
	SubscriptionArn string  `json:"subscriptionArn"`
	TopicArn        string  `json:"topicArn"`
	Protocol        string  `json:"protocol"`
	Endpoint        string  `json:"endpoint"`
	Token           *string `json:"token,omitempty"`
}

// PendingConfirmationsResponse contains pending SNS subscription confirmations.
type PendingConfirmationsResponse struct {
	PendingConfirmations []PendingConfirmation `json:"pendingConfirmations"`
}

// ConfirmSubscriptionRequest is the payload for confirming an SNS subscription.
type ConfirmSubscriptionRequest struct {
	SubscriptionArn string `json:"subscriptionArn"`
}

// ConfirmSubscriptionResponse is returned after confirming a subscription.
type ConfirmSubscriptionResponse struct {
	Confirmed bool `json:"confirmed"`
}

// ── SQS ────────────────────────────────────────────────────────────

// SQSMessageInfo describes a message in an SQS queue.
type SQSMessageInfo struct {
	MessageID    string `json:"messageId"`
	Body         string `json:"body"`
	ReceiveCount uint64 `json:"receiveCount"`
	InFlight     bool   `json:"inFlight"`
	CreatedAt    string `json:"createdAt"`
}

// SQSQueueMessages contains messages for a single queue.
type SQSQueueMessages struct {
	QueueURL  string           `json:"queueUrl"`
	QueueName string           `json:"queueName"`
	Messages  []SQSMessageInfo `json:"messages"`
}

// SQSMessagesResponse contains messages across all queues.
type SQSMessagesResponse struct {
	Queues []SQSQueueMessages `json:"queues"`
}

// ExpirationTickResponse is returned after ticking the SQS expiration processor.
type ExpirationTickResponse struct {
	ExpiredMessages uint64 `json:"expiredMessages"`
}

// ForceDLQResponse is returned after forcing messages to a DLQ.
type ForceDLQResponse struct {
	MovedMessages uint64 `json:"movedMessages"`
}

// AppAsTickResponse reports how many Application Auto Scaling
// policies applied a capacity change on a forced watcher tick.
type AppAsTickResponse struct {
	Applied int `json:"applied"`
}

// AppAsScheduledTickResponse reports how many Application Auto Scaling
// scheduled actions fired on a forced executor tick.
type AppAsScheduledTickResponse struct {
	Fired int `json:"fired"`
}

// ── EventBridge ────────────────────────────────────────────────────

// EventBridgeEvent represents an event put to EventBridge.
type EventBridgeEvent struct {
	EventID    string `json:"eventId"`
	Source     string `json:"source"`
	DetailType string `json:"detailType"`
	Detail     string `json:"detail"`
	BusName    string `json:"busName"`
	Timestamp  string `json:"timestamp"`
}

// EventBridgeLambdaDelivery represents a delivery to a Lambda target.
type EventBridgeLambdaDelivery struct {
	FunctionArn string `json:"functionArn"`
	Payload     string `json:"payload"`
	Timestamp   string `json:"timestamp"`
}

// EventBridgeLogDelivery represents a delivery to a CloudWatch Logs target.
type EventBridgeLogDelivery struct {
	LogGroupArn string `json:"logGroupArn"`
	Payload     string `json:"payload"`
	Timestamp   string `json:"timestamp"`
}

// EventBridgeDeliveries contains all deliveries from EventBridge rules.
type EventBridgeDeliveries struct {
	Lambda []EventBridgeLambdaDelivery `json:"lambda"`
	Logs   []EventBridgeLogDelivery    `json:"logs"`
}

// EventHistoryResponse contains event history and delivery records.
type EventHistoryResponse struct {
	Events     []EventBridgeEvent    `json:"events"`
	Deliveries EventBridgeDeliveries `json:"deliveries"`
}

// FireRuleRequest is the payload for manually firing an EventBridge rule.
type FireRuleRequest struct {
	BusName  *string `json:"busName,omitempty"`
	RuleName string  `json:"ruleName"`
}

// FireRuleTarget describes a target that was invoked by a fired rule.
type FireRuleTarget struct {
	Type string `json:"type"`
	Arn  string `json:"arn"`
}

// FireRuleResponse is returned after manually firing a rule.
type FireRuleResponse struct {
	Targets []FireRuleTarget `json:"targets"`
}

// ── Scheduler (EventBridge Scheduler) ───────────────────────────────

// SchedulerSchedule describes one schedule managed by EventBridge
// Scheduler. Returned by the /_fakecloud/scheduler/schedules endpoint.
type SchedulerSchedule struct {
	AccountID          string  `json:"accountId"`
	GroupName          string  `json:"groupName"`
	Name               string  `json:"name"`
	Arn                string  `json:"arn"`
	State              string  `json:"state"`
	ScheduleExpression string  `json:"scheduleExpression"`
	TargetArn          string  `json:"targetArn"`
	LastFired          *string `json:"lastFired,omitempty"`
}

// SchedulerSchedulesResponse contains every schedule registered on the server.
type SchedulerSchedulesResponse struct {
	Schedules []SchedulerSchedule `json:"schedules"`
}

// FireScheduleResponse is returned after manually firing a schedule.
type FireScheduleResponse struct {
	ScheduleArn string `json:"scheduleArn"`
	TargetArn   string `json:"targetArn"`
}

// ── S3 ─────────────────────────────────────────────────────────────

// S3Notification represents an S3 event notification.
type S3Notification struct {
	Bucket    string `json:"bucket"`
	Key       string `json:"key"`
	EventType string `json:"eventType"`
	Timestamp string `json:"timestamp"`
}

// S3NotificationsResponse contains S3 notification events.
type S3NotificationsResponse struct {
	Notifications []S3Notification `json:"notifications"`
}

// LifecycleTickResponse is returned after ticking the S3 lifecycle processor.
type LifecycleTickResponse struct {
	ProcessedBuckets    uint64 `json:"processedBuckets"`
	ExpiredObjects      uint64 `json:"expiredObjects"`
	TransitionedObjects uint64 `json:"transitionedObjects"`
}

// S3AccessPointEntry describes a single S3 access point.
type S3AccessPointEntry struct {
	Name              string  `json:"name"`
	Alias             string  `json:"alias"`
	Bucket            string  `json:"bucket"`
	AccountID         string  `json:"accountId"`
	NetworkOrigin     string  `json:"networkOrigin"`
	VpcConfiguration  *string `json:"vpcConfiguration,omitempty"`
	PublicAccessBlock *string `json:"publicAccessBlock,omitempty"`
	CreatedAt         string  `json:"createdAt"`
}

// S3AccessPointsResponse holds the S3 access point registry.
type S3AccessPointsResponse struct {
	AccessPoints []S3AccessPointEntry `json:"accessPoints"`
}

// S3ObjectLambdaResponse is a recorded WriteGetObjectResponse call body.
type S3ObjectLambdaResponse struct {
	RequestToken string            `json:"requestToken"`
	RequestRoute string            `json:"requestRoute"`
	StatusCode   *uint16           `json:"statusCode,omitempty"`
	BodyBase64   string            `json:"bodyBase64"`
	BodySize     uint64            `json:"bodySize"`
	ContentType  *string           `json:"contentType,omitempty"`
	ErrorMessage *string           `json:"errorMessage,omitempty"`
	Metadata     map[string]string `json:"metadata"`
}

// S3ObjectLambdaResponsesResponse holds stored object-lambda response bodies.
type S3ObjectLambdaResponsesResponse struct {
	Responses []S3ObjectLambdaResponse `json:"responses"`
}

// ── DynamoDB ───────────────────────────────────────────────────────

// TTLTickResponse is returned after ticking the DynamoDB TTL processor.
type TTLTickResponse struct {
	ExpiredItems uint64 `json:"expiredItems"`
}

// ── SecretsManager ─────────────────────────────────────────────────

// RotationTickResponse is returned after ticking the rotation scheduler.
type RotationTickResponse struct {
	RotatedSecrets []string `json:"rotatedSecrets"`
}

// ── Cognito ────────────────────────────────────────────────────────

// UserConfirmationCodes contains codes for a specific user.
type UserConfirmationCodes struct {
	ConfirmationCode           *string                `json:"confirmationCode,omitempty"`
	AttributeVerificationCodes map[string]interface{} `json:"attributeVerificationCodes"`
}

// ConfirmationCode represents a confirmation code across all pools.
type ConfirmationCode struct {
	PoolID    string  `json:"poolId"`
	Username  string  `json:"username"`
	Code      string  `json:"code"`
	Type      string  `json:"type"`
	Attribute *string `json:"attribute,omitempty"`
}

// ConfirmationCodesResponse contains all confirmation codes.
type ConfirmationCodesResponse struct {
	Codes []ConfirmationCode `json:"codes"`
}

// ConfirmUserRequest is the payload for confirming a Cognito user.
type ConfirmUserRequest struct {
	UserPoolID string `json:"userPoolId"`
	Username   string `json:"username"`
}

// ConfirmUserResponse is returned after confirming a user.
type ConfirmUserResponse struct {
	Confirmed bool    `json:"confirmed"`
	Error     *string `json:"error,omitempty"`
}

// TokenInfo describes an active Cognito token.
type TokenInfo struct {
	Type     string  `json:"type"`
	Username string  `json:"username"`
	PoolID   string  `json:"poolId"`
	ClientID string  `json:"clientId"`
	IssuedAt float64 `json:"issuedAt"`
}

// TokensResponse contains all active tokens.
type TokensResponse struct {
	Tokens []TokenInfo `json:"tokens"`
}

// ExpireTokensRequest is the payload for expiring Cognito tokens.
type ExpireTokensRequest struct {
	UserPoolID *string `json:"userPoolId,omitempty"`
	Username   *string `json:"username,omitempty"`
}

// ExpireTokensResponse is returned after expiring tokens.
type ExpireTokensResponse struct {
	ExpiredTokens uint64 `json:"expiredTokens"`
}

// AuthEvent represents a Cognito authentication event.
type AuthEvent struct {
	EventType  string  `json:"eventType"`
	Username   string  `json:"username"`
	UserPoolID string  `json:"userPoolId"`
	ClientID   *string `json:"clientId,omitempty"`
	Timestamp  float64 `json:"timestamp"`
	Success    bool    `json:"success"`
}

// AuthEventsResponse contains Cognito auth events.
type AuthEventsResponse struct {
	Events []AuthEvent `json:"events"`
}

// PreTokenGenInvocation is one PreTokenGeneration Lambda trigger
// invocation captured by InitiateAuth. ClaimsAdded / ClaimsOverridden /
// GroupOverrides are pre-parsed from the Lambda response.
type PreTokenGenInvocation struct {
	PoolID           string                 `json:"poolId"`
	UserPoolARN      string                 `json:"userPoolArn"`
	Username         string                 `json:"username"`
	TriggerSource    string                 `json:"triggerSource"`
	LambdaARN        string                 `json:"lambdaArn"`
	RequestPayload   map[string]interface{} `json:"requestPayload"`
	ResponsePayload  map[string]interface{} `json:"responsePayload,omitempty"`
	ClaimsAdded      []string               `json:"claimsAdded"`
	ClaimsOverridden []string               `json:"claimsOverridden"`
	GroupOverrides   []string               `json:"groupOverrides"`
	InvokedAt        string                 `json:"invokedAt"`
	DurationMs       uint64                 `json:"durationMs"`
}

// PreTokenGenInvocationsResponse is the shape returned by
// /_fakecloud/cognito/pretokengen/invocations.
type PreTokenGenInvocationsResponse struct {
	Invocations []PreTokenGenInvocation `json:"invocations"`
}

// MintAuthorizationCodeRequest is the payload for the
// /_fakecloud/cognito/authorization-codes admin endpoint. Lets test
// harnesses mint a single-use OAuth2 authorization code that the
// /oauth2/token authorization_code grant can later consume.
type MintAuthorizationCodeRequest struct {
	UserPoolID          string   `json:"userPoolId"`
	ClientID            string   `json:"clientId"`
	Username            string   `json:"username"`
	RedirectURI         string   `json:"redirectUri"`
	Scopes              []string `json:"scopes,omitempty"`
	CodeChallenge       *string  `json:"codeChallenge,omitempty"`
	CodeChallengeMethod *string  `json:"codeChallengeMethod,omitempty"`
	Nonce               *string  `json:"nonce,omitempty"`
}

// MintAuthorizationCodeResponse is returned after minting a code.
type MintAuthorizationCodeResponse struct {
	Code string `json:"code"`
}

// CompromisedPasswordsRequest is the payload for the
// /_fakecloud/cognito/compromised-passwords admin endpoint. Each
// supplied plaintext password is SHA-256 hashed and added to the
// compromised-password set; subsequent `SignUp` / `AdminInitiateAuth`
// calls fail with `InvalidPasswordException` when a user pool has
// `CompromisedCredentialsRiskConfiguration.Actions.EventAction = BLOCK`
// and the supplied password hashes to a member of that set.
type CompromisedPasswordsRequest struct {
	Passwords []string `json:"passwords"`
}

// CompromisedPasswordsResponse is returned after registering passwords.
type CompromisedPasswordsResponse struct {
	Added uint64 `json:"added"`
}

// WebAuthnCredential describes a registered WebAuthn credential
// surfaced by the introspection endpoint. `AttestationInfo` is the
// parsed-attestation JSON object (packed format details, AAGUID,
// certificate chain summary, signature counter) and is left as raw
// JSON because its shape depends on the attestation format.
type WebAuthnCredential struct {
	AccountID       string          `json:"account_id"`
	PoolUser        string          `json:"pool_user"`
	CredentialID    string          `json:"credential_id"`
	RelyingPartyID  string          `json:"relying_party_id"`
	AttestationInfo json.RawMessage `json:"attestation_info"`
}

// WebAuthnCredentialsResponse contains all registered WebAuthn credentials.
type WebAuthnCredentialsResponse struct {
	Credentials []WebAuthnCredential `json:"credentials"`
}

// ── Step Functions ─────────────────────────────────────────────────

// StepFunctionsExecution represents a state machine execution.
type StepFunctionsExecution struct {
	ExecutionARN    string  `json:"executionArn"`
	StateMachineARN string  `json:"stateMachineArn"`
	Name            string  `json:"name"`
	Status          string  `json:"status"`
	StartDate       string  `json:"startDate"`
	Input           *string `json:"input,omitempty"`
	Output          *string `json:"output,omitempty"`
	StopDate        *string `json:"stopDate,omitempty"`
}

// StepFunctionsExecutionsResponse contains all recorded executions.
type StepFunctionsExecutionsResponse struct {
	Executions []StepFunctionsExecution `json:"executions"`
}

// SfnEnqueueActivityTaskRequest queues a task for an activity worker without
// running an ASL execution.
type SfnEnqueueActivityTaskRequest struct {
	ActivityARN      string  `json:"activityArn"`
	Input            *string `json:"input,omitempty"`
	HeartbeatSeconds *int64  `json:"heartbeatSeconds,omitempty"`
	TimeoutSeconds   *int64  `json:"timeoutSeconds,omitempty"`
}

// SfnEnqueueActivityTaskResponse carries the synthesized task token.
type SfnEnqueueActivityTaskResponse struct {
	TaskToken string `json:"taskToken"`
}

// ── Bedrock ───────────────────────────────────────────────────────

// BedrockInvocation represents a recorded Bedrock model invocation.
type BedrockInvocation struct {
	ModelID   string `json:"modelId"`
	Input     string `json:"input"`
	Output    string `json:"output"`
	Timestamp string `json:"timestamp"`
	// Error is non-nil for calls that were faulted via QueueFault.
	Error *string `json:"error"`
}

// BedrockInvocationsResponse contains recorded Bedrock invocations.
type BedrockInvocationsResponse struct {
	Invocations []BedrockInvocation `json:"invocations"`
}

// BedrockModelResponseConfig is returned after setting a model response.
type BedrockModelResponseConfig struct {
	Status  string `json:"status"`
	ModelID string `json:"modelId"`
}

// BedrockResponseRule is one prompt-conditional response rule for a model.
// PromptContains is an optional substring; if nil or empty, the rule matches any prompt.
type BedrockResponseRule struct {
	PromptContains *string `json:"promptContains"`
	Response       string  `json:"response"`
}

// BedrockFaultRule configures a fault to inject on Bedrock runtime calls.
// Zero-value fields are omitted from the wire request, letting the server apply its defaults.
type BedrockFaultRule struct {
	ErrorType  string `json:"errorType"`
	Message    string `json:"message,omitempty"`
	HTTPStatus int    `json:"httpStatus,omitempty"`
	Count      int    `json:"count,omitempty"`
	ModelID    string `json:"modelId,omitempty"`
	Operation  string `json:"operation,omitempty"`
}

// BedrockFaultRuleState is the server-side view of a queued fault rule.
type BedrockFaultRuleState struct {
	ErrorType  string  `json:"errorType"`
	Message    string  `json:"message"`
	HTTPStatus int     `json:"httpStatus"`
	Remaining  int     `json:"remaining"`
	ModelID    *string `json:"modelId"`
	Operation  *string `json:"operation"`
}

// BedrockFaultsResponse is the list-faults response body.
type BedrockFaultsResponse struct {
	Faults []BedrockFaultRuleState `json:"faults"`
}

// BedrockStatusResponse is a generic {status: "ok"} body.
type BedrockStatusResponse struct {
	Status string `json:"status"`
}

// ── Bedrock Agent (control plane) ─────────────────────────────────

// BedrockAgentAliasSummary is one alias row attached to an agent.
type BedrockAgentAliasSummary struct {
	AliasID      string `json:"aliasId"`
	AliasName    string `json:"aliasName"`
	AgentVersion string `json:"agentVersion"`
	AliasArn     string `json:"aliasArn"`
	Status       string `json:"status"`
	CreatedAt    string `json:"createdAt"`
	UpdatedAt    string `json:"updatedAt"`
}

// BedrockAgentVersionSummary is one captured version of an agent.
type BedrockAgentVersionSummary struct {
	AgentVersion    string  `json:"agentVersion"`
	CreatedAt       string  `json:"createdAt"`
	Instruction     *string `json:"instruction"`
	FoundationModel *string `json:"foundationModel"`
}

// BedrockAgentKnowledgeBaseSummary is one knowledge-base attachment.
type BedrockAgentKnowledgeBaseSummary struct {
	KnowledgeBaseID string  `json:"knowledgeBaseId"`
	State           string  `json:"state"`
	Description     *string `json:"description"`
}

// BedrockAgentCollaboratorSummary is one collaborator attachment.
type BedrockAgentCollaboratorSummary struct {
	CollaboratorID           string `json:"collaboratorId"`
	CollaboratorName         string `json:"collaboratorName"`
	CollaboratorAliasArn     string `json:"collaboratorAliasArn"`
	RelayConversationHistory string `json:"relayConversationHistory"`
}

// BedrockAgentRow is one Bedrock Agent flattened for introspection.
type BedrockAgentRow struct {
	AgentID         string                             `json:"agentId"`
	AgentName       string                             `json:"agentName"`
	AgentArn        string                             `json:"agentArn"`
	AgentStatus     string                             `json:"agentStatus"`
	FoundationModel *string                            `json:"foundationModel"`
	Instruction     *string                            `json:"instruction"`
	KnowledgeBases  []BedrockAgentKnowledgeBaseSummary `json:"knowledgeBases"`
	ActionGroups    []any                              `json:"actionGroups"`
	Collaborators   []BedrockAgentCollaboratorSummary  `json:"collaborators"`
	Aliases         []BedrockAgentAliasSummary         `json:"aliases"`
	Versions        []BedrockAgentVersionSummary       `json:"versions"`
	PromptOverrides any                                `json:"promptOverrides"`
	CreatedAt       string                             `json:"createdAt"`
	UpdatedAt       string                             `json:"updatedAt"`
}

// BedrockAgentAgentsResponse is the list-agents response body.
type BedrockAgentAgentsResponse struct {
	Agents []BedrockAgentRow `json:"agents"`
}

// ── Bedrock Agent Runtime (data plane) ────────────────────────────

// BedrockAgentRuntimeInvocation is one recorded data-plane call.
type BedrockAgentRuntimeInvocation struct {
	InvocationID string  `json:"invocationId"`
	Op           string  `json:"op"`
	AgentID      *string `json:"agentId"`
	FlowID       *string `json:"flowId"`
	SessionID    *string `json:"sessionId"`
	Input        string  `json:"input"`
	Output       string  `json:"output"`
	OutputChunks uint32  `json:"outputChunks"`
	Trace        any     `json:"trace"`
	Citations    []any   `json:"citations"`
	InvokedAt    string  `json:"invokedAt"`
	DurationMs   uint64  `json:"durationMs"`
}

// BedrockAgentRuntimeInvocationsResponse is the list-invocations body.
type BedrockAgentRuntimeInvocationsResponse struct {
	Invocations []BedrockAgentRuntimeInvocation `json:"invocations"`
}

// ── IAM ───────────────────────────────────────────────────────────

// CreateAdminRequest is the payload for creating an IAM admin user.
type CreateAdminRequest struct {
	AccountID string `json:"accountId"`
	UserName  string `json:"userName"`
}

// CreateAdminResponse is returned after creating an IAM admin user.
type CreateAdminResponse struct {
	AccessKeyID     string `json:"accessKeyId"`
	SecretAccessKey string `json:"secretAccessKey"`
	AccountID       string `json:"accountId"`
	Arn             string `json:"arn"`
}

// ── API Gateway v2 ─────────────────────────────────────────────────

// ApiGatewayV2Request represents an HTTP API request that was received.
type ApiGatewayV2Request struct {
	RequestID   string            `json:"requestId"`
	ApiID       string            `json:"apiId"`
	Stage       string            `json:"stage"`
	Method      string            `json:"method"`
	Path        string            `json:"path"`
	Headers     map[string]string `json:"headers"`
	QueryParams map[string]string `json:"queryParams"`
	Body        *string           `json:"body,omitempty"`
	Timestamp   string            `json:"timestamp"`
	StatusCode  uint16            `json:"statusCode"`
}

// ApiGatewayV2RequestsResponse contains all recorded HTTP API requests.
type ApiGatewayV2RequestsResponse struct {
	Requests []ApiGatewayV2Request `json:"requests"`
}

// ── ECR ────────────────────────────────────────────────────────────

// ECRTag is a key/value tag attached to an ECR repository.
type ECRTag struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// ECRRepository describes a fakecloud-managed ECR repository.
type ECRRepository struct {
	RepositoryName     string   `json:"repositoryName"`
	RepositoryArn      string   `json:"repositoryArn"`
	RegistryID         string   `json:"registryId"`
	RepositoryURI      string   `json:"repositoryUri"`
	ImageTagMutability string   `json:"imageTagMutability"`
	ScanOnPush         bool     `json:"scanOnPush"`
	CreatedAt          string   `json:"createdAt"`
	Tags               []ECRTag `json:"tags"`
	HasPolicy          bool     `json:"hasPolicy"`
	HasLifecyclePolicy bool     `json:"hasLifecyclePolicy"`
	ImageCount         uint64   `json:"imageCount"`
	LayerCount         uint64   `json:"layerCount"`
}

// ECRRepositoriesResponse is returned by GET /_fakecloud/ecr/repositories.
type ECRRepositoriesResponse struct {
	Repositories []ECRRepository `json:"repositories"`
}

// ECRImage describes a stored container image.
type ECRImage struct {
	RepositoryName         string   `json:"repositoryName"`
	ImageDigest            string   `json:"imageDigest"`
	ImageTags              []string `json:"imageTags"`
	ImageSizeInBytes       uint64   `json:"imageSizeInBytes"`
	ImageManifestMediaType string   `json:"imageManifestMediaType"`
	ImagePushedAt          string   `json:"imagePushedAt"`
}

// ECRImagesResponse is returned by GET /_fakecloud/ecr/images.
type ECRImagesResponse struct {
	Images []ECRImage `json:"images"`
}

// ECRPullThroughRule describes a pull-through cache rule.
type ECRPullThroughRule struct {
	ECRRepositoryPrefix string  `json:"ecrRepositoryPrefix"`
	UpstreamRegistryURL string  `json:"upstreamRegistryUrl"`
	UpstreamRegistry    *string `json:"upstreamRegistry,omitempty"`
	CredentialArn       *string `json:"credentialArn,omitempty"`
	CustomRoleArn       *string `json:"customRoleArn,omitempty"`
	CreatedAt           string  `json:"createdAt"`
	UpdatedAt           string  `json:"updatedAt"`
}

// ECRPullThroughRulesResponse is returned by GET /_fakecloud/ecr/pull-through-rules.
type ECRPullThroughRulesResponse struct {
	Rules []ECRPullThroughRule `json:"rules"`
}

// ── ECS ────────────────────────────────────────────────────────────

// EcsTag is a key/value pair attached to an ECS cluster or task definition.
type EcsTag struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// EcsCluster is a snapshot of a single ECS cluster as seen by fakecloud.
type EcsCluster struct {
	ClusterName                       string   `json:"clusterName"`
	ClusterArn                        string   `json:"clusterArn"`
	Status                            string   `json:"status"`
	RunningTasksCount                 int32    `json:"runningTasksCount"`
	PendingTasksCount                 int32    `json:"pendingTasksCount"`
	ActiveServicesCount               int32    `json:"activeServicesCount"`
	RegisteredContainerInstancesCount int32    `json:"registeredContainerInstancesCount"`
	CapacityProviders                 []string `json:"capacityProviders"`
	Tags                              []EcsTag `json:"tags"`
	CreatedAt                         string   `json:"createdAt"`
}

// EcsClustersResponse contains all ECS clusters currently in state.
type EcsClustersResponse struct {
	Clusters []EcsCluster `json:"clusters"`
}

// EcsTaskContainer is a snapshot of one container in a task.
type EcsTaskContainer struct {
	Name       string  `json:"name"`
	Image      string  `json:"image"`
	LastStatus string  `json:"lastStatus"`
	ExitCode   *int64  `json:"exitCode,omitempty"`
	RuntimeID  *string `json:"runtimeId,omitempty"`
	Essential  bool    `json:"essential"`
}

// EcsTask is a snapshot of one ECS task as fakecloud sees it.
// Optional fields are pointers so JSON decoding accepts both absent and
// explicit `null` values from the server.
type EcsTask struct {
	TaskArn           string             `json:"taskArn"`
	TaskID            string             `json:"taskId"`
	ClusterArn        string             `json:"clusterArn"`
	ClusterName       string             `json:"clusterName"`
	TaskDefinitionArn string             `json:"taskDefinitionArn"`
	Family            string             `json:"family"`
	Revision          int32              `json:"revision"`
	LastStatus        string             `json:"lastStatus"`
	DesiredStatus     string             `json:"desiredStatus"`
	LaunchType        string             `json:"launchType"`
	CreatedAt         string             `json:"createdAt"`
	StartedAt         *string            `json:"startedAt,omitempty"`
	StoppingAt        *string            `json:"stoppingAt,omitempty"`
	StoppedAt         *string            `json:"stoppedAt,omitempty"`
	StopCode          *string            `json:"stopCode,omitempty"`
	StoppedReason     *string            `json:"stoppedReason,omitempty"`
	Containers        []EcsTaskContainer `json:"containers"`
	CapturedLogBytes  int                `json:"capturedLogBytes"`
}

// EcsTasksResponse contains every task fakecloud is tracking.
type EcsTasksResponse struct {
	Tasks []EcsTask `json:"tasks"`
}

// EcsTaskLogsResponse returns the docker stdout/stderr captured for a
// task, plus its exit code if known.
type EcsTaskLogsResponse struct {
	TaskArn    string `json:"taskArn"`
	Logs       string `json:"logs"`
	LastStatus string `json:"lastStatus"`
	ExitCode   *int64 `json:"exitCode,omitempty"`
}

// EcsMarkFailedRequest is the payload for POST /ecs/tasks/{id}/mark-failed.
type EcsMarkFailedRequest struct {
	ExitCode *int64  `json:"exitCode,omitempty"`
	Reason   *string `json:"reason,omitempty"`
}

// EcsLifecycleEvent is one entry in the introspection event log.
// Optional fields are pointers so null-safe JSON decoding works.
type EcsLifecycleEvent struct {
	At         string      `json:"at"`
	EventType  string      `json:"eventType"`
	TaskArn    *string     `json:"taskArn,omitempty"`
	ClusterArn *string     `json:"clusterArn,omitempty"`
	LastStatus *string     `json:"lastStatus,omitempty"`
	Detail     interface{} `json:"detail"`
}

// EcsEventsResponse contains the lifecycle event log.
type EcsEventsResponse struct {
	Events []EcsLifecycleEvent `json:"events"`
}

// EcsTaskMetadataLimits mirrors the `Limits` block of the ECS
// container-metadata v4 response.
type EcsTaskMetadataLimits struct {
	CPU    *float64 `json:"cpu,omitempty"`
	Memory *int64   `json:"memory,omitempty"`
}

// EcsTaskMetadataPort is one entry in a container's published port list.
type EcsTaskMetadataPort struct {
	ContainerPort *int64  `json:"containerPort,omitempty"`
	HostPort      *int64  `json:"hostPort,omitempty"`
	Protocol      *string `json:"protocol,omitempty"`
}

// EcsTaskMetadataContainer is one container inside a task-metadata dump.
type EcsTaskMetadataContainer struct {
	Name          string                `json:"name"`
	Image         string                `json:"image"`
	ImageID       *string               `json:"imageId,omitempty"`
	Ports         []EcsTaskMetadataPort `json:"ports"`
	Labels        map[string]string     `json:"labels"`
	DesiredStatus string                `json:"desiredStatus"`
	KnownStatus   string                `json:"knownStatus"`
	Limits        EcsTaskMetadataLimits `json:"limits"`
	CreatedAt     *string               `json:"createdAt,omitempty"`
	StartedAt     *string               `json:"startedAt,omitempty"`
	ExitCode      *int64                `json:"exitCode,omitempty"`
}

// EcsTaskMetadata is the v4 metadata-URI aggregate for one task.
type EcsTaskMetadata struct {
	Cluster          string                     `json:"cluster"`
	TaskArn          string                     `json:"taskArn"`
	Family           string                     `json:"family"`
	Revision         int32                      `json:"revision"`
	DesiredStatus    string                     `json:"desiredStatus"`
	KnownStatus      string                     `json:"knownStatus"`
	Containers       []EcsTaskMetadataContainer `json:"containers"`
	PullStartedAt    *string                    `json:"pullStartedAt,omitempty"`
	PullStoppedAt    *string                    `json:"pullStoppedAt,omitempty"`
	AvailabilityZone string                     `json:"availabilityZone"`
	LaunchType       string                     `json:"launchType"`
	VpcID            *string                    `json:"vpcId,omitempty"`
	EniID            *string                    `json:"eniId,omitempty"`
}

// EcsTaskMetadataResponse wraps a single task's metadata dump.
type EcsTaskMetadataResponse struct {
	Task EcsTaskMetadata `json:"task"`
}

// ── ELBv2 ──────────────────────────────────────────────────────────

type Elbv2Tag struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type Elbv2AvailabilityZone struct {
	ZoneName string `json:"zoneName"`
	SubnetID string `json:"subnetId"`
}

// Elbv2LoadBalancer is a snapshot of one ALB / NLB / GWLB.
type Elbv2LoadBalancer struct {
	ARN               string                  `json:"arn"`
	Name              string                  `json:"name"`
	DNSName           string                  `json:"dnsName"`
	Scheme            string                  `json:"scheme"`
	VpcID             string                  `json:"vpcId"`
	StateCode         string                  `json:"stateCode"`
	StateReason       *string                 `json:"stateReason,omitempty"`
	LbType            string                  `json:"lbType"`
	IPAddressType     string                  `json:"ipAddressType"`
	AvailabilityZones []Elbv2AvailabilityZone `json:"availabilityZones"`
	SecurityGroups    []string                `json:"securityGroups"`
	CreatedTime       string                  `json:"createdTime"`
	Tags              []Elbv2Tag              `json:"tags"`
}

type Elbv2LoadBalancersResponse struct {
	LoadBalancers []Elbv2LoadBalancer `json:"loadBalancers"`
}

type Elbv2Target struct {
	ID                string  `json:"id"`
	Port              *int32  `json:"port,omitempty"`
	AvailabilityZone  *string `json:"availabilityZone,omitempty"`
	HealthState       string  `json:"healthState"`
	HealthReason      *string `json:"healthReason,omitempty"`
	HealthDescription *string `json:"healthDescription,omitempty"`
}

type Elbv2TargetGroup struct {
	ARN                     string        `json:"arn"`
	Name                    string        `json:"name"`
	Protocol                *string       `json:"protocol,omitempty"`
	Port                    *int32        `json:"port,omitempty"`
	VpcID                   *string       `json:"vpcId,omitempty"`
	TargetType              string        `json:"targetType"`
	LoadBalancerARNs        []string      `json:"loadBalancerArns"`
	Targets                 []Elbv2Target `json:"targets"`
	HealthCheckProtocol     *string       `json:"healthCheckProtocol,omitempty"`
	HealthCheckPort         *string       `json:"healthCheckPort,omitempty"`
	HealthCheckPath         *string       `json:"healthCheckPath,omitempty"`
	HealthyThresholdCount   int32         `json:"healthyThresholdCount"`
	UnhealthyThresholdCount int32         `json:"unhealthyThresholdCount"`
	CreatedTime             string        `json:"createdTime"`
	Tags                    []Elbv2Tag    `json:"tags"`
}

type Elbv2TargetGroupsResponse struct {
	TargetGroups []Elbv2TargetGroup `json:"targetGroups"`
}

type Elbv2Listener struct {
	ARN                   string   `json:"arn"`
	LoadBalancerARN       string   `json:"loadBalancerArn"`
	Port                  *int32   `json:"port,omitempty"`
	Protocol              *string  `json:"protocol,omitempty"`
	SslPolicy             *string  `json:"sslPolicy,omitempty"`
	CertificateARNs       []string `json:"certificateArns"`
	DefaultActionType     *string  `json:"defaultActionType,omitempty"`
	DefaultTargetGroupARN *string  `json:"defaultTargetGroupArn,omitempty"`
}

type Elbv2ListenersResponse struct {
	Listeners []Elbv2Listener `json:"listeners"`
}

type Elbv2Rule struct {
	ARN             string   `json:"arn"`
	ListenerARN     string   `json:"listenerArn"`
	Priority        string   `json:"priority"`
	IsDefault       bool     `json:"isDefault"`
	ConditionFields []string `json:"conditionFields"`
	ActionType      *string  `json:"actionType,omitempty"`
}

type Elbv2RulesResponse struct {
	Rules []Elbv2Rule `json:"rules"`
}

// ── Glue ────────────────────────────────────────────────────────────

// GlueJob describes one Glue Job recorded by CreateJob. Returned by the
// /_fakecloud/glue/jobs endpoint.
type GlueJob struct {
	AccountID        string            `json:"accountId"`
	Name             string            `json:"name"`
	Role             string            `json:"role"`
	Command          json.RawMessage   `json:"command"`
	DefaultArguments map[string]string `json:"defaultArguments"`
	MaxCapacity      *float64          `json:"maxCapacity,omitempty"`
	MaxRetries       int64             `json:"maxRetries"`
	Timeout          *int64            `json:"timeout,omitempty"`
	GlueVersion      *string           `json:"glueVersion,omitempty"`
	WorkerType       *string           `json:"workerType,omitempty"`
	NumberOfWorkers  *int64            `json:"numberOfWorkers,omitempty"`
	CreatedOn        string            `json:"createdOn"`
	LastModifiedOn   string            `json:"lastModifiedOn"`
}

// GlueJobsResponse contains every Glue Job registered on the server.
type GlueJobsResponse struct {
	Jobs []GlueJob `json:"jobs"`
}

// GlueJobRun describes one JobRun recorded by StartJobRun. Returned by
// the /_fakecloud/glue/job-runs endpoint.
type GlueJobRun struct {
	AccountID     string            `json:"accountId"`
	ID            string            `json:"id"`
	JobName       string            `json:"jobName"`
	Attempt       int64             `json:"attempt"`
	StartedOn     string            `json:"startedOn"`
	CompletedOn   *string           `json:"completedOn,omitempty"`
	JobRunState   string            `json:"jobRunState"`
	Arguments     map[string]string `json:"arguments"`
	ErrorMessage  *string           `json:"errorMessage,omitempty"`
	ExecutionTime int64             `json:"executionTime"`
}

// GlueJobRunsResponse contains every JobRun observed by the server,
// optionally filtered by job name.
type GlueJobRunsResponse struct {
	Runs []GlueJobRun `json:"runs"`
}

// OrganizationsTag is a single key/value tag attached to an
// Organizations resource (account, OU, root, or policy).
type OrganizationsTag struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// OrganizationsAccount mirrors the AWS Organizations Account shape
// plus two fakecloud-only fields: ParentOuID (the resolved parent OU
// or root id) and ScpAttached (SCP ids directly attached — does not
// walk up the hierarchy).
type OrganizationsAccount struct {
	ID              string             `json:"id"`
	ARN             string             `json:"arn"`
	Email           string             `json:"email"`
	Name            string             `json:"name"`
	Status          string             `json:"status"`
	JoinedMethod    string             `json:"joinedMethod"`
	JoinedTimestamp string             `json:"joinedTimestamp"`
	ParentOuID      *string            `json:"parentOuId,omitempty"`
	Tags            []OrganizationsTag `json:"tags"`
	ScpAttached     []string           `json:"scpAttached"`
}

// OrganizationsAccountsResponse is the payload for
// GET /_fakecloud/organizations/accounts. Both management and master
// id fields are populated (AWS renamed Master to Management in 2020
// but kept both around for back-compat). Empty when no org exists.
type OrganizationsAccountsResponse struct {
	Accounts            []OrganizationsAccount `json:"accounts"`
	ManagementAccountID *string                `json:"managementAccountId,omitempty"`
	MasterAccountID     *string                `json:"masterAccountId,omitempty"`
}

// AthenaNamedQuery is one row in the Athena named-query introspection
// listing returned by GET /_fakecloud/athena/named-queries.
type AthenaNamedQuery struct {
	NamedQueryID string  `json:"namedQueryId"`
	Name         string  `json:"name"`
	Description  *string `json:"description,omitempty"`
	Database     string  `json:"database"`
	QueryString  string  `json:"queryString"`
	Workgroup    string  `json:"workgroup"`
	// LastUsedAt is the RFC3339 timestamp of the most recent
	// StartQueryExecution that resolved its query string from this named
	// query. Nil until the first such invocation.
	LastUsedAt *string `json:"lastUsedAt,omitempty"`
}

type AthenaNamedQueriesResponse struct {
	Queries []AthenaNamedQuery `json:"queries"`
}
