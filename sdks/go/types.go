package fakecloud

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
	MessageID    string   `json:"messageId"`
	From         string   `json:"from"`
	To           []string `json:"to"`
	CC           []string `json:"cc"`
	BCC          []string `json:"bcc"`
	Subject      *string  `json:"subject,omitempty"`
	HTMLBody     *string  `json:"htmlBody,omitempty"`
	TextBody     *string  `json:"textBody,omitempty"`
	RawData      *string  `json:"rawData,omitempty"`
	TemplateName *string  `json:"templateName,omitempty"`
	TemplateData *string  `json:"templateData,omitempty"`
	Timestamp    string   `json:"timestamp"`
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
