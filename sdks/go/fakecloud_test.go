package fakecloud

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// helper to create a test server that returns the given JSON for a specific path and method.
func newTestServer(t *testing.T, method, path string, response interface{}) (*httptest.Server, *FakeCloud) {
	t.Helper()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != method {
			t.Errorf("expected method %s, got %s", method, r.Method)
		}
		if r.URL.Path != path {
			t.Errorf("expected path %s, got %s", path, r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	}))
	fc := New(ts.URL)
	return ts, fc
}

func TestHealth(t *testing.T) {
	ts, fc := newTestServer(t, "GET", "/_fakecloud/health", HealthResponse{
		Status:   "ok",
		Version:  "1.0.0",
		Services: []string{"sqs", "sns", "s3"},
	})
	defer ts.Close()

	resp, err := fc.Health(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Status != "ok" {
		t.Errorf("expected status ok, got %s", resp.Status)
	}
	if resp.Version != "1.0.0" {
		t.Errorf("expected version 1.0.0, got %s", resp.Version)
	}
	if len(resp.Services) != 3 {
		t.Errorf("expected 3 services, got %d", len(resp.Services))
	}
}

func TestReset(t *testing.T) {
	ts, fc := newTestServer(t, "POST", "/_reset", ResetResponse{Status: "ok"})
	defer ts.Close()

	err := fc.Reset(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestResetService(t *testing.T) {
	ts, fc := newTestServer(t, "POST", "/_fakecloud/reset/sqs", ResetServiceResponse{Reset: "sqs"})
	defer ts.Close()

	resp, err := fc.ResetService(context.Background(), "sqs")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Reset != "sqs" {
		t.Errorf("expected reset sqs, got %s", resp.Reset)
	}
}

func TestAPIError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("internal error"))
	}))
	defer ts.Close()

	fc := New(ts.URL)
	_, err := fc.Health(context.Background())
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	apiErr, ok := err.(*APIError)
	if !ok {
		t.Fatalf("expected *APIError, got %T", err)
	}
	if apiErr.StatusCode != 500 {
		t.Errorf("expected status 500, got %d", apiErr.StatusCode)
	}
	if apiErr.Body != "internal error" {
		t.Errorf("expected body 'internal error', got %q", apiErr.Body)
	}
}

func TestSESGetEmails(t *testing.T) {
	subject := "Hello"
	ts, fc := newTestServer(t, "GET", "/_fakecloud/ses/emails", SESEmailsResponse{
		Emails: []SentEmail{
			{
				MessageID: "msg-1",
				From:      "a@b.com",
				To:        []string{"c@d.com"},
				Subject:   &subject,
				Timestamp: "2024-01-01T00:00:00Z",
			},
		},
	})
	defer ts.Close()

	resp, err := fc.SES().GetEmails(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.Emails) != 1 {
		t.Fatalf("expected 1 email, got %d", len(resp.Emails))
	}
	if resp.Emails[0].From != "a@b.com" {
		t.Errorf("expected from a@b.com, got %s", resp.Emails[0].From)
	}
	if *resp.Emails[0].Subject != "Hello" {
		t.Errorf("expected subject Hello, got %s", *resp.Emails[0].Subject)
	}
}

func TestSESSimulateInbound(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			t.Errorf("expected POST, got %s", r.Method)
		}
		if r.URL.Path != "/_fakecloud/ses/inbound" {
			t.Errorf("expected path /_fakecloud/ses/inbound, got %s", r.URL.Path)
		}
		if r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("expected content-type application/json, got %s", r.Header.Get("Content-Type"))
		}
		var req InboundEmailRequest
		json.NewDecoder(r.Body).Decode(&req)
		if req.From != "sender@test.com" {
			t.Errorf("expected from sender@test.com, got %s", req.From)
		}
		json.NewEncoder(w).Encode(InboundEmailResponse{
			MessageID:    "msg-2",
			MatchedRules: []string{"rule-1"},
			ActionsExecuted: []InboundActionExecuted{
				{Rule: "rule-1", ActionType: "Lambda"},
			},
		})
	}))
	defer ts.Close()

	fc := New(ts.URL)
	resp, err := fc.SES().SimulateInbound(context.Background(), &InboundEmailRequest{
		From:    "sender@test.com",
		To:      []string{"recv@test.com"},
		Subject: "Test",
		Body:    "body",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.MessageID != "msg-2" {
		t.Errorf("expected message id msg-2, got %s", resp.MessageID)
	}
	if len(resp.ActionsExecuted) != 1 {
		t.Fatalf("expected 1 action, got %d", len(resp.ActionsExecuted))
	}
}

func TestSNSGetMessages(t *testing.T) {
	ts, fc := newTestServer(t, "GET", "/_fakecloud/sns/messages", SNSMessagesResponse{
		Messages: []SNSMessage{
			{MessageID: "m1", TopicArn: "arn:aws:sns:us-east-1:000000000000:topic1", Message: "hello", Timestamp: "2024-01-01T00:00:00Z"},
		},
	})
	defer ts.Close()

	resp, err := fc.SNS().GetMessages(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.Messages) != 1 {
		t.Fatalf("expected 1 message, got %d", len(resp.Messages))
	}
	if resp.Messages[0].Message != "hello" {
		t.Errorf("expected message 'hello', got %s", resp.Messages[0].Message)
	}
}

func TestSQSGetMessages(t *testing.T) {
	ts, fc := newTestServer(t, "GET", "/_fakecloud/sqs/messages", SQSMessagesResponse{
		Queues: []SQSQueueMessages{
			{
				QueueURL:  "http://localhost:4566/000000000000/q1",
				QueueName: "q1",
				Messages: []SQSMessageInfo{
					{MessageID: "m1", Body: "body1", ReceiveCount: 0, InFlight: false, CreatedAt: "2024-01-01T00:00:00Z"},
				},
			},
		},
	})
	defer ts.Close()

	resp, err := fc.SQS().GetMessages(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.Queues) != 1 {
		t.Fatalf("expected 1 queue, got %d", len(resp.Queues))
	}
	if resp.Queues[0].QueueName != "q1" {
		t.Errorf("expected queue name q1, got %s", resp.Queues[0].QueueName)
	}
}

func TestSQSForceDLQ(t *testing.T) {
	ts, fc := newTestServer(t, "POST", "/_fakecloud/sqs/my-queue/force-dlq", ForceDLQResponse{MovedMessages: 5})
	defer ts.Close()

	resp, err := fc.SQS().ForceDLQ(context.Background(), "my-queue")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.MovedMessages != 5 {
		t.Errorf("expected 5 moved messages, got %d", resp.MovedMessages)
	}
}

func TestEventsGetHistory(t *testing.T) {
	ts, fc := newTestServer(t, "GET", "/_fakecloud/events/history", EventHistoryResponse{
		Events: []EventBridgeEvent{
			{EventID: "e1", Source: "my.app", DetailType: "OrderCreated", Detail: "{}", BusName: "default", Timestamp: "2024-01-01T00:00:00Z"},
		},
		Deliveries: EventBridgeDeliveries{
			Lambda: []EventBridgeLambdaDelivery{},
			Logs:   []EventBridgeLogDelivery{},
		},
	})
	defer ts.Close()

	resp, err := fc.Events().GetHistory(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.Events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(resp.Events))
	}
	if resp.Events[0].Source != "my.app" {
		t.Errorf("expected source my.app, got %s", resp.Events[0].Source)
	}
}

func TestEventsFireRule(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			t.Errorf("expected POST, got %s", r.Method)
		}
		if r.URL.Path != "/_fakecloud/events/fire-rule" {
			t.Errorf("expected path /_fakecloud/events/fire-rule, got %s", r.URL.Path)
		}
		var req FireRuleRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatal(err)
		}
		if req.RuleName != "my-rule" {
			t.Errorf("expected rule name my-rule, got %s", req.RuleName)
		}
		if err := json.NewEncoder(w).Encode(FireRuleResponse{
			Targets: []FireRuleTarget{{Type: "lambda", Arn: "arn:aws:lambda:us-east-1:000000000000:function:fn"}},
		}); err != nil {
			t.Fatal(err)
		}
	}))
	defer ts.Close()

	fc := New(ts.URL)
	resp, err := fc.Events().FireRule(context.Background(), &FireRuleRequest{RuleName: "my-rule"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.Targets) != 1 {
		t.Fatalf("expected 1 target, got %d", len(resp.Targets))
	}
	if resp.Targets[0].Type != "lambda" {
		t.Errorf("expected target type lambda, got %s", resp.Targets[0].Type)
	}
}

func TestS3GetNotifications(t *testing.T) {
	ts, fc := newTestServer(t, "GET", "/_fakecloud/s3/notifications", S3NotificationsResponse{
		Notifications: []S3Notification{
			{Bucket: "my-bucket", Key: "file.txt", EventType: "s3:ObjectCreated:Put", Timestamp: "2024-01-01T00:00:00Z"},
		},
	})
	defer ts.Close()

	resp, err := fc.S3().GetNotifications(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.Notifications) != 1 {
		t.Fatalf("expected 1 notification, got %d", len(resp.Notifications))
	}
	if resp.Notifications[0].Bucket != "my-bucket" {
		t.Errorf("expected bucket my-bucket, got %s", resp.Notifications[0].Bucket)
	}
}

func TestLambdaGetInvocations(t *testing.T) {
	ts, fc := newTestServer(t, "GET", "/_fakecloud/lambda/invocations", LambdaInvocationsResponse{
		Invocations: []LambdaInvocation{
			{FunctionArn: "arn:aws:lambda:us-east-1:000000000000:function:fn", Payload: "{}", Source: "api", Timestamp: "2024-01-01T00:00:00Z"},
		},
	})
	defer ts.Close()

	resp, err := fc.Lambda().GetInvocations(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.Invocations) != 1 {
		t.Fatalf("expected 1 invocation, got %d", len(resp.Invocations))
	}
}

func TestLambdaEvictContainer(t *testing.T) {
	ts, fc := newTestServer(t, "POST", "/_fakecloud/lambda/my-fn/evict-container", EvictContainerResponse{Evicted: true})
	defer ts.Close()

	resp, err := fc.Lambda().EvictContainer(context.Background(), "my-fn")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !resp.Evicted {
		t.Error("expected evicted to be true")
	}
}

func TestDynamoDBTickTTL(t *testing.T) {
	ts, fc := newTestServer(t, "POST", "/_fakecloud/dynamodb/ttl-processor/tick", TTLTickResponse{ExpiredItems: 3})
	defer ts.Close()

	resp, err := fc.DynamoDB().TickTTL(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.ExpiredItems != 3 {
		t.Errorf("expected 3 expired items, got %d", resp.ExpiredItems)
	}
}

func TestSecretsManagerTickRotation(t *testing.T) {
	ts, fc := newTestServer(t, "POST", "/_fakecloud/secretsmanager/rotation-scheduler/tick", RotationTickResponse{
		RotatedSecrets: []string{"secret-1", "secret-2"},
	})
	defer ts.Close()

	resp, err := fc.SecretsManager().TickRotation(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.RotatedSecrets) != 2 {
		t.Fatalf("expected 2 rotated secrets, got %d", len(resp.RotatedSecrets))
	}
}

func TestCognitoGetConfirmationCodes(t *testing.T) {
	ts, fc := newTestServer(t, "GET", "/_fakecloud/cognito/confirmation-codes", ConfirmationCodesResponse{
		Codes: []ConfirmationCode{
			{PoolID: "pool-1", Username: "alice", Code: "123456", Type: "signup"},
		},
	})
	defer ts.Close()

	resp, err := fc.Cognito().GetConfirmationCodes(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.Codes) != 1 {
		t.Fatalf("expected 1 code, got %d", len(resp.Codes))
	}
	if resp.Codes[0].Username != "alice" {
		t.Errorf("expected username alice, got %s", resp.Codes[0].Username)
	}
}

func TestCognitoConfirmUser(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req ConfirmUserRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatal(err)
		}
		if req.Username != "bob" {
			t.Errorf("expected username bob, got %s", req.Username)
		}
		json.NewEncoder(w).Encode(ConfirmUserResponse{Confirmed: true})
	}))
	defer ts.Close()

	fc := New(ts.URL)
	resp, err := fc.Cognito().ConfirmUser(context.Background(), &ConfirmUserRequest{
		UserPoolID: "pool-1",
		Username:   "bob",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !resp.Confirmed {
		t.Error("expected confirmed to be true")
	}
}

func TestCognitoConfirmUserNotFound(t *testing.T) {
	errMsg := "user not found"
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(ConfirmUserResponse{Confirmed: false, Error: &errMsg})
	}))
	defer ts.Close()

	fc := New(ts.URL)
	_, err := fc.Cognito().ConfirmUser(context.Background(), &ConfirmUserRequest{
		UserPoolID: "pool-1",
		Username:   "ghost",
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	apiErr, ok := err.(*APIError)
	if !ok {
		t.Fatalf("expected *APIError, got %T", err)
	}
	if apiErr.StatusCode != 404 {
		t.Errorf("expected status 404, got %d", apiErr.StatusCode)
	}
}

func TestCognitoGetTokens(t *testing.T) {
	ts, fc := newTestServer(t, "GET", "/_fakecloud/cognito/tokens", TokensResponse{
		Tokens: []TokenInfo{
			{Type: "access", Username: "alice", PoolID: "pool-1", ClientID: "client-1", IssuedAt: 1704067200},
		},
	})
	defer ts.Close()

	resp, err := fc.Cognito().GetTokens(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.Tokens) != 1 {
		t.Fatalf("expected 1 token, got %d", len(resp.Tokens))
	}
	if resp.Tokens[0].Type != "access" {
		t.Errorf("expected token type access, got %s", resp.Tokens[0].Type)
	}
}

func TestCognitoExpireTokens(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req ExpireTokensRequest
		json.NewDecoder(r.Body).Decode(&req)
		json.NewEncoder(w).Encode(ExpireTokensResponse{ExpiredTokens: 2})
	}))
	defer ts.Close()

	fc := New(ts.URL)
	resp, err := fc.Cognito().ExpireTokens(context.Background(), &ExpireTokensRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.ExpiredTokens != 2 {
		t.Errorf("expected 2 expired tokens, got %d", resp.ExpiredTokens)
	}
}

func TestCognitoGetAuthEvents(t *testing.T) {
	ts, fc := newTestServer(t, "GET", "/_fakecloud/cognito/auth-events", AuthEventsResponse{
		Events: []AuthEvent{
			{EventType: "SignIn", Username: "alice", UserPoolID: "pool-1", Timestamp: 1704067200, Success: true},
		},
	})
	defer ts.Close()

	resp, err := fc.Cognito().GetAuthEvents(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.Events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(resp.Events))
	}
	if !resp.Events[0].Success {
		t.Error("expected success to be true")
	}
}

func TestNewTrimsTrailingSlash(t *testing.T) {
	fc := New("http://localhost:4566/")
	if fc.BaseURL != "http://localhost:4566" {
		t.Errorf("expected trailing slash trimmed, got %s", fc.BaseURL)
	}
}

func TestSNSConfirmSubscription(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			t.Errorf("expected POST, got %s", r.Method)
		}
		if r.URL.Path != "/_fakecloud/sns/confirm-subscription" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		var req ConfirmSubscriptionRequest
		json.NewDecoder(r.Body).Decode(&req)
		if req.SubscriptionArn != "arn:aws:sns:us-east-1:000000000000:topic1:sub-1" {
			t.Errorf("unexpected subscription arn: %s", req.SubscriptionArn)
		}
		json.NewEncoder(w).Encode(ConfirmSubscriptionResponse{Confirmed: true})
	}))
	defer ts.Close()

	fc := New(ts.URL)
	resp, err := fc.SNS().ConfirmSubscription(context.Background(), &ConfirmSubscriptionRequest{
		SubscriptionArn: "arn:aws:sns:us-east-1:000000000000:topic1:sub-1",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !resp.Confirmed {
		t.Error("expected confirmed to be true")
	}
}

func TestS3TickLifecycle(t *testing.T) {
	ts, fc := newTestServer(t, "POST", "/_fakecloud/s3/lifecycle-processor/tick", LifecycleTickResponse{
		ProcessedBuckets:    2,
		ExpiredObjects:      5,
		TransitionedObjects: 1,
	})
	defer ts.Close()

	resp, err := fc.S3().TickLifecycle(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.ProcessedBuckets != 2 {
		t.Errorf("expected 2 processed buckets, got %d", resp.ProcessedBuckets)
	}
	if resp.ExpiredObjects != 5 {
		t.Errorf("expected 5 expired objects, got %d", resp.ExpiredObjects)
	}
}

func TestSQSTickExpiration(t *testing.T) {
	ts, fc := newTestServer(t, "POST", "/_fakecloud/sqs/expiration-processor/tick", ExpirationTickResponse{ExpiredMessages: 7})
	defer ts.Close()

	resp, err := fc.SQS().TickExpiration(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.ExpiredMessages != 7 {
		t.Errorf("expected 7 expired messages, got %d", resp.ExpiredMessages)
	}
}

func TestLambdaGetWarmContainers(t *testing.T) {
	ts, fc := newTestServer(t, "GET", "/_fakecloud/lambda/warm-containers", WarmContainersResponse{
		Containers: []WarmContainer{
			{FunctionName: "fn1", Runtime: "nodejs18.x", ContainerID: "abc123", LastUsedSecsAgo: 30},
		},
	})
	defer ts.Close()

	resp, err := fc.Lambda().GetWarmContainers(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.Containers) != 1 {
		t.Fatalf("expected 1 container, got %d", len(resp.Containers))
	}
	if resp.Containers[0].Runtime != "nodejs18.x" {
		t.Errorf("expected runtime nodejs18.x, got %s", resp.Containers[0].Runtime)
	}
}

func TestCognitoGetUserCodes(t *testing.T) {
	code := "654321"
	ts, fc := newTestServer(t, "GET", "/_fakecloud/cognito/confirmation-codes/pool-1/alice", UserConfirmationCodes{
		ConfirmationCode:           &code,
		AttributeVerificationCodes: map[string]interface{}{"email": "111111"},
	})
	defer ts.Close()

	resp, err := fc.Cognito().GetUserCodes(context.Background(), "pool-1", "alice")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.ConfirmationCode == nil || *resp.ConfirmationCode != "654321" {
		t.Errorf("expected confirmation code 654321, got %v", resp.ConfirmationCode)
	}
}
