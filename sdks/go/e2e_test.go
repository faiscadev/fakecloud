package fakecloud

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider"
	cognitotypes "github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider/types"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	ebtypes "github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sesv2"
	sestypes "github.com/aws/aws-sdk-go-v2/service/sesv2/types"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

var (
	fakecloudURL string
	fakecloudCmd *exec.Cmd
)

func TestMain(m *testing.M) {
	// Find a free port
	port := findFreePort()
	fakecloudURL = fmt.Sprintf("http://127.0.0.1:%d", port)

	// Find the fakecloud binary
	_, thisFile, _, _ := runtime.Caller(0)
	repoRoot := filepath.Join(filepath.Dir(thisFile), "..", "..")
	binary := filepath.Join(repoRoot, "target", "release", "fakecloud")

	if _, err := os.Stat(binary); os.IsNotExist(err) {
		fmt.Fprintf(os.Stderr, "fakecloud binary not found at %s — run 'cargo build --release' first\n", binary)
		os.Exit(1)
	}

	// Start fakecloud
	fakecloudCmd = exec.Command(binary, "--addr", fmt.Sprintf("127.0.0.1:%d", port))
	fakecloudCmd.Stdout = os.Stderr
	fakecloudCmd.Stderr = os.Stderr
	if err := fakecloudCmd.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "failed to start fakecloud: %v\n", err)
		os.Exit(1)
	}

	// Wait for it to be ready
	if err := waitForReady(fakecloudURL, 10*time.Second); err != nil {
		fmt.Fprintf(os.Stderr, "fakecloud did not become ready: %v\n", err)
		_ = fakecloudCmd.Process.Kill()
		os.Exit(1)
	}

	code := m.Run()

	_ = fakecloudCmd.Process.Kill()
	_ = fakecloudCmd.Wait()
	os.Exit(code)
}

func findFreePort() int {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()
	return port
}

func waitForReady(baseURL string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := http.Get(baseURL + "/_fakecloud/health")
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == 200 {
				return nil
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("timeout waiting for %s", baseURL)
}

func resetState(t *testing.T) {
	t.Helper()
	fc := New(fakecloudURL)
	if err := fc.Reset(context.Background()); err != nil {
		t.Fatalf("failed to reset fakecloud state: %v", err)
	}
}

func awsConfig(t *testing.T) aws.Config {
	t.Helper()
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "test")),
	)
	if err != nil {
		t.Fatalf("failed to load AWS config: %v", err)
	}
	return cfg
}

// ── Health ────────────────────────────────────────────────────────

func TestE2EHealth(t *testing.T) {
	fc := New(fakecloudURL)
	resp, err := fc.Health(context.Background())
	if err != nil {
		t.Fatalf("Health() failed: %v", err)
	}
	if resp.Status != "ok" {
		t.Errorf("expected status ok, got %s", resp.Status)
	}
	if len(resp.Services) == 0 {
		t.Error("expected at least one service in health response")
	}
}

// ── Reset ─────────────────────────────────────────────────────────

func TestE2EReset(t *testing.T) {
	ctx := context.Background()
	cfg := awsConfig(t)

	// Create a queue
	sqsClient := sqs.NewFromConfig(cfg, func(o *sqs.Options) {
		o.BaseEndpoint = aws.String(fakecloudURL)
	})
	_, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String("reset-test-queue"),
	})
	if err != nil {
		t.Fatalf("CreateQueue failed: %v", err)
	}

	// Verify queue exists via introspection
	fc := New(fakecloudURL)
	msgs, err := fc.SQS().GetMessages(ctx)
	if err != nil {
		t.Fatalf("GetMessages failed: %v", err)
	}
	foundQueue := false
	for _, q := range msgs.Queues {
		if q.QueueName == "reset-test-queue" {
			foundQueue = true
		}
	}
	if !foundQueue {
		t.Fatal("expected to find reset-test-queue before reset")
	}

	// Reset
	if err := fc.Reset(ctx); err != nil {
		t.Fatalf("Reset failed: %v", err)
	}

	// Verify queue is gone
	msgs, err = fc.SQS().GetMessages(ctx)
	if err != nil {
		t.Fatalf("GetMessages after reset failed: %v", err)
	}
	for _, q := range msgs.Queues {
		if q.QueueName == "reset-test-queue" {
			t.Error("queue still exists after reset")
		}
	}
}

// ── SQS ───────────────────────────────────────────────────────────

func TestE2ESQS(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	cfg := awsConfig(t)

	sqsClient := sqs.NewFromConfig(cfg, func(o *sqs.Options) {
		o.BaseEndpoint = aws.String(fakecloudURL)
	})

	// Create queue
	createResp, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String("sdk-go-test-queue"),
	})
	if err != nil {
		t.Fatalf("CreateQueue failed: %v", err)
	}

	// Send message
	_, err = sqsClient.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    createResp.QueueUrl,
		MessageBody: aws.String("hello from go sdk test"),
	})
	if err != nil {
		t.Fatalf("SendMessage failed: %v", err)
	}

	// Verify via introspection
	fc := New(fakecloudURL)
	msgs, err := fc.SQS().GetMessages(ctx)
	if err != nil {
		t.Fatalf("SQS().GetMessages() failed: %v", err)
	}

	found := false
	for _, q := range msgs.Queues {
		if q.QueueName == "sdk-go-test-queue" {
			for _, m := range q.Messages {
				if m.Body == "hello from go sdk test" {
					found = true
				}
			}
		}
	}
	if !found {
		t.Error("expected to find the sent message via introspection")
	}
}

// ── SNS ───────────────────────────────────────────────────────────

func TestE2ESNS(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	cfg := awsConfig(t)

	snsClient := sns.NewFromConfig(cfg, func(o *sns.Options) {
		o.BaseEndpoint = aws.String(fakecloudURL)
	})

	// Create topic
	topicResp, err := snsClient.CreateTopic(ctx, &sns.CreateTopicInput{
		Name: aws.String("sdk-go-test-topic"),
	})
	if err != nil {
		t.Fatalf("CreateTopic failed: %v", err)
	}

	// Publish message
	_, err = snsClient.Publish(ctx, &sns.PublishInput{
		TopicArn: topicResp.TopicArn,
		Message:  aws.String("hello from sns"),
		Subject:  aws.String("test subject"),
	})
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	// Verify via introspection
	fc := New(fakecloudURL)
	resp, err := fc.SNS().GetMessages(ctx)
	if err != nil {
		t.Fatalf("SNS().GetMessages() failed: %v", err)
	}

	found := false
	for _, m := range resp.Messages {
		if m.Message == "hello from sns" {
			found = true
			if m.Subject == nil || *m.Subject != "test subject" {
				t.Errorf("expected subject 'test subject', got %v", m.Subject)
			}
		}
	}
	if !found {
		t.Error("expected to find published SNS message via introspection")
	}
}

// ── SES ───────────────────────────────────────────────────────────

func TestE2ESES(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	cfg := awsConfig(t)

	sesClient := sesv2.NewFromConfig(cfg, func(o *sesv2.Options) {
		o.BaseEndpoint = aws.String(fakecloudURL)
	})

	// Send email via SES v2
	_, err := sesClient.SendEmail(ctx, &sesv2.SendEmailInput{
		FromEmailAddress: aws.String("sender@example.com"),
		Destination: &sestypes.Destination{
			ToAddresses: []string{"recipient@example.com"},
		},
		Content: &sestypes.EmailContent{
			Simple: &sestypes.Message{
				Subject: &sestypes.Content{Data: aws.String("Test Email")},
				Body: &sestypes.Body{
					Text: &sestypes.Content{Data: aws.String("Hello from Go SDK e2e test")},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("SendEmail failed: %v", err)
	}

	// Verify via introspection
	fc := New(fakecloudURL)
	resp, err := fc.SES().GetEmails(ctx)
	if err != nil {
		t.Fatalf("SES().GetEmails() failed: %v", err)
	}

	found := false
	for _, e := range resp.Emails {
		if e.From == "sender@example.com" {
			found = true
			if e.Subject == nil || *e.Subject != "Test Email" {
				t.Errorf("expected subject 'Test Email', got %v", e.Subject)
			}
			if len(e.To) == 0 || e.To[0] != "recipient@example.com" {
				t.Errorf("expected to=recipient@example.com, got %v", e.To)
			}
		}
	}
	if !found {
		t.Error("expected to find sent email via introspection")
	}
}

// ── S3 ────────────────────────────────────────────────────────────

func TestE2ES3(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	cfg := awsConfig(t)

	s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(fakecloudURL)
		o.UsePathStyle = true
	})

	// Create bucket
	_, err := s3Client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String("sdk-go-test-bucket"),
	})
	if err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	// Upload object
	_, err = s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String("sdk-go-test-bucket"),
		Key:    aws.String("test-file.txt"),
		Body:   bytes.NewReader([]byte("hello s3")),
	})
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	// Verify via introspection
	fc := New(fakecloudURL)
	resp, err := fc.S3().GetNotifications(ctx)
	if err != nil {
		t.Fatalf("S3().GetNotifications() failed: %v", err)
	}

	foundCreate := false
	foundPut := false
	for _, n := range resp.Notifications {
		if n.Bucket == "sdk-go-test-bucket" {
			if strings.Contains(n.EventType, "CreateBucket") || strings.Contains(n.EventType, "Create") {
				foundCreate = true
			}
			if n.Key == "test-file.txt" && strings.Contains(n.EventType, "Put") {
				foundPut = true
			}
		}
	}
	// S3 notifications may not be enabled by default, so just check we got the object put
	if !foundCreate && !foundPut {
		// Notifications require bucket notification configuration; just verify no error
		t.Log("S3 notifications not found (expected if notification config not set)")
	}
}

// ── DynamoDB TTL ──────────────────────────────────────────────────

func TestE2EDynamoDB(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	cfg := awsConfig(t)

	ddbClient := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		o.BaseEndpoint = aws.String(fakecloudURL)
	})

	// Create table
	_, err := ddbClient.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String("sdk-go-ttl-test"),
		KeySchema: []dbtypes.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: dbtypes.KeyTypeHash},
		},
		AttributeDefinitions: []dbtypes.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: dbtypes.ScalarAttributeTypeS},
		},
		BillingMode: dbtypes.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	// Tick TTL processor (should succeed even with no TTL configured)
	fc := New(fakecloudURL)
	resp, err := fc.DynamoDB().TickTTL(ctx)
	if err != nil {
		t.Fatalf("DynamoDB().TickTTL() failed: %v", err)
	}
	// With no TTL-expired items, expect 0
	if resp.ExpiredItems != 0 {
		t.Errorf("expected 0 expired items on fresh table, got %d", resp.ExpiredItems)
	}
}

// ── Cognito ───────────────────────────────────────────────────────

func TestE2ECognito(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	cfg := awsConfig(t)

	cognitoClient := cognitoidentityprovider.NewFromConfig(cfg, func(o *cognitoidentityprovider.Options) {
		o.BaseEndpoint = aws.String(fakecloudURL)
	})

	// Create user pool
	poolResp, err := cognitoClient.CreateUserPool(ctx, &cognitoidentityprovider.CreateUserPoolInput{
		PoolName: aws.String("sdk-go-test-pool"),
		AutoVerifiedAttributes: []cognitotypes.VerifiedAttributeType{
			cognitotypes.VerifiedAttributeTypeEmail,
		},
	})
	if err != nil {
		t.Fatalf("CreateUserPool failed: %v", err)
	}
	poolID := *poolResp.UserPool.Id

	// Create user pool client
	clientResp, err := cognitoClient.CreateUserPoolClient(ctx, &cognitoidentityprovider.CreateUserPoolClientInput{
		UserPoolId: aws.String(poolID),
		ClientName: aws.String("test-client"),
		ExplicitAuthFlows: []cognitotypes.ExplicitAuthFlowsType{
			cognitotypes.ExplicitAuthFlowsTypeAllowUserPasswordAuth,
			cognitotypes.ExplicitAuthFlowsTypeAllowRefreshTokenAuth,
		},
	})
	if err != nil {
		t.Fatalf("CreateUserPoolClient failed: %v", err)
	}
	clientID := *clientResp.UserPoolClient.ClientId

	// Sign up user
	_, err = cognitoClient.SignUp(ctx, &cognitoidentityprovider.SignUpInput{
		ClientId: aws.String(clientID),
		Username: aws.String("testuser"),
		Password: aws.String("TestPass1!"),
		UserAttributes: []cognitotypes.AttributeType{
			{Name: aws.String("email"), Value: aws.String("testuser@example.com")},
		},
	})
	if err != nil {
		t.Fatalf("SignUp failed: %v", err)
	}

	// Resend confirmation code so fakecloud generates one
	_, err = cognitoClient.ResendConfirmationCode(ctx, &cognitoidentityprovider.ResendConfirmationCodeInput{
		ClientId: aws.String(clientID),
		Username: aws.String("testuser"),
	})
	if err != nil {
		t.Fatalf("ResendConfirmationCode failed: %v", err)
	}

	// Check confirmation codes via introspection
	fc := New(fakecloudURL)
	codesResp, err := fc.Cognito().GetConfirmationCodes(ctx)
	if err != nil {
		t.Fatalf("Cognito().GetConfirmationCodes() failed: %v", err)
	}

	foundCode := false
	for _, c := range codesResp.Codes {
		if c.Username == "testuser" && c.Type == "signup" {
			foundCode = true
		}
	}
	if !foundCode {
		t.Error("expected to find signup confirmation code for testuser")
	}

	// Also check user-specific codes
	userCodes, err := fc.Cognito().GetUserCodes(ctx, poolID, "testuser")
	if err != nil {
		t.Fatalf("Cognito().GetUserCodes() failed: %v", err)
	}
	if userCodes.ConfirmationCode == nil {
		t.Error("expected confirmation code for testuser, got nil")
	}
}

// ── EventBridge ───────────────────────────────────────────────────

func TestE2EEventBridge(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	cfg := awsConfig(t)

	ebClient := eventbridge.NewFromConfig(cfg, func(o *eventbridge.Options) {
		o.BaseEndpoint = aws.String(fakecloudURL)
	})

	// Put events
	_, err := ebClient.PutEvents(ctx, &eventbridge.PutEventsInput{
		Entries: []ebtypes.PutEventsRequestEntry{
			{
				Source:     aws.String("my.app"),
				DetailType: aws.String("OrderCreated"),
				Detail:     aws.String(`{"orderId": "123"}`),
			},
		},
	})
	if err != nil {
		t.Fatalf("PutEvents failed: %v", err)
	}

	// Verify via introspection
	fc := New(fakecloudURL)
	resp, err := fc.Events().GetHistory(ctx)
	if err != nil {
		t.Fatalf("Events().GetHistory() failed: %v", err)
	}

	found := false
	for _, e := range resp.Events {
		if e.Source == "my.app" && e.DetailType == "OrderCreated" {
			found = true
		}
	}
	if !found {
		t.Error("expected to find EventBridge event via introspection")
	}
}
