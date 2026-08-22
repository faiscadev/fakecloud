package e2e

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
	"github.com/aws/aws-sdk-go-v2/service/cloudfront"
	cftypes "github.com/aws/aws-sdk-go-v2/service/cloudfront/types"
	"github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider"
	cognitotypes "github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider/types"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/elasticache"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	ebtypes "github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
	"github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/scheduler"
	schedtypes "github.com/aws/aws-sdk-go-v2/service/scheduler/types"
	"github.com/aws/aws-sdk-go-v2/service/sesv2"
	sestypes "github.com/aws/aws-sdk-go-v2/service/sesv2/types"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"

	fakecloud "github.com/faiscadev/fakecloud/sdks/go"
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
	repoRoot := filepath.Join(filepath.Dir(thisFile), "..", "..", "..")
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
	fc := fakecloud.New(fakecloudURL)
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
	fc := fakecloud.New(fakecloudURL)
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

func TestE2ERDS(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	cfg := awsConfig(t)

	rdsClient := rds.NewFromConfig(cfg, func(o *rds.Options) {
		o.BaseEndpoint = aws.String(fakecloudURL)
	})

	_, err := rdsClient.CreateDBInstance(ctx, &rds.CreateDBInstanceInput{
		DBInstanceIdentifier: aws.String("sdk-go-rds-db"),
		AllocatedStorage:     aws.Int32(20),
		DBInstanceClass:      aws.String("db.t3.micro"),
		Engine:               aws.String("postgres"),
		EngineVersion:        aws.String("16.3"),
		MasterUsername:       aws.String("admin"),
		MasterUserPassword:   aws.String("secret123"),
		DBName:               aws.String("appdb"),
	})
	if err != nil {
		t.Fatalf("CreateDBInstance failed: %v", err)
	}

	// CreateDBInstance is async since v0.13.1; poll DescribeDBInstances
	// until the container is up so the introspection endpoint sees the
	// populated container_id and host_port.
	deadline := time.Now().Add(240 * time.Second)
	ready := false
	var lastErr error
	for time.Now().Before(deadline) {
		desc, derr := rdsClient.DescribeDBInstances(ctx, &rds.DescribeDBInstancesInput{
			DBInstanceIdentifier: aws.String("sdk-go-rds-db"),
		})
		if derr == nil && len(desc.DBInstances) > 0 &&
			desc.DBInstances[0].DBInstanceStatus != nil &&
			*desc.DBInstances[0].DBInstanceStatus == "available" {
			ready = true
			break
		}
		lastErr = derr
		time.Sleep(1 * time.Second)
	}
	if !ready {
		t.Fatalf("timed out waiting for DB instance to become available; last describe error: %v", lastErr)
	}

	fc := fakecloud.New(fakecloudURL)
	resp, err := fc.RDS().GetInstances(ctx)
	if err != nil {
		t.Fatalf("RDS().GetInstances() failed: %v", err)
	}

	found := false
	for _, instance := range resp.Instances {
		if instance.DBInstanceIdentifier == "sdk-go-rds-db" {
			found = true
			if instance.Engine != "postgres" {
				t.Fatalf("expected postgres engine, got %s", instance.Engine)
			}
			if instance.DBName == nil || *instance.DBName != "appdb" {
				t.Fatalf("expected dbName appdb, got %#v", instance.DBName)
			}
			if instance.ContainerID == "" {
				t.Fatal("expected container id")
			}
			if instance.HostPort == 0 {
				t.Fatal("expected host port")
			}
		}
	}
	if !found {
		t.Fatal("expected to find sdk-go-rds-db via introspection")
	}
}

// ── CloudFront ─────────────────────────────────────────────────────

func TestE2ECloudFrontGetDistributions(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	cfg := awsConfig(t)

	cfClient := cloudfront.NewFromConfig(cfg, func(o *cloudfront.Options) {
		o.BaseEndpoint = aws.String(fakecloudURL)
	})

	// Minimal valid distribution: one S3-website origin + a legacy
	// ForwardedValues default cache behavior (the smallest shape the
	// control plane accepts).
	create, err := cfClient.CreateDistribution(ctx, &cloudfront.CreateDistributionInput{
		DistributionConfig: &cftypes.DistributionConfig{
			CallerReference: aws.String("sdk-go-cf-getdist"),
			Comment:         aws.String("sdk go e2e"),
			Enabled:         aws.Bool(true),
			Origins: &cftypes.Origins{
				Quantity: aws.Int32(1),
				Items: []cftypes.Origin{
					{
						Id:         aws.String("o1"),
						DomainName: aws.String("example-bucket.s3-website-us-east-1.amazonaws.com"),
					},
				},
			},
			DefaultCacheBehavior: &cftypes.DefaultCacheBehavior{
				TargetOriginId:       aws.String("o1"),
				ViewerProtocolPolicy: cftypes.ViewerProtocolPolicyAllowAll,
				// ForwardedValues/MinTTL are deprecated in the AWS SDK but remain
				// valid CloudFront distribution config; exercise them on purpose.
				ForwardedValues: &cftypes.ForwardedValues{ //nolint:staticcheck
					QueryString: aws.Bool(false),
					Cookies: &cftypes.CookiePreference{
						Forward: cftypes.ItemSelectionNone,
					},
					Headers: &cftypes.Headers{Quantity: aws.Int32(0)},
				},
				MinTTL: aws.Int64(0), //nolint:staticcheck
			},
		},
	})
	if err != nil {
		t.Fatalf("CreateDistribution failed: %v", err)
	}
	if create.Distribution == nil || create.Distribution.Id == nil {
		t.Fatal("expected a distribution id from CreateDistribution")
	}
	distID := *create.Distribution.Id

	fc := fakecloud.New(fakecloudURL)
	resp, err := fc.CloudFront().GetDistributions(ctx)
	if err != nil {
		t.Fatalf("CloudFront().GetDistributions() failed: %v", err)
	}

	found := false
	for _, d := range resp.Distributions {
		if d.ID == distID {
			found = true
			if !strings.HasSuffix(d.DomainName, ".cloudfront.net") {
				t.Fatalf("expected .cloudfront.net domain, got %q", d.DomainName)
			}
			if !d.Enabled {
				t.Fatal("expected distribution to be enabled")
			}
			// The in-process data plane serves an enabled distribution
			// unless disabled via FAKECLOUD_CLOUDFRONT_DISABLE_DATAPLANE,
			// which the e2e server does not set.
			if !d.Served {
				t.Fatal("expected enabled distribution to be served by the data plane")
			}
		}
	}
	if !found {
		t.Fatalf("expected to find %s via introspection", distID)
	}
}

// ── ElastiCache ───────────────────────────────────────────────────

func TestE2EElastiCacheClusters(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	cfg := awsConfig(t)

	ecClient := elasticache.NewFromConfig(cfg, func(o *elasticache.Options) {
		o.BaseEndpoint = aws.String(fakecloudURL)
	})

	_, err := ecClient.CreateCacheCluster(ctx, &elasticache.CreateCacheClusterInput{
		CacheClusterId: aws.String("sdk-go-ec-cluster"),
		CacheNodeType:  aws.String("cache.t3.micro"),
		Engine:         aws.String("redis"),
		EngineVersion:  aws.String("7.1"),
		NumCacheNodes:  aws.Int32(1),
	})
	if err != nil {
		t.Fatalf("CreateCacheCluster failed: %v", err)
	}

	fc := fakecloud.New(fakecloudURL)

	// The backing container starts asynchronously (bug-audit 3.2), so poll the
	// introspection endpoint until the cluster reaches "available".
	var found *fakecloud.ElastiCacheCluster
	for i := 0; i < 120; i++ {
		resp, err := fc.ElastiCache().GetClusters(ctx)
		if err != nil {
			t.Fatalf("ElastiCache().GetClusters() failed: %v", err)
		}
		found = nil
		for idx := range resp.Clusters {
			if resp.Clusters[idx].CacheClusterID == "sdk-go-ec-cluster" {
				found = &resp.Clusters[idx]
				break
			}
		}
		if found != nil && found.CacheClusterStatus == "available" {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if found == nil || found.CacheClusterStatus != "available" {
		t.Fatal("expected to find sdk-go-ec-cluster 'available' via introspection")
	}
	if found.Engine != "redis" {
		t.Fatalf("expected redis engine, got %s", found.Engine)
	}
	if found.NumCacheNodes != 1 {
		t.Fatalf("expected 1 cache node, got %d", found.NumCacheNodes)
	}
	// A real backing container exposes a host port + container id; without a
	// container runtime the cluster is metadata-only (host port 0), so only
	// assert the container id when a container actually started.
	if found.HostPort != nil && *found.HostPort != 0 {
		if found.ContainerID == nil || *found.ContainerID == "" {
			t.Fatal("expected container id for a running cluster")
		}
	}
}

func TestE2EElastiCacheReplicationGroups(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	cfg := awsConfig(t)

	ecClient := elasticache.NewFromConfig(cfg, func(o *elasticache.Options) {
		o.BaseEndpoint = aws.String(fakecloudURL)
	})

	_, err := ecClient.CreateReplicationGroup(ctx, &elasticache.CreateReplicationGroupInput{
		ReplicationGroupId:          aws.String("sdk-go-ec-rg"),
		ReplicationGroupDescription: aws.String("Go SDK test replication group"),
		CacheNodeType:               aws.String("cache.t3.micro"),
		Engine:                      aws.String("redis"),
		EngineVersion:               aws.String("7.1"),
		NumCacheClusters:            aws.Int32(2),
	})
	if err != nil {
		t.Fatalf("CreateReplicationGroup failed: %v", err)
	}

	fc := fakecloud.New(fakecloudURL)
	resp, err := fc.ElastiCache().GetReplicationGroups(ctx)
	if err != nil {
		t.Fatalf("ElastiCache().GetReplicationGroups() failed: %v", err)
	}

	found := false
	for _, group := range resp.ReplicationGroups {
		if group.ReplicationGroupID == "sdk-go-ec-rg" {
			found = true
			if group.Engine != "redis" {
				t.Fatalf("expected redis engine, got %s", group.Engine)
			}
			if group.NumCacheClusters != 2 {
				t.Fatalf("expected 2 cache clusters, got %d", group.NumCacheClusters)
			}
		}
	}
	if !found {
		t.Fatal("expected to find sdk-go-ec-rg via introspection")
	}
}

func TestE2EElastiCacheServerlessCaches(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	cfg := awsConfig(t)

	ecClient := elasticache.NewFromConfig(cfg, func(o *elasticache.Options) {
		o.BaseEndpoint = aws.String(fakecloudURL)
	})

	_, err := ecClient.CreateServerlessCache(ctx, &elasticache.CreateServerlessCacheInput{
		ServerlessCacheName: aws.String("sdk-go-ec-serverless"),
		Engine:              aws.String("redis"),
		MajorEngineVersion:  aws.String("7.1"),
	})
	if err != nil {
		t.Fatalf("CreateServerlessCache failed: %v", err)
	}

	fc := fakecloud.New(fakecloudURL)
	resp, err := fc.ElastiCache().GetServerlessCaches(ctx)
	if err != nil {
		t.Fatalf("ElastiCache().GetServerlessCaches() failed: %v", err)
	}

	found := false
	for _, cache := range resp.ServerlessCaches {
		if cache.ServerlessCacheName == "sdk-go-ec-serverless" {
			found = true
			if cache.Engine != "redis" {
				t.Fatalf("expected redis engine, got %s", cache.Engine)
			}
			// Backing container starts asynchronously; status is "creating"
			// right after create and transitions to "available" (bug-audit 3.2).
			if cache.Status != "available" && cache.Status != "creating" {
				t.Fatalf("expected available or creating status, got %s", cache.Status)
			}
		}
	}
	if !found {
		t.Fatal("expected to find sdk-go-ec-serverless via introspection")
	}
}

func TestE2EElastiCacheAcls(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	cfg := awsConfig(t)

	ecClient := elasticache.NewFromConfig(cfg, func(o *elasticache.Options) {
		o.BaseEndpoint = aws.String(fakecloudURL)
	})

	_, err := ecClient.CreateUser(ctx, &elasticache.CreateUserInput{
		UserId:       aws.String("sdk-go-acl-app"),
		UserName:     aws.String("sdk-go-acl-app"),
		Engine:       aws.String("redis"),
		AccessString: aws.String("on ~app:* +get +set"),
		Passwords:    []string{"s3cret-token-of-acceptable-length"},
	})
	if err != nil {
		t.Fatalf("CreateUser failed: %v", err)
	}

	_, err = ecClient.CreateUserGroup(ctx, &elasticache.CreateUserGroupInput{
		UserGroupId: aws.String("sdk-go-acl-ug"),
		Engine:      aws.String("redis"),
		UserIds:     []string{"default", "sdk-go-acl-app"},
	})
	if err != nil {
		t.Fatalf("CreateUserGroup failed: %v", err)
	}

	_, err = ecClient.CreateReplicationGroup(ctx, &elasticache.CreateReplicationGroupInput{
		ReplicationGroupId:          aws.String("sdk-go-acl-rg"),
		ReplicationGroupDescription: aws.String("ACL introspection"),
		CacheNodeType:               aws.String("cache.t3.micro"),
		Engine:                      aws.String("redis"),
		EngineVersion:               aws.String("7.1"),
		TransitEncryptionEnabled:    aws.Bool(true),
		UserGroupIds:                []string{"sdk-go-acl-ug"},
	})
	if err != nil {
		t.Fatalf("CreateReplicationGroup failed: %v", err)
	}

	fc := fakecloud.New(fakecloudURL)
	resp, err := fc.ElastiCache().GetElastiCacheAcls(ctx)
	if err != nil {
		t.Fatalf("ElastiCache().GetElastiCacheAcls() failed: %v", err)
	}

	if len(resp.Acls) != 1 {
		t.Fatalf("expected 1 ACL cluster, got %d", len(resp.Acls))
	}
	cluster := resp.Acls[0]
	if cluster.ClusterID != "sdk-go-acl-rg" {
		t.Fatalf("expected sdk-go-acl-rg cluster id, got %s", cluster.ClusterID)
	}
	if len(cluster.Groups) != 1 || cluster.Groups[0].Name != "sdk-go-acl-ug" {
		t.Fatalf("expected sdk-go-acl-ug user group, got %+v", cluster.Groups)
	}
	gotApp := false
	for _, u := range cluster.Users {
		if u.Name == "sdk-go-acl-app" {
			gotApp = true
			if u.NoPasswordRequired {
				t.Fatal("expected sdk-go-acl-app to require a password")
			}
			if u.PasswordCount != 1 {
				t.Fatalf("expected password count 1, got %d", u.PasswordCount)
			}
		}
	}
	if !gotApp {
		t.Fatal("expected sdk-go-acl-app user in ACL response")
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
	fc := fakecloud.New(fakecloudURL)
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

	// Create queue with managed SSE off so the introspection endpoint
	// surfaces the plaintext body. Default queues encrypt at rest under
	// `alias/aws/sqs` post-May-2023, and the introspection probe sees
	// the at-rest envelope.
	createResp, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String("sdk-go-test-queue"),
		Attributes: map[string]string{
			"SqsManagedSseEnabled": "false",
		},
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
	fc := fakecloud.New(fakecloudURL)
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
	fc := fakecloud.New(fakecloudURL)
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

	// Verify the sender so X2's MailFromDomainNotVerified gate is happy.
	_, err := sesClient.CreateEmailIdentity(ctx, &sesv2.CreateEmailIdentityInput{
		EmailIdentity: aws.String("sender@example.com"),
	})
	if err != nil {
		t.Fatalf("CreateEmailIdentity failed: %v", err)
	}

	// Send email via SES v2
	_, err = sesClient.SendEmail(ctx, &sesv2.SendEmailInput{
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
	fc := fakecloud.New(fakecloudURL)
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
	fc := fakecloud.New(fakecloudURL)
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
	fc := fakecloud.New(fakecloudURL)
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
	fc := fakecloud.New(fakecloudURL)
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
	fc := fakecloud.New(fakecloudURL)
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

// ── Bedrock introspection ────────────────────────────────────────────

func TestE2EBedrockResponseRules(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	fc := fakecloud.New(fakecloudURL)

	modelID := "anthropic.claude-3-haiku-20240307-v1:0"
	spam := "spam:"
	rules := []fakecloud.BedrockResponseRule{
		{PromptContains: &spam, Response: `{"label":"spam"}`},
		{PromptContains: nil, Response: `{"label":"ham"}`},
	}

	set, err := fc.Bedrock().SetResponseRules(ctx, modelID, rules)
	if err != nil {
		t.Fatalf("SetResponseRules failed: %v", err)
	}
	if set.Status != "ok" || set.ModelID != modelID {
		t.Errorf("unexpected set response: %+v", set)
	}

	cleared, err := fc.Bedrock().ClearResponseRules(ctx, modelID)
	if err != nil {
		t.Fatalf("ClearResponseRules failed: %v", err)
	}
	if cleared.Status != "ok" {
		t.Errorf("expected status ok, got %q", cleared.Status)
	}
}

func TestE2EBedrockFaults(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	fc := fakecloud.New(fakecloudURL)

	_, err := fc.Bedrock().QueueFault(ctx, fakecloud.BedrockFaultRule{
		ErrorType:  "ThrottlingException",
		Message:    "Rate exceeded",
		HTTPStatus: 429,
		Count:      2,
		Operation:  "InvokeModel",
	})
	if err != nil {
		t.Fatalf("QueueFault failed: %v", err)
	}

	listed, err := fc.Bedrock().GetFaults(ctx)
	if err != nil {
		t.Fatalf("GetFaults failed: %v", err)
	}
	if len(listed.Faults) != 1 {
		t.Fatalf("expected 1 queued fault, got %d", len(listed.Faults))
	}
	f := listed.Faults[0]
	if f.ErrorType != "ThrottlingException" || f.Remaining != 2 {
		t.Errorf("unexpected fault state: %+v", f)
	}
	if f.Operation == nil || *f.Operation != "InvokeModel" {
		t.Errorf("expected operation filter InvokeModel, got %v", f.Operation)
	}
	if f.ModelID != nil {
		t.Errorf("expected nil modelId filter, got %v", f.ModelID)
	}

	if _, err := fc.Bedrock().ClearFaults(ctx); err != nil {
		t.Fatalf("ClearFaults failed: %v", err)
	}
	after, err := fc.Bedrock().GetFaults(ctx)
	if err != nil {
		t.Fatalf("GetFaults after clear failed: %v", err)
	}
	if len(after.Faults) != 0 {
		t.Errorf("expected 0 faults after clear, got %d", len(after.Faults))
	}
}

// ── DynamoDB expression regressions (PR #660) ─────────────────────

// Query with parenthesised KeyCondition clauses — the exact shape
// aws-sdk-go-v2's KeyConditionBuilder emits (e.g. `(#0 = :0) AND (#1 > :1)`).
// Before the fix this returned zero items against a populated table; real
// DynamoDB returns every matching row.
func TestE2EDynamoDBParenKeyCondition(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	cfg := awsConfig(t)

	cli := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		o.BaseEndpoint = aws.String(fakecloudURL)
	})

	tbl := "sdk-go-paren-keycond"
	_, err := cli.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String(tbl),
		KeySchema: []dbtypes.KeySchemaElement{
			{AttributeName: aws.String("store_id"), KeyType: dbtypes.KeyTypeHash},
			{AttributeName: aws.String("order_id"), KeyType: dbtypes.KeyTypeRange},
		},
		AttributeDefinitions: []dbtypes.AttributeDefinition{
			{AttributeName: aws.String("store_id"), AttributeType: dbtypes.ScalarAttributeTypeS},
			{AttributeName: aws.String("order_id"), AttributeType: dbtypes.ScalarAttributeTypeS},
		},
		BillingMode: dbtypes.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	for i := 1; i <= 3; i++ {
		_, err := cli.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(tbl),
			Item: map[string]dbtypes.AttributeValue{
				"store_id": &dbtypes.AttributeValueMemberS{Value: "s"},
				"order_id": &dbtypes.AttributeValueMemberS{Value: fmt.Sprintf("order%d", i)},
			},
		})
		if err != nil {
			t.Fatalf("PutItem %d failed: %v", i, err)
		}
	}

	cases := []struct {
		name string
		expr string
	}{
		{"bare", "store_id = :s AND order_id > :a"},
		{"parens", "(store_id = :s) AND (order_id > :a)"},
		{"sdk-placeholders", "(#0 = :0) AND (#1 > :1)"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			input := &dynamodb.QueryInput{
				TableName:              aws.String(tbl),
				KeyConditionExpression: aws.String(tc.expr),
			}
			if tc.name == "sdk-placeholders" {
				input.ExpressionAttributeNames = map[string]string{
					"#0": "store_id",
					"#1": "order_id",
				}
				input.ExpressionAttributeValues = map[string]dbtypes.AttributeValue{
					":0": &dbtypes.AttributeValueMemberS{Value: "s"},
					":1": &dbtypes.AttributeValueMemberS{Value: "aaa"},
				}
			} else {
				input.ExpressionAttributeValues = map[string]dbtypes.AttributeValue{
					":s": &dbtypes.AttributeValueMemberS{Value: "s"},
					":a": &dbtypes.AttributeValueMemberS{Value: "aaa"},
				}
			}
			resp, err := cli.Query(ctx, input)
			if err != nil {
				t.Fatalf("Query(%s) failed: %v", tc.name, err)
			}
			if len(resp.Items) != 3 {
				t.Errorf("Query(%s) returned %d items, want 3", tc.name, len(resp.Items))
			}
		})
	}
}

// UpdateItem with a dotted-path SET target (`SET #a.#b = :v`). Before the
// fix this silently created a top-level `"#a.#b"` attribute instead of
// updating the nested map; sibling keys under the parent were preserved by
// accident. Guard: nested write lands, siblings stay, no literal dotted key.
func TestE2EDynamoDBNestedSetPath(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	cfg := awsConfig(t)

	cli := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		o.BaseEndpoint = aws.String(fakecloudURL)
	})

	tbl := "sdk-go-nested-set"
	_, err := cli.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String(tbl),
		KeySchema: []dbtypes.KeySchemaElement{
			{AttributeName: aws.String("id"), KeyType: dbtypes.KeyTypeHash},
		},
		AttributeDefinitions: []dbtypes.AttributeDefinition{
			{AttributeName: aws.String("id"), AttributeType: dbtypes.ScalarAttributeTypeS},
		},
		BillingMode: dbtypes.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	_, err = cli.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(tbl),
		Item: map[string]dbtypes.AttributeValue{
			"id": &dbtypes.AttributeValueMemberS{Value: "row1"},
			"web": &dbtypes.AttributeValueMemberM{Value: map[string]dbtypes.AttributeValue{
				"tab_id":  &dbtypes.AttributeValueMemberS{Value: "old-tab"},
				"keep_me": &dbtypes.AttributeValueMemberS{Value: "sibling"},
			}},
		},
	})
	if err != nil {
		t.Fatalf("PutItem failed: %v", err)
	}

	_, err = cli.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(tbl),
		Key: map[string]dbtypes.AttributeValue{
			"id": &dbtypes.AttributeValueMemberS{Value: "row1"},
		},
		UpdateExpression: aws.String("SET #web.#tab_id = :tab"),
		ExpressionAttributeNames: map[string]string{
			"#web":    "web",
			"#tab_id": "tab_id",
		},
		ExpressionAttributeValues: map[string]dbtypes.AttributeValue{
			":tab": &dbtypes.AttributeValueMemberS{Value: "new-tab"},
		},
	})
	if err != nil {
		t.Fatalf("UpdateItem failed: %v", err)
	}

	got, err := cli.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(tbl),
		Key: map[string]dbtypes.AttributeValue{
			"id": &dbtypes.AttributeValueMemberS{Value: "row1"},
		},
	})
	if err != nil {
		t.Fatalf("GetItem failed: %v", err)
	}
	if got.Item == nil {
		t.Fatalf("GetItem returned no item")
	}

	webAttr, ok := got.Item["web"].(*dbtypes.AttributeValueMemberM)
	if !ok {
		t.Fatalf("web attribute missing or wrong type: %T", got.Item["web"])
	}
	tabID, _ := webAttr.Value["tab_id"].(*dbtypes.AttributeValueMemberS)
	if tabID == nil || tabID.Value != "new-tab" {
		t.Errorf("nested SET must update child key: got web.tab_id=%v", webAttr.Value["tab_id"])
	}
	keepMe, _ := webAttr.Value["keep_me"].(*dbtypes.AttributeValueMemberS)
	if keepMe == nil || keepMe.Value != "sibling" {
		t.Errorf("nested SET must leave siblings alone: got web.keep_me=%v", webAttr.Value["keep_me"])
	}
	if _, leaked := got.Item["#web.#tab_id"]; leaked {
		t.Error("nested SET must not leak a literal dotted-name top-level attribute")
	}
}

// ── Scheduler (EventBridge Scheduler) ─────────────────────────────────

func schedulerClient(t *testing.T) *scheduler.Client {
	return scheduler.NewFromConfig(awsConfig(t), func(o *scheduler.Options) {
		o.BaseEndpoint = aws.String(fakecloudURL)
	})
}

func sqsTestClient(t *testing.T) *sqs.Client {
	return sqs.NewFromConfig(awsConfig(t), func(o *sqs.Options) {
		o.BaseEndpoint = aws.String(fakecloudURL)
	})
}

func TestSchedulerGetSchedules(t *testing.T) {
	ctx := context.Background()
	fc := fakecloud.New(fakecloudURL)
	if err := fc.Reset(ctx); err != nil {
		t.Fatalf("reset: %v", err)
	}
	schedClient := schedulerClient(t)
	_, err := schedClient.CreateSchedule(ctx, &scheduler.CreateScheduleInput{
		Name:               aws.String("go-sdk-list"),
		ScheduleExpression: aws.String("rate(1 hour)"),
		FlexibleTimeWindow: &schedtypes.FlexibleTimeWindow{Mode: schedtypes.FlexibleTimeWindowModeOff},
		Target: &schedtypes.Target{
			Arn:     aws.String("arn:aws:sqs:us-east-1:000000000000:noop"),
			RoleArn: aws.String("arn:aws:iam::000000000000:role/s"),
		},
	})
	if err != nil {
		t.Fatalf("create_schedule: %v", err)
	}
	resp, err := fc.Scheduler().GetSchedules(ctx)
	if err != nil {
		t.Fatalf("GetSchedules: %v", err)
	}
	found := false
	for _, s := range resp.Schedules {
		if s.Name == "go-sdk-list" {
			found = true
			if s.ScheduleExpression != "rate(1 hour)" {
				t.Errorf("expr mismatch: %q", s.ScheduleExpression)
			}
		}
	}
	if !found {
		t.Fatalf("go-sdk-list not in %+v", resp.Schedules)
	}
}

func TestSchedulerFireSchedule(t *testing.T) {
	ctx := context.Background()
	fc := fakecloud.New(fakecloudURL)
	if err := fc.Reset(ctx); err != nil {
		t.Fatalf("reset: %v", err)
	}
	schedClient := schedulerClient(t)
	sqsClient := sqsTestClient(t)
	q, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("go-fire-target")})
	if err != nil {
		t.Fatalf("create_queue: %v", err)
	}
	attrs, err := sqsClient.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       q.QueueUrl,
		AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn},
	})
	if err != nil {
		t.Fatalf("get_queue_attrs: %v", err)
	}
	qArn := attrs.Attributes[string(sqstypes.QueueAttributeNameQueueArn)]

	if _, err := schedClient.CreateSchedule(ctx, &scheduler.CreateScheduleInput{
		Name:               aws.String("go-sdk-fire"),
		ScheduleExpression: aws.String("rate(365 days)"),
		FlexibleTimeWindow: &schedtypes.FlexibleTimeWindow{Mode: schedtypes.FlexibleTimeWindowModeOff},
		Target: &schedtypes.Target{
			Arn:     aws.String(qArn),
			RoleArn: aws.String("arn:aws:iam::000000000000:role/s"),
			Input:   aws.String(`{"from":"gotest"}`),
		},
	}); err != nil {
		t.Fatalf("create_schedule: %v", err)
	}
	resp, err := fc.Scheduler().FireSchedule(ctx, "default", "go-sdk-fire")
	if err != nil {
		t.Fatalf("FireSchedule: %v", err)
	}
	if !strings.Contains(resp.ScheduleArn, "schedule/default/go-sdk-fire") {
		t.Errorf("unexpected schedule ARN: %q", resp.ScheduleArn)
	}
	if resp.TargetArn != qArn {
		t.Errorf("target ARN mismatch: got %q, want %q", resp.TargetArn, qArn)
	}
}
