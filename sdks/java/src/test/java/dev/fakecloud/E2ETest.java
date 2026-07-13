package dev.fakecloud;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.fakecloud.Types.BedrockFaultRule;
import dev.fakecloud.Types.BedrockResponseRule;
import dev.fakecloud.Types.ConfirmUserRequest;

import java.net.URI;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudfront.CloudFrontClient;
import software.amazon.awssdk.services.cloudfront.model.CookiePreference;
import software.amazon.awssdk.services.cloudfront.model.CreateDistributionRequest;
import software.amazon.awssdk.services.cloudfront.model.DefaultCacheBehavior;
import software.amazon.awssdk.services.cloudfront.model.DistributionConfig;
import software.amazon.awssdk.services.cloudfront.model.ForwardedValues;
import software.amazon.awssdk.services.cloudfront.model.Headers;
import software.amazon.awssdk.services.cloudfront.model.ItemSelection;
import software.amazon.awssdk.services.cloudfront.model.Origin;
import software.amazon.awssdk.services.cloudfront.model.Origins;
import software.amazon.awssdk.services.cloudfront.model.ViewerProtocolPolicy;
import software.amazon.awssdk.services.cognitoidentityprovider.CognitoIdentityProviderClient;
import software.amazon.awssdk.services.cognitoidentityprovider.model.AttributeType;
import software.amazon.awssdk.services.cognitoidentityprovider.model.CreateUserPoolClientRequest;
import software.amazon.awssdk.services.cognitoidentityprovider.model.CreateUserPoolRequest;
import software.amazon.awssdk.services.cognitoidentityprovider.model.ForgotPasswordRequest;
import software.amazon.awssdk.services.cognitoidentityprovider.model.SignUpRequest;
import software.amazon.awssdk.services.cognitoidentityprovider.model.VerifiedAttributeType;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.TimeToLiveSpecification;
import software.amazon.awssdk.services.dynamodb.model.UpdateTimeToLiveRequest;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.RunInstancesRequest;
import software.amazon.awssdk.services.elasticache.ElastiCacheClient;
import software.amazon.awssdk.services.elasticache.model.CreateCacheClusterRequest;
import software.amazon.awssdk.services.elasticache.model.CreateReplicationGroupRequest;
import software.amazon.awssdk.services.elasticache.model.CreateServerlessCacheRequest;
import software.amazon.awssdk.services.eventbridge.EventBridgeClient;
import software.amazon.awssdk.services.eventbridge.model.PutEventsRequest;
import software.amazon.awssdk.services.eventbridge.model.PutEventsRequestEntry;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.CreateDbInstanceRequest;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.sesv2.SesV2Client;
import software.amazon.awssdk.services.sesv2.model.Body;
import software.amazon.awssdk.services.sesv2.model.Content;
import software.amazon.awssdk.services.sesv2.model.CreateEmailIdentityRequest;
import software.amazon.awssdk.services.sesv2.model.Destination;
import software.amazon.awssdk.services.sesv2.model.EmailContent;
import software.amazon.awssdk.services.sesv2.model.Message;
import software.amazon.awssdk.services.sesv2.model.SendEmailRequest;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.CreateTopicRequest;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import software.amazon.awssdk.services.sns.model.SubscribeRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.ListQueuesRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

@ExtendWith(FakeCloudServer.class)
class E2ETest {

    private static final AwsBasicCredentials CREDS =
            AwsBasicCredentials.create("AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");

    private FakeCloud fc;

    @BeforeEach
    void resetState() {
        fc = new FakeCloud(FakeCloudServer.endpoint());
        fc.reset();
    }

    private static URI endpoint() {
        return URI.create(FakeCloudServer.endpoint());
    }

    private static <T extends software.amazon.awssdk.awscore.client.builder.AwsClientBuilder<?, ?>> T configure(T b) {
        b.region(Region.US_EAST_1);
        b.credentialsProvider(StaticCredentialsProvider.create(CREDS));
        b.endpointOverride(endpoint());
        return b;
    }

    // ── Health ─────────────────────────────────────────────────────
    @Test
    void healthReturnsServerStatusAndServices() {
        var h = fc.health();
        assertEquals("ok", h.status());
        assertNotNull(h.version());
        assertFalse(h.services().isEmpty());
    }

    // ── Reset ──────────────────────────────────────────────────────
    @Test
    void resetClearsAllState() {
        SqsClient sqs = configure(SqsClient.builder()).build();
        sqs.createQueue(CreateQueueRequest.builder().queueName("reset-test-queue").build());
        var before = sqs.listQueues(ListQueuesRequest.builder().build());
        assertTrue(before.queueUrls() != null && !before.queueUrls().isEmpty());

        fc.reset();

        var after = sqs.listQueues(ListQueuesRequest.builder().build());
        assertTrue(after.queueUrls() == null || after.queueUrls().isEmpty());
    }

    // ── SQS ────────────────────────────────────────────────────────
    @Test
    void sqsGetMessagesReturnsSentMessages() {
        SqsClient sqs = configure(SqsClient.builder()).build();
        // Disable SSE-SQS so the introspection endpoint surfaces the
        // plaintext body. Default queues encrypt at rest under
        // `alias/aws/sqs` (AWS default since May 2023) and the body
        // would be the at-rest envelope.
        var q = sqs.createQueue(CreateQueueRequest.builder()
                .queueName("test-queue")
                .attributesWithStrings(java.util.Map.of("SqsManagedSseEnabled", "false"))
                .build());
        sqs.sendMessage(SendMessageRequest.builder()
                .queueUrl(q.queueUrl())
                .messageBody("hello from e2e")
                .build());

        var result = fc.sqs().getMessages();
        assertFalse(result.queues().isEmpty());
        var queue = result.queues().stream()
                .filter(qq -> "test-queue".equals(qq.queueName()))
                .findFirst()
                .orElseThrow();
        assertEquals(1, queue.messages().size());
        assertEquals("hello from e2e", queue.messages().get(0).body());
    }

    // ── SNS ────────────────────────────────────────────────────────
    @Test
    void snsGetMessagesReturnsPublishedMessages() {
        SnsClient sns = configure(SnsClient.builder()).build();
        var topic = sns.createTopic(CreateTopicRequest.builder().name("test-topic").build());
        sns.publish(PublishRequest.builder()
                .topicArn(topic.topicArn())
                .message("hello sns")
                .subject("test subject")
                .build());

        var result = fc.sns().getMessages();
        assertFalse(result.messages().isEmpty());
        assertEquals("hello sns", result.messages().get(0).message());
        assertEquals(topic.topicArn(), result.messages().get(0).topicArn());
    }

    @Test
    void snsGetPendingConfirmationsReturnsUnconfirmedHttpSubscriptions() {
        SnsClient sns = configure(SnsClient.builder()).build();
        var topic = sns.createTopic(CreateTopicRequest.builder().name("confirm-topic").build());
        sns.subscribe(SubscribeRequest.builder()
                .topicArn(topic.topicArn())
                .protocol("https")
                .endpoint("https://example.com/webhook")
                .build());

        var result = fc.sns().getPendingConfirmations();
        var sub = result.pendingConfirmations().stream()
                .filter(p -> "https://example.com/webhook".equals(p.endpoint()))
                .findFirst()
                .orElseThrow();
        assertEquals("https", sub.protocol());
    }

    // ── SES ────────────────────────────────────────────────────────
    @Test
    void sesGetEmailsReturnsSentEmails() {
        SesV2Client ses = configure(SesV2Client.builder()).build();
        ses.createEmailIdentity(CreateEmailIdentityRequest.builder()
                .emailIdentity("sender@example.com")
                .build());
        ses.sendEmail(SendEmailRequest.builder()
                .fromEmailAddress("sender@example.com")
                .destination(Destination.builder().toAddresses("recipient@example.com").build())
                .content(EmailContent.builder()
                        .simple(Message.builder()
                                .subject(Content.builder().data("Test email").build())
                                .body(Body.builder()
                                        .text(Content.builder().data("Hello from e2e test").build())
                                        .build())
                                .build())
                        .build())
                .build());

        var result = fc.ses().getEmails();
        assertFalse(result.emails().isEmpty());
        var email = result.emails().stream()
                .filter(e -> "Test email".equals(e.subject()))
                .findFirst()
                .orElseThrow();
        assertEquals("sender@example.com", email.from());
        assertTrue(email.to().contains("recipient@example.com"));
    }

    // ── S3 ─────────────────────────────────────────────────────────
    @Test
    void s3GetNotificationsWorksAfterUpload() {
        S3Client s3 = configure(S3Client.builder())
                .serviceConfiguration(S3Configuration.builder()
                        .pathStyleAccessEnabled(true)
                        .checksumValidationEnabled(false)
                        .build())
                .build();
        s3.createBucket(CreateBucketRequest.builder().bucket("test-bucket").build());
        s3.putObject(
                PutObjectRequest.builder().bucket("test-bucket").key("test-key.txt").build(),
                RequestBody.fromString("hello s3"));

        var result = fc.s3().getNotifications();
        assertNotNull(result.notifications());
    }

    // ── DynamoDB ───────────────────────────────────────────────────
    @Test
    void dynamodbTickTtlRunsProcessor() {
        DynamoDbClient ddb = configure(DynamoDbClient.builder()).build();
        ddb.createTable(CreateTableRequest.builder()
                .tableName("ttl-table")
                .keySchema(KeySchemaElement.builder().attributeName("pk").keyType(KeyType.HASH).build())
                .attributeDefinitions(AttributeDefinition.builder()
                        .attributeName("pk")
                        .attributeType(ScalarAttributeType.S)
                        .build())
                .billingMode(BillingMode.PAY_PER_REQUEST)
                .build());
        ddb.updateTimeToLive(UpdateTimeToLiveRequest.builder()
                .tableName("ttl-table")
                .timeToLiveSpecification(TimeToLiveSpecification.builder()
                        .attributeName("ttl")
                        .enabled(true)
                        .build())
                .build());
        ddb.putItem(PutItemRequest.builder()
                .tableName("ttl-table")
                .item(Map.of(
                        "pk", AttributeValue.builder().s("item-1").build(),
                        "ttl", AttributeValue.builder().n("0").build()))
                .build());

        var result = fc.dynamodb().tickTtl();
        assertTrue(result.expiredItems() >= 1);
    }

    // ── Cognito ────────────────────────────────────────────────────
    @Test
    void cognitoGetConfirmationCodesAfterForgotPassword() {
        CognitoIdentityProviderClient cognito = configure(CognitoIdentityProviderClient.builder()).build();
        var pool = cognito.createUserPool(CreateUserPoolRequest.builder()
                .poolName("test-pool")
                .autoVerifiedAttributes(VerifiedAttributeType.EMAIL)
                .build());
        String poolId = pool.userPool().id();
        var client = cognito.createUserPoolClient(CreateUserPoolClientRequest.builder()
                .userPoolId(poolId)
                .clientName("test-client")
                .build());
        String clientId = client.userPoolClient().clientId();

        cognito.signUp(SignUpRequest.builder()
                .clientId(clientId)
                .username("testuser")
                .password("Test1234!@")
                .userAttributes(AttributeType.builder().name("email").value("test@example.com").build())
                .build());

        fc.cognito().confirmUser(new ConfirmUserRequest(poolId, "testuser"));

        cognito.forgotPassword(ForgotPasswordRequest.builder()
                .clientId(clientId)
                .username("testuser")
                .build());

        var codes = fc.cognito().getConfirmationCodes();
        assertFalse(codes.codes().isEmpty());
        var code = codes.codes().stream()
                .filter(c -> "testuser".equals(c.username()))
                .findFirst()
                .orElseThrow();
        assertNotNull(code.code());

        var userCodes = fc.cognito().getUserCodes(poolId, "testuser");
        assertNotNull(userCodes.confirmationCode());
    }

    @Test
    void cognitoConfirmUserOn404ThrowsFakeCloudError() {
        // Create a pool so we have a valid pool ID — but a missing username.
        CognitoIdentityProviderClient cognito = configure(CognitoIdentityProviderClient.builder()).build();
        var pool = cognito.createUserPool(CreateUserPoolRequest.builder().poolName("p").build());
        String poolId = pool.userPool().id();

        FakeCloudError err = assertThrows(
                FakeCloudError.class,
                () -> fc.cognito().confirmUser(new ConfirmUserRequest(poolId, "nobody-here")));
        assertEquals(404, err.status());
    }

    // ── EventBridge ────────────────────────────────────────────────
    @Test
    void eventbridgeGetHistoryReturnsPutEvents() {
        EventBridgeClient eb = configure(EventBridgeClient.builder()).build();
        eb.putEvents(PutEventsRequest.builder()
                .entries(PutEventsRequestEntry.builder()
                        .source("test.source")
                        .detailType("TestEvent")
                        .detail("{\"key\":\"value\"}")
                        .build())
                .build());

        var result = fc.events().getHistory();
        var event = result.events().stream()
                .filter(e -> "test.source".equals(e.source()))
                .findFirst()
                .orElseThrow();
        assertEquals("TestEvent", event.detailType());
    }

    // ── RDS ────────────────────────────────────────────────────────
    @Test
    void rdsGetInstancesReturnsManagedInstances() throws Exception {
        RdsClient rds = configure(RdsClient.builder()).build();
        rds.createDBInstance(CreateDbInstanceRequest.builder()
                .dbInstanceIdentifier("java-rds-db")
                .allocatedStorage(20)
                .dbInstanceClass("db.t3.micro")
                .engine("postgres")
                .engineVersion("16.3")
                .masterUsername("admin")
                .masterUserPassword("secret123")
                .dbName("appdb")
                .build());

        // CreateDBInstance returns a `creating` placeholder; poll until
        // the container is up so the introspection endpoint reports the
        // populated host_port / container_id.
        long deadline = System.currentTimeMillis() + 240_000;
        while (System.currentTimeMillis() < deadline) {
            var desc = rds.describeDBInstances(b -> b.dbInstanceIdentifier("java-rds-db"));
            if (!desc.dbInstances().isEmpty()
                    && "available".equals(desc.dbInstances().get(0).dbInstanceStatus())) {
                break;
            }
            Thread.sleep(1000);
        }

        var instance = fc.rds().getInstances().instances().stream()
                .filter(i -> "java-rds-db".equals(i.dbInstanceIdentifier()))
                .findFirst()
                .orElseThrow();
        assertEquals("postgres", instance.engine());
        assertEquals("appdb", instance.dbName());
        assertTrue(instance.hostPort() > 0);
    }

    // ── EC2 ────────────────────────────────────────────────────────
    @Test
    void ec2GetInstancesReturnsManagedInstances() {
        Ec2Client ec2 = configure(Ec2Client.builder()).build();
        var run = ec2.runInstances(RunInstancesRequest.builder()
                .imageId("ami-12345678")
                .instanceType("t3.micro")
                .minCount(1)
                .maxCount(1)
                .build());
        String instanceId = run.instances().get(0).instanceId();

        var instance = fc.ec2().getInstances().instances().stream()
                .filter(i -> instanceId.equals(i.instanceId()))
                .findFirst()
                .orElseThrow();
        assertEquals("ami-12345678", instance.imageId());
        assertEquals("t3.micro", instance.instanceType());
        assertNotNull(instance.state());
    }

    // ── ElastiCache ────────────────────────────────────────────────
    @Test
    void elasticacheGetClustersReturnsManagedClusters() {
        ElastiCacheClient ec = configure(ElastiCacheClient.builder()).build();
        ec.createCacheCluster(CreateCacheClusterRequest.builder()
                .cacheClusterId("java-ec-cluster")
                .cacheNodeType("cache.t3.micro")
                .engine("redis")
                .engineVersion("7.1")
                .numCacheNodes(1)
                .build());

        var cluster = fc.elasticache().getClusters().clusters().stream()
                .filter(c -> "java-ec-cluster".equals(c.cacheClusterId()))
                .findFirst()
                .orElseThrow();
        assertEquals("redis", cluster.engine());
        assertEquals(1, cluster.numCacheNodes());
    }

    @Test
    void elasticacheGetReplicationGroupsReturnsManagedGroups() {
        ElastiCacheClient ec = configure(ElastiCacheClient.builder()).build();
        ec.createReplicationGroup(CreateReplicationGroupRequest.builder()
                .replicationGroupId("java-ec-rg")
                .replicationGroupDescription("Java test replication group")
                .cacheNodeType("cache.t3.micro")
                .engine("redis")
                .engineVersion("7.1")
                .numCacheClusters(2)
                .build());

        var group = fc.elasticache().getReplicationGroups().replicationGroups().stream()
                .filter(g -> "java-ec-rg".equals(g.replicationGroupId()))
                .findFirst()
                .orElseThrow();
        assertEquals("redis", group.engine());
        assertEquals(2, group.numCacheClusters());
    }

    @Test
    void elasticacheGetServerlessCachesReturnsManagedCaches() {
        ElastiCacheClient ec = configure(ElastiCacheClient.builder()).build();
        ec.createServerlessCache(CreateServerlessCacheRequest.builder()
                .serverlessCacheName("java-ec-serverless")
                .engine("redis")
                .majorEngineVersion("7.1")
                .build());

        var cache = fc.elasticache().getServerlessCaches().serverlessCaches().stream()
                .filter(c -> "java-ec-serverless".equals(c.serverlessCacheName()))
                .findFirst()
                .orElseThrow();
        assertEquals("redis", cache.engine());
        // Backing container starts asynchronously; status is "creating" right
        // after create and transitions to "available" (bug-audit 3.2).
        assertTrue(
                "creating".equals(cache.status()) || "available".equals(cache.status()),
                "unexpected status: " + cache.status());
    }

    // ── CloudFront ─────────────────────────────────────────────────
    @Test
    void cloudfrontGetDistributionsReturnsManagedDistributions() {
        CloudFrontClient cf = configure(CloudFrontClient.builder()).build();
        var config = DistributionConfig.builder()
                .callerReference("java-cf-" + System.nanoTime())
                .comment("java e2e")
                .enabled(true)
                .origins(Origins.builder()
                        .quantity(1)
                        .items(Origin.builder()
                                .id("o1")
                                .domainName("example-bucket.s3-website-us-east-1.amazonaws.com")
                                .build())
                        .build())
                .defaultCacheBehavior(DefaultCacheBehavior.builder()
                        .targetOriginId("o1")
                        .viewerProtocolPolicy(ViewerProtocolPolicy.ALLOW_ALL)
                        .forwardedValues(ForwardedValues.builder()
                                .queryString(false)
                                .cookies(CookiePreference.builder()
                                        .forward(ItemSelection.NONE)
                                        .build())
                                .headers(Headers.builder().quantity(0).build())
                                .build())
                        .minTTL(0L)
                        .build())
                .build();
        var created = cf.createDistribution(CreateDistributionRequest.builder()
                .distributionConfig(config)
                .build());
        String id = created.distribution().id();

        var dist = fc.cloudfront().getDistributions().distributions().stream()
                .filter(d -> id.equals(d.id()))
                .findFirst()
                .orElseThrow();
        assertTrue(dist.enabled());
        assertNotNull(dist.domainName());
        assertTrue(
                dist.domainName().endsWith(".cloudfront.net"),
                "unexpected domainName: " + dist.domainName());
        // The distribution is enabled and the data plane is on by default, so
        // the in-process data plane serves it.
        assertTrue(dist.served());
    }

    // ── Bedrock introspection ──────────────────────────────────────
    @Test
    void bedrockSetAndClearResponseRulesRoundTrips() {
        String modelId = "anthropic.claude-3-haiku-20240307-v1:0";
        var set = fc.bedrock().setResponseRules(
                modelId,
                List.of(
                        new BedrockResponseRule("spam:", "{\"label\":\"spam\"}"),
                        new BedrockResponseRule(null, "{\"label\":\"ham\"}")));
        assertEquals("ok", set.status());
        assertEquals(modelId, set.modelId());

        var cleared = fc.bedrock().clearResponseRules(modelId);
        assertEquals("ok", cleared.status());
    }

    @Test
    void bedrockQueueGetClearFaultsRoundTrips() {
        var queued = fc.bedrock().queueFault(new BedrockFaultRule(
                "ThrottlingException", "Rate exceeded", 429, 2, null, "InvokeModel"));
        assertEquals("ok", queued.status());

        var list = fc.bedrock().getFaults();
        assertEquals(1, list.faults().size());
        assertEquals("ThrottlingException", list.faults().get(0).errorType());
        assertEquals(2, list.faults().get(0).remaining());
        assertEquals("InvokeModel", list.faults().get(0).operation());
        assertNull(list.faults().get(0).modelId());

        var cleared = fc.bedrock().clearFaults();
        assertEquals("ok", cleared.status());

        assertEquals(0, fc.bedrock().getFaults().faults().size());
    }

    @Test
    void bedrockGetInvocationsReturnsErrorField() {
        var result = fc.bedrock().getInvocations();
        assertNotNull(result.invocations());
    }
}
