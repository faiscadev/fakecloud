import { describe, it, expect, beforeEach } from "vitest";
import { FakeCloud } from "../src/client.js";

// AWS SDK clients
import {
  SQSClient,
  CreateQueueCommand,
  SendMessageCommand,
  ListQueuesCommand,
} from "@aws-sdk/client-sqs";
import {
  SNSClient,
  CreateTopicCommand,
  SubscribeCommand,
  PublishCommand,
} from "@aws-sdk/client-sns";
import {
  SESv2Client,
  CreateEmailIdentityCommand,
  SendEmailCommand as SESv2SendEmailCommand,
} from "@aws-sdk/client-sesv2";
import {
  S3Client as AWSS3Client,
  CreateBucketCommand,
  PutObjectCommand,
} from "@aws-sdk/client-s3";
import {
  DynamoDBClient,
  CreateTableCommand,
  UpdateTimeToLiveCommand,
  PutItemCommand,
} from "@aws-sdk/client-dynamodb";
import {
  CognitoIdentityProviderClient,
  CreateUserPoolCommand,
  CreateUserPoolClientCommand,
  SignUpCommand,
  ForgotPasswordCommand,
} from "@aws-sdk/client-cognito-identity-provider";
import {
  EventBridgeClient,
  PutEventsCommand,
} from "@aws-sdk/client-eventbridge";
import {
  SchedulerClient as AwsSchedulerClient,
  CreateScheduleCommand,
  FlexibleTimeWindowMode,
} from "@aws-sdk/client-scheduler";
import {
  RDSClient,
  CreateDBInstanceCommand,
  DescribeDBInstancesCommand,
} from "@aws-sdk/client-rds";
import {
  ElastiCacheClient,
  CreateCacheClusterCommand,
  CreateReplicationGroupCommand,
  CreateServerlessCacheCommand,
} from "@aws-sdk/client-elasticache";

function getEndpoint(): string {
  const ep = process.env.FAKECLOUD_ENDPOINT;
  if (!ep)
    throw new Error("FAKECLOUD_ENDPOINT not set — is global setup running?");
  return ep;
}

const credentials = {
  accessKeyId: "AKIAIOSFODNN7EXAMPLE",
  secretAccessKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
};

function awsConfig() {
  return {
    endpoint: getEndpoint(),
    region: "us-east-1",
    credentials,
  };
}

function s3Config() {
  return {
    ...awsConfig(),
    forcePathStyle: true,
  };
}

let fc: FakeCloud;

beforeEach(async () => {
  fc = new FakeCloud(getEndpoint());
  await fc.reset();
});

// ── Health ──────────────────────────────────────────────────────────

describe("health", () => {
  it("returns server status and services list", async () => {
    const health = await fc.health();
    expect(health.status).toBe("ok");
    expect(health.version).toBeDefined();
    expect(Array.isArray(health.services)).toBe(true);
    expect(health.services.length).toBeGreaterThan(0);
  });
});

describe("rds", () => {
  // CreateDBInstance returns ~immediately with status `creating`. Poll
  // DescribeDBInstances until the container is up; the underlying image
  // pull/build still takes up to ~3 minutes on a cold runner.
  it("getInstances() returns fakecloud-managed DB instances", async () => {
    const rds = new RDSClient(awsConfig());

    await rds.send(
      new CreateDBInstanceCommand({
        DBInstanceIdentifier: "ts-rds-db",
        AllocatedStorage: 20,
        DBInstanceClass: "db.t3.micro",
        Engine: "postgres",
        EngineVersion: "16.3",
        MasterUsername: "admin",
        MasterUserPassword: "secret123",
        DBName: "appdb",
      }),
    );

    const deadline = Date.now() + 240_000;
    while (Date.now() < deadline) {
      const desc = await rds.send(
        new DescribeDBInstancesCommand({
          DBInstanceIdentifier: "ts-rds-db",
        }),
      );
      const status = desc.DBInstances?.[0]?.DBInstanceStatus;
      if (status === "available") break;
      await new Promise((r) => setTimeout(r, 1000));
    }

    const result = await fc.rds.getInstances();
    const instance = result.instances.find(
      (candidate) => candidate.dbInstanceIdentifier === "ts-rds-db",
    );
    expect(instance).toBeDefined();
    expect(instance!.engine).toBe("postgres");
    expect(instance!.dbName).toBe("appdb");
    expect(instance!.containerId.length).toBeGreaterThan(0);
    expect(instance!.hostPort).toBeGreaterThan(0);
  }, 300_000);
});

// ── ElastiCache ─────────────────────────────────────────────────────

describe("elasticache", () => {
  it("getClusters() returns fakecloud-managed cache clusters", async () => {
    const ec = new ElastiCacheClient(awsConfig());

    await ec.send(
      new CreateCacheClusterCommand({
        CacheClusterId: "ts-ec-cluster",
        CacheNodeType: "cache.t3.micro",
        Engine: "redis",
        EngineVersion: "7.1",
        NumCacheNodes: 1,
      }),
    );

    const result = await fc.elasticache.getClusters();
    const cluster = result.clusters.find(
      (c) => c.cacheClusterId === "ts-ec-cluster",
    );
    expect(cluster).toBeDefined();
    expect(cluster!.engine).toBe("redis");
    expect(cluster!.numCacheNodes).toBe(1);
    expect(cluster!.containerId).toBeDefined();
  });

  it("getReplicationGroups() returns fakecloud-managed replication groups", async () => {
    const ec = new ElastiCacheClient(awsConfig());

    await ec.send(
      new CreateReplicationGroupCommand({
        ReplicationGroupId: "ts-ec-rg",
        ReplicationGroupDescription: "TS test replication group",
        CacheNodeType: "cache.t3.micro",
        Engine: "redis",
        EngineVersion: "7.1",
        NumCacheClusters: 2,
      }),
    );

    const result = await fc.elasticache.getReplicationGroups();
    const group = result.replicationGroups.find(
      (g) => g.replicationGroupId === "ts-ec-rg",
    );
    expect(group).toBeDefined();
    expect(group!.engine).toBe("redis");
    expect(group!.numCacheClusters).toBe(2);
  });

  it("getServerlessCaches() returns fakecloud-managed serverless caches", async () => {
    const ec = new ElastiCacheClient(awsConfig());

    await ec.send(
      new CreateServerlessCacheCommand({
        ServerlessCacheName: "ts-ec-serverless",
        Engine: "redis",
        MajorEngineVersion: "7.1",
      }),
    );

    const result = await fc.elasticache.getServerlessCaches();
    const cache = result.serverlessCaches.find(
      (c) => c.serverlessCacheName === "ts-ec-serverless",
    );
    expect(cache).toBeDefined();
    expect(cache!.engine).toBe("redis");
    // The backing container starts asynchronously, so the cache is "creating"
    // right after create and transitions to "available" (bug-audit 3.2).
    expect(["creating", "available"]).toContain(cache!.status);
  });
});

// ── Reset ───────────────────────────────────────────────────────────

describe("reset", () => {
  it("clears all state", async () => {
    const sqs = new SQSClient(awsConfig());
    await sqs.send(new CreateQueueCommand({ QueueName: "reset-test-queue" }));

    // Verify queue exists
    const before = await sqs.send(new ListQueuesCommand({}));
    expect(before.QueueUrls?.length).toBeGreaterThan(0);

    // Reset
    await fc.reset();

    // Verify state is cleared
    const after = await sqs.send(new ListQueuesCommand({}));
    expect(after.QueueUrls ?? []).toHaveLength(0);
  });
});

// ── SQS ─────────────────────────────────────────────────────────────

describe("sqs", () => {
  it("getMessages() returns sent messages", async () => {
    const sqs = new SQSClient(awsConfig());

    // Disable SSE-SQS so the introspection endpoint surfaces the
    // plaintext body. Default queues encrypt at rest under
    // `alias/aws/sqs` (AWS default since May 2023).
    const { QueueUrl } = await sqs.send(
      new CreateQueueCommand({
        QueueName: "test-queue",
        Attributes: { SqsManagedSseEnabled: "false" },
      }),
    );
    expect(QueueUrl).toBeDefined();

    await sqs.send(
      new SendMessageCommand({
        QueueUrl: QueueUrl!,
        MessageBody: "hello from e2e",
      }),
    );

    const result = await fc.sqs.getMessages();
    expect(result.queues.length).toBeGreaterThan(0);

    const queue = result.queues.find((q) => q.queueName === "test-queue");
    expect(queue).toBeDefined();
    expect(queue!.messages.length).toBe(1);
    expect(queue!.messages[0].body).toBe("hello from e2e");
  });
});

// ── SNS ─────────────────────────────────────────────────────────────

describe("sns", () => {
  it("getMessages() returns published messages", async () => {
    const sns = new SNSClient(awsConfig());

    const { TopicArn } = await sns.send(
      new CreateTopicCommand({ Name: "test-topic" }),
    );
    expect(TopicArn).toBeDefined();

    await sns.send(
      new PublishCommand({
        TopicArn: TopicArn!,
        Message: "hello sns",
        Subject: "test subject",
      }),
    );

    const result = await fc.sns.getMessages();
    expect(result.messages.length).toBeGreaterThan(0);
    expect(result.messages[0].message).toBe("hello sns");
    expect(result.messages[0].topicArn).toBe(TopicArn);
  });

  it("getPendingConfirmations() returns unconfirmed HTTP subscriptions", async () => {
    const sns = new SNSClient(awsConfig());

    const { TopicArn } = await sns.send(
      new CreateTopicCommand({ Name: "confirm-topic" }),
    );

    await sns.send(
      new SubscribeCommand({
        TopicArn: TopicArn!,
        Protocol: "https",
        Endpoint: "https://example.com/webhook",
      }),
    );

    const result = await fc.sns.getPendingConfirmations();
    expect(result.pendingConfirmations.length).toBeGreaterThan(0);

    const sub = result.pendingConfirmations.find(
      (p) => p.endpoint === "https://example.com/webhook",
    );
    expect(sub).toBeDefined();
    expect(sub!.protocol).toBe("https");
  });
});

// ── SES ─────────────────────────────────────────────────────────────

describe("ses", () => {
  it("getEmails() returns sent emails", async () => {
    const ses = new SESv2Client(awsConfig());

    // Create email identity first (SES v2 API)
    await ses.send(
      new CreateEmailIdentityCommand({
        EmailIdentity: "sender@example.com",
      }),
    );

    await ses.send(
      new SESv2SendEmailCommand({
        FromEmailAddress: "sender@example.com",
        Destination: { ToAddresses: ["recipient@example.com"] },
        Content: {
          Simple: {
            Subject: { Data: "Test email" },
            Body: { Text: { Data: "Hello from e2e test" } },
          },
        },
      }),
    );

    const result = await fc.ses.getEmails();
    expect(result.emails.length).toBeGreaterThan(0);

    const email = result.emails.find((e) => e.subject === "Test email");
    expect(email).toBeDefined();
    expect(email!.from).toBe("sender@example.com");
    expect(email!.to).toContain("recipient@example.com");
  });
});

// ── S3 ──────────────────────────────────────────────────────────────

describe("s3", () => {
  it("getNotifications() works after uploading an object", async () => {
    const s3 = new AWSS3Client(s3Config());

    await s3.send(new CreateBucketCommand({ Bucket: "test-bucket" }));
    await s3.send(
      new PutObjectCommand({
        Bucket: "test-bucket",
        Key: "test-key.txt",
        Body: "hello s3",
      }),
    );

    // Notifications may be empty if no notification config is set — that's fine.
    // We just verify the introspection endpoint works.
    const result = await fc.s3.getNotifications();
    expect(Array.isArray(result.notifications)).toBe(true);
  });
});

// ── DynamoDB ────────────────────────────────────────────────────────

describe("dynamodb", () => {
  it("tickTtl() runs TTL processor", async () => {
    const ddb = new DynamoDBClient(awsConfig());

    await ddb.send(
      new CreateTableCommand({
        TableName: "ttl-table",
        KeySchema: [{ AttributeName: "pk", KeyType: "HASH" }],
        AttributeDefinitions: [{ AttributeName: "pk", AttributeType: "S" }],
        BillingMode: "PAY_PER_REQUEST",
      }),
    );

    await ddb.send(
      new UpdateTimeToLiveCommand({
        TableName: "ttl-table",
        TimeToLiveSpecification: {
          AttributeName: "ttl",
          Enabled: true,
        },
      }),
    );

    // Insert an item with an expired TTL (epoch 0 = long expired)
    await ddb.send(
      new PutItemCommand({
        TableName: "ttl-table",
        Item: {
          pk: { S: "item-1" },
          ttl: { N: "0" },
        },
      }),
    );

    const result = await fc.dynamodb.tickTtl();
    expect(typeof result.expiredItems).toBe("number");
    expect(result.expiredItems).toBeGreaterThanOrEqual(1);
  });
});

// ── Cognito ─────────────────────────────────────────────────────────

describe("cognito", () => {
  it("getConfirmationCodes() returns codes after ForgotPassword", async () => {
    const cognito = new CognitoIdentityProviderClient(awsConfig());

    const pool = await cognito.send(
      new CreateUserPoolCommand({
        PoolName: "test-pool",
        AutoVerifiedAttributes: ["email"],
      }),
    );
    const poolId = pool.UserPool!.Id!;

    const appClient = await cognito.send(
      new CreateUserPoolClientCommand({
        UserPoolId: poolId,
        ClientName: "test-client",
      }),
    );
    const clientId = appClient.UserPoolClient!.ClientId!;

    // Sign up user (no confirmation code generated yet)
    await cognito.send(
      new SignUpCommand({
        ClientId: clientId,
        Username: "testuser",
        Password: "Test1234!@",
        UserAttributes: [{ Name: "email", Value: "test@example.com" }],
      }),
    );

    // Force-confirm the user via introspection API so ForgotPassword works
    await fc.cognito.confirmUser({ userPoolId: poolId, username: "testuser" });

    // Trigger ForgotPassword to generate a confirmation code
    await cognito.send(
      new ForgotPasswordCommand({
        ClientId: clientId,
        Username: "testuser",
      }),
    );

    // Check all confirmation codes
    const allCodes = await fc.cognito.getConfirmationCodes();
    expect(allCodes.codes.length).toBeGreaterThan(0);

    const code = allCodes.codes.find((c) => c.username === "testuser");
    expect(code).toBeDefined();
    expect(code!.code).toBeDefined();

    // Check user-specific codes
    const userCodes = await fc.cognito.getUserCodes(poolId, "testuser");
    expect(userCodes.confirmationCode).toBeDefined();
    expect(typeof userCodes.confirmationCode).toBe("string");
  });
});

// ── EventBridge ─────────────────────────────────────────────────────

describe("eventbridge", () => {
  it("getHistory() returns put events", async () => {
    const eb = new EventBridgeClient(awsConfig());

    await eb.send(
      new PutEventsCommand({
        Entries: [
          {
            Source: "test.source",
            DetailType: "TestEvent",
            Detail: JSON.stringify({ key: "value" }),
          },
        ],
      }),
    );

    const result = await fc.events.getHistory();
    expect(result.events.length).toBeGreaterThan(0);

    const event = result.events.find((e) => e.source === "test.source");
    expect(event).toBeDefined();
    expect(event!.detailType).toBe("TestEvent");
    expect(JSON.parse(event!.detail)).toEqual({ key: "value" });
  });
});

// ── Scheduler (EventBridge Scheduler) ───────────────────────────────

describe("scheduler", () => {
  it("getSchedules() lists created schedules", async () => {
    const sched = new AwsSchedulerClient(awsConfig());
    const name = `ts-sdk-list-${Date.now()}`;
    await sched.send(
      new CreateScheduleCommand({
        Name: name,
        ScheduleExpression: "rate(1 hour)",
        FlexibleTimeWindow: { Mode: FlexibleTimeWindowMode.OFF },
        Target: {
          Arn: "arn:aws:sqs:us-east-1:000000000000:noop",
          RoleArn: "arn:aws:iam::000000000000:role/s",
        },
      }),
    );
    const resp = await fc.scheduler.getSchedules();
    const found = resp.schedules.find((s) => s.name === name);
    expect(found).toBeDefined();
    expect(found!.groupName).toBe("default");
    expect(found!.scheduleExpression).toBe("rate(1 hour)");
  });

  it("fireSchedule() echoes the schedule ARN", async () => {
    const sched = new AwsSchedulerClient(awsConfig());
    const sqs = new SQSClient(awsConfig());
    const q = await sqs.send(
      new CreateQueueCommand({
        QueueName: `ts-sdk-sched-${Date.now()}`,
      }),
    );
    const qUrl = q.QueueUrl!;
    const queueName = qUrl.split("/").pop()!;
    const arn = `arn:aws:sqs:us-east-1:000000000000:${queueName}`;
    const name = `ts-sdk-fire-${Date.now()}`;
    await sched.send(
      new CreateScheduleCommand({
        Name: name,
        ScheduleExpression: "rate(365 days)",
        FlexibleTimeWindow: { Mode: FlexibleTimeWindowMode.OFF },
        Target: {
          Arn: arn,
          RoleArn: "arn:aws:iam::000000000000:role/s",
          Input: '{"from":"sdk"}',
        },
      }),
    );
    const fired = await fc.scheduler.fireSchedule("default", name);
    expect(fired.scheduleArn).toContain(`schedule/default/${name}`);
    expect(fired.targetArn).toBe(arn);
  });
});

// ── Bedrock introspection ───────────────────────────────────────────

describe("bedrock", () => {
  it("setResponseRules / clearResponseRules round-trips", async () => {
    const set = await fc.bedrock.setResponseRules(
      "anthropic.claude-3-haiku-20240307-v1:0",
      [
        { promptContains: "spam:", response: '{"label":"spam"}' },
        { promptContains: null, response: '{"label":"ham"}' },
      ],
    );
    expect(set.status).toBe("ok");
    expect(set.modelId).toBe("anthropic.claude-3-haiku-20240307-v1:0");

    const cleared = await fc.bedrock.clearResponseRules(
      "anthropic.claude-3-haiku-20240307-v1:0",
    );
    expect(cleared.status).toBe("ok");
  });

  it("queueFault / getFaults / clearFaults", async () => {
    const queued = await fc.bedrock.queueFault({
      errorType: "ThrottlingException",
      message: "Rate exceeded",
      httpStatus: 429,
      count: 2,
      operation: "InvokeModel",
    });
    expect(queued.status).toBe("ok");

    const { faults } = await fc.bedrock.getFaults();
    expect(faults).toHaveLength(1);
    expect(faults[0].errorType).toBe("ThrottlingException");
    expect(faults[0].remaining).toBe(2);
    expect(faults[0].operation).toBe("InvokeModel");
    expect(faults[0].modelId).toBeNull();

    const cleared = await fc.bedrock.clearFaults();
    expect(cleared.status).toBe("ok");

    const { faults: after } = await fc.bedrock.getFaults();
    expect(after).toHaveLength(0);
  });

  it("getInvocations returns an error field (null for healthy calls)", async () => {
    const { invocations } = await fc.bedrock.getInvocations();
    // Fresh reset — no calls yet, but the shape must carry `error`.
    expect(Array.isArray(invocations)).toBe(true);
    for (const inv of invocations) {
      // error is string | null
      expect(inv.error === null || typeof inv.error === "string").toBe(true);
    }
  });
});
