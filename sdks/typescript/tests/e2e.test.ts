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

    const { QueueUrl } = await sqs.send(
      new CreateQueueCommand({ QueueName: "test-queue" }),
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
