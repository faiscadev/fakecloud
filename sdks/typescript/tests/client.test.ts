import { describe, it, expect, vi, beforeEach } from "vitest";
import { FakeCloud, FakeCloudError } from "../src/client.js";

const BASE = "http://localhost:4566";

function mockFetch(body: unknown, status = 200): void {
  vi.stubGlobal(
    "fetch",
    vi.fn().mockResolvedValue({
      ok: status >= 200 && status < 300,
      status,
      json: () => Promise.resolve(body),
      text: () => Promise.resolve(JSON.stringify(body)),
    }),
  );
}

function lastFetchCall(): { url: string; init?: RequestInit } {
  const mock = fetch as ReturnType<typeof vi.fn>;
  const [url, init] = mock.mock.calls[mock.mock.calls.length - 1];
  return { url, init };
}

beforeEach(() => {
  vi.restoreAllMocks();
});

// ── FakeCloud (top-level) ──────────────────────────────────────────

describe("FakeCloud", () => {
  it("strips trailing slashes from base URL", () => {
    const fc = new FakeCloud("http://example.com///");
    mockFetch({ status: "ok", version: "0.1.0", services: [] });
    fc.health();
    expect(lastFetchCall().url).toBe("http://example.com/_fakecloud/health");
  });

  it("defaults to localhost:4566", () => {
    const fc = new FakeCloud();
    mockFetch({ status: "ok", version: "0.1.0", services: [] });
    fc.health();
    expect(lastFetchCall().url).toBe("http://localhost:4566/_fakecloud/health");
  });

  it("health() returns parsed response", async () => {
    const fc = new FakeCloud(BASE);
    const body = { status: "ok", version: "1.0.0", services: ["sqs", "sns"] };
    mockFetch(body);
    const result = await fc.health();
    expect(result).toEqual(body);
  });

  it("reset() sends POST to /_reset", async () => {
    const fc = new FakeCloud(BASE);
    mockFetch({ status: "ok" });
    await fc.reset();
    const call = lastFetchCall();
    expect(call.url).toBe(`${BASE}/_reset`);
    expect(call.init?.method).toBe("POST");
  });

  it("resetService() sends POST with service name", async () => {
    const fc = new FakeCloud(BASE);
    mockFetch({ reset: "sqs" });
    const result = await fc.resetService("sqs");
    expect(result.reset).toBe("sqs");
    expect(lastFetchCall().url).toBe(`${BASE}/_fakecloud/reset/sqs`);
  });

  it("throws FakeCloudError on non-OK response", async () => {
    const fc = new FakeCloud(BASE);
    mockFetch("not found", 404);
    await expect(fc.health()).rejects.toThrow(FakeCloudError);
    await expect(fc.health()).rejects.toMatchObject({ status: 404 });
  });
});

// ── Lambda ─────────────────────────────────────────────────────────

describe("LambdaClient", () => {
  it("getInvocations() fetches correct URL", async () => {
    const fc = new FakeCloud(BASE);
    mockFetch({ invocations: [] });
    const result = await fc.lambda.getInvocations();
    expect(lastFetchCall().url).toBe(`${BASE}/_fakecloud/lambda/invocations`);
    expect(result.invocations).toEqual([]);
  });

  it("getWarmContainers() fetches correct URL", async () => {
    const fc = new FakeCloud(BASE);
    mockFetch({ containers: [] });
    await fc.lambda.getWarmContainers();
    expect(lastFetchCall().url).toBe(
      `${BASE}/_fakecloud/lambda/warm-containers`,
    );
  });

  it("evictContainer() sends POST with function name", async () => {
    const fc = new FakeCloud(BASE);
    mockFetch({ evicted: true });
    const result = await fc.lambda.evictContainer("my-func");
    expect(result.evicted).toBe(true);
    expect(lastFetchCall().url).toBe(
      `${BASE}/_fakecloud/lambda/my-func/evict-container`,
    );
    expect(lastFetchCall().init?.method).toBe("POST");
  });
});

// ── SES ────────────────────────────────────────────────────────────

describe("SesClient", () => {
  it("getEmails() fetches correct URL", async () => {
    const fc = new FakeCloud(BASE);
    mockFetch({ emails: [] });
    const result = await fc.ses.getEmails();
    expect(result.emails).toEqual([]);
    expect(lastFetchCall().url).toBe(`${BASE}/_fakecloud/ses/emails`);
  });

  it("simulateInbound() sends POST with JSON body", async () => {
    const fc = new FakeCloud(BASE);
    const req = {
      from: "a@b.com",
      to: ["c@d.com"],
      subject: "hi",
      body: "hello",
    };
    mockFetch({ messageId: "m1", matchedRules: [], actionsExecuted: [] });
    const result = await fc.ses.simulateInbound(req);
    expect(result.messageId).toBe("m1");
    const call = lastFetchCall();
    expect(call.init?.method).toBe("POST");
    expect(call.init?.headers).toEqual({
      "Content-Type": "application/json",
    });
    expect(JSON.parse(call.init?.body as string)).toEqual(req);
  });
});

// ── SNS ────────────────────────────────────────────────────────────

describe("SnsClient", () => {
  it("getMessages() fetches correct URL", async () => {
    const fc = new FakeCloud(BASE);
    mockFetch({ messages: [] });
    await fc.sns.getMessages();
    expect(lastFetchCall().url).toBe(`${BASE}/_fakecloud/sns/messages`);
  });

  it("getPendingConfirmations() fetches correct URL", async () => {
    const fc = new FakeCloud(BASE);
    mockFetch({ pendingConfirmations: [] });
    await fc.sns.getPendingConfirmations();
    expect(lastFetchCall().url).toBe(
      `${BASE}/_fakecloud/sns/pending-confirmations`,
    );
  });

  it("confirmSubscription() sends POST with body", async () => {
    const fc = new FakeCloud(BASE);
    mockFetch({ confirmed: true });
    const result = await fc.sns.confirmSubscription({
      subscriptionArn: "arn:aws:sns:us-east-1:000000000000:topic:sub-id",
    });
    expect(result.confirmed).toBe(true);
    expect(lastFetchCall().init?.method).toBe("POST");
  });
});

// ── SQS ────────────────────────────────────────────────────────────

describe("SqsClient", () => {
  it("getMessages() fetches correct URL", async () => {
    const fc = new FakeCloud(BASE);
    mockFetch({ queues: [] });
    await fc.sqs.getMessages();
    expect(lastFetchCall().url).toBe(`${BASE}/_fakecloud/sqs/messages`);
  });

  it("tickExpiration() sends POST", async () => {
    const fc = new FakeCloud(BASE);
    mockFetch({ expiredMessages: 3 });
    const result = await fc.sqs.tickExpiration();
    expect(result.expiredMessages).toBe(3);
    expect(lastFetchCall().init?.method).toBe("POST");
  });

  it("forceDlq() sends POST with queue name in URL", async () => {
    const fc = new FakeCloud(BASE);
    mockFetch({ movedMessages: 5 });
    const result = await fc.sqs.forceDlq("my-queue");
    expect(result.movedMessages).toBe(5);
    expect(lastFetchCall().url).toBe(
      `${BASE}/_fakecloud/sqs/my-queue/force-dlq`,
    );
  });
});

// ── EventBridge ────────────────────────────────────────────────────

describe("EventsClient", () => {
  it("getHistory() fetches correct URL", async () => {
    const fc = new FakeCloud(BASE);
    mockFetch({
      events: [],
      deliveries: { lambda: [], logs: [] },
    });
    const result = await fc.events.getHistory();
    expect(result.events).toEqual([]);
    expect(result.deliveries.lambda).toEqual([]);
    expect(lastFetchCall().url).toBe(`${BASE}/_fakecloud/events/history`);
  });

  it("fireRule() sends POST with JSON body", async () => {
    const fc = new FakeCloud(BASE);
    mockFetch({ targets: [{ type: "lambda", arn: "arn:x" }] });
    const result = await fc.events.fireRule({ ruleName: "my-rule" });
    expect(result.targets).toHaveLength(1);
    const call = lastFetchCall();
    expect(call.init?.method).toBe("POST");
    expect(JSON.parse(call.init?.body as string)).toEqual({
      ruleName: "my-rule",
    });
  });
});

// ── S3 ─────────────────────────────────────────────────────────────

describe("S3Client", () => {
  it("getNotifications() fetches correct URL", async () => {
    const fc = new FakeCloud(BASE);
    mockFetch({ notifications: [] });
    await fc.s3.getNotifications();
    expect(lastFetchCall().url).toBe(`${BASE}/_fakecloud/s3/notifications`);
  });

  it("tickLifecycle() sends POST", async () => {
    const fc = new FakeCloud(BASE);
    mockFetch({
      processedBuckets: 1,
      expiredObjects: 2,
      transitionedObjects: 0,
    });
    const result = await fc.s3.tickLifecycle();
    expect(result.expiredObjects).toBe(2);
    expect(lastFetchCall().init?.method).toBe("POST");
  });
});

// ── DynamoDB ───────────────────────────────────────────────────────

describe("DynamoDbClient", () => {
  it("tickTtl() sends POST", async () => {
    const fc = new FakeCloud(BASE);
    mockFetch({ expiredItems: 7 });
    const result = await fc.dynamodb.tickTtl();
    expect(result.expiredItems).toBe(7);
    expect(lastFetchCall().url).toBe(
      `${BASE}/_fakecloud/dynamodb/ttl-processor/tick`,
    );
    expect(lastFetchCall().init?.method).toBe("POST");
  });
});

// ── SecretsManager ─────────────────────────────────────────────────

describe("SecretsManagerClient", () => {
  it("tickRotation() sends POST", async () => {
    const fc = new FakeCloud(BASE);
    mockFetch({ rotatedSecrets: ["secret-1"] });
    const result = await fc.secretsmanager.tickRotation();
    expect(result.rotatedSecrets).toEqual(["secret-1"]);
    expect(lastFetchCall().init?.method).toBe("POST");
  });
});

// ── Cognito ────────────────────────────────────────────────────────

describe("CognitoClient", () => {
  it("getUserCodes() encodes pool ID and username in URL", async () => {
    const fc = new FakeCloud(BASE);
    mockFetch({ confirmationCode: "123456", attributeVerificationCodes: {} });
    const result = await fc.cognito.getUserCodes("us-east-1/pool", "user@name");
    expect(result.confirmationCode).toBe("123456");
    expect(lastFetchCall().url).toBe(
      `${BASE}/_fakecloud/cognito/confirmation-codes/us-east-1%2Fpool/user%40name`,
    );
  });

  it("getConfirmationCodes() fetches correct URL", async () => {
    const fc = new FakeCloud(BASE);
    mockFetch({ codes: [] });
    await fc.cognito.getConfirmationCodes();
    expect(lastFetchCall().url).toBe(
      `${BASE}/_fakecloud/cognito/confirmation-codes`,
    );
  });

  it("confirmUser() sends POST with JSON body", async () => {
    const fc = new FakeCloud(BASE);
    mockFetch({ confirmed: true });
    const result = await fc.cognito.confirmUser({
      userPoolId: "pool-1",
      username: "alice",
    });
    expect(result.confirmed).toBe(true);
    expect(lastFetchCall().init?.method).toBe("POST");
  });

  it("confirmUser() throws FakeCloudError on 404", async () => {
    const fc = new FakeCloud(BASE);
    mockFetch({ confirmed: false, error: "user not found" }, 404);
    await expect(
      fc.cognito.confirmUser({ userPoolId: "pool-1", username: "nobody" }),
    ).rejects.toThrow(FakeCloudError);
  });

  it("getTokens() fetches correct URL", async () => {
    const fc = new FakeCloud(BASE);
    mockFetch({ tokens: [] });
    await fc.cognito.getTokens();
    expect(lastFetchCall().url).toBe(`${BASE}/_fakecloud/cognito/tokens`);
  });

  it("expireTokens() sends POST with JSON body", async () => {
    const fc = new FakeCloud(BASE);
    mockFetch({ expiredTokens: 2 });
    const result = await fc.cognito.expireTokens({ userPoolId: "pool-1" });
    expect(result.expiredTokens).toBe(2);
    const call = lastFetchCall();
    expect(call.init?.method).toBe("POST");
    expect(JSON.parse(call.init?.body as string)).toEqual({
      userPoolId: "pool-1",
    });
  });

  it("getAuthEvents() fetches correct URL", async () => {
    const fc = new FakeCloud(BASE);
    mockFetch({ events: [] });
    await fc.cognito.getAuthEvents();
    expect(lastFetchCall().url).toBe(`${BASE}/_fakecloud/cognito/auth-events`);
  });
});
