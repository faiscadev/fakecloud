import { afterEach, describe, expect, it, vi } from "vitest";
import { FakeCloud } from "../src/client.js";

function mockFetch(reply: unknown): string[] {
  const urls: string[] = [];
  vi.stubGlobal(
    "fetch",
    vi.fn(async (url: string) => {
      urls.push(url);
      return new Response(JSON.stringify(reply), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      });
    }),
  );
  return urls;
}

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("introspection payload shapes", () => {
  it("decodes waf-counts entries", async () => {
    const urls = mockFetch({
      counts: [{ webAclArn: "arn:acl", ruleName: "count-admin", count: 3 }],
    });
    const out = await new FakeCloud("http://fc.test").elbv2.getWafCounts();
    expect(urls[0]).toBe("http://fc.test/_fakecloud/elbv2/waf-counts");
    expect(out.counts[0].webAclArn).toBe("arn:acl");
    expect(out.counts[0].ruleName).toBe("count-admin");
    expect(out.counts[0].count).toBe(3);
  });

  it("decodes camelCase webauthn credentials", async () => {
    mockFetch({
      credentials: [
        {
          accountId: "123456789012",
          poolId: "us-east-1_abc",
          username: "a:b",
          credentialId: "cred",
          friendlyCredentialName: null,
          relyingPartyId: "rp",
          authenticatorAttachment: "platform",
          authenticatorTransports: ["internal"],
          createdAt: "2026-01-01T00:00:00+00:00",
          attestationInfo: null,
        },
      ],
    });
    const out = await new FakeCloud(
      "http://fc.test",
    ).cognito.getWebAuthnCredentials();
    expect(out.credentials[0].poolId).toBe("us-east-1_abc");
    expect(out.credentials[0].username).toBe("a:b");
    expect(out.credentials[0].attestationInfo).toBeNull();
  });

  it("decodes apigatewayv2 isBase64Encoded", async () => {
    mockFetch({
      requests: [
        {
          requestId: "r",
          apiId: "a",
          stage: "prod",
          method: "POST",
          path: "/u",
          headers: {},
          queryParams: {},
          body: "/wD+",
          isBase64Encoded: true,
          timestamp: "t",
          statusCode: 200,
        },
      ],
    });
    const out = await new FakeCloud(
      "http://fc.test",
    ).apigatewayv2.getRequests();
    expect(out.requests[0].isBase64Encoded).toBe(true);
    expect(out.requests[0].body).toBe("/wD+");
  });
});
