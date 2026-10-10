import { afterEach, describe, expect, it, vi } from "vitest";
import { FakeCloud, FakeCloudError } from "../src/client.js";

interface Call {
  url: string;
  method: string;
  body: string | undefined;
}

/** Stub `fetch` with one canned reply and record what was sent. */
function mockFetch(status: number, reply: unknown): Call[] {
  const calls: Call[] = [];
  vi.stubGlobal(
    "fetch",
    vi.fn(async (url: string, init?: RequestInit) => {
      calls.push({
        url,
        method: init?.method ?? "GET",
        body: init?.body as string | undefined,
      });
      return new Response(JSON.stringify(reply), {
        status,
        headers: { "Content-Type": "application/json" },
      });
    }),
  );
  return calls;
}

describe("CognitoClient.setSoftwareToken", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("posts the secret and parses the response", async () => {
    const calls = mockFetch(200, { enrolled: true });
    const resp = await new FakeCloud().cognito.setSoftwareToken({
      userPoolId: "us-east-1_Local",
      username: "alice",
      secretCode: "JBSWY3DPEHPK3PXP",
    });
    expect(calls[0].url).toBe(
      "http://localhost:4566/_fakecloud/cognito/software-token",
    );
    expect(calls[0].method).toBe("POST");
    expect(JSON.parse(calls[0].body!)).toEqual({
      userPoolId: "us-east-1_Local",
      username: "alice",
      secretCode: "JBSWY3DPEHPK3PXP",
    });
    expect(resp.enrolled).toBe(true);
  });

  it("surfaces a 404 as FakeCloudError", async () => {
    mockFetch(404, { error: "user not found in pool" });
    const err = await new FakeCloud().cognito
      .setSoftwareToken({
        userPoolId: "us-east-1_Local",
        username: "nobody",
        secretCode: "JBSWY3DPEHPK3PXP",
      })
      .catch((e: unknown) => e);
    expect(err).toBeInstanceOf(FakeCloudError);
    expect((err as FakeCloudError).status).toBe(404);
  });
});
