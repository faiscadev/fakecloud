import { withDurableExecution, DurableExecutionApiClient } from "@aws/durable-execution-sdk-js";
import { LambdaClient, CheckpointDurableExecutionCommand, GetDurableExecutionStateCommand } from "@aws-sdk/client-lambda";
import { NodeHttpHandler } from "@smithy/node-http-handler";
import { Readable } from "node:stream";

class RecordingHandler {
  constructor() { this.inner = new NodeHttpHandler(); }
  async handle(request, options) {
    const { response } = await this.inner.handle(request, options);
    const chunks = [];
    for await (const c of response.body) chunks.push(c);
    const buf = Buffer.concat(chunks);
    let body = request.body;
    if (body && typeof body !== "string") body = Buffer.from(body).toString("utf8");
    console.log("WIRE " + JSON.stringify({ method: request.method, path: request.path, query: request.query,
      requestBody: body ?? null, status: response.statusCode, body: buf.toString("utf8") }));
    response.body = Readable.from([buf]);
    return { response };
  }
}
const raw = new LambdaClient({ requestHandler: new RecordingHandler(), maxAttempts: 1 });
const recordingClient = new DurableExecutionApiClient(raw);

async function tryCall(label, cmd) {
  try { const r = await raw.send(cmd); return r; }
  catch (e) { console.log("PROBEERR " + JSON.stringify({ label, name: e.name, message: e.message, status: e.$metadata?.httpStatusCode })); return undefined; }
}

// Drives the checkpoint API directly with the invocation's own token.
async function probe(event) {
  const arn = event.DurableExecutionArn;
  let token = event.CheckpointToken;
  const first = token;
  const ck = async (label, updates, extra = {}) => {
    console.log("PROBE " + label);
    const r = await tryCall(label, new CheckpointDurableExecutionCommand({ DurableExecutionArn: arn, CheckpointToken: token, Updates: updates, ...extra }));
    if (r?.CheckpointToken) token = r.CheckpointToken;
    return r;
  };
  console.log("PROBE state");
  await tryCall("state", new GetDurableExecutionStateCommand({ DurableExecutionArn: arn, CheckpointToken: token, MaxItems: 1 }));
  await ck("empty", []);
  console.log("PROBE stale-token");
  await tryCall("stale", new CheckpointDurableExecutionCommand({ DurableExecutionArn: arn, CheckpointToken: first, Updates: [] }));
  console.log("PROBE state-stale-token");
  await tryCall("state-stale", new GetDurableExecutionStateCommand({ DurableExecutionArn: arn, CheckpointToken: first }));
  await ck("step-start", [{ Id: "s1", Type: "STEP", Action: "START", Name: "probe-step", SubType: "Step" }]);
  await ck("step-succeed-with-error", [{ Id: "s1", Type: "STEP", Action: "SUCCEED", Error: { ErrorMessage: "x" } }]);
  await ck("step-retry", [{ Id: "s1", Type: "STEP", Action: "RETRY", Error: { ErrorMessage: "try again", ErrorType: "Oops" }, StepOptions: { NextAttemptDelaySeconds: 1 } }]);
  await ck("step-retry-no-options", [{ Id: "s2", Type: "STEP", Action: "RETRY" }]);
  await ck("wait-no-options", [{ Id: "w0", Type: "WAIT", Action: "START" }]);
  await ck("wait-zero", [{ Id: "w0", Type: "WAIT", Action: "START", WaitOptions: { WaitSeconds: 0 } }]);
  await ck("wait-start", [{ Id: "w1", Type: "WAIT", Action: "START", Name: "probe-wait", SubType: "Wait", WaitOptions: { WaitSeconds: 60 } }]);
  await ck("wait-start-again", [{ Id: "w1", Type: "WAIT", Action: "START", WaitOptions: { WaitSeconds: 60 } }]);
  await ck("wait-cancel", [{ Id: "w1", Type: "WAIT", Action: "CANCEL" }]);
  await ck("callback-succeed", [{ Id: "c1", Type: "CALLBACK", Action: "SUCCEED" }]);
  await ck("callback-start", [{ Id: "c1", Type: "CALLBACK", Action: "START", CallbackOptions: { TimeoutSeconds: 0 } }]);
  await ck("bad-parent", [{ Id: "x1", ParentId: "nope", Type: "STEP", Action: "START" }]);
  await ck("parent-not-context", [{ Id: "x2", ParentId: "s1", Type: "STEP", Action: "START" }]);
  await ck("dup-in-batch", [{ Id: "x3", Type: "WAIT", Action: "START", WaitOptions: { WaitSeconds: 5 } }, { Id: "x3", Type: "WAIT", Action: "CANCEL" }]);
  await ck("name-mismatch", [{ Id: "s1", Type: "STEP", Action: "START", Name: "other" }]);
  await ck("exec-not-last", [{ Id: "e1", Type: "EXECUTION", Action: "SUCCEED" }, { Id: "x4", Type: "STEP", Action: "START" }]);
  await ck("invalid-type", [{ Id: "x5", Type: "BOGUS", Action: "START" }]);
  await ck("ctx-start", [{ Id: "k1", Type: "CONTEXT", Action: "START", Name: "ctx", SubType: "RunInChildContext" }]);
  await ck("ctx-child-step", [{ Id: "k1s", ParentId: "k1", Type: "STEP", Action: "START", Name: "inner" }, { Id: "k1s", ParentId: "k1", Type: "STEP", Action: "SUCCEED", Payload: "\"in\"" }]);
  await ck("ctx-succeed", [{ Id: "k1", Type: "CONTEXT", Action: "SUCCEED", Payload: "\"done\"" }]);
  console.log("PROBE state-after-ctx");
  await tryCall("state2", new GetDurableExecutionStateCommand({ DurableExecutionArn: arn, CheckpointToken: token }));
  await ck("client-token-1", [{ Id: "s9", Type: "STEP", Action: "START" }], { ClientToken: "ct-1" });
  return { Status: "SUCCEEDED", Result: "\"probed\"" };
}

const durable = withDurableExecution(async (event, context) => {
  if (event.mode === "retry") {
    const v = await context.step("flaky", async (ctx) => {
      if ((ctx.attempt ?? 0) < 1 && !globalThis.__flaked) { globalThis.__flaked = true; throw new Error("flake once"); }
      return "ok";
    }, { retryStrategy: (err, attempt) => ({ shouldRetry: attempt < 3, delay: { seconds: 2 } }) });
    return { v };
  }
  if (event.mode === "chain") {
    const r = await context.invoke("call-target", event.target, { hello: "world" });
    return { r };
  }
  if (event.mode === "chainfail") {
    try { await context.invoke("call-bad", event.target, { boom: true }); }
    catch (e) { return { caught: e.message, name: e.name }; }
  }
  if (event.mode === "cbtimeout") {
    try {
      await context.waitForCallback("short", async () => {}, { timeout: { seconds: 2 } });
    } catch (e) { return { caught: e.message, name: e.name }; }
  }
  if (event.mode === "hbtimeout") {
    try {
      await context.waitForCallback("hb", async () => {}, { timeout: { seconds: 60 }, heartbeatTimeout: { seconds: 2 } });
    } catch (e) { return { caught: e.message, name: e.name }; }
  }
  if (event.mode === "forever") {
    await context.waitForCallback("never", async () => {});
  }
  if (event.mode === "chainforever") {
    const r = await context.invoke("call-child", event.target, { mode: "forever" });
    return { r };
  }
  if (event.mode === "sleep") {
    await context.wait("nap", { seconds: event.seconds });
    return { slept: event.seconds };
  }
  if (event.mode === "parallel") {
    const r = await context.parallel("fan", [
      async (c) => c.step("a", async () => 1),
      async (c) => { await c.wait({ seconds: 1 }); return 2; },
    ]);
    return { r: r.getResults() };
  }
  return { plain: true };
}, { durableExecutionClient: recordingClient });

export const handler = async (event, context) => {
  console.log("EVENT " + JSON.stringify(event));
  const input = JSON.parse(event.InitialExecutionState?.Operations?.[0]?.ExecutionDetails?.InputPayload ?? "{}");
  let out;
  if (input.mode === "probe") out = await probe(event);
  else if (input.mode === "pending") out = { Status: "PENDING" };
  else if (input.mode === "garbage") out = { hello: 1 };
  else if (input.mode === "crash") throw new Error("crashed invocation");
  else out = await durable(event, context);
  console.log("OUTPUT " + JSON.stringify(out));
  return out;
};
