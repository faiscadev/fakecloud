// Records real AWS Lambda durable-execution behavior. Every resource created
// here is deleted in the finally block.
import fs from "node:fs";
import { Readable } from "node:stream";
import { NodeHttpHandler } from "@smithy/node-http-handler";
import * as L from "@aws-sdk/client-lambda";
import * as I from "@aws-sdk/client-iam";
import * as CW from "@aws-sdk/client-cloudwatch-logs";

const OUT = process.argv[2];
const ZIP = process.argv[3];
const out = fs.createWriteStream(OUT);
let label = "";
const rec = (o) => out.write(JSON.stringify({ label, ...o }) + "\n");

class RecordingHandler {
  constructor() { this.inner = new NodeHttpHandler(); }
  async handle(request, options) {
    const { response } = await this.inner.handle(request, options);
    const chunks = [];
    for await (const c of response.body) chunks.push(c);
    const buf = Buffer.concat(chunks);
    let reqBody = request.body;
    if (reqBody && typeof reqBody !== "string") {
      reqBody = Buffer.isBuffer(reqBody) || reqBody instanceof Uint8Array ? Buffer.from(reqBody).toString("utf8") : "<stream>";
    }
    const h = {};
    for (const [k, v] of Object.entries(response.headers)) {
      if (k.startsWith("x-amz") || k === "content-type") h[k] = v;
    }
    const rh = {};
    for (const [k, v] of Object.entries(request.headers)) {
      if (k.startsWith("x-amz-") && !k.includes("security") && !k.includes("date") && !k.includes("content-sha")) rh[k] = v;
    }
    rec({ method: request.method, path: request.path, query: request.query, reqHeaders: rh,
      reqBody: request.path.includes("/functions/") && request.method === "POST" && request.path.endsWith("/functions") ? "<create-function>" : reqBody,
      status: response.statusCode, headers: h, body: buf.toString("utf8").slice(0, 20000) });
    response.body = Readable.from([buf]);
    return { response };
  }
}

const cfg = { region: "us-east-1", requestHandler: new RecordingHandler() };
const lambda = new L.LambdaClient(cfg);
const iam = new I.IAMClient({ region: "us-east-1" });
const logs = new CW.CloudWatchLogsClient({ region: "us-east-1" });
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const FN = "fc-durable-rec";
const ROLE = "fc-durable-rec-role";
const enc = (o) => new TextEncoder().encode(JSON.stringify(o));
async function attempt(l, f) {
  label = l;
  try { return await f(); } catch (e) { rec({ note: "error", name: e.name, message: e.message }); return undefined; }
}
async function waitStatus(arn, want, maxMs = 120000) {
  const t0 = Date.now();
  while (Date.now() - t0 < maxMs) {
    const r = await lambda.send(new L.GetDurableExecutionCommand({ DurableExecutionArn: arn }));
    if (want.includes(r.Status)) return r;
    await sleep(2000);
  }
  throw new Error("timeout waiting for " + want);
}
async function callbackId(arn, maxMs = 90000) {
  const t0 = Date.now();
  while (Date.now() - t0 < maxMs) {
    const r = await lambda.send(new L.GetDurableExecutionHistoryCommand({ DurableExecutionArn: arn, IncludeExecutionData: true }));
    const ev = (r.Events || []).find((e) => e.EventType === "CallbackStarted");
    if (ev) return ev.CallbackStartedDetails.CallbackId;
    await sleep(2000);
  }
  throw new Error("no callback");
}

let roleArn;
try {
  label = "setup-role";
  const role = await iam.send(new I.CreateRoleCommand({ RoleName: ROLE, AssumeRolePolicyDocument: JSON.stringify({ Version: "2012-10-17", Statement: [{ Effect: "Allow", Principal: { Service: "lambda.amazonaws.com" }, Action: "sts:AssumeRole" }] }) }));
  roleArn = role.Role.Arn;
  await iam.send(new I.AttachRolePolicyCommand({ RoleName: ROLE, PolicyArn: "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicDurableExecutionRolePolicy" }));
  await sleep(12000);

  await attempt("create-function", async () => {
    for (let i = 0; i < 6; i++) {
      try {
        return await lambda.send(new L.CreateFunctionCommand({ FunctionName: FN, Runtime: "nodejs22.x", Role: roleArn, Handler: "index.handler", Code: { ZipFile: fs.readFileSync(ZIP) }, Timeout: 60, MemorySize: 256, DurableConfig: { ExecutionTimeout: 600, RetentionPeriodInDays: 1 } }));
      } catch (e) { if (!String(e.message).includes("assume")) throw e; await sleep(5000); }
    }
  });
  label = "wait-active";
  await L.waitUntilFunctionActiveV2({ client: lambda, maxWaitTime: 120 }, { FunctionName: FN });
  await attempt("get-function-configuration", () => lambda.send(new L.GetFunctionConfigurationCommand({ FunctionName: FN })));

  // Unqualified invoke is refused for a durable function.
  await attempt("invoke-unqualified", () => lambda.send(new L.InvokeCommand({ FunctionName: FN, Payload: enc({ n: 1 }) })));
  // Sync invoke, step + short wait.
  const sync = await attempt("invoke-sync-step-wait", () => lambda.send(new L.InvokeCommand({ FunctionName: FN + ":$LATEST", Payload: enc({ n: 21, waitSeconds: 3 }), DurableExecutionName: "sync-step-wait" })));
  const syncArn = sync?.DurableExecutionArn;
  if (syncArn) {
    await attempt("get-sync", () => lambda.send(new L.GetDurableExecutionCommand({ DurableExecutionArn: syncArn })));
    await attempt("get-sync-no-data", () => lambda.send(new L.GetDurableExecutionCommand({ DurableExecutionArn: syncArn, IncludeExecutionData: false })));
    await attempt("history-sync", () => lambda.send(new L.GetDurableExecutionHistoryCommand({ DurableExecutionArn: syncArn, IncludeExecutionData: true })));
    await attempt("history-sync-default", () => lambda.send(new L.GetDurableExecutionHistoryCommand({ DurableExecutionArn: syncArn })));
    await attempt("history-sync-page", () => lambda.send(new L.GetDurableExecutionHistoryCommand({ DurableExecutionArn: syncArn, MaxItems: 2, ReverseOrder: true })));
    await attempt("state-bad-token", () => lambda.send(new L.GetDurableExecutionStateCommand({ DurableExecutionArn: syncArn, CheckpointToken: "AAAA" })));
    await attempt("checkpoint-bad-token", () => lambda.send(new L.CheckpointDurableExecutionCommand({ DurableExecutionArn: syncArn, CheckpointToken: "AAAA", Updates: [] })));
    await attempt("stop-finished", () => lambda.send(new L.StopDurableExecutionCommand({ DurableExecutionArn: syncArn })));
    // Same name, same payload -> idempotent; different payload -> conflict.
    await attempt("invoke-dup-same", () => lambda.send(new L.InvokeCommand({ FunctionName: FN + ":$LATEST", Payload: enc({ n: 21, waitSeconds: 3 }), DurableExecutionName: "sync-step-wait" })));
    await attempt("invoke-dup-different", () => lambda.send(new L.InvokeCommand({ FunctionName: FN + ":$LATEST", Payload: enc({ n: 22 }), DurableExecutionName: "sync-step-wait" })));
  }
  // Sync invoke that fails.
  const failed = await attempt("invoke-sync-fail", () => lambda.send(new L.InvokeCommand({ FunctionName: FN + ":$LATEST", Payload: enc({ n: 2, fail: true }) })));
  if (failed?.DurableExecutionArn) {
    await attempt("get-failed", () => lambda.send(new L.GetDurableExecutionCommand({ DurableExecutionArn: failed.DurableExecutionArn })));
    await attempt("history-failed", () => lambda.send(new L.GetDurableExecutionHistoryCommand({ DurableExecutionArn: failed.DurableExecutionArn, IncludeExecutionData: true })));
  }

  // Async invoke with a callback, completed with success.
  const cbOk = await attempt("invoke-async-callback", () => lambda.send(new L.InvokeCommand({ FunctionName: FN + ":$LATEST", InvocationType: "Event", Payload: enc({ n: 5, callback: true, heartbeat: 120 }), DurableExecutionName: "cb-ok" })));
  if (cbOk?.DurableExecutionArn) {
    const arn = cbOk.DurableExecutionArn;
    const id = await attempt("find-callback", () => callbackId(arn));
    await attempt("get-running", () => lambda.send(new L.GetDurableExecutionCommand({ DurableExecutionArn: arn })));
    await attempt("list-running", () => lambda.send(new L.ListDurableExecutionsByFunctionCommand({ FunctionName: FN, Statuses: ["RUNNING"] })));
    await attempt("history-running", () => lambda.send(new L.GetDurableExecutionHistoryCommand({ DurableExecutionArn: arn, IncludeExecutionData: true })));
    await attempt("invoke-dup-running-same", () => lambda.send(new L.InvokeCommand({ FunctionName: FN + ":$LATEST", InvocationType: "Event", Payload: enc({ n: 5, callback: true, heartbeat: 120 }), DurableExecutionName: "cb-ok" })));
    await attempt("invoke-dup-running-different", () => lambda.send(new L.InvokeCommand({ FunctionName: FN + ":$LATEST", InvocationType: "Event", Payload: enc({ n: 6 }), DurableExecutionName: "cb-ok" })));
    if (id) {
      await attempt("callback-heartbeat", () => lambda.send(new L.SendDurableExecutionCallbackHeartbeatCommand({ CallbackId: id })));
      await attempt("callback-success", () => lambda.send(new L.SendDurableExecutionCallbackSuccessCommand({ CallbackId: id, Result: enc({ approved: true }) })));
      await attempt("callback-success-again", () => lambda.send(new L.SendDurableExecutionCallbackSuccessCommand({ CallbackId: id, Result: enc({ approved: true }) })));
      await attempt("callback-failure-after-success", () => lambda.send(new L.SendDurableExecutionCallbackFailureCommand({ CallbackId: id, Error: { ErrorMessage: "late" } })));
      await attempt("callback-heartbeat-after-success", () => lambda.send(new L.SendDurableExecutionCallbackHeartbeatCommand({ CallbackId: id })));
    }
    await attempt("wait-cb-ok", () => waitStatus(arn, ["SUCCEEDED", "FAILED", "TIMED_OUT", "STOPPED"]));
    await attempt("get-cb-ok", () => lambda.send(new L.GetDurableExecutionCommand({ DurableExecutionArn: arn })));
    await attempt("history-cb-ok", () => lambda.send(new L.GetDurableExecutionHistoryCommand({ DurableExecutionArn: arn, IncludeExecutionData: true })));
  }

  // Callback without heartbeat, completed with failure.
  const cbFail = await attempt("invoke-async-callback-fail", () => lambda.send(new L.InvokeCommand({ FunctionName: FN + ":$LATEST", InvocationType: "Event", Payload: enc({ n: 7, callback: true }) })));
  if (cbFail?.DurableExecutionArn) {
    const arn = cbFail.DurableExecutionArn;
    const id = await attempt("find-callback-2", () => callbackId(arn));
    if (id) {
      await attempt("callback-heartbeat-no-hb", () => lambda.send(new L.SendDurableExecutionCallbackHeartbeatCommand({ CallbackId: id })));
      await attempt("callback-failure", () => lambda.send(new L.SendDurableExecutionCallbackFailureCommand({ CallbackId: id, Error: { ErrorMessage: "denied", ErrorType: "Rejected", ErrorData: "{\"why\":\"no\"}" } })));
    }
    await attempt("wait-cb-fail", () => waitStatus(arn, ["SUCCEEDED", "FAILED", "TIMED_OUT", "STOPPED"]));
    await attempt("get-cb-fail", () => lambda.send(new L.GetDurableExecutionCommand({ DurableExecutionArn: arn })));
    await attempt("history-cb-fail", () => lambda.send(new L.GetDurableExecutionHistoryCommand({ DurableExecutionArn: arn, IncludeExecutionData: true })));
  }

  // Stop a running execution.
  const stopMe = await attempt("invoke-async-stop", () => lambda.send(new L.InvokeCommand({ FunctionName: FN + ":$LATEST", InvocationType: "Event", Payload: enc({ n: 9, callback: true }) })));
  if (stopMe?.DurableExecutionArn) {
    const arn = stopMe.DurableExecutionArn;
    const id = await attempt("find-callback-3", () => callbackId(arn));
    await attempt("stop-running", () => lambda.send(new L.StopDurableExecutionCommand({ DurableExecutionArn: arn, Error: { ErrorMessage: "operator stop", ErrorType: "Manual" } })));
    await attempt("get-stopped", () => lambda.send(new L.GetDurableExecutionCommand({ DurableExecutionArn: arn })));
    await attempt("stop-again", () => lambda.send(new L.StopDurableExecutionCommand({ DurableExecutionArn: arn })));
    if (id) await attempt("callback-after-stop", () => lambda.send(new L.SendDurableExecutionCallbackSuccessCommand({ CallbackId: id, Result: enc("x") })));
    await sleep(3000);
    await attempt("history-stopped", () => lambda.send(new L.GetDurableExecutionHistoryCommand({ DurableExecutionArn: arn, IncludeExecutionData: true })));
  }

  await attempt("list-all", () => lambda.send(new L.ListDurableExecutionsByFunctionCommand({ FunctionName: FN })));
  await attempt("list-page", () => lambda.send(new L.ListDurableExecutionsByFunctionCommand({ FunctionName: FN, MaxItems: 1, ReverseOrder: true })));
  await attempt("list-qualifier", () => lambda.send(new L.ListDurableExecutionsByFunctionCommand({ FunctionName: FN, Qualifier: "$LATEST", DurableExecutionName: "cb-ok" })));

  // Unknown / malformed ids.
  const fakeArn = `arn:aws:lambda:us-east-1:454591548454:function:${FN}:$LATEST/durable-execution/nope/00000000-0000-0000-0000-000000000000`;
  await attempt("get-unknown", () => lambda.send(new L.GetDurableExecutionCommand({ DurableExecutionArn: fakeArn })));
  await attempt("history-unknown", () => lambda.send(new L.GetDurableExecutionHistoryCommand({ DurableExecutionArn: fakeArn })));
  await attempt("state-unknown", () => lambda.send(new L.GetDurableExecutionStateCommand({ DurableExecutionArn: fakeArn, CheckpointToken: "AAAA" })));
  await attempt("checkpoint-unknown", () => lambda.send(new L.CheckpointDurableExecutionCommand({ DurableExecutionArn: fakeArn, CheckpointToken: "AAAA" })));
  await attempt("stop-unknown", () => lambda.send(new L.StopDurableExecutionCommand({ DurableExecutionArn: fakeArn })));
  await attempt("callback-success-unknown", () => lambda.send(new L.SendDurableExecutionCallbackSuccessCommand({ CallbackId: "bm9wZQ==" })));
  await attempt("callback-failure-unknown", () => lambda.send(new L.SendDurableExecutionCallbackFailureCommand({ CallbackId: "bm9wZQ==" })));
  await attempt("callback-heartbeat-unknown", () => lambda.send(new L.SendDurableExecutionCallbackHeartbeatCommand({ CallbackId: "bm9wZQ==" })));
  await attempt("list-unknown-function", () => lambda.send(new L.ListDurableExecutionsByFunctionCommand({ FunctionName: "fc-durable-does-not-exist" })));

  // Logs from inside the function: invocation envelope + raw checkpoint wire.
  label = "logs";
  await sleep(10000);
  let token;
  const lines = [];
  do {
    const r = await logs.send(new CW.FilterLogEventsCommand({ logGroupName: `/aws/lambda/${FN}`, nextToken: token }));
    for (const e of r.events || []) lines.push(e.message);
    token = r.nextToken;
  } while (token);
  fs.writeFileSync(OUT.replace(/\.jsonl$/, "-logs.txt"), lines.join(""));
} finally {
  label = "cleanup";
  try { await lambda.send(new L.DeleteFunctionCommand({ FunctionName: FN })); } catch (e) { rec({ note: "cleanup", what: "function", e: e.name }); }
  try { await iam.send(new I.DetachRolePolicyCommand({ RoleName: ROLE, PolicyArn: "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicDurableExecutionRolePolicy" })); } catch (e) { rec({ note: "cleanup", what: "detach", e: e.name }); }
  try { await iam.send(new I.DeleteRoleCommand({ RoleName: ROLE })); } catch (e) { rec({ note: "cleanup", what: "role", e: e.name }); }
  try { await logs.send(new CW.DeleteLogGroupCommand({ logGroupName: `/aws/lambda/${FN}` })); } catch (e) { rec({ note: "cleanup", what: "logs", e: e.name }); }
  out.end();
}
