// Second recording round: timeouts, retries, chained invokes, raw checkpoint
// validation, and odd handler outputs. Every resource is deleted at the end.
import fs from "node:fs";
import { Readable } from "node:stream";
import { NodeHttpHandler } from "@smithy/node-http-handler";
import * as L from "@aws-sdk/client-lambda";
import * as I from "@aws-sdk/client-iam";
import * as CW from "@aws-sdk/client-cloudwatch-logs";

const [OUT, ZIP] = process.argv.slice(2);
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
    if (reqBody && typeof reqBody !== "string") reqBody = request.path.endsWith("/functions") ? "<zip>" : Buffer.from(reqBody).toString("utf8");
    const h = {};
    for (const [k, v] of Object.entries(response.headers)) if (k.startsWith("x-amz") || k === "content-type") h[k] = v;
    const rh = {};
    for (const [k, v] of Object.entries(request.headers)) if (k.startsWith("x-amz-") && !/security|date|content-sha/.test(k)) rh[k] = v;
    rec({ method: request.method, path: request.path, query: request.query, reqHeaders: rh, reqBody: request.path.endsWith("/functions") ? "<create>" : reqBody, status: response.statusCode, headers: h, body: buf.toString("utf8").slice(0, 30000) });
    response.body = Readable.from([buf]);
    return { response };
  }
}

import * as K from "@aws-sdk/client-kms";
const lambda = new L.LambdaClient({ region: "us-east-1", requestHandler: new RecordingHandler(), maxAttempts: 1 });
const iam = new I.IAMClient({ region: "us-east-1" });
const kms = new K.KMSClient({ region: "us-east-1", requestHandler: new RecordingHandler(), maxAttempts: 1 });
const logs = new CW.CloudWatchLogsClient({ region: "us-east-1" });
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const P = "fc-durable-rec3";
const ROLE = "fc-durable-rec3-role";
const POLICY = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicDurableExecutionRolePolicy";
const enc = (o) => new TextEncoder().encode(JSON.stringify(o));
const created = [];
let keyId;
async function attempt(l, f) {
  label = l;
  try { return await f(); } catch (e) { rec({ note: "error", name: e.name, message: e.message }); return undefined; }
}
async function settle(arn, maxMs = 60000) {
  const t0 = Date.now();
  while (Date.now() - t0 < maxMs) {
    const r = await lambda.send(new L.GetDurableExecutionCommand({ DurableExecutionArn: arn }));
    if (r.Status !== "RUNNING") return r;
    await sleep(2500);
  }
}
async function historyHas(arn, type, maxMs = 60000) {
  const t0 = Date.now();
  while (Date.now() - t0 < maxMs) {
    const h = await lambda.send(new L.GetDurableExecutionHistoryCommand({ DurableExecutionArn: arn, IncludeExecutionData: true }));
    const ev = (h.Events || []).find((e) => e.EventType === type);
    if (ev) return ev;
    await sleep(2000);
  }
}
try {
  label = "setup";
  await iam.send(new I.CreateRoleCommand({ RoleName: ROLE, AssumeRolePolicyDocument: JSON.stringify({ Version: "2012-10-17", Statement: [{ Effect: "Allow", Principal: { Service: "lambda.amazonaws.com" }, Action: "sts:AssumeRole" }] }) }));
  await iam.send(new I.AttachRolePolicyCommand({ RoleName: ROLE, PolicyArn: POLICY }));
  await iam.send(new I.PutRolePolicyCommand({ RoleName: ROLE, PolicyName: "invoke", PolicyDocument: JSON.stringify({ Version: "2012-10-17", Statement: [{ Effect: "Allow", Action: "lambda:InvokeFunction", Resource: "*" }] }) }));
  const roleArn = (await iam.send(new I.GetRoleCommand({ RoleName: ROLE }))).Role.Arn;
  await sleep(12000);
  const mk = async (name, durable, timeout = 60) => {
    for (let i = 0; i < 8; i++) {
      try {
        const r = await lambda.send(new L.CreateFunctionCommand({ FunctionName: name, Runtime: "nodejs22.x", Role: roleArn, Handler: "index.handler", Code: { ZipFile: fs.readFileSync(ZIP) }, Timeout: timeout, MemorySize: 256, ...(durable !== undefined ? { DurableConfig: durable } : {}) }));
        created.push(name);
        await L.waitUntilFunctionActiveV2({ client: lambda, maxWaitTime: 120 }, { FunctionName: name });
        return r;
      } catch (e) { if (!/assume/i.test(e.message)) throw e; await sleep(5000); }
    }
  };
  // A. DurableConfig validation on CreateFunction.
  const cases = [
    ["cfg-empty", {}],
    ["cfg-retention-only", { RetentionPeriodInDays: 7 }],
    ["cfg-timeout-0", { ExecutionTimeout: 0 }],
    ["cfg-timeout-max", { ExecutionTimeout: 31622400 }],
    ["cfg-timeout-over", { ExecutionTimeout: 31622401 }],
    ["cfg-retention-0", { ExecutionTimeout: 60, RetentionPeriodInDays: 0 }],
    ["cfg-retention-90", { ExecutionTimeout: 60, RetentionPeriodInDays: 90 }],
    ["cfg-retention-91", { ExecutionTimeout: 60, RetentionPeriodInDays: 91 }],
    ["cfg-kms-bad", { ExecutionTimeout: 60, KMSKeyArn: "not-an-arn" }],
    ["cfg-kms-empty", { ExecutionTimeout: 60, KMSKeyArn: "" }],
    ["cfg-kms-missing", { ExecutionTimeout: 60, KMSKeyArn: "arn:aws:kms:us-east-1:454591548454:key/00000000-0000-0000-0000-000000000000" }],
  ];
  for (const [l, cfg] of cases) {
    const name = `${P}-${l}`;
    const r = await attempt(`create-${l}`, () => mk(name, cfg));
    if (r) await attempt(`delete-${l}`, async () => { await lambda.send(new L.DeleteFunctionCommand({ FunctionName: name })); created.splice(created.indexOf(name), 1); });
  }
  // B. UpdateFunctionConfiguration.
  const DUR = `${P}-main`;
  const PLAIN = `${P}-plain`;
  await attempt("create-main", () => mk(DUR, { ExecutionTimeout: 600, RetentionPeriodInDays: 3 }));
  await attempt("create-plain", () => mk(PLAIN, undefined));
  const upd = async (l, fn, cfg) => {
    await attempt(`update-${l}`, () => lambda.send(new L.UpdateFunctionConfigurationCommand({ FunctionName: fn, DurableConfig: cfg })));
    await attempt(`update-${l}-wait`, () => L.waitUntilFunctionUpdatedV2({ client: lambda, maxWaitTime: 60 }, { FunctionName: fn }));
    await attempt(`update-${l}-get`, () => lambda.send(new L.GetFunctionConfigurationCommand({ FunctionName: fn })));
  };
  await upd("durable-empty", DUR, {});
  await upd("durable-timeout-0", DUR, { ExecutionTimeout: 0 });
  await upd("durable-timeout-only", DUR, { ExecutionTimeout: 900 });
  await upd("durable-retention-only", DUR, { RetentionPeriodInDays: 5 });
  await upd("plain-add", PLAIN, { ExecutionTimeout: 60 });
  await upd("durable-back", DUR, { ExecutionTimeout: 600, RetentionPeriodInDays: 3 });

  // C. Stop a parent that waits on a durable child.
  const parent = await attempt("stop-parent-invoke", () => lambda.send(new L.InvokeCommand({ FunctionName: DUR + ":$LATEST", InvocationType: "Event", Payload: enc({ mode: "chainforever", target: DUR + ":$LATEST" }), DurableExecutionName: "parent-stop" })));
  if (parent?.DurableExecutionArn) {
    const started = await attempt("stop-parent-chain-started", () => historyHas(parent.DurableExecutionArn, "ChainedInvokeStarted"));
    const child = started?.ChainedInvokeStartedDetails?.DurableExecutionArn;
    if (child) await attempt("stop-parent-child-callback", () => historyHas(child, "CallbackStarted"));
    await sleep(3000);
    await attempt("stop-parent", () => lambda.send(new L.StopDurableExecutionCommand({ DurableExecutionArn: parent.DurableExecutionArn, Error: { ErrorMessage: "stop the parent" } })));
    await sleep(8000);
    await attempt("stop-parent-get", () => lambda.send(new L.GetDurableExecutionCommand({ DurableExecutionArn: parent.DurableExecutionArn })));
    await attempt("stop-parent-history", () => lambda.send(new L.GetDurableExecutionHistoryCommand({ DurableExecutionArn: parent.DurableExecutionArn, IncludeExecutionData: true })));
    if (child) {
      await attempt("stop-parent-child-get", () => lambda.send(new L.GetDurableExecutionCommand({ DurableExecutionArn: child })));
      await attempt("stop-parent-child-history", () => lambda.send(new L.GetDurableExecutionHistoryCommand({ DurableExecutionArn: child, IncludeExecutionData: true })));
      await attempt("stop-parent-child-stop", () => lambda.send(new L.StopDurableExecutionCommand({ DurableExecutionArn: child })));
    }
  }
  // D. Stop the child while the parent waits on it.
  const parent2 = await attempt("stop-child-invoke", () => lambda.send(new L.InvokeCommand({ FunctionName: DUR + ":$LATEST", InvocationType: "Event", Payload: enc({ mode: "chainforever", target: DUR + ":$LATEST" }), DurableExecutionName: "child-stop" })));
  if (parent2?.DurableExecutionArn) {
    const started = await attempt("stop-child-chain-started", () => historyHas(parent2.DurableExecutionArn, "ChainedInvokeStarted"));
    const child = started?.ChainedInvokeStartedDetails?.DurableExecutionArn;
    if (child) {
      await attempt("stop-child-callback", () => historyHas(child, "CallbackStarted"));
      await sleep(3000);
      await attempt("stop-child", () => lambda.send(new L.StopDurableExecutionCommand({ DurableExecutionArn: child, Error: { ErrorMessage: "stop the child", ErrorType: "Manual" } })));
      await attempt("stop-child-parent-settle", () => settle(parent2.DurableExecutionArn));
      await attempt("stop-child-parent-get", () => lambda.send(new L.GetDurableExecutionCommand({ DurableExecutionArn: parent2.DurableExecutionArn })));
      await attempt("stop-child-parent-history", () => lambda.send(new L.GetDurableExecutionHistoryCommand({ DurableExecutionArn: parent2.DurableExecutionArn, IncludeExecutionData: true })));
    }
  }
  // F. Sync invoke whose execution outlives the function timeout.
  const SHORT = `${P}-shorttimeout`;
  await attempt("create-shorttimeout", () => mk(SHORT, { ExecutionTimeout: 300 }, 10));
  const t0 = Date.now();
  await attempt("sync-beyond-function-timeout", () => lambda.send(new L.InvokeCommand({ FunctionName: SHORT + ":$LATEST", Payload: enc({ mode: "sleep", seconds: 25 }), DurableExecutionName: "beyond" })));
  rec({ note: "elapsed", ms: Date.now() - t0 });

  // G. KMSKeyArn.
  const key = await attempt("kms-create", () => kms.send(new K.CreateKeyCommand({ Description: "fakecloud durable recording (temporary)" })));
  keyId = key?.KeyMetadata?.KeyId;
  const keyArn = key?.KeyMetadata?.Arn;
  if (keyArn) {
    const KF = `${P}-kms`;
    await attempt("kms-create-function", () => mk(KF, { ExecutionTimeout: 300, KMSKeyArn: keyArn }));
    await attempt("kms-get-config", () => lambda.send(new L.GetFunctionConfigurationCommand({ FunctionName: KF })));
    const a = await attempt("kms-noperm-invoke", () => lambda.send(new L.InvokeCommand({ FunctionName: KF + ":$LATEST", Payload: enc({ mode: "plain" }), DurableExecutionName: "noperm" })));
    if (a?.DurableExecutionArn) {
      await attempt("kms-noperm-settle", () => settle(a.DurableExecutionArn));
      await attempt("kms-noperm-get", () => lambda.send(new L.GetDurableExecutionCommand({ DurableExecutionArn: a.DurableExecutionArn })));
      await attempt("kms-noperm-history", () => lambda.send(new L.GetDurableExecutionHistoryCommand({ DurableExecutionArn: a.DurableExecutionArn, IncludeExecutionData: true })));
      await attempt("kms-noperm-list", () => lambda.send(new L.ListDurableExecutionsByFunctionCommand({ FunctionName: KF })));
    }
    await attempt("kms-grant", () => iam.send(new I.PutRolePolicyCommand({ RoleName: ROLE, PolicyName: "kms", PolicyDocument: JSON.stringify({ Version: "2012-10-17", Statement: [{ Effect: "Allow", Action: ["kms:Encrypt", "kms:Decrypt", "kms:GenerateDataKey"], Resource: keyArn }] }) })));
    await sleep(12000);
    const b = await attempt("kms-perm-invoke", () => lambda.send(new L.InvokeCommand({ FunctionName: KF + ":$LATEST", Payload: enc({ mode: "sleep", seconds: 2 }), DurableExecutionName: "perm" })));
    if (b?.DurableExecutionArn) {
      await attempt("kms-perm-settle", () => settle(b.DurableExecutionArn));
      await attempt("kms-perm-history", () => lambda.send(new L.GetDurableExecutionHistoryCommand({ DurableExecutionArn: b.DurableExecutionArn, IncludeExecutionData: true })));
    }
    const waiting = await attempt("kms-waiting-invoke", () => lambda.send(new L.InvokeCommand({ FunctionName: KF + ":$LATEST", InvocationType: "Event", Payload: enc({ mode: "forever" }), DurableExecutionName: "waiting" })));
    let cbId;
    if (waiting?.DurableExecutionArn) cbId = (await attempt("kms-waiting-callback", () => historyHas(waiting.DurableExecutionArn, "CallbackStarted")))?.CallbackStartedDetails?.CallbackId;
    await attempt("kms-disable", () => kms.send(new K.DisableKeyCommand({ KeyId: keyId })));
    await sleep(15000);
    await attempt("kms-disabled-sync-invoke", () => lambda.send(new L.InvokeCommand({ FunctionName: KF + ":$LATEST", Payload: enc({ mode: "plain" }), DurableExecutionName: "disabled" })));
    await attempt("kms-disabled-async-invoke", () => lambda.send(new L.InvokeCommand({ FunctionName: KF + ":$LATEST", InvocationType: "Event", Payload: enc({ mode: "plain" }), DurableExecutionName: "disabled-async" })));
    if (b?.DurableExecutionArn) {
      await attempt("kms-disabled-get", () => lambda.send(new L.GetDurableExecutionCommand({ DurableExecutionArn: b.DurableExecutionArn })));
      await attempt("kms-disabled-get-nodata", () => lambda.send(new L.GetDurableExecutionCommand({ DurableExecutionArn: b.DurableExecutionArn, IncludeExecutionData: false })));
      await attempt("kms-disabled-history", () => lambda.send(new L.GetDurableExecutionHistoryCommand({ DurableExecutionArn: b.DurableExecutionArn, IncludeExecutionData: true })));
      await attempt("kms-disabled-history-nodata", () => lambda.send(new L.GetDurableExecutionHistoryCommand({ DurableExecutionArn: b.DurableExecutionArn })));
    }
    await attempt("kms-disabled-list", () => lambda.send(new L.ListDurableExecutionsByFunctionCommand({ FunctionName: KF })));
    if (cbId) {
      await attempt("kms-disabled-callback", () => lambda.send(new L.SendDurableExecutionCallbackSuccessCommand({ CallbackId: cbId, Result: enc("x") })));
      await sleep(8000);
      await attempt("kms-disabled-callback-get", () => lambda.send(new L.GetDurableExecutionCommand({ DurableExecutionArn: waiting.DurableExecutionArn, IncludeExecutionData: false })));
      await attempt("kms-disabled-callback-history", () => lambda.send(new L.GetDurableExecutionHistoryCommand({ DurableExecutionArn: waiting.DurableExecutionArn })));
    }
    if (waiting?.DurableExecutionArn) await attempt("kms-disabled-stop", () => lambda.send(new L.StopDurableExecutionCommand({ DurableExecutionArn: waiting.DurableExecutionArn })));
    await attempt("kms-schedule-delete", () => kms.send(new K.ScheduleKeyDeletionCommand({ KeyId: keyId, PendingWindowInDays: 7 })));
    await sleep(10000);
    await attempt("kms-pending-delete-invoke", () => lambda.send(new L.InvokeCommand({ FunctionName: KF + ":$LATEST", Payload: enc({ mode: "plain" }), DurableExecutionName: "pending-delete" })));
    if (b?.DurableExecutionArn) await attempt("kms-pending-delete-get", () => lambda.send(new L.GetDurableExecutionCommand({ DurableExecutionArn: b.DurableExecutionArn })));
  }
  label = "logs";
  await sleep(10000);
  const lines = [];
  for (const g of created) {
    let token;
    do {
      try {
        const r = await logs.send(new CW.FilterLogEventsCommand({ logGroupName: `/aws/lambda/${g}`, nextToken: token }));
        for (const e of r.events || []) lines.push(`@@${g}@@` + e.message);
        token = r.nextToken;
      } catch (e) { token = undefined; }
    } while (token);
  }
  fs.writeFileSync(OUT.replace(/\.jsonl$/, "-logs.txt"), lines.join("\n"));
} finally {
  label = "cleanup";
  for (const n of [...created]) {
    try { await lambda.send(new L.DeleteFunctionCommand({ FunctionName: n })); } catch (e) { rec({ note: "cleanup", what: n, e: e.name }); }
  }
  if (keyId) {
    try { await kms.send(new K.ScheduleKeyDeletionCommand({ KeyId: keyId, PendingWindowInDays: 7 })); } catch (e) { rec({ note: "cleanup", what: "key", e: e.name }); }
  }
  for (const pn of ["invoke", "kms"]) { try { await iam.send(new I.DeleteRolePolicyCommand({ RoleName: ROLE, PolicyName: pn })); } catch (e) { rec({ note: "cleanup", what: pn, e: e.name }); } }
  try { await iam.send(new I.DetachRolePolicyCommand({ RoleName: ROLE, PolicyArn: POLICY })); } catch (e) { rec({ note: "cleanup", what: "detach", e: e.name }); }
  try { await iam.send(new I.DeleteRoleCommand({ RoleName: ROLE })); } catch (e) { rec({ note: "cleanup", what: "role", e: e.name }); }
  for (const g of [...created, `${P}-main`, `${P}-plain`, `${P}-shorttimeout`, `${P}-kms`]) {
    try { await logs.send(new CW.DeleteLogGroupCommand({ logGroupName: `/aws/lambda/${g}` })); } catch (e) {}
  }
  out.end();
}
