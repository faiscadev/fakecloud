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
const FN = "fc-durable-rec3d-kms";
const FN2 = "fc-durable-rec3d-kms-pending";
const ROLE = "fc-durable-rec3d-role";
const POLICY = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicDurableExecutionRolePolicy";
const ACCOUNT = "454591548454";
const enc = (o) => new TextEncoder().encode(JSON.stringify(o));
const created = [];
let keyId;
async function attempt(l, f) {
  label = l;
  try { return await f(); } catch (e) { rec({ note: "error", name: e.name, message: e.message, status: e.$metadata?.httpStatusCode }); return undefined; }
}
try {
  label = "setup";
  await iam.send(new I.CreateRoleCommand({ RoleName: ROLE, AssumeRolePolicyDocument: JSON.stringify({ Version: "2012-10-17", Statement: [{ Effect: "Allow", Principal: { Service: "lambda.amazonaws.com" }, Action: "sts:AssumeRole" }] }) }));
  await iam.send(new I.AttachRolePolicyCommand({ RoleName: ROLE, PolicyArn: POLICY }));
  const roleArn = (await iam.send(new I.GetRoleCommand({ RoleName: ROLE }))).Role.Arn;
  await sleep(12000);
  const policy = { Version: "2012-10-17", Statement: [
    { Sid: "root", Effect: "Allow", Principal: { AWS: `arn:aws:iam::${ACCOUNT}:root` }, Action: "kms:*", Resource: "*" },
    { Sid: "lambda", Effect: "Allow", Principal: { Service: "lambda.amazonaws.com" }, Action: ["kms:GenerateDataKey", "kms:Decrypt"], Resource: "*" },
    { Sid: "role", Effect: "Allow", Principal: { AWS: roleArn }, Action: "kms:Decrypt", Resource: "*" } ] };
  const key = await attempt("kms-create", () => kms.send(new K.CreateKeyCommand({ Description: "fakecloud durable recording (temporary)", Policy: JSON.stringify(policy) })));
  keyId = key.KeyMetadata.KeyId;
  const keyArn = key.KeyMetadata.Arn;
  const mk = async (name, cfg) => {
    for (let i = 0; i < 8; i++) {
      try {
        const r = await lambda.send(new L.CreateFunctionCommand({ FunctionName: name, Runtime: "nodejs22.x", Role: roleArn, Handler: "index.handler", Code: { ZipFile: fs.readFileSync(ZIP) }, Timeout: 60, MemorySize: 256, DurableConfig: cfg }));
        created.push(name);
        await L.waitUntilFunctionActiveV2({ client: lambda, maxWaitTime: 120 }, { FunctionName: name });
        return r;
      } catch (e) { if (!/assume/i.test(e.message)) throw e; await sleep(5000); }
    }
  };
  await attempt("create-with-key", () => mk(FN, { ExecutionTimeout: 1800, KMSKeyArn: keyArn }));
  const ok = await attempt("invoke-ok", () => lambda.send(new L.InvokeCommand({ FunctionName: FN + ":$LATEST", Payload: enc({ mode: "plain" }), DurableExecutionName: "ok" })));
  await attempt("schedule-delete", () => kms.send(new K.ScheduleKeyDeletionCommand({ KeyId: keyId, PendingWindowInDays: 7 })));
  await attempt("create-pending-key", () => mk(FN2, { ExecutionTimeout: 60, KMSKeyArn: keyArn }));
  const t0 = Date.now();
  for (let round = 0; Date.now() - t0 < 17 * 60 * 1000; round++) {
    const tag = `t${Math.round((Date.now() - t0) / 1000)}`;
    const g = await attempt(`${tag}-get-data`, () => lambda.send(new L.GetDurableExecutionCommand({ DurableExecutionArn: ok.DurableExecutionArn })));
    await attempt(`${tag}-invoke-new`, () => lambda.send(new L.InvokeCommand({ FunctionName: FN + ":$LATEST", Payload: enc({ mode: "plain" }), DurableExecutionName: `new-${round}` })));
    await sleep(120000);
  }
} finally {
  label = "cleanup";
  for (const n of [...created]) { try { await lambda.send(new L.DeleteFunctionCommand({ FunctionName: n })); } catch (e) { rec({ note: "cleanup", what: n, e: e.name }); } }
  try { await iam.send(new I.DetachRolePolicyCommand({ RoleName: ROLE, PolicyArn: POLICY })); } catch (e) {}
  try { await iam.send(new I.DeleteRoleCommand({ RoleName: ROLE })); } catch (e) { rec({ note: "cleanup", what: "role", e: e.name }); }
  for (const g of [FN, FN2]) { try { await logs.send(new CW.DeleteLogGroupCommand({ logGroupName: `/aws/lambda/${g}` })); } catch (e) {} }
  out.end();
}
