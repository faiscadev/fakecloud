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

const lambda = new L.LambdaClient({ region: "us-east-1", requestHandler: new RecordingHandler(), maxAttempts: 1 });
const iam = new I.IAMClient({ region: "us-east-1" });
const logs = new CW.CloudWatchLogsClient({ region: "us-east-1" });
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const FN = "fc-durable-rec3-long";
const ROLE = "fc-durable-rec3-long-role";
const POLICY = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicDurableExecutionRolePolicy";
const enc = (o) => new TextEncoder().encode(JSON.stringify(o));
let created = false;
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
  for (let i = 0; i < 8; i++) {
    try {
      await lambda.send(new L.CreateFunctionCommand({ FunctionName: FN, Runtime: "nodejs22.x", Role: roleArn, Handler: "index.handler", Code: { ZipFile: fs.readFileSync(ZIP) }, Timeout: 60, MemorySize: 256, DurableConfig: { ExecutionTimeout: 1200, RetentionPeriodInDays: 1 } }));
      created = true;
      break;
    } catch (e) { if (!/assume/i.test(e.message)) throw e; await sleep(5000); }
  }
  await L.waitUntilFunctionActiveV2({ client: lambda, maxWaitTime: 120 }, { FunctionName: FN });
  const t0 = Date.now();
  const r = await attempt("sync-over-900", () => lambda.send(new L.InvokeCommand({ FunctionName: FN + ":$LATEST", Payload: enc({ mode: "forever" }), DurableExecutionName: "over-900" })));
  rec({ note: "elapsed", ms: Date.now() - t0 });
  const list = await attempt("list", () => lambda.send(new L.ListDurableExecutionsByFunctionCommand({ FunctionName: FN })));
  const arn = r?.DurableExecutionArn ?? list?.DurableExecutions?.[0]?.DurableExecutionArn;
  if (arn) {
    await attempt("get-after", () => lambda.send(new L.GetDurableExecutionCommand({ DurableExecutionArn: arn })));
    await attempt("history-after", () => lambda.send(new L.GetDurableExecutionHistoryCommand({ DurableExecutionArn: arn, IncludeExecutionData: true })));
    await attempt("stop", () => lambda.send(new L.StopDurableExecutionCommand({ DurableExecutionArn: arn })));
  }
} finally {
  label = "cleanup";
  if (created) { try { await lambda.send(new L.DeleteFunctionCommand({ FunctionName: FN })); } catch (e) { rec({ note: "cleanup", what: "fn", e: e.name }); } }
  try { await iam.send(new I.DetachRolePolicyCommand({ RoleName: ROLE, PolicyArn: POLICY })); } catch (e) { rec({ note: "cleanup", what: "detach", e: e.name }); }
  try { await iam.send(new I.DeleteRoleCommand({ RoleName: ROLE })); } catch (e) { rec({ note: "cleanup", what: "role", e: e.name }); }
  try { await logs.send(new CW.DeleteLogGroupCommand({ logGroupName: `/aws/lambda/${FN}` })); } catch (e) {}
  out.end();
}
