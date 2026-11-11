// Second recording round: timeouts, retries, chained invokes, raw checkpoint
// validation, and odd handler outputs. Every resource is deleted at the end.
import fs from "node:fs";
import { Readable } from "node:stream";
import { NodeHttpHandler } from "@smithy/node-http-handler";
import * as L from "@aws-sdk/client-lambda";
import * as I from "@aws-sdk/client-iam";
import * as CW from "@aws-sdk/client-cloudwatch-logs";

const [OUT, ZIP, TZIP] = process.argv.slice(2);
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
const FN = "fc-durable-rec2";
const SHORT = "fc-durable-rec2-short";
const TARGET = "fc-durable-rec2-target";
const ROLE = "fc-durable-rec2-role";
const POLICY = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicDurableExecutionRolePolicy";
const enc = (o) => new TextEncoder().encode(JSON.stringify(o));
async function attempt(l, f) {
  label = l;
  try { return await f(); } catch (e) { rec({ note: "error", name: e.name, message: e.message }); return undefined; }
}
async function settle(arn, maxMs = 90000) {
  const t0 = Date.now();
  while (Date.now() - t0 < maxMs) {
    const r = await lambda.send(new L.GetDurableExecutionCommand({ DurableExecutionArn: arn }));
    if (r.Status !== "RUNNING") return r;
    await sleep(2500);
  }
  return undefined;
}
async function scenario(name, fn, payload, { type = "RequestResponse", wait = 90000 } = {}) {
  const r = await attempt(`${name}-invoke`, () => lambda.send(new L.InvokeCommand({ FunctionName: fn + ":$LATEST", InvocationType: type, Payload: enc(payload), DurableExecutionName: name })));
  const arn = r?.DurableExecutionArn;
  if (!arn) return undefined;
  await attempt(`${name}-settle`, () => settle(arn, wait));
  await attempt(`${name}-get`, () => lambda.send(new L.GetDurableExecutionCommand({ DurableExecutionArn: arn })));
  await attempt(`${name}-history`, () => lambda.send(new L.GetDurableExecutionHistoryCommand({ DurableExecutionArn: arn, IncludeExecutionData: true })));
  return arn;
}

// Minimal CBOR text encoding for a callback id naming a missing execution.
function cborText(s) { const b = Buffer.from(s); return Buffer.concat([Buffer.from(b.length < 24 ? [0x60 + b.length] : [0x78, b.length]), b]); }

const created = [];
try {
  label = "setup";
  await iam.send(new I.CreateRoleCommand({ RoleName: ROLE, AssumeRolePolicyDocument: JSON.stringify({ Version: "2012-10-17", Statement: [{ Effect: "Allow", Principal: { Service: "lambda.amazonaws.com" }, Action: "sts:AssumeRole" }] }) }));
  await iam.send(new I.AttachRolePolicyCommand({ RoleName: ROLE, PolicyArn: POLICY }));
  await iam.send(new I.PutRolePolicyCommand({ RoleName: ROLE, PolicyName: "invoke", PolicyDocument: JSON.stringify({ Version: "2012-10-17", Statement: [{ Effect: "Allow", Action: "lambda:InvokeFunction", Resource: "*" }] }) }));
  const roleArn = (await iam.send(new I.GetRoleCommand({ RoleName: ROLE }))).Role.Arn;
  await sleep(12000);
  const mk = async (name, zip, handler, durable) => {
    for (let i = 0; i < 8; i++) {
      try {
        const r = await lambda.send(new L.CreateFunctionCommand({ FunctionName: name, Runtime: "nodejs22.x", Role: roleArn, Handler: handler, Code: { ZipFile: fs.readFileSync(zip) }, Timeout: 60, MemorySize: 256, ...(durable ? { DurableConfig: durable } : {}) }));
        created.push(name);
        return r;
      } catch (e) { if (!/assume/i.test(e.message)) throw e; await sleep(5000); }
    }
  };
  await attempt("create-main", () => mk(FN, ZIP, "index.handler", { ExecutionTimeout: 600, RetentionPeriodInDays: 1 }));
  await attempt("create-short", () => mk(SHORT, ZIP, "index.handler", { ExecutionTimeout: 15 }));
  await attempt("create-target", () => mk(TARGET, TZIP, "target.handler", null));
  for (const n of [FN, SHORT, TARGET]) await L.waitUntilFunctionActiveV2({ client: lambda, maxWaitTime: 120 }, { FunctionName: n });
  await attempt("short-config", () => lambda.send(new L.GetFunctionConfigurationCommand({ FunctionName: SHORT })));

  await scenario("probe", FN, { mode: "probe" });
  await scenario("retry", FN, { mode: "retry" });
  await scenario("chain", FN, { mode: "chain", target: TARGET + ":$LATEST" });
  await scenario("chainfail", FN, { mode: "chainfail", target: TARGET + ":$LATEST" });
  await scenario("chain-unqualified", FN, { mode: "chain", target: TARGET });
  await scenario("chain-durable", FN, { mode: "chain", target: FN + ":$LATEST" });
  await scenario("cbtimeout", FN, { mode: "cbtimeout" });
  await scenario("hbtimeout", FN, { mode: "hbtimeout" });
  await scenario("parallel", FN, { mode: "parallel" });
  await scenario("pending", SHORT, { mode: "pending" }, { type: "Event", wait: 40000 });
  await scenario("garbage", SHORT, { mode: "garbage" }, { type: "Event", wait: 40000 });
  await scenario("crash", SHORT, { mode: "crash" }, { type: "Event", wait: 40000 });
  await scenario("forever", SHORT, { mode: "forever" }, { type: "Event", wait: 40000 });
  await scenario("sync-timeout", SHORT, { mode: "forever" }, { wait: 40000 });

  // Async start, then same name with a different payload.
  const dup = await attempt("dupe-a", () => lambda.send(new L.InvokeCommand({ FunctionName: FN + ":$LATEST", InvocationType: "Event", Payload: enc({ mode: "forever" }), DurableExecutionName: "dupe" })));
  await attempt("dupe-b", () => lambda.send(new L.InvokeCommand({ FunctionName: FN + ":$LATEST", InvocationType: "Event", Payload: enc({ mode: "other" }), DurableExecutionName: "dupe" })));
  await attempt("dupe-sync-b", () => lambda.send(new L.InvokeCommand({ FunctionName: FN + ":$LATEST", Payload: enc({ mode: "other" }), DurableExecutionName: "dupe" })));
  await attempt("dupe-other-version", () => lambda.send(new L.InvokeCommand({ FunctionName: SHORT + ":$LATEST", InvocationType: "Event", Payload: enc({ mode: "forever" }), DurableExecutionName: "dupe" })));
  if (dup?.DurableExecutionArn) {
    await sleep(3000);
    await attempt("dupe-stop", () => lambda.send(new L.StopDurableExecutionCommand({ DurableExecutionArn: dup.DurableExecutionArn })));
    await attempt("dupe-get", () => lambda.send(new L.GetDurableExecutionCommand({ DurableExecutionArn: dup.DurableExecutionArn })));
    await attempt("dupe-history", () => lambda.send(new L.GetDurableExecutionHistoryCommand({ DurableExecutionArn: dup.DurableExecutionArn, IncludeExecutionData: true })));
  }
  await attempt("dryrun", () => lambda.send(new L.InvokeCommand({ FunctionName: FN + ":$LATEST", InvocationType: "DryRun", Payload: enc({}) })));

  const fake = `arn:aws:lambda:us-east-1:454591548454:function:${FN}:$LATEST/durable-execution/nope/00000000-0000-0000-0000-000000000000`;
  const cb = Buffer.concat([Buffer.from([0x01, 0xbf]), cborText("e"), cborText(fake), cborText("i"), cborText("11111111-1111-1111-1111-111111111111"), cborText("r"), cborText("$LATEST:g-22222222-2222-2222-2222-222222222222"), Buffer.from([0xff])]).toString("base64").replace(/=+$/, "");
  await attempt("callback-missing-execution", () => lambda.send(new L.SendDurableExecutionCallbackSuccessCommand({ CallbackId: cb, Result: enc(1) })));

  const fnArn = `arn:aws:lambda:us-east-1:454591548454:function:${FN}`;
  await attempt("list-by-arn", () => lambda.send(new L.ListDurableExecutionsByFunctionCommand({ FunctionName: fnArn, MaxItems: 2 })));
  await attempt("list-by-qualified-arn", () => lambda.send(new L.ListDurableExecutionsByFunctionCommand({ FunctionName: fnArn + ":$LATEST", MaxItems: 2 })));
  await attempt("list-qualifier", () => lambda.send(new L.ListDurableExecutionsByFunctionCommand({ FunctionName: FN, Qualifier: "$LATEST", MaxItems: 2 })));
  await attempt("list-qualifier-1", () => lambda.send(new L.ListDurableExecutionsByFunctionCommand({ FunctionName: FN, Qualifier: "1" })));
  await attempt("list-status", () => lambda.send(new L.ListDurableExecutionsByFunctionCommand({ FunctionName: FN, Statuses: ["FAILED", "STOPPED"] })));
  await attempt("list-name", () => lambda.send(new L.ListDurableExecutionsByFunctionCommand({ FunctionName: FN, DurableExecutionName: "retry" })));
  await attempt("list-after", () => lambda.send(new L.ListDurableExecutionsByFunctionCommand({ FunctionName: FN, StartedAfter: new Date(Date.now() + 3600e3) })));
  await attempt("list-plain-function", () => lambda.send(new L.ListDurableExecutionsByFunctionCommand({ FunctionName: TARGET })));

  label = "logs";
  await sleep(10000);
  const lines = [];
  for (const g of [FN, SHORT, TARGET]) {
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
  for (const n of created) {
    try { await lambda.send(new L.DeleteFunctionCommand({ FunctionName: n })); } catch (e) { rec({ note: "cleanup", what: n, e: e.name }); }
  }
  try { await iam.send(new I.DeleteRolePolicyCommand({ RoleName: ROLE, PolicyName: "invoke" })); } catch (e) { rec({ note: "cleanup", what: "inline", e: e.name }); }
  try { await iam.send(new I.DetachRolePolicyCommand({ RoleName: ROLE, PolicyArn: POLICY })); } catch (e) { rec({ note: "cleanup", what: "detach", e: e.name }); }
  try { await iam.send(new I.DeleteRoleCommand({ RoleName: ROLE })); } catch (e) { rec({ note: "cleanup", what: "role", e: e.name }); }
  for (const g of [FN, SHORT, TARGET]) {
    try { await logs.send(new CW.DeleteLogGroupCommand({ logGroupName: `/aws/lambda/${g}` })); } catch (e) { rec({ note: "cleanup", what: "logs " + g, e: e.name }); }
  }
  out.end();
}
