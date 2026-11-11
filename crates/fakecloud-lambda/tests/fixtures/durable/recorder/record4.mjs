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
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const ROLE = "fc-durable-rec4-role";
const POLICY = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicDurableExecutionRolePolicy";
const created = [];
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
  const mk = async (name, cfg) => {
    for (let i = 0; i < 8; i++) {
      try {
        const r = await lambda.send(new L.CreateFunctionCommand({ FunctionName: name, Runtime: "nodejs22.x", Role: roleArn, Handler: "index.handler", Code: { ZipFile: fs.readFileSync(ZIP) }, Timeout: 60, DurableConfig: cfg }));
        created.push(name);
        return r;
      } catch (e) { if (!/assume/i.test(e.message)) throw e; await sleep(5000); }
    }
  };
  const managed = await attempt("describe-aws-lambda-alias", () => kms.send(new K.DescribeKeyCommand({ KeyId: "alias/aws/lambda" })));
  await attempt("create-alias-arn", () => mk("fc-durable-rec4-alias", { ExecutionTimeout: 60, KMSKeyArn: "arn:aws:kms:us-east-1:454591548454:alias/aws/lambda" }));
  if (managed?.KeyMetadata?.Arn) await attempt("create-managed-key-arn", () => mk("fc-durable-rec4-managed", { ExecutionTimeout: 60, KMSKeyArn: managed.KeyMetadata.Arn }));
  await attempt("create-missing-alias-arn", () => mk("fc-durable-rec4-missing-alias", { ExecutionTimeout: 60, KMSKeyArn: "arn:aws:kms:us-east-1:454591548454:alias/fc-durable-no-such-alias" }));
  await attempt("create-other-service-arn", () => mk("fc-durable-rec4-other", { ExecutionTimeout: 60, KMSKeyArn: "arn:aws:s3:::bucket" }));
} finally {
  label = "cleanup";
  for (const n of [...created]) { try { await lambda.send(new L.DeleteFunctionCommand({ FunctionName: n })); } catch (e) { rec({ note: "cleanup", what: n, e: e.name }); } }
  try { await iam.send(new I.DetachRolePolicyCommand({ RoleName: ROLE, PolicyArn: POLICY })); } catch (e) {}
  try { await iam.send(new I.DeleteRoleCommand({ RoleName: ROLE })); } catch (e) { rec({ note: "cleanup", what: "role", e: e.name }); }
  out.end();
}
