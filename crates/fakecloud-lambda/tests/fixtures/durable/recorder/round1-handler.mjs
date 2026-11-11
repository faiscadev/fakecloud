import { withDurableExecution, DurableExecutionApiClient } from "@aws/durable-execution-sdk-js";
import { LambdaClient } from "@aws-sdk/client-lambda";
import { NodeHttpHandler } from "@smithy/node-http-handler";
import { Readable } from "node:stream";

// Logs the raw wire exchange of every checkpoint / state call so the
// recording captures the exact JSON the service sends back.
class RecordingHandler {
  constructor() { this.inner = new NodeHttpHandler(); }
  async handle(request, options) {
    const { response } = await this.inner.handle(request, options);
    const chunks = [];
    for await (const c of response.body) chunks.push(c);
    const buf = Buffer.concat(chunks);
    console.log("WIRE " + JSON.stringify({
      method: request.method, path: request.path, query: request.query,
      requestBody: typeof request.body === "string" ? request.body : null,
      status: response.statusCode, headers: response.headers, body: buf.toString("utf8"),
    }));
    response.body = Readable.from([buf]);
    return { response };
  }
}

const recordingClient = new DurableExecutionApiClient(
  new LambdaClient({ requestHandler: new RecordingHandler() }),
);

const durable = withDurableExecution(async (event, context) => {
  const doubled = await context.step("double", async () => (event.n ?? 1) * 2);
  if (event.waitSeconds) {
    await context.wait("pause", { seconds: event.waitSeconds });
  }
  let approval = null;
  if (event.callback) {
    const cfg = { timeout: { seconds: event.callbackTimeout ?? 600 } };
    if (event.heartbeat) cfg.heartbeatTimeout = { seconds: event.heartbeat };
    approval = await context.waitForCallback("approval", async (callbackId) => {
      console.log("CALLBACK_ID " + callbackId);
    }, cfg);
  }
  if (event.fail) {
    throw new Error("requested failure");
  }
  return { doubled, approval };
}, { durableExecutionClient: recordingClient });

export const handler = async (event, context) => {
  console.log("EVENT " + JSON.stringify(event));
  const out = await durable(event, context);
  console.log("OUTPUT " + JSON.stringify(out));
  return out;
};
