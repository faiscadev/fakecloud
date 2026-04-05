/**
 * FakeCloud demo: SQS + SNS + SSM using AWS SDK v3.
 *
 * Prerequisites:
 *   npm install @aws-sdk/client-sqs @aws-sdk/client-sns @aws-sdk/client-ssm
 *
 * Usage:
 *   1. Start FakeCloud: cargo run --bin fakecloud-server
 *   2. Run this script: node examples/node/demo.js
 */

import {
  SQSClient,
  CreateQueueCommand,
  SendMessageCommand,
  ReceiveMessageCommand,
  DeleteMessageCommand,
  DeleteQueueCommand,
} from "@aws-sdk/client-sqs";

import {
  SNSClient,
  CreateTopicCommand,
  SubscribeCommand,
  PublishCommand,
  DeleteTopicCommand,
} from "@aws-sdk/client-sns";

import {
  SSMClient,
  PutParameterCommand,
  GetParametersByPathCommand,
  DeleteParametersCommand,
} from "@aws-sdk/client-ssm";

const ENDPOINT = "http://localhost:4566";
const REGION = "us-east-1";
const ACCOUNT_ID = "123456789012";
const credentials = { accessKeyId: "test", secretAccessKey: "test" };

const sqs = new SQSClient({ endpoint: ENDPOINT, region: REGION, credentials });
const sns = new SNSClient({ endpoint: ENDPOINT, region: REGION, credentials });
const ssm = new SSMClient({ endpoint: ENDPOINT, region: REGION, credentials });

async function main() {
  console.log("=== FakeCloud Demo: SQS + SNS + SSM ===\n");

  // --- SSM Parameter Store ---
  console.log("[SSM] Storing application config...");
  await ssm.send(new PutParameterCommand({ Name: "/app/db-host", Value: "localhost", Type: "String" }));
  await ssm.send(new PutParameterCommand({ Name: "/app/db-port", Value: "5432", Type: "String" }));
  await ssm.send(new PutParameterCommand({ Name: "/app/db-password", Value: "s3cret", Type: "SecureString" }));

  const params = await ssm.send(new GetParametersByPathCommand({ Path: "/app/", Recursive: true }));
  for (const p of params.Parameters) {
    console.log(`  ${p.Name} = ${p.Value} (type: ${p.Type})`);
  }

  // --- SQS ---
  console.log("\n[SQS] Creating queues...");
  const { QueueUrl: ordersUrl } = await sqs.send(new CreateQueueCommand({ QueueName: "orders" }));
  const { QueueUrl: notificationsUrl } = await sqs.send(new CreateQueueCommand({ QueueName: "notifications" }));
  console.log(`  orders: ${ordersUrl}`);
  console.log(`  notifications: ${notificationsUrl}`);

  // --- SNS ---
  console.log("\n[SNS] Setting up topic and subscriptions...");
  const { TopicArn } = await sns.send(new CreateTopicCommand({ Name: "order-events" }));
  console.log(`  topic: ${TopicArn}`);

  const notificationsArn = `arn:aws:sqs:${REGION}:${ACCOUNT_ID}:notifications`;
  await sns.send(new SubscribeCommand({
    TopicArn,
    Protocol: "sqs",
    Endpoint: notificationsArn,
  }));
  console.log(`  subscribed: notifications queue -> ${TopicArn}`);

  // --- Publish through SNS -> SQS ---
  const order = { order_id: "ORD-001", item: "Widget", quantity: 3 };

  console.log("\n[SNS] Publishing order event...");
  await sns.send(new PublishCommand({
    TopicArn,
    Message: JSON.stringify(order),
    Subject: "New Order",
  }));

  console.log("[SQS] Sending direct message to orders queue...");
  await sqs.send(new SendMessageCommand({
    QueueUrl: ordersUrl,
    MessageBody: JSON.stringify(order),
  }));

  // Brief pause for delivery
  await new Promise((r) => setTimeout(r, 500));

  // --- Receive messages ---
  console.log("\n[SQS] Receiving from orders queue...");
  const ordersResp = await sqs.send(new ReceiveMessageCommand({
    QueueUrl: ordersUrl,
    MaxNumberOfMessages: 10,
  }));
  for (const msg of ordersResp.Messages || []) {
    console.log(`  received: ${msg.Body}`);
    await sqs.send(new DeleteMessageCommand({
      QueueUrl: ordersUrl,
      ReceiptHandle: msg.ReceiptHandle,
    }));
  }

  console.log("\n[SQS] Receiving from notifications queue (via SNS fan-out)...");
  const notifResp = await sqs.send(new ReceiveMessageCommand({
    QueueUrl: notificationsUrl,
    MaxNumberOfMessages: 10,
  }));
  for (const msg of notifResp.Messages || []) {
    try {
      const body = JSON.parse(msg.Body);
      if (body.Message) {
        console.log(`  received via SNS: ${body.Message}`);
      } else {
        console.log(`  received: ${msg.Body}`);
      }
    } catch {
      console.log(`  received: ${msg.Body}`);
    }
    await sqs.send(new DeleteMessageCommand({
      QueueUrl: notificationsUrl,
      ReceiptHandle: msg.ReceiptHandle,
    }));
  }

  // --- Cleanup ---
  console.log("\n[Cleanup] Deleting resources...");
  await sqs.send(new DeleteQueueCommand({ QueueUrl: ordersUrl }));
  await sqs.send(new DeleteQueueCommand({ QueueUrl: notificationsUrl }));
  await sns.send(new DeleteTopicCommand({ TopicArn }));
  await ssm.send(new DeleteParametersCommand({ Names: ["/app/db-host", "/app/db-port", "/app/db-password"] }));

  console.log("\nDone.");
}

main().catch(console.error);
