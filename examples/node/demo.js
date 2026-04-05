/**
 * FakeCloud demo: S3 + SQS + SNS + SSM + DynamoDB using AWS SDK v3.
 *
 * Prerequisites:
 *   npm install @aws-sdk/client-s3 @aws-sdk/client-sqs @aws-sdk/client-sns @aws-sdk/client-ssm @aws-sdk/client-dynamodb
 *
 * Usage:
 *   1. Start FakeCloud: cargo run --bin fakecloud
 *   2. Run this script: node examples/node/demo.js
 */

import {
  S3Client,
  CreateBucketCommand,
  PutObjectCommand,
  GetObjectCommand,
  ListObjectsV2Command,
  DeleteObjectCommand,
  DeleteBucketCommand,
} from "@aws-sdk/client-s3";

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

import {
  DynamoDBClient,
  CreateTableCommand as DDBCreateTableCommand,
  PutItemCommand,
  QueryCommand as DDBQueryCommand,
  DeleteTableCommand as DDBDeleteTableCommand,
} from "@aws-sdk/client-dynamodb";

const ENDPOINT = "http://localhost:4566";
const REGION = "us-east-1";
const ACCOUNT_ID = "123456789012";
const credentials = { accessKeyId: "test", secretAccessKey: "test" };

const s3 = new S3Client({ endpoint: ENDPOINT, region: REGION, credentials, forcePathStyle: true });
const sqs = new SQSClient({ endpoint: ENDPOINT, region: REGION, credentials });
const sns = new SNSClient({ endpoint: ENDPOINT, region: REGION, credentials });
const ssm = new SSMClient({ endpoint: ENDPOINT, region: REGION, credentials });
const dynamodb = new DynamoDBClient({ endpoint: ENDPOINT, region: REGION, credentials });

async function main() {
  console.log("=== FakeCloud Demo: S3 + SQS + SNS + SSM + DynamoDB ===\n");

  // --- S3 ---
  console.log("[S3] Creating bucket and uploading objects...");
  await s3.send(new CreateBucketCommand({ Bucket: "demo-bucket" }));

  await s3.send(new PutObjectCommand({
    Bucket: "demo-bucket",
    Key: "hello.txt",
    Body: "Hello from FakeCloud!",
  }));
  await s3.send(new PutObjectCommand({
    Bucket: "demo-bucket",
    Key: "data/config.json",
    Body: JSON.stringify({ env: "local" }),
  }));

  const getResp = await s3.send(new GetObjectCommand({ Bucket: "demo-bucket", Key: "hello.txt" }));
  const content = await getResp.Body.transformToString();
  console.log(`  get hello.txt: ${content}`);

  const listResp = await s3.send(new ListObjectsV2Command({ Bucket: "demo-bucket" }));
  const keys = (listResp.Contents || []).map((o) => o.Key);
  console.log(`  objects in bucket: ${JSON.stringify(keys)}`);

  await s3.send(new DeleteObjectCommand({ Bucket: "demo-bucket", Key: "hello.txt" }));
  await s3.send(new DeleteObjectCommand({ Bucket: "demo-bucket", Key: "data/config.json" }));
  await s3.send(new DeleteBucketCommand({ Bucket: "demo-bucket" }));
  console.log("  bucket cleaned up");

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

  // --- DynamoDB ---
  console.log("\n[DynamoDB] Creating table and working with items...");
  await dynamodb.send(new DDBCreateTableCommand({
    TableName: "demo-orders",
    KeySchema: [
      { AttributeName: "userId", KeyType: "HASH" },
      { AttributeName: "orderId", KeyType: "RANGE" },
    ],
    AttributeDefinitions: [
      { AttributeName: "userId", AttributeType: "S" },
      { AttributeName: "orderId", AttributeType: "S" },
    ],
    BillingMode: "PAY_PER_REQUEST",
  }));

  await dynamodb.send(new PutItemCommand({
    TableName: "demo-orders",
    Item: {
      userId: { S: "user1" },
      orderId: { S: "order-001" },
      total: { N: "29.99" },
      status: { S: "shipped" },
    },
  }));
  await dynamodb.send(new PutItemCommand({
    TableName: "demo-orders",
    Item: {
      userId: { S: "user1" },
      orderId: { S: "order-002" },
      total: { N: "59.99" },
      status: { S: "pending" },
    },
  }));

  const queryResp = await dynamodb.send(new DDBQueryCommand({
    TableName: "demo-orders",
    KeyConditionExpression: "userId = :uid",
    ExpressionAttributeValues: { ":uid": { S: "user1" } },
  }));
  for (const item of queryResp.Items || []) {
    console.log(`  ${item.orderId.S}: $${item.total.N} (${item.status.S})`);
  }

  await dynamodb.send(new DDBDeleteTableCommand({ TableName: "demo-orders" }));
  console.log("  table cleaned up");

  // --- Cleanup ---
  console.log("\n[Cleanup] Deleting resources...");
  await sqs.send(new DeleteQueueCommand({ QueueUrl: ordersUrl }));
  await sqs.send(new DeleteQueueCommand({ QueueUrl: notificationsUrl }));
  await sns.send(new DeleteTopicCommand({ TopicArn }));
  await ssm.send(new DeleteParametersCommand({ Names: ["/app/db-host", "/app/db-port", "/app/db-password"] }));

  console.log("\nDone.");
}

main().catch(console.error);
