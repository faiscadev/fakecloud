import { S3Client, CreateBucketCommand, PutObjectCommand, GetObjectCommand, DeleteObjectCommand, DeleteBucketCommand } from "@aws-sdk/client-s3";
import { SQSClient, CreateQueueCommand, SendMessageCommand, ReceiveMessageCommand, DeleteQueueCommand } from "@aws-sdk/client-sqs";
import { SNSClient, CreateTopicCommand, SubscribeCommand, PublishCommand, DeleteTopicCommand } from "@aws-sdk/client-sns";
import { SSMClient, PutParameterCommand, GetParameterCommand, DeleteParametersCommand } from "@aws-sdk/client-ssm";

const ENDPOINT = process.env.FAKECLOUD_ENDPOINT || "http://localhost:4566";
const REGION = "us-east-1";
const credentials = { accessKeyId: "test", secretAccessKey: "test" };

const s3 = new S3Client({ endpoint: ENDPOINT, region: REGION, credentials, forcePathStyle: true });
const sqs = new SQSClient({ endpoint: ENDPOINT, region: REGION, credentials });
const sns = new SNSClient({ endpoint: ENDPOINT, region: REGION, credentials });
const ssm = new SSMClient({ endpoint: ENDPOINT, region: REGION, credentials });

async function main() {
  console.log("=== FakeCloud Node.js Demo ===\n");

  // --- S3 ---
  console.log("[S3] Creating bucket and uploading object...");
  await s3.send(new CreateBucketCommand({ Bucket: "demo-bucket" }));
  await s3.send(new PutObjectCommand({ Bucket: "demo-bucket", Key: "hello.txt", Body: "Hello from Node.js!" }));
  const obj = await s3.send(new GetObjectCommand({ Bucket: "demo-bucket", Key: "hello.txt" }));
  const body = await obj.Body.transformToString();
  console.log(`[S3] Got object: ${body}`);
  await s3.send(new DeleteObjectCommand({ Bucket: "demo-bucket", Key: "hello.txt" }));
  await s3.send(new DeleteBucketCommand({ Bucket: "demo-bucket" }));

  // --- SQS ---
  console.log("\n[SQS] Creating queue and sending message...");
  const { QueueUrl } = await sqs.send(new CreateQueueCommand({ QueueName: "demo-queue" }));
  await sqs.send(new SendMessageCommand({ QueueUrl, MessageBody: "hello from node" }));
  const { Messages } = await sqs.send(new ReceiveMessageCommand({ QueueUrl }));
  console.log(`[SQS] Received: ${Messages[0].Body}`);
  await sqs.send(new DeleteQueueCommand({ QueueUrl }));

  // --- SNS ---
  console.log("\n[SNS] Creating topic and publishing...");
  const { TopicArn } = await sns.send(new CreateTopicCommand({ Name: "demo-topic" }));
  await sns.send(new SubscribeCommand({ TopicArn, Protocol: "sqs", Endpoint: "arn:aws:sqs:us-east-1:123456789012:demo-queue" }));
  await sns.send(new PublishCommand({ TopicArn, Message: "hello from node" }));
  console.log(`[SNS] Published to ${TopicArn}`);
  await sns.send(new DeleteTopicCommand({ TopicArn }));

  // --- SSM ---
  console.log("\n[SSM] Putting and getting parameter...");
  await ssm.send(new PutParameterCommand({ Name: "/app/db-host", Value: "localhost", Type: "String" }));
  const param = await ssm.send(new GetParameterCommand({ Name: "/app/db-host" }));
  console.log(`[SSM] Got parameter: ${param.Parameter.Value}`);
  await ssm.send(new DeleteParametersCommand({ Names: ["/app/db-host"] }));

  console.log("\nDone.");
}

main().catch(console.error);
