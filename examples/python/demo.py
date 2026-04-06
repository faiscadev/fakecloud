"""
FakeCloud demo: S3 + SQS + SNS + SSM using boto3.

Prerequisites:
    pip install boto3

Usage:
    1. Start FakeCloud: cargo run --bin fakecloud
    2. Run this script: python examples/python/demo.py
"""

import json
import time

import boto3

ENDPOINT = "http://localhost:4566"
REGION = "us-east-1"
ACCOUNT_ID = "123456789012"

session = boto3.Session(
    aws_access_key_id="test",
    aws_secret_access_key="test",
    region_name=REGION,
)

s3 = session.client("s3", endpoint_url=ENDPOINT)
sqs = session.client("sqs", endpoint_url=ENDPOINT)
sns = session.client("sns", endpoint_url=ENDPOINT)
ssm = session.client("ssm", endpoint_url=ENDPOINT)


def main():
    print("=== FakeCloud Demo: S3 + SQS + SNS + SSM ===\n")

    # --- S3 ---
    print("[S3] Creating bucket and uploading objects...")
    s3.create_bucket(Bucket="demo-bucket")

    s3.put_object(Bucket="demo-bucket", Key="hello.txt", Body=b"Hello from FakeCloud!")
    s3.put_object(Bucket="demo-bucket", Key="data/config.json", Body=json.dumps({"env": "local"}).encode())

    response = s3.get_object(Bucket="demo-bucket", Key="hello.txt")
    content = response["Body"].read().decode()
    print(f"  get hello.txt: {content}")

    objects = s3.list_objects_v2(Bucket="demo-bucket")
    print(f"  objects in bucket: {[obj['Key'] for obj in objects.get('Contents', [])]}")

    s3.delete_object(Bucket="demo-bucket", Key="hello.txt")
    s3.delete_object(Bucket="demo-bucket", Key="data/config.json")
    s3.delete_bucket(Bucket="demo-bucket")
    print("  bucket cleaned up")

    # --- SSM Parameter Store ---
    print("\n[SSM] Storing application config...")
    ssm.put_parameter(Name="/app/db-host", Value="localhost", Type="String")
    ssm.put_parameter(Name="/app/db-port", Value="5432", Type="String")
    ssm.put_parameter(
        Name="/app/db-password", Value="s3cret", Type="SecureString"
    )

    params = ssm.get_parameters_by_path(Path="/app/", Recursive=True)
    for p in params["Parameters"]:
        print(f"  {p['Name']} = {p['Value']} (type: {p['Type']})")

    # --- SQS ---
    print("\n[SQS] Creating queues...")
    orders_queue = sqs.create_queue(QueueName="orders")
    notifications_queue = sqs.create_queue(QueueName="notifications")

    orders_url = orders_queue["QueueUrl"]
    notifications_url = notifications_queue["QueueUrl"]
    print(f"  orders: {orders_url}")
    print(f"  notifications: {notifications_url}")

    # --- SNS ---
    print("\n[SNS] Setting up topic and subscriptions...")
    topic = sns.create_topic(Name="order-events")
    topic_arn = topic["TopicArn"]
    print(f"  topic: {topic_arn}")

    # Subscribe both queues to the topic
    notifications_arn = f"arn:aws:sqs:{REGION}:{ACCOUNT_ID}:notifications"
    sns.subscribe(
        TopicArn=topic_arn,
        Protocol="sqs",
        Endpoint=notifications_arn,
    )
    print(f"  subscribed: notifications queue -> {topic_arn}")

    # --- Publish through SNS -> SQS ---
    print("\n[SNS] Publishing order event...")
    order = {"order_id": "ORD-001", "item": "Widget", "quantity": 3}
    sns.publish(
        TopicArn=topic_arn,
        Message=json.dumps(order),
        Subject="New Order",
    )

    # Also send directly to SQS
    print("[SQS] Sending direct message to orders queue...")
    sqs.send_message(QueueUrl=orders_url, MessageBody=json.dumps(order))

    # --- Receive messages ---
    time.sleep(0.5)  # brief pause for delivery

    print("\n[SQS] Receiving from orders queue...")
    response = sqs.receive_message(QueueUrl=orders_url, MaxNumberOfMessages=10)
    for msg in response.get("Messages", []):
        print(f"  received: {msg['Body']}")
        sqs.delete_message(
            QueueUrl=orders_url, ReceiptHandle=msg["ReceiptHandle"]
        )

    print("\n[SQS] Receiving from notifications queue (via SNS fan-out)...")
    response = sqs.receive_message(
        QueueUrl=notifications_url, MaxNumberOfMessages=10
    )
    for msg in response.get("Messages", []):
        body = json.loads(msg["Body"])
        # SNS wraps the message in an envelope
        if "Message" in body:
            print(f"  received via SNS: {body['Message']}")
        else:
            print(f"  received: {msg['Body']}")
        sqs.delete_message(
            QueueUrl=notifications_url, ReceiptHandle=msg["ReceiptHandle"]
        )

    # --- S3 Bucket Notifications ---
    print("\n[S3 Notifications] Setting up bucket -> SQS delivery...")
    s3.create_bucket(Bucket="events-bucket")
    events_queue = sqs.create_queue(QueueName="s3-events")
    events_url = events_queue["QueueUrl"]
    events_arn = f"arn:aws:sqs:{REGION}:{ACCOUNT_ID}:s3-events"

    s3.put_bucket_notification_configuration(
        Bucket="events-bucket",
        NotificationConfiguration={
            "QueueConfigurations": [
                {
                    "QueueArn": events_arn,
                    "Events": ["s3:ObjectCreated:*"],
                }
            ]
        },
    )

    s3.put_object(Bucket="events-bucket", Key="trigger.txt", Body=b"trigger!")
    time.sleep(0.5)

    response = sqs.receive_message(QueueUrl=events_url, MaxNumberOfMessages=10)
    for msg in response.get("Messages", []):
        body = json.loads(msg["Body"])
        print(f"  S3 event received: {body.get('Event', body.get('Records', [{}])[0].get('eventName', 'unknown'))}")
        sqs.delete_message(QueueUrl=events_url, ReceiptHandle=msg["ReceiptHandle"])

    s3.delete_object(Bucket="events-bucket", Key="trigger.txt")
    s3.delete_bucket(Bucket="events-bucket")
    sqs.delete_queue(QueueUrl=events_url)
    print("  notification pipeline cleaned up")

    # --- Cleanup ---
    print("\n[Cleanup] Deleting resources...")
    sqs.delete_queue(QueueUrl=orders_url)
    sqs.delete_queue(QueueUrl=notifications_url)
    sns.delete_topic(TopicArn=topic_arn)
    ssm.delete_parameters(Names=["/app/db-host", "/app/db-port", "/app/db-password"])

    print("\nDone.")


if __name__ == "__main__":
    main()
