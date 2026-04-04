"""
FakeCloud demo: SQS + SNS + SSM using boto3.

Prerequisites:
    pip install boto3

Usage:
    1. Start FakeCloud: cargo run --bin fakecloud-server
    2. Run this script: python examples/python/demo.py
"""

import json
import time

import boto3

ENDPOINT = "http://localhost:4566"
REGION = "us-east-1"
ACCOUNT_ID = "000000000000"

session = boto3.Session(
    aws_access_key_id="test",
    aws_secret_access_key="test",
    region_name=REGION,
)

sqs = session.client("sqs", endpoint_url=ENDPOINT)
sns = session.client("sns", endpoint_url=ENDPOINT)
ssm = session.client("ssm", endpoint_url=ENDPOINT)


def main():
    print("=== FakeCloud Demo: SQS + SNS + SSM ===\n")

    # --- SSM Parameter Store ---
    print("[SSM] Storing application config...")
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

    # --- Cleanup ---
    print("\n[Cleanup] Deleting resources...")
    sqs.delete_queue(QueueUrl=orders_url)
    sqs.delete_queue(QueueUrl=notifications_url)
    sns.delete_topic(TopicArn=topic_arn)
    ssm.delete_parameters(Names=["/app/db-host", "/app/db-port", "/app/db-password"])

    print("\nDone.")


if __name__ == "__main__":
    main()
