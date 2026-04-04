"""
Sample Order Processing Pipeline running against FakeCloud.

Demonstrates: SQS, SNS, EventBridge, SSM, IAM working together.
"""

import json
import time
import boto3

ENDPOINT = "http://localhost:4566"
REGION = "us-east-1"


def client(service):
    return boto3.client(
        service,
        endpoint_url=ENDPOINT,
        region_name=REGION,
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )


def main():
    sqs = client("sqs")
    sns = client("sns")
    events = client("events")
    ssm = client("ssm")
    sts = client("sts")

    print("=== FakeCloud Sample App: Order Processing Pipeline ===\n")

    # 1. Verify identity
    identity = sts.get_caller_identity()
    print(f"1. Connected as account {identity['Account']}")

    # 2. Store configuration in SSM
    ssm.put_parameter(
        Name="/orders/queue-name",
        Value="orders-queue",
        Type="String",
        Overwrite=True,
    )
    ssm.put_parameter(
        Name="/orders/notification-topic",
        Value="order-notifications",
        Type="String",
        Overwrite=True,
    )
    print("2. Configuration stored in SSM Parameter Store")

    # 3. Read config from SSM
    queue_name = ssm.get_parameter(Name="/orders/queue-name")["Parameter"]["Value"]
    topic_name = ssm.get_parameter(Name="/orders/notification-topic")["Parameter"][
        "Value"
    ]
    print(f"3. Read config: queue={queue_name}, topic={topic_name}")

    # 4. Create SQS queues
    order_queue = sqs.create_queue(QueueName=queue_name)
    order_queue_url = order_queue["QueueUrl"]
    order_queue_arn = sqs.get_queue_attributes(
        QueueUrl=order_queue_url, AttributeNames=["QueueArn"]
    )["Attributes"]["QueueArn"]

    notification_queue = sqs.create_queue(QueueName="notification-queue")
    notification_queue_url = notification_queue["QueueUrl"]
    notification_queue_arn = sqs.get_queue_attributes(
        QueueUrl=notification_queue_url, AttributeNames=["QueueArn"]
    )["Attributes"]["QueueArn"]
    print(f"4. Created SQS queues: {queue_name}, notification-queue")

    # 5. Create SNS topic and subscribe the notification queue
    topic = sns.create_topic(Name=topic_name)
    topic_arn = topic["TopicArn"]
    sns.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=notification_queue_arn)
    print(f"5. Created SNS topic and subscribed notification queue")

    # 6. Create EventBridge rule: order.completed → SNS
    events.put_rule(
        Name="order-completed-rule",
        EventPattern=json.dumps(
            {"source": ["orders"], "detail-type": ["OrderCompleted"]}
        ),
    )
    events.put_targets(
        Rule="order-completed-rule",
        Targets=[{"Id": "sns-notify", "Arn": topic_arn}],
    )
    print("6. Created EventBridge rule: OrderCompleted → SNS → SQS")

    # 7. Simulate: place an order
    order = {"orderId": "ORD-001", "item": "Widget", "quantity": 3, "total": 29.99}
    sqs.send_message(QueueUrl=order_queue_url, MessageBody=json.dumps(order))
    print(f"\n7. Order placed: {order['orderId']} ({order['item']} x{order['quantity']})")

    # 8. Process the order (receive from SQS)
    messages = sqs.receive_message(
        QueueUrl=order_queue_url, MaxNumberOfMessages=1, WaitTimeSeconds=2
    )
    if messages.get("Messages"):
        msg = messages["Messages"][0]
        order_data = json.loads(msg["Body"])
        sqs.delete_message(
            QueueUrl=order_queue_url, ReceiptHandle=msg["ReceiptHandle"]
        )
        print(f"8. Order processed: {order_data['orderId']}")

        # 9. Emit OrderCompleted event to EventBridge
        events.put_events(
            Entries=[
                {
                    "Source": "orders",
                    "DetailType": "OrderCompleted",
                    "Detail": json.dumps(
                        {
                            "orderId": order_data["orderId"],
                            "status": "completed",
                            "total": order_data["total"],
                        }
                    ),
                }
            ]
        )
        print("9. EventBridge event emitted: OrderCompleted")

    # 10. Check notification queue (EventBridge → SNS → SQS chain)
    time.sleep(1)  # Give delivery a moment
    notifications = sqs.receive_message(
        QueueUrl=notification_queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=2
    )
    if notifications.get("Messages"):
        for msg in notifications["Messages"]:
            body = json.loads(msg["Body"])
            if body.get("Type") == "Notification":
                event = json.loads(body["Message"])
                print(
                    f"10. Notification received! Event: {event.get('detail-type', 'unknown')}"
                )
                if "detail" in event:
                    detail = event["detail"]
                    print(
                        f"    Order {detail.get('orderId')} - status: {detail.get('status')}, total: ${detail.get('total')}"
                    )
            else:
                print(f"10. Notification received: {msg['Body'][:100]}")
    else:
        print("10. No notifications yet (delivery may be async)")

    print("\n=== Pipeline complete! ===")
    print(
        "Flow: Order → SQS → Process → EventBridge → SNS → SQS notification queue"
    )


if __name__ == "__main__":
    main()
