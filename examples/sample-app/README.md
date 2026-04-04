# Sample App: Order Processing Pipeline

A realistic event-driven application running entirely against FakeCloud.

## Architecture

```
User places order (SSM config → SQS)
    → Order queue (SQS)
    → Processed → EventBridge event
    → EventBridge rule matches "order.completed"
    → SNS topic notification
    → Notification queue (SQS via SNS fan-out)
```

## Prerequisites

- FakeCloud running on port 4566
- Python 3.8+ with boto3 (`pip install boto3`)

## Run

```bash
# Start FakeCloud
docker run -p 4566:4566 ghcr.io/faiscadev/fakecloud

# Run the sample app
python app.py
```
