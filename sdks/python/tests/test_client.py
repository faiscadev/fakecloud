"""E2E tests for the fakecloud Python SDK.

These tests start a real fakecloud server as a subprocess and use boto3 to
create AWS resources, then verify the fakecloud introspection SDK returns the
correct data.
"""

from __future__ import annotations

import json
import os
import socket
import subprocess
import time

import boto3
import pytest

from fakecloud import FakeCloudSync
from fakecloud.types import (
    ConfirmSubscriptionRequest,
    ConfirmUserRequest,
    ExpireTokensRequest,
    FireRuleRequest,
    InboundEmailRequest,
)

# ── Fixtures ──────────────────────────────────────────────────────────

_DEFAULT_BIN = os.path.join(
    os.path.dirname(__file__), "..", "..", "..", "target", "release", "fakecloud"
)
FAKECLOUD_BIN = os.environ.get("FAKECLOUD_BIN", _DEFAULT_BIN)


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_for_ready(url: str, timeout: float = 15.0) -> None:
    """Poll the health endpoint until fakecloud is ready."""
    import httpx

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            r = httpx.get(f"{url}/_fakecloud/health", timeout=2.0)
            if r.status_code == 200:
                return
        except httpx.ConnectError:
            pass
        time.sleep(0.1)
    raise RuntimeError(f"fakecloud did not become ready at {url} within {timeout}s")


@pytest.fixture(scope="session")
def fakecloud_url() -> str:  # type: ignore[misc]
    """Start fakecloud and yield its base URL. Kills it after the session."""
    port = _free_port()
    binary = os.path.abspath(FAKECLOUD_BIN)
    if not os.path.isfile(binary):
        pytest.skip(
            f"fakecloud binary not found at {binary} — run cargo build --release first"
        )

    proc = subprocess.Popen(
        [binary, "--addr", f"127.0.0.1:{port}"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    url = f"http://127.0.0.1:{port}"
    try:
        _wait_for_ready(url)
        yield url
    finally:
        proc.terminate()
        proc.wait(timeout=5)


@pytest.fixture()
def fc(fakecloud_url: str) -> FakeCloudSync:  # type: ignore[misc]
    """Return a sync SDK client and reset state before each test."""
    client = FakeCloudSync(fakecloud_url)
    client.reset()
    yield client  # type: ignore[misc]
    client.close()


def _boto_kwargs(fakecloud_url: str) -> dict:  # type: ignore[type-arg]
    return dict(
        endpoint_url=fakecloud_url,
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )


# ── Health ────────────────────────────────────────────────────────────


def test_health(fc: FakeCloudSync, fakecloud_url: str) -> None:
    h = fc.health()
    assert h.status == "ok"
    assert isinstance(h.services, list)
    assert len(h.services) > 0


# ── Reset ─────────────────────────────────────────────────────────────


def test_reset_clears_state(fc: FakeCloudSync, fakecloud_url: str) -> None:
    # Create a queue so there's state
    sqs = boto3.client("sqs", **_boto_kwargs(fakecloud_url))
    sqs.create_queue(QueueName="reset-test-queue")

    # Verify the queue exists via SQS list
    queues = sqs.list_queues().get("QueueUrls", [])
    assert any("reset-test-queue" in q for q in queues)

    # Reset
    r = fc.reset()
    assert r.status == "ok"

    # After reset, queue should be gone
    queues = sqs.list_queues().get("QueueUrls", [])
    assert not any("reset-test-queue" in q for q in queues)


# ── SQS ──────────────────────────────────────────────────────────────


def test_sqs_messages(fc: FakeCloudSync, fakecloud_url: str) -> None:
    sqs = boto3.client("sqs", **_boto_kwargs(fakecloud_url))
    queue_url = sqs.create_queue(QueueName="sdk-test-queue")["QueueUrl"]
    sqs.send_message(QueueUrl=queue_url, MessageBody="hello from sdk test")

    result = fc.sqs.get_messages()
    assert len(result.queues) >= 1
    queue = next(q for q in result.queues if q.queue_name == "sdk-test-queue")
    assert len(queue.messages) == 1
    assert queue.messages[0].body == "hello from sdk test"


# ── SNS ──────────────────────────────────────────────────────────────


def test_sns_messages(fc: FakeCloudSync, fakecloud_url: str) -> None:
    sns = boto3.client("sns", **_boto_kwargs(fakecloud_url))
    topic = sns.create_topic(Name="sdk-test-topic")
    topic_arn = topic["TopicArn"]
    sns.publish(TopicArn=topic_arn, Message="hello from sns test")

    result = fc.sns.get_messages()
    assert len(result.messages) >= 1
    msg = next(m for m in result.messages if "sdk-test-topic" in m.topic_arn)
    assert msg.message == "hello from sns test"


# ── SES ──────────────────────────────────────────────────────────────


def test_ses_emails(fc: FakeCloudSync, fakecloud_url: str) -> None:
    sesv2 = boto3.client("sesv2", **_boto_kwargs(fakecloud_url))
    sesv2.send_email(
        FromEmailAddress="sender@example.com",
        Destination={"ToAddresses": ["recipient@example.com"]},
        Content={
            "Simple": {
                "Subject": {"Data": "SDK Test"},
                "Body": {"Text": {"Data": "hello from ses test"}},
            }
        },
    )

    result = fc.ses.get_emails()
    assert len(result.emails) >= 1
    email = result.emails[0]
    assert email.from_addr == "sender@example.com"
    assert "recipient@example.com" in email.to
    assert email.subject == "SDK Test"


# ── S3 ───────────────────────────────────────────────────────────────


def test_s3_notifications(fc: FakeCloudSync, fakecloud_url: str) -> None:
    s3 = boto3.client("s3", **_boto_kwargs(fakecloud_url))
    s3.create_bucket(Bucket="sdk-test-bucket")
    s3.put_object(Bucket="sdk-test-bucket", Key="test.txt", Body=b"hello")

    result = fc.s3.get_notifications()
    # S3 notifications are only emitted when notification configuration is set,
    # so we just verify the endpoint works and returns a valid response.
    assert isinstance(result.notifications, list)


# ── DynamoDB ─────────────────────────────────────────────────────────


def test_dynamodb_ttl_tick(fc: FakeCloudSync, fakecloud_url: str) -> None:
    ddb = boto3.client("dynamodb", **_boto_kwargs(fakecloud_url))
    ddb.create_table(
        TableName="sdk-test-table",
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )

    result = fc.dynamodb.tick_ttl()
    assert result.expired_items >= 0


# ── Cognito ──────────────────────────────────────────────────────────


def test_cognito_confirm_user(fc: FakeCloudSync, fakecloud_url: str) -> None:
    cognito = boto3.client("cognito-idp", **_boto_kwargs(fakecloud_url))
    pool = cognito.create_user_pool(PoolName="sdk-test-pool")
    pool_id = pool["UserPool"]["Id"]
    client_resp = cognito.create_user_pool_client(
        UserPoolId=pool_id, ClientName="sdk-test-client"
    )
    client_id = client_resp["UserPoolClient"]["ClientId"]

    cognito.sign_up(
        ClientId=client_id,
        Username="testuser",
        Password="Test1234!@#$",
    )

    # User should be UNCONFIRMED
    user = cognito.admin_get_user(UserPoolId=pool_id, Username="testuser")
    assert user["UserStatus"] == "UNCONFIRMED"

    # Confirm via introspection SDK
    result = fc.cognito.confirm_user(
        ConfirmUserRequest(user_pool_id=pool_id, username="testuser")
    )
    assert result.confirmed is True

    # User should now be CONFIRMED
    user = cognito.admin_get_user(UserPoolId=pool_id, Username="testuser")
    assert user["UserStatus"] == "CONFIRMED"


# ── EventBridge ──────────────────────────────────────────────────────


def test_events_history(fc: FakeCloudSync, fakecloud_url: str) -> None:
    eb = boto3.client("events", **_boto_kwargs(fakecloud_url))
    eb.put_events(
        Entries=[
            {
                "Source": "sdk.test",
                "DetailType": "TestEvent",
                "Detail": json.dumps({"key": "value"}),
                "EventBusName": "default",
            }
        ]
    )

    result = fc.events.get_history()
    assert len(result.events) >= 1
    event = next(e for e in result.events if e.source == "sdk.test")
    assert event.detail_type == "TestEvent"
    assert event.bus_name == "default"


# ── Unit tests for serialization logic ────────────────────────────────


def test_inbound_email_request_to_dict() -> None:
    req = InboundEmailRequest(
        from_addr="a@b.com", to=["c@d.com"], subject="Hi", body="Hello"
    )
    d = req.to_dict()
    assert d == {"from": "a@b.com", "to": ["c@d.com"], "subject": "Hi", "body": "Hello"}


def test_fire_rule_request_to_dict() -> None:
    req = FireRuleRequest(rule_name="my-rule", bus_name="default")
    d = req.to_dict()
    assert d == {"ruleName": "my-rule", "busName": "default"}


def test_fire_rule_request_to_dict_no_bus() -> None:
    req = FireRuleRequest(rule_name="my-rule")
    d = req.to_dict()
    assert d == {"ruleName": "my-rule"}


def test_confirm_subscription_request_to_dict() -> None:
    req = ConfirmSubscriptionRequest(subscription_arn="arn:...")
    d = req.to_dict()
    assert d == {"subscriptionArn": "arn:..."}


def test_confirm_user_request_to_dict() -> None:
    req = ConfirmUserRequest(user_pool_id="pool-1", username="alice")
    d = req.to_dict()
    assert d == {"userPoolId": "pool-1", "username": "alice"}


def test_expire_tokens_request_to_dict() -> None:
    req = ExpireTokensRequest(user_pool_id="pool-1")
    d = req.to_dict()
    assert d == {"userPoolId": "pool-1"}


def test_expire_tokens_request_to_dict_empty() -> None:
    req = ExpireTokensRequest()
    d = req.to_dict()
    assert d == {}


def test_trailing_slash_stripped() -> None:
    from fakecloud import FakeCloud

    fc = FakeCloud("http://localhost:4566/")
    assert fc._base == "http://localhost:4566"


def test_trailing_slash_stripped_sync() -> None:
    fc = FakeCloudSync("http://localhost:4566/")
    assert fc._base == "http://localhost:4566"
