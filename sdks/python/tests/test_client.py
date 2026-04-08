"""Tests for the fakecloud Python SDK client."""

from __future__ import annotations

import pytest
import httpx
import respx

from fakecloud import FakeCloud, FakeCloudSync
from fakecloud.client import FakeCloudError
from fakecloud.types import (
    ConfirmSubscriptionRequest,
    ConfirmUserRequest,
    ExpireTokensRequest,
    FireRuleRequest,
    InboundEmailRequest,
)

BASE = "http://localhost:4566"


# ── Health & Reset ──────────────────────────────────────────────────


@respx.mock
@pytest.mark.asyncio
async def test_health_async():
    respx.get(f"{BASE}/_fakecloud/health").mock(
        return_value=httpx.Response(
            200,
            json={"status": "ok", "version": "0.1.0", "services": ["sqs", "sns"]},
        )
    )
    async with FakeCloud(BASE) as fc:
        h = await fc.health()
    assert h.status == "ok"
    assert h.version == "0.1.0"
    assert h.services == ["sqs", "sns"]


@respx.mock
@pytest.mark.asyncio
async def test_reset_async():
    respx.post(f"{BASE}/_reset").mock(
        return_value=httpx.Response(200, json={"status": "ok"})
    )
    async with FakeCloud(BASE) as fc:
        r = await fc.reset()
    assert r.status == "ok"


@respx.mock
@pytest.mark.asyncio
async def test_reset_service_async():
    respx.post(f"{BASE}/_fakecloud/reset/sqs").mock(
        return_value=httpx.Response(200, json={"reset": "sqs"})
    )
    async with FakeCloud(BASE) as fc:
        r = await fc.reset_service("sqs")
    assert r.reset == "sqs"


@respx.mock
@pytest.mark.asyncio
async def test_error_raises():
    respx.get(f"{BASE}/_fakecloud/health").mock(
        return_value=httpx.Response(500, text="internal error")
    )
    async with FakeCloud(BASE) as fc:
        with pytest.raises(FakeCloudError) as exc_info:
            await fc.health()
    assert exc_info.value.status == 500
    assert "internal error" in exc_info.value.body


# ── Lambda ──────────────────────────────────────────────────────────


@respx.mock
@pytest.mark.asyncio
async def test_lambda_invocations():
    respx.get(f"{BASE}/_fakecloud/lambda/invocations").mock(
        return_value=httpx.Response(
            200,
            json={
                "invocations": [
                    {
                        "functionArn": "arn:aws:lambda:us-east-1:000000000000:function:my-fn",
                        "payload": '{"key": "val"}',
                        "source": "api",
                        "timestamp": "2026-01-01T00:00:00Z",
                    }
                ]
            },
        )
    )
    async with FakeCloud(BASE) as fc:
        r = await fc.lambda_.get_invocations()
    assert len(r.invocations) == 1
    assert r.invocations[0].function_arn.endswith("my-fn")
    assert r.invocations[0].source == "api"


@respx.mock
@pytest.mark.asyncio
async def test_lambda_warm_containers():
    respx.get(f"{BASE}/_fakecloud/lambda/warm-containers").mock(
        return_value=httpx.Response(
            200,
            json={
                "containers": [
                    {
                        "functionName": "my-fn",
                        "runtime": "python3.12",
                        "containerId": "abc123",
                        "lastUsedSecsAgo": 30,
                    }
                ]
            },
        )
    )
    async with FakeCloud(BASE) as fc:
        r = await fc.lambda_.get_warm_containers()
    assert len(r.containers) == 1
    assert r.containers[0].function_name == "my-fn"
    assert r.containers[0].last_used_secs_ago == 30


@respx.mock
@pytest.mark.asyncio
async def test_lambda_evict_container():
    respx.post(f"{BASE}/_fakecloud/lambda/my-fn/evict-container").mock(
        return_value=httpx.Response(200, json={"evicted": True})
    )
    async with FakeCloud(BASE) as fc:
        r = await fc.lambda_.evict_container("my-fn")
    assert r.evicted is True


# ── SES ─────────────────────────────────────────────────────────────


@respx.mock
@pytest.mark.asyncio
async def test_ses_get_emails():
    respx.get(f"{BASE}/_fakecloud/ses/emails").mock(
        return_value=httpx.Response(
            200,
            json={
                "emails": [
                    {
                        "messageId": "msg-1",
                        "from": "a@b.com",
                        "to": ["c@d.com"],
                        "cc": [],
                        "bcc": [],
                        "subject": "Hello",
                        "htmlBody": "<p>Hi</p>",
                        "textBody": "Hi",
                        "rawData": None,
                        "templateName": None,
                        "templateData": None,
                        "timestamp": "2026-01-01T00:00:00Z",
                    }
                ]
            },
        )
    )
    async with FakeCloud(BASE) as fc:
        r = await fc.ses.get_emails()
    assert len(r.emails) == 1
    assert r.emails[0].from_addr == "a@b.com"
    assert r.emails[0].subject == "Hello"


@respx.mock
@pytest.mark.asyncio
async def test_ses_simulate_inbound():
    respx.post(f"{BASE}/_fakecloud/ses/inbound").mock(
        return_value=httpx.Response(
            200,
            json={
                "messageId": "msg-2",
                "matchedRules": ["rule1"],
                "actionsExecuted": [
                    {"rule": "rule1", "actionType": "Lambda"}
                ],
            },
        )
    )
    req = InboundEmailRequest(
        from_addr="sender@x.com",
        to=["rcpt@y.com"],
        subject="Test",
        body="Body text",
    )
    async with FakeCloud(BASE) as fc:
        r = await fc.ses.simulate_inbound(req)
    assert r.message_id == "msg-2"
    assert r.matched_rules == ["rule1"]
    assert r.actions_executed[0].action_type == "Lambda"


# ── SNS ─────────────────────────────────────────────────────────────


@respx.mock
@pytest.mark.asyncio
async def test_sns_get_messages():
    respx.get(f"{BASE}/_fakecloud/sns/messages").mock(
        return_value=httpx.Response(
            200,
            json={
                "messages": [
                    {
                        "messageId": "m1",
                        "topicArn": "arn:aws:sns:us-east-1:000000000000:my-topic",
                        "message": "hello",
                        "subject": None,
                        "timestamp": "2026-01-01T00:00:00Z",
                    }
                ]
            },
        )
    )
    async with FakeCloud(BASE) as fc:
        r = await fc.sns.get_messages()
    assert len(r.messages) == 1
    assert r.messages[0].topic_arn.endswith("my-topic")


@respx.mock
@pytest.mark.asyncio
async def test_sns_pending_confirmations():
    respx.get(f"{BASE}/_fakecloud/sns/pending-confirmations").mock(
        return_value=httpx.Response(
            200,
            json={
                "pendingConfirmations": [
                    {
                        "subscriptionArn": "arn:...",
                        "topicArn": "arn:...",
                        "protocol": "https",
                        "endpoint": "https://example.com",
                        "token": "tok-1",
                    }
                ]
            },
        )
    )
    async with FakeCloud(BASE) as fc:
        r = await fc.sns.get_pending_confirmations()
    assert len(r.pending_confirmations) == 1
    assert r.pending_confirmations[0].token == "tok-1"


@respx.mock
@pytest.mark.asyncio
async def test_sns_confirm_subscription():
    respx.post(f"{BASE}/_fakecloud/sns/confirm-subscription").mock(
        return_value=httpx.Response(200, json={"confirmed": True})
    )
    req = ConfirmSubscriptionRequest(subscription_arn="arn:...")
    async with FakeCloud(BASE) as fc:
        r = await fc.sns.confirm_subscription(req)
    assert r.confirmed is True


# ── SQS ─────────────────────────────────────────────────────────────


@respx.mock
@pytest.mark.asyncio
async def test_sqs_get_messages():
    respx.get(f"{BASE}/_fakecloud/sqs/messages").mock(
        return_value=httpx.Response(
            200,
            json={
                "queues": [
                    {
                        "queueUrl": "http://localhost:4566/000000000000/my-queue",
                        "queueName": "my-queue",
                        "messages": [
                            {
                                "messageId": "m1",
                                "body": "hello",
                                "receiveCount": 0,
                                "inFlight": False,
                                "createdAt": "2026-01-01T00:00:00Z",
                            }
                        ],
                    }
                ]
            },
        )
    )
    async with FakeCloud(BASE) as fc:
        r = await fc.sqs.get_messages()
    assert len(r.queues) == 1
    assert r.queues[0].queue_name == "my-queue"
    assert r.queues[0].messages[0].in_flight is False


@respx.mock
@pytest.mark.asyncio
async def test_sqs_tick_expiration():
    respx.post(f"{BASE}/_fakecloud/sqs/expiration-processor/tick").mock(
        return_value=httpx.Response(200, json={"expiredMessages": 3})
    )
    async with FakeCloud(BASE) as fc:
        r = await fc.sqs.tick_expiration()
    assert r.expired_messages == 3


@respx.mock
@pytest.mark.asyncio
async def test_sqs_force_dlq():
    respx.post(f"{BASE}/_fakecloud/sqs/my-queue/force-dlq").mock(
        return_value=httpx.Response(200, json={"movedMessages": 5})
    )
    async with FakeCloud(BASE) as fc:
        r = await fc.sqs.force_dlq("my-queue")
    assert r.moved_messages == 5


# ── EventBridge ─────────────────────────────────────────────────────


@respx.mock
@pytest.mark.asyncio
async def test_events_history():
    respx.get(f"{BASE}/_fakecloud/events/history").mock(
        return_value=httpx.Response(
            200,
            json={
                "events": [
                    {
                        "eventId": "e1",
                        "source": "my.app",
                        "detailType": "OrderPlaced",
                        "detail": "{}",
                        "busName": "default",
                        "timestamp": "2026-01-01T00:00:00Z",
                    }
                ],
                "deliveries": {
                    "lambda": [
                        {
                            "functionArn": "arn:...",
                            "payload": "{}",
                            "timestamp": "2026-01-01T00:00:00Z",
                        }
                    ],
                    "logs": [],
                },
            },
        )
    )
    async with FakeCloud(BASE) as fc:
        r = await fc.events.get_history()
    assert len(r.events) == 1
    assert r.events[0].detail_type == "OrderPlaced"
    assert len(r.deliveries.lambda_deliveries) == 1


@respx.mock
@pytest.mark.asyncio
async def test_events_fire_rule():
    respx.post(f"{BASE}/_fakecloud/events/fire-rule").mock(
        return_value=httpx.Response(
            200,
            json={"targets": [{"type": "lambda", "arn": "arn:..."}]},
        )
    )
    req = FireRuleRequest(rule_name="my-rule", bus_name="default")
    async with FakeCloud(BASE) as fc:
        r = await fc.events.fire_rule(req)
    assert len(r.targets) == 1
    assert r.targets[0].target_type == "lambda"


# ── S3 ──────────────────────────────────────────────────────────────


@respx.mock
@pytest.mark.asyncio
async def test_s3_notifications():
    respx.get(f"{BASE}/_fakecloud/s3/notifications").mock(
        return_value=httpx.Response(
            200,
            json={
                "notifications": [
                    {
                        "bucket": "my-bucket",
                        "key": "obj.txt",
                        "eventType": "s3:ObjectCreated:Put",
                        "timestamp": "2026-01-01T00:00:00Z",
                    }
                ]
            },
        )
    )
    async with FakeCloud(BASE) as fc:
        r = await fc.s3.get_notifications()
    assert len(r.notifications) == 1
    assert r.notifications[0].bucket == "my-bucket"


@respx.mock
@pytest.mark.asyncio
async def test_s3_tick_lifecycle():
    respx.post(f"{BASE}/_fakecloud/s3/lifecycle-processor/tick").mock(
        return_value=httpx.Response(
            200,
            json={
                "processedBuckets": 2,
                "expiredObjects": 1,
                "transitionedObjects": 0,
            },
        )
    )
    async with FakeCloud(BASE) as fc:
        r = await fc.s3.tick_lifecycle()
    assert r.processed_buckets == 2
    assert r.expired_objects == 1


# ── DynamoDB ────────────────────────────────────────────────────────


@respx.mock
@pytest.mark.asyncio
async def test_dynamodb_tick_ttl():
    respx.post(f"{BASE}/_fakecloud/dynamodb/ttl-processor/tick").mock(
        return_value=httpx.Response(200, json={"expiredItems": 7})
    )
    async with FakeCloud(BASE) as fc:
        r = await fc.dynamodb.tick_ttl()
    assert r.expired_items == 7


# ── SecretsManager ──────────────────────────────────────────────────


@respx.mock
@pytest.mark.asyncio
async def test_secretsmanager_tick_rotation():
    respx.post(f"{BASE}/_fakecloud/secretsmanager/rotation-scheduler/tick").mock(
        return_value=httpx.Response(
            200, json={"rotatedSecrets": ["secret-1"]}
        )
    )
    async with FakeCloud(BASE) as fc:
        r = await fc.secretsmanager.tick_rotation()
    assert r.rotated_secrets == ["secret-1"]


# ── Cognito ─────────────────────────────────────────────────────────


@respx.mock
@pytest.mark.asyncio
async def test_cognito_get_user_codes():
    respx.get(
        f"{BASE}/_fakecloud/cognito/confirmation-codes/pool-1/alice"
    ).mock(
        return_value=httpx.Response(
            200,
            json={
                "confirmationCode": "123456",
                "attributeVerificationCodes": {},
            },
        )
    )
    async with FakeCloud(BASE) as fc:
        r = await fc.cognito.get_user_codes("pool-1", "alice")
    assert r.confirmation_code == "123456"


@respx.mock
@pytest.mark.asyncio
async def test_cognito_confirm_user():
    respx.post(f"{BASE}/_fakecloud/cognito/confirm-user").mock(
        return_value=httpx.Response(200, json={"confirmed": True})
    )
    req = ConfirmUserRequest(user_pool_id="pool-1", username="alice")
    async with FakeCloud(BASE) as fc:
        r = await fc.cognito.confirm_user(req)
    assert r.confirmed is True


@respx.mock
@pytest.mark.asyncio
async def test_cognito_confirm_user_not_found():
    respx.post(f"{BASE}/_fakecloud/cognito/confirm-user").mock(
        return_value=httpx.Response(
            404, json={"confirmed": False, "error": "user not found"}
        )
    )
    req = ConfirmUserRequest(user_pool_id="pool-1", username="nobody")
    async with FakeCloud(BASE) as fc:
        with pytest.raises(FakeCloudError) as exc_info:
            await fc.cognito.confirm_user(req)
    assert exc_info.value.status == 404


@respx.mock
@pytest.mark.asyncio
async def test_cognito_tokens():
    respx.get(f"{BASE}/_fakecloud/cognito/tokens").mock(
        return_value=httpx.Response(
            200,
            json={
                "tokens": [
                    {
                        "type": "access",
                        "username": "alice",
                        "poolId": "pool-1",
                        "clientId": "client-1",
                        "issuedAt": 1700000000.0,
                    }
                ]
            },
        )
    )
    async with FakeCloud(BASE) as fc:
        r = await fc.cognito.get_tokens()
    assert len(r.tokens) == 1
    assert r.tokens[0].token_type == "access"


@respx.mock
@pytest.mark.asyncio
async def test_cognito_expire_tokens():
    respx.post(f"{BASE}/_fakecloud/cognito/expire-tokens").mock(
        return_value=httpx.Response(200, json={"expiredTokens": 2})
    )
    req = ExpireTokensRequest(user_pool_id="pool-1")
    async with FakeCloud(BASE) as fc:
        r = await fc.cognito.expire_tokens(req)
    assert r.expired_tokens == 2


@respx.mock
@pytest.mark.asyncio
async def test_cognito_auth_events():
    respx.get(f"{BASE}/_fakecloud/cognito/auth-events").mock(
        return_value=httpx.Response(
            200,
            json={
                "events": [
                    {
                        "eventType": "SignIn",
                        "username": "alice",
                        "userPoolId": "pool-1",
                        "clientId": "client-1",
                        "timestamp": 1700000000.0,
                        "success": True,
                    }
                ]
            },
        )
    )
    async with FakeCloud(BASE) as fc:
        r = await fc.cognito.get_auth_events()
    assert len(r.events) == 1
    assert r.events[0].success is True


# ── Sync client tests ──────────────────────────────────────────────


@respx.mock
def test_health_sync():
    respx.get(f"{BASE}/_fakecloud/health").mock(
        return_value=httpx.Response(
            200,
            json={"status": "ok", "version": "0.1.0", "services": ["sqs"]},
        )
    )
    with FakeCloudSync(BASE) as fc:
        h = fc.health()
    assert h.status == "ok"


@respx.mock
def test_reset_sync():
    respx.post(f"{BASE}/_reset").mock(
        return_value=httpx.Response(200, json={"status": "ok"})
    )
    with FakeCloudSync(BASE) as fc:
        r = fc.reset()
    assert r.status == "ok"


@respx.mock
def test_sync_lambda_invocations():
    respx.get(f"{BASE}/_fakecloud/lambda/invocations").mock(
        return_value=httpx.Response(200, json={"invocations": []})
    )
    with FakeCloudSync(BASE) as fc:
        r = fc.lambda_.get_invocations()
    assert r.invocations == []


@respx.mock
def test_sync_ses_emails():
    respx.get(f"{BASE}/_fakecloud/ses/emails").mock(
        return_value=httpx.Response(200, json={"emails": []})
    )
    with FakeCloudSync(BASE) as fc:
        r = fc.ses.get_emails()
    assert r.emails == []


@respx.mock
def test_sync_sqs_messages():
    respx.get(f"{BASE}/_fakecloud/sqs/messages").mock(
        return_value=httpx.Response(200, json={"queues": []})
    )
    with FakeCloudSync(BASE) as fc:
        r = fc.sqs.get_messages()
    assert r.queues == []


@respx.mock
def test_sync_error_raises():
    respx.get(f"{BASE}/_fakecloud/health").mock(
        return_value=httpx.Response(503, text="unavailable")
    )
    with FakeCloudSync(BASE) as fc:
        with pytest.raises(FakeCloudError) as exc_info:
            fc.health()
    assert exc_info.value.status == 503


# ── Request serialization ──────────────────────────────────────────


def test_inbound_email_request_to_dict():
    req = InboundEmailRequest(
        from_addr="a@b.com", to=["c@d.com"], subject="Hi", body="Hello"
    )
    d = req.to_dict()
    assert d == {"from": "a@b.com", "to": ["c@d.com"], "subject": "Hi", "body": "Hello"}


def test_fire_rule_request_to_dict():
    req = FireRuleRequest(rule_name="my-rule", bus_name="default")
    d = req.to_dict()
    assert d == {"ruleName": "my-rule", "busName": "default"}


def test_fire_rule_request_to_dict_no_bus():
    req = FireRuleRequest(rule_name="my-rule")
    d = req.to_dict()
    assert d == {"ruleName": "my-rule"}


def test_confirm_subscription_request_to_dict():
    req = ConfirmSubscriptionRequest(subscription_arn="arn:...")
    d = req.to_dict()
    assert d == {"subscriptionArn": "arn:..."}


def test_confirm_user_request_to_dict():
    req = ConfirmUserRequest(user_pool_id="pool-1", username="alice")
    d = req.to_dict()
    assert d == {"userPoolId": "pool-1", "username": "alice"}


def test_expire_tokens_request_to_dict():
    req = ExpireTokensRequest(user_pool_id="pool-1")
    d = req.to_dict()
    assert d == {"userPoolId": "pool-1"}


def test_expire_tokens_request_to_dict_empty():
    req = ExpireTokensRequest()
    d = req.to_dict()
    assert d == {}


# ── URL construction ───────────────────────────────────────────────


def test_trailing_slash_stripped():
    fc = FakeCloud("http://localhost:4566/")
    assert fc._base == "http://localhost:4566"


def test_trailing_slash_stripped_sync():
    fc = FakeCloudSync("http://localhost:4566/")
    assert fc._base == "http://localhost:4566"
