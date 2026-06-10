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
from botocore.config import Config as BotocoreConfig

from fakecloud import FakeCloudSync
from fakecloud.types import (
    BedrockFaultRule,
    BedrockResponseRule,
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


def _wait_for_ready(url: str, timeout: float = 30.0) -> None:
    """Poll the health endpoint until fakecloud is ready."""
    import httpx

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            r = httpx.get(f"{url}/_fakecloud/health", timeout=2.0)
            if r.status_code == 200:
                return
        except httpx.HTTPError:
            # ConnectError, ReadTimeout, ConnectTimeout — server still
            # warming up. Retry until deadline.
            pass
        time.sleep(0.1)
    raise RuntimeError(f"fakecloud did not become ready at {url} within {timeout}s")


@pytest.fixture(scope="session")
def fakecloud_url() -> str:  # type: ignore[misc]
    """Start fakecloud and yield its base URL. Kills it after the session."""
    port = _free_port()
    binary = os.path.abspath(FAKECLOUD_BIN)
    if not os.path.isfile(binary):
        raise RuntimeError(
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


def test_rds_instances(fc: FakeCloudSync, fakecloud_url: str) -> None:
    import time

    rds = boto3.client("rds", **_boto_kwargs(fakecloud_url))
    rds.create_db_instance(
        DBInstanceIdentifier="py-sdk-rds-db",
        AllocatedStorage=20,
        DBInstanceClass="db.t3.micro",
        Engine="postgres",
        EngineVersion="16.3",
        MasterUsername="admin",
        MasterUserPassword="secret123",
        DBName="appdb",
    )

    # CreateDBInstance returns a `creating` placeholder; poll until the
    # container is up so the introspection endpoint reports the populated
    # container_id and host_port.
    deadline = time.time() + 240
    while time.time() < deadline:
        desc = rds.describe_db_instances(DBInstanceIdentifier="py-sdk-rds-db")
        if (
            desc["DBInstances"]
            and desc["DBInstances"][0]["DBInstanceStatus"] == "available"
        ):
            break
        time.sleep(1)

    result = fc.rds.get_instances()
    instance = next(
        item
        for item in result.instances
        if item.db_instance_identifier == "py-sdk-rds-db"
    )
    assert instance.engine == "postgres"
    assert instance.db_name == "appdb"
    assert instance.container_id
    assert instance.host_port > 0


# ── EC2 ───────────────────────────────────────────────────────────────


def test_ec2_instances(fc: FakeCloudSync, fakecloud_url: str) -> None:
    ec2 = boto3.client("ec2", **_boto_kwargs(fakecloud_url))
    run = ec2.run_instances(
        ImageId="ami-12345678",
        InstanceType="t3.micro",
        MinCount=1,
        MaxCount=1,
    )
    instance_id = run["Instances"][0]["InstanceId"]

    result = fc.ec2.get_instances()
    instance = next(
        item for item in result.instances if item.instance_id == instance_id
    )
    assert instance.image_id == "ami-12345678"
    assert instance.instance_type == "t3.micro"
    assert instance.private_ip
    assert isinstance(instance.security_group_ids, list)


# ── ElastiCache ───────────────────────────────────────────────────────


def test_elasticache_get_clusters(fc: FakeCloudSync, fakecloud_url: str) -> None:
    ec = boto3.client("elasticache", **_boto_kwargs(fakecloud_url))
    ec.create_cache_cluster(
        CacheClusterId="py-sdk-ec-cluster",
        CacheNodeType="cache.t3.micro",
        Engine="redis",
        EngineVersion="7.1",
        NumCacheNodes=1,
    )

    result = fc.elasticache.get_clusters()
    cluster = next(
        c for c in result.clusters if c.cache_cluster_id == "py-sdk-ec-cluster"
    )
    assert cluster.engine == "redis"
    assert cluster.num_cache_nodes == 1
    assert cluster.container_id is not None


def test_elasticache_get_replication_groups(
    fc: FakeCloudSync, fakecloud_url: str
) -> None:
    ec = boto3.client("elasticache", **_boto_kwargs(fakecloud_url))
    ec.create_replication_group(
        ReplicationGroupId="py-sdk-ec-rg",
        ReplicationGroupDescription="Python SDK test replication group",
        CacheNodeType="cache.t3.micro",
        Engine="redis",
        EngineVersion="7.1",
        NumCacheClusters=2,
    )

    result = fc.elasticache.get_replication_groups()
    group = next(
        g for g in result.replication_groups if g.replication_group_id == "py-sdk-ec-rg"
    )
    assert group.engine == "redis"
    assert group.num_cache_clusters == 2


def test_elasticache_get_serverless_caches(
    fc: FakeCloudSync, fakecloud_url: str
) -> None:
    ec = boto3.client("elasticache", **_boto_kwargs(fakecloud_url))
    ec.create_serverless_cache(
        ServerlessCacheName="py-sdk-ec-serverless",
        Engine="redis",
        MajorEngineVersion="7.1",
    )

    result = fc.elasticache.get_serverless_caches()
    cache = next(
        c
        for c in result.serverless_caches
        if c.serverless_cache_name == "py-sdk-ec-serverless"
    )
    assert cache.engine == "redis"
    # Backing container starts asynchronously; status is "creating" right after
    # create and transitions to "available" (bug-audit 3.2).
    assert cache.status in ("creating", "available")


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
    # Disable SSE-SQS so the introspection endpoint surfaces the
    # plaintext body. Default queues encrypt at rest under
    # `alias/aws/sqs` (AWS default since May 2023) and the body would
    # otherwise be the at-rest envelope.
    queue_url = sqs.create_queue(
        QueueName="sdk-test-queue",
        Attributes={"SqsManagedSseEnabled": "false"},
    )["QueueUrl"]
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
    sesv2.create_email_identity(EmailIdentity="sender@example.com")
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


# ── Bedrock introspection ────────────────────────────────────────────


def test_bedrock_response_rules_roundtrip(
    fc: FakeCloudSync, fakecloud_url: str
) -> None:
    model_id = "anthropic.claude-3-haiku-20240307-v1:0"
    cfg = fc.bedrock.set_response_rules(
        model_id,
        [
            BedrockResponseRule(prompt_contains="spam:", response='{"label":"spam"}'),
            BedrockResponseRule(prompt_contains=None, response='{"label":"ham"}'),
        ],
    )
    assert cfg.status == "ok"
    assert cfg.model_id == model_id

    cleared = fc.bedrock.clear_response_rules(model_id)
    assert cleared.status == "ok"


def test_bedrock_faults_roundtrip(fc: FakeCloudSync, fakecloud_url: str) -> None:
    queued = fc.bedrock.queue_fault(
        BedrockFaultRule(
            error_type="ThrottlingException",
            message="Rate exceeded",
            http_status=429,
            count=2,
            operation="InvokeModel",
        )
    )
    assert queued.status == "ok"

    listed = fc.bedrock.get_faults()
    assert len(listed.faults) == 1
    assert listed.faults[0].error_type == "ThrottlingException"
    assert listed.faults[0].remaining == 2
    assert listed.faults[0].operation == "InvokeModel"
    assert listed.faults[0].model_id is None

    cleared = fc.bedrock.clear_faults()
    assert cleared.status == "ok"
    assert fc.bedrock.get_faults().faults == []


def test_bedrock_invocation_decodes_error_field(
    fc: FakeCloudSync, fakecloud_url: str
) -> None:
    """End-to-end coverage: inject a fault, make a Bedrock call via boto3,
    then confirm the SDK decodes the populated `error` field on the faulted
    invocation and `None` on the successful one.
    """
    bedrock = boto3.client(
        "bedrock-runtime",
        **_boto_kwargs(fakecloud_url),
        config=BotocoreConfig(retries={"max_attempts": 1, "mode": "standard"}),
    )
    model_id = "anthropic.claude-3-haiku-20240307-v1:0"

    fc.bedrock.queue_fault(
        BedrockFaultRule(
            error_type="ThrottlingException",
            message="Rate exceeded",
            http_status=429,
            count=1,
        )
    )

    body = json.dumps(
        {
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": 16,
            "messages": [{"role": "user", "content": "hello"}],
        }
    )

    # First call faults
    try:
        bedrock.invoke_model(modelId=model_id, body=body)
    except Exception:
        pass

    # Second call succeeds
    bedrock.invoke_model(modelId=model_id, body=body)

    # boto3 may auto-retry 429s so the exact count depends on retry config;
    # the property we care about is that the SDK decodes both a populated
    # `error` (on the faulted call) and `None` (on any successful call).
    invs = fc.bedrock.get_invocations().invocations
    assert len(invs) >= 2
    faulted = [i for i in invs if i.error is not None]
    succeeded = [i for i in invs if i.error is None]
    assert len(faulted) >= 1
    assert len(succeeded) >= 1
    assert "ThrottlingException" in faulted[0].error  # type: ignore[operator]


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


def test_bedrock_response_rule_to_dict() -> None:
    rule = BedrockResponseRule(prompt_contains="spam", response="x")
    assert rule.to_dict() == {"promptContains": "spam", "response": "x"}


def test_bedrock_fault_rule_to_dict_minimal() -> None:
    rule = BedrockFaultRule(error_type="ThrottlingException")
    assert rule.to_dict() == {"errorType": "ThrottlingException"}


def test_bedrock_fault_rule_to_dict_full() -> None:
    rule = BedrockFaultRule(
        error_type="ValidationException",
        message="bad input",
        http_status=400,
        count=3,
        model_id="anthropic.claude-3-haiku-20240307-v1:0",
        operation="Converse",
    )
    assert rule.to_dict() == {
        "errorType": "ValidationException",
        "message": "bad input",
        "httpStatus": 400,
        "count": 3,
        "modelId": "anthropic.claude-3-haiku-20240307-v1:0",
        "operation": "Converse",
    }


def test_trailing_slash_stripped() -> None:
    from fakecloud import FakeCloud

    fc = FakeCloud("http://localhost:4566/")
    assert fc._base == "http://localhost:4566"


def test_trailing_slash_stripped_sync() -> None:
    fc = FakeCloudSync("http://localhost:4566/")
    assert fc._base == "http://localhost:4566"


# ── Scheduler (EventBridge Scheduler) ─────────────────────────────────


def test_scheduler_get_schedules(fc: FakeCloudSync, fakecloud_url: str) -> None:
    sched = boto3.client("scheduler", **_boto_kwargs(fakecloud_url))
    sched.create_schedule(
        Name="py-sdk-sched-list",
        ScheduleExpression="rate(1 hour)",
        FlexibleTimeWindow={"Mode": "OFF"},
        Target={
            "Arn": "arn:aws:sqs:us-east-1:000000000000:noop",
            "RoleArn": "arn:aws:iam::000000000000:role/s",
        },
    )
    resp = fc.scheduler.get_schedules()
    names = [s.name for s in resp.schedules]
    assert "py-sdk-sched-list" in names


def test_scheduler_fire_schedule(fc: FakeCloudSync, fakecloud_url: str) -> None:
    sched = boto3.client("scheduler", **_boto_kwargs(fakecloud_url))
    sqs = boto3.client("sqs", **_boto_kwargs(fakecloud_url))
    q_url = sqs.create_queue(QueueName="py-sdk-fire-target")["QueueUrl"]
    q_arn = sqs.get_queue_attributes(QueueUrl=q_url, AttributeNames=["QueueArn"])[
        "Attributes"
    ]["QueueArn"]
    sched.create_schedule(
        Name="py-sdk-sched-fire",
        ScheduleExpression="rate(365 days)",
        FlexibleTimeWindow={"Mode": "OFF"},
        Target={
            "Arn": q_arn,
            "RoleArn": "arn:aws:iam::000000000000:role/s",
            "Input": '{"from":"pytest"}',
        },
    )
    resp = fc.scheduler.fire_schedule("default", "py-sdk-sched-fire")
    assert "schedule/default/py-sdk-sched-fire" in resp.schedule_arn
    assert resp.target_arn == q_arn


# ── Route 53 admin ────────────────────────────────────────────────────


def test_route53_set_health_check_status(fc: FakeCloudSync, fakecloud_url: str) -> None:
    r53 = boto3.client("route53", **_boto_kwargs(fakecloud_url))
    created = r53.create_health_check(
        CallerReference="py-sdk-route53-flip",
        HealthCheckConfig={
            "Type": "TCP",
            "IPAddress": "203.0.113.50",
            "Port": 80,
            "RequestInterval": 30,
            "FailureThreshold": 3,
        },
    )
    hc_id = created["HealthCheck"]["Id"]

    fc.route53.set_health_check_status(hc_id, "Failure", reason="endpoint timed out")

    after = r53.get_health_check_status(HealthCheckId=hc_id)
    for obs in after["HealthCheckObservations"]:
        assert obs["StatusReport"]["Status"] == "Failure: endpoint timed out"

    fc.route53.set_health_check_status(hc_id, "Success")
    after = r53.get_health_check_status(HealthCheckId=hc_id)
    for obs in after["HealthCheckObservations"]:
        assert obs["StatusReport"]["Status"] == "Success: HTTP Status Code 200"


def test_route53_set_health_check_status_unknown_id(fc: FakeCloudSync) -> None:
    import pytest as _pytest

    from fakecloud.client import FakeCloudError

    with _pytest.raises(FakeCloudError) as exc:
        fc.route53.set_health_check_status("ghost-hc", "Failure", reason="x")
    assert exc.value.status == 404
