# fakecloud

Python SDK for [fakecloud](https://github.com/faiscadev/fakecloud) — a local AWS cloud emulator.

This package provides async and sync clients for the fakecloud introspection and simulation API (`/_fakecloud/*` endpoints), letting you inspect sent emails, published messages, Lambda invocations, and more from your tests.

## Installation

```bash
pip install fakecloud
```

## Quick start

### Async

```python
import asyncio
from fakecloud import FakeCloud

async def main():
    async with FakeCloud("http://localhost:4566") as fc:
        # Check server health
        health = await fc.health()
        print(health.status, health.version)

        # List sent SES emails
        emails = await fc.ses.get_emails()
        for email in emails.emails:
            print(f"{email.from_addr} -> {email.to}: {email.subject}")

        # List SNS messages
        messages = await fc.sns.get_messages()
        for msg in messages.messages:
            print(f"{msg.topic_arn}: {msg.message}")

        # Inspect Lambda invocations
        invocations = await fc.lambda_.get_invocations()
        for inv in invocations.invocations:
            print(f"{inv.function_arn}: {inv.payload}")

        # Reset all state between tests
        await fc.reset()

asyncio.run(main())
```

### Sync

```python
from fakecloud import FakeCloudSync

with FakeCloudSync("http://localhost:4566") as fc:
    health = fc.health()
    print(health.status)

    emails = fc.ses.get_emails()
    for email in emails.emails:
        print(email.subject)
```

## API reference

The SDK ships two top-level clients with identical surfaces:

- `FakeCloud` — async (`await fc.x.y()`), context-manager friendly
- `FakeCloudSync` — sync (`fc.x.y()`)

Tables below document the async API. Drop the `await` and use `FakeCloudSync` for the sync variant; method names and arguments match exactly.

### Top-level

Pass `base_url` (default `http://localhost:4566`).

| Method | Description |
|---|---|
| `health()` | Server health check |
| `reset()` | Reset all service state |
| `reset_service(service)` | Reset a single service |
| `credentials()` | Fetch container/instance credentials (`GET /_fakecloud/credentials`) |
| `instance_identity_document()` | EC2 instance identity document (`/latest/dynamic/instance-identity/document`) |
| `dns_resolve(name, type="A")` | Resolve a name against Route 53 records like the `--dns` resolver (`GET /_fakecloud/dns/resolve`) |
| `create_admin(account_id, user_name)` | Create an IAM admin user in a specific account |
| `aclose()` *(async only)* | Close the underlying `httpx.AsyncClient` (or use `async with`) |

### `fc.lambda_`

`lambda` is a Python keyword, so the attribute is `lambda_`.

| Method | Description |
|---|---|
| `get_invocations()` | List recorded Lambda invocations |
| `get_warm_containers()` | List warm containers |
| `evict_container(function_name)` | Evict a warm container |
| `download_function_code(account_id, function_name, qualifier_or_latest="latest")` | Download function-code zip (bytes) |
| `download_layer_content(account_id, layer_name, version)` | Download layer-version zip (bytes) |

### `fc.rds`

| Method | Description |
|---|---|
| `get_instances()` | List managed RDS instances |
| `lambda_invoke(req)` | Invoke a Lambda via the RDS `aws_lambda` PG extension bridge |
| `s3_import(req)` | Fetch an S3 object via the RDS `aws_s3` extension bridge |
| `s3_export(req)` | Upload an object via the RDS `aws_s3` extension bridge |

### `fc.elasticache`

| Method | Description |
|---|---|
| `get_clusters()` | List cache clusters |
| `get_replication_groups()` | List replication groups |
| `get_serverless_caches()` | List serverless caches |
| `get_elasti_cache_acls()` | List ElastiCache ACLs |

### `fc.athena`

| Method | Description |
|---|---|
| `get_named_queries()` | List every named query across workgroups (with `last_used_at`) |

### `fc.ecr`

| Method | Description |
|---|---|
| `get_repositories()` | List ECR repositories |
| `get_images(repository_name=None)` | List images, optionally filtered by repo |
| `get_pull_through_rules()` | List pull-through cache rules |

### `fc.ec2`

| Method | Description |
|---|---|
| `get_instances()` | List EC2 instances with control-plane + runtime metadata |
| `get_instance_networks()` | Inspect each instance's backing network (Docker/Podman network or k8s NetworkPolicy), container IP, isolation backend, and whether security-group enforcement is active |

### `fc.ecs`

| Method | Description |
|---|---|
| `get_clusters()` | List ECS clusters |
| `get_tasks(cluster=None, status=None)` | List tasks, optionally filtered by cluster/status |
| `get_task(task_id)` | Get a single task |
| `get_task_logs(task_id)` | Tail captured stdout/stderr for a task |
| `force_stop_task(task_id)` | Force-stop a running task |
| `mark_task_failed(task_id, req)` | Mark a task as failed with a given reason |
| `get_events()` | List ECS service events |
| `get_task_metadata(task_arn)` | v4 metadata-URI dump for a task by ARN |
| `get_task_credentials(task_id)` | IAM credentials a running task would see |
| `get_task_metadata_v3(task_id)` | v3 task metadata document (pass-through dict) |
| `get_task_metadata_v4(task_id)` | v4 task metadata document (pass-through dict) |

### `fc.elbv2`

| Method | Description |
|---|---|
| `get_load_balancers()` | List ALBs / NLBs / GWLBs |
| `get_target_groups()` | List target groups |
| `get_listeners()` | List listeners |
| `get_rules()` | List listener rules |
| `flush_access_logs()` | Force buffered access + connection logs to S3 |
| `get_waf_counts()` | Snapshot WAF-association counts across ALBs |

### `fc.route53`

| Method | Description |
|---|---|
| `set_health_check_status(health_check_id, status, reason=None)` | Flip a health check (`Success`/`Failure`/`Timeout`/`DnsError`/`InsufficientDataPoints`/`Unknown`) |
| `get_dnssec_material(zone_id)` | Deterministic DNSSEC KSK material for a zone |
| `sign_dnssec_rrset(zone_id, req)` | Sign an RRset under the zone's first ACTIVE KSK |

### `fc.ssm`

| Method | Description |
|---|---|
| `set_command_status(command_id, status, account_id=None)` | Force a stored `SendCommand` into a given status |
| `fail_command(command_id, req=None)` | Flip every (or one) invocation on a command to `Failed` |
| `get_parameter_policy_events(account_id=None)` | List Parameter Store policy events |
| `inject_session(req)` | Drop a fake Session Manager record into state |

### `fc.kms`

| Method | Description |
|---|---|
| `get_usage()` | Snapshot recorded KMS usage events across services |

### `fc.wafv2`

| Method | Description |
|---|---|
| `evaluate(body)` | Run a synthetic request through the WAFv2 evaluator |

### `fc.cloudfront`

| Method | Description |
|---|---|
| `get_distributions()` | List stored distributions with their `domain_name` (`Host` header) + `served` reachability |
| `set_distribution_status(distribution_id, status)` | Force a distribution into a given status |

### `fc.acm`

| Method | Description |
|---|---|
| `set_certificate_status(arn_or_id, status, reason=None)` | Flip cert status (`ISSUED`/`FAILED`/`VALIDATION_TIMED_OUT`) |
| `approve_certificate(arn_or_id)` | Approve a `PENDING_VALIDATION` cert (EMAIL validation flow) |
| `get_certificate_chain_info(arn_or_id)` | PEM-block / byte counts for a stored cert + chain |

### `fc.application_autoscaling`

| Method | Description |
|---|---|
| `tick()` | Force the watcher to evaluate every scaling policy now |
| `scheduled_tick()` | Force the scheduled-action executor to evaluate every action now |

### `fc.logs`

| Method | Description |
|---|---|
| `inject_anomaly(req)` | Seed a synthetic anomaly for ListAnomalies/UpdateAnomaly |
| `get_delivery_config()` | Persisted CloudWatch Logs delivery configurations |
| `get_field_indexes(log_group_name)` | Parsed `Fields` from index policies on a log group |

### `fc.organizations`

Called as a method on the main client: `fc.organizations()`.

| Method | Description |
|---|---|
| `get_accounts()` | List member accounts with lifecycle state, parent OU, tags, attached SCPs |

### `fc.ses`

| Method | Description |
|---|---|
| `get_emails()` | List all sent emails |
| `simulate_inbound(req)` | Simulate an inbound email (receipt rules) |
| `get_metrics()` | Send/bounce/complaint metrics |
| `set_mail_from_status(identity, status)` | Set MAIL FROM verification status for an identity |
| `get_dkim_public_key(identity)` | Public DKIM key for an identity |
| `set_sandbox(sandbox)` | Enable/disable account-level sandbox |
| `get_bounces()` | List recorded bounces |
| `get_message_insights(message_id)` | Message insights record for a sent message |
| `get_smtp_submissions()` | List SMTP-submission events |
| `get_event_destination_deliveries()` | Deliveries fanned out to event destinations |

### `fc.sns`

| Method | Description |
|---|---|
| `get_messages()` | List all published messages |
| `get_pending_confirmations()` | List subscriptions pending confirmation |
| `confirm_subscription(req)` | Confirm a pending subscription |
| `get_cert_pem()` | Signing cert as a PEM string |
| `get_sms()` | List SMS messages accepted by the SNS fake |

### `fc.sqs`

| Method | Description |
|---|---|
| `get_messages()` | List all messages across queues |
| `tick_expiration()` | Tick the message-expiration processor |
| `force_dlq(queue_name)` | Force all messages to the queue's DLQ |

### `fc.events`

| Method | Description |
|---|---|
| `get_history()` | Get EventBridge event history |
| `fire_rule(req)` | Fire an EventBridge rule manually |

### `fc.scheduler`

| Method | Description |
|---|---|
| `get_schedules()` | List scheduler schedules |
| `fire_schedule(group, name)` | Manually fire a schedule by group/name |

### `fc.glue`

| Method | Description |
|---|---|
| `get_jobs()` | List Glue jobs |
| `get_job_runs(job_name=None)` | List job runs, optionally filtered by job |
| `get_crawlers()` | List Glue crawlers with state and target summary |

### `fc.cloudwatch`

| Method | Description |
|---|---|
| `get_alarms()` | List metric and composite alarms across accounts/regions |
| `get_metrics()` | List unique metric series with datapoint count and latest value |

### `fc.s3`

| Method | Description |
|---|---|
| `get_notifications()` | List S3 notification events |
| `tick_lifecycle()` | Tick the lifecycle processor |
| `get_access_points()` | List S3 access points |
| `get_object_lambda_responses()` | List recorded Object Lambda responses |

### `fc.dynamodb`

| Method | Description |
|---|---|
| `tick_ttl()` | Tick the TTL processor |
| `save_snapshot(data_path=None)` | Save a DynamoDB snapshot on demand (`None` -> configured store) |

### `fc.secretsmanager`

| Method | Description |
|---|---|
| `tick_rotation()` | Tick the rotation scheduler |

### `fc.cognito`

| Method | Description |
|---|---|
| `get_user_codes(pool_id, username)` | Confirmation codes for a specific user |
| `get_confirmation_codes()` | List all pending confirmation codes |
| `confirm_user(req)` | Force-confirm a user |
| `get_tokens()` | List active tokens |
| `expire_tokens(req)` | Expire tokens for a pool/user |
| `get_auth_events()` | List auth events |
| `get_pre_token_gen_invocations()` | PreTokenGeneration Lambda trigger invocation log |
| `mint_authorization_code(req)` | Mint an OAuth authorization code |
| `set_compromised_passwords(req)` | Seed compromised-password records |
| `get_webauthn_credentials()` | List stored WebAuthn credentials |

### `fc.apigatewayv2`

| Method | Description |
|---|---|
| `get_requests()` | List recorded HTTP API requests |
| `get_connections()` | List active WebSocket connections |
| `get_mtls_info(domain_name)` | mTLS trust-store summary for a custom domain (dict) |
| `ws_url(api_id, stage=None)` | Build the `ws(s)://` WebSocket URL for an API + stage |

### `fc.stepfunctions`

| Method | Description |
|---|---|
| `get_executions()` | List all executions |
| `get_sync_executions()` | List sync (Express) executions |
| `get_execution_tree(arn)` | Nested execution tree for a parent execution |
| `enqueue_activity_task(req)` | Insert a pending task into an activity-worker queue |

### `fc.bedrock`

| Method | Description |
|---|---|
| `get_invocations()` | List recorded Bedrock runtime invocations |
| `set_model_response(model_id, text)` | Configure a single canned response for a model |
| `set_response_rules(model_id, rules)` | Replace prompt-conditional response rules |
| `clear_response_rules(model_id)` | Clear all prompt-conditional response rules for a model |
| `queue_fault(rule)` | Queue a fault rule for the next N matching calls |
| `get_faults()` | List currently queued fault rules |
| `clear_faults()` | Clear all queued fault rules |

### `fc.bedrock_agent`

| Method | Description |
|---|---|
| `get_agents()` | List Bedrock Agents (control plane) |

### `fc.bedrock_agent_runtime`

| Method | Description |
|---|---|
| `get_invocations()` | List Bedrock Agent runtime invocations (data plane) |

### Testing Bedrock-calling code end-to-end

```python
from fakecloud import FakeCloudSync
from fakecloud.types import BedrockResponseRule, BedrockFaultRule

fc = FakeCloudSync()
model_id = "anthropic.claude-3-haiku-20240307-v1:0"


def test_classifier_branches_on_spam_vs_ham():
    fc.reset()
    fc.bedrock.set_response_rules(
        model_id,
        [
            BedrockResponseRule(prompt_contains="buy now", response='{"label":"spam"}'),
            BedrockResponseRule(prompt_contains=None, response='{"label":"ham"}'),
        ],
    )

    classify("hello friend")           # user code that calls Bedrock
    classify("buy now cheap pills")

    invocations = fc.bedrock.get_invocations().invocations
    assert len(invocations) == 2
    assert "ham" in invocations[0].output
    assert "spam" in invocations[1].output


def test_retries_on_throttling():
    fc.reset()
    fc.bedrock.queue_fault(
        BedrockFaultRule(
            error_type="ThrottlingException",
            message="Rate exceeded",
            http_status=429,
            count=1,  # only the first call faults; the retry succeeds
        )
    )

    classify("hello")

    invocations = fc.bedrock.get_invocations().invocations
    assert len(invocations) == 2
    assert "ThrottlingException" in (invocations[0].error or "")
    assert invocations[1].error is None
```

### Error handling

All methods raise `FakeCloudError` on non-2xx responses:

```python
from fakecloud.client import FakeCloudError

try:
    await fc.health()
except FakeCloudError as e:
    print(e.status, e.body)
```

## pytest fixture example

```python
import pytest
from fakecloud import FakeCloudSync

@pytest.fixture(autouse=True)
def reset_fakecloud():
    fc = FakeCloudSync()
    fc.reset()
    yield fc
    fc.close()

def test_email_sent(reset_fakecloud):
    # ... your code that sends an email via SES ...
    emails = reset_fakecloud.ses.get_emails()
    assert len(emails.emails) == 1
    assert emails.emails[0].subject == "Welcome"
```
