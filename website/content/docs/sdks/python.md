+++
title = "Python SDK"
description = "Install and use the fakecloud SDK for Python tests (sync and async)."
weight = 2
+++

## Install

```sh
pip install fakecloud
```

Works with Python 3.9+. Type hints bundled (mypy-compatible).

## Initialize

The SDK ships two top-level clients with identical surface area:

- `FakeCloud` — async, returns coroutines
- `FakeCloudSync` — sync, returns values directly

Tables below document the async API. Drop the `await` and use `FakeCloudSync` for the sync variant; method names and arguments are the same.

### Async

```python
import asyncio
from fakecloud import FakeCloud

async def main():
    async with FakeCloud() as fc:  # defaults to http://localhost:4566
        await fc.reset()
        emails = await fc.ses.get_emails()

asyncio.run(main())
```

### Sync

```python
from fakecloud import FakeCloudSync

fc = FakeCloudSync("http://localhost:4566")
fc.reset()
emails = fc.ses.get_emails()
```

## Top-level

| Method                                | Description                                              |
| ------------------------------------- | -------------------------------------------------------- |
| `health()`                            | Server health check                                      |
| `reset()`                             | Reset all service state                                  |
| `reset_service(service)`              | Reset a single service                                   |
| `credentials()`                       | Fetch container/instance credentials (`GET /_fakecloud/credentials`) |
| `instance_identity_document()`        | EC2 instance identity document (`/latest/dynamic/instance-identity/document`) |
| `create_admin(account_id, user_name)` | Create an IAM admin user in a specific account           |
| `aclose()` (async only)               | Close the underlying `httpx.AsyncClient` (or use `async with`) |

## `fc.lambda_`

`lambda` is a Python keyword, so the attribute is `lambda_`.

| Method                                                            | Description                                  |
| ----------------------------------------------------------------- | -------------------------------------------- |
| `get_invocations()`                                               | List recorded Lambda invocations             |
| `get_warm_containers()`                                           | List warm containers                         |
| `evict_container(function_name)`                                  | Evict a warm container                       |
| `download_function_code(account_id, function_name, qualifier_or_latest="latest")` | Download a function-code zip blob (bytes) |
| `download_layer_content(account_id, layer_name, version)`         | Download a layer-version zip blob (bytes)    |

## `fc.rds`

| Method                | Description                                                       |
| --------------------- | ----------------------------------------------------------------- |
| `get_instances()`     | List managed RDS instances                                        |
| `lambda_invoke(req)`  | Invoke a Lambda via the RDS `aws_lambda` PG extension bridge      |
| `s3_import(req)`      | Fetch an S3 object via the RDS `aws_s3` extension bridge          |
| `s3_export(req)`      | Upload an object via the RDS `aws_s3` extension bridge            |

## `fc.elasticache`

| Method                     | Description                          |
| -------------------------- | ------------------------------------ |
| `get_clusters()`           | List cache clusters                  |
| `get_replication_groups()` | List replication groups              |
| `get_serverless_caches()`  | List serverless caches               |
| `get_elasti_cache_acls()`  | List ElastiCache ACLs                |

## `fc.athena`

| Method                | Description                                          |
| --------------------- | ---------------------------------------------------- |
| `get_named_queries()` | List every named query across workgroups (with `last_used_at`) |

## `fc.ecr`

| Method                            | Description                                  |
| --------------------------------- | -------------------------------------------- |
| `get_repositories()`              | List ECR repositories                        |
| `get_images(repository_name=None)`| List images, optionally filtered by repo     |
| `get_pull_through_rules()`        | List pull-through cache rules                |

## `fc.ec2`

| Method                      | Description                                                                                                                                                  |
| --------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `get_instances()`           | List EC2 instances with control-plane + runtime metadata                                                                                                    |
| `get_instance_networks()`   | Inspect each instance's backing network (Docker/Podman network or k8s NetworkPolicy), container IP, isolation backend, and whether security-group enforcement is active |

## `fc.ecs`

| Method                                          | Description                                                |
| ----------------------------------------------- | ---------------------------------------------------------- |
| `get_clusters()`                                | List ECS clusters                                          |
| `get_tasks(cluster=None, status=None)`          | List tasks, optionally filtered by cluster/status          |
| `get_task(task_id)`                             | Get a single task                                          |
| `get_task_logs(task_id)`                        | Tail captured stdout/stderr for a task                     |
| `force_stop_task(task_id)`                      | Force-stop a running task                                  |
| `mark_task_failed(task_id, req)`                | Mark a task as failed with a given reason                  |
| `get_events()`                                  | List ECS service events                                    |
| `get_task_metadata(task_arn)`                   | v4 metadata-URI dump for a task by ARN                     |
| `get_task_credentials(task_id)`                 | IAM credentials a running task would see                   |
| `get_task_metadata_v3(task_id)`                 | v3 task metadata document (pass-through dict)              |
| `get_task_metadata_v4(task_id)`                 | v4 task metadata document (pass-through dict)              |

## `fc.elbv2`

| Method                  | Description                                            |
| ----------------------- | ------------------------------------------------------ |
| `get_load_balancers()`  | List ALBs / NLBs / GWLBs                               |
| `get_target_groups()`   | List target groups                                     |
| `get_listeners()`       | List listeners                                         |
| `get_rules()`           | List listener rules                                    |
| `flush_access_logs()`   | Force buffered access + connection logs to S3          |
| `get_waf_counts()`      | Snapshot WAF-association counts across ALBs            |

## `fc.route53`

| Method                                                | Description                                                          |
| ----------------------------------------------------- | -------------------------------------------------------------------- |
| `set_health_check_status(health_check_id, status, reason=None)` | Flip a health check (`Success`/`Failure`/`Timeout`/`DnsError`/`InsufficientDataPoints`/`Unknown`) |
| `get_dnssec_material(zone_id)`                        | Deterministic DNSSEC KSK material for a zone                         |
| `sign_dnssec_rrset(zone_id, req)`                     | Sign an RRset with the zone's first ACTIVE KSK, return RRSIG fields  |

## `fc.ssm`

| Method                                                       | Description                                              |
| ------------------------------------------------------------ | -------------------------------------------------------- |
| `set_command_status(command_id, status, account_id=None)`    | Force a stored `SendCommand` into a given status         |
| `fail_command(command_id, req=None)`                         | Flip every (or one) invocation on a command to `Failed`  |
| `get_parameter_policy_events(account_id=None)`               | List Parameter Store policy events                       |
| `inject_session(req)`                                        | Drop a fake Session Manager record into state            |

## `fc.kms`

| Method        | Description                                       |
| ------------- | ------------------------------------------------- |
| `get_usage()` | Snapshot recorded KMS usage events across services |

## `fc.wafv2`

| Method           | Description                                         |
| ---------------- | --------------------------------------------------- |
| `evaluate(body)` | Run a synthetic request through the WAFv2 evaluator |

## `fc.cloudfront`

| Method                                         | Description                                          |
| ---------------------------------------------- | ---------------------------------------------------- |
| `get_distributions()`                          | List stored distributions with their `domain_name` (`Host` header) + `served` reachability |
| `set_distribution_status(distribution_id, status)` | Force a distribution into a given status (204 / `FakeCloudError`) |

## `fc.acm`

| Method                                                          | Description                                                            |
| --------------------------------------------------------------- | ---------------------------------------------------------------------- |
| `set_certificate_status(arn_or_id, status, reason=None)`        | Flip a cert (`ISSUED`/`FAILED`/`VALIDATION_TIMED_OUT`)                 |
| `approve_certificate(arn_or_id)`                                | Approve a `PENDING_VALIDATION` cert (EMAIL validation flow)            |
| `get_certificate_chain_info(arn_or_id)`                         | PEM-block / byte counts for a stored cert + chain                      |

## `fc.application_autoscaling`

| Method             | Description                                                      |
| ------------------ | ---------------------------------------------------------------- |
| `tick()`           | Force the watcher to evaluate every scaling policy now           |
| `scheduled_tick()` | Force the scheduled-action executor to evaluate every action now |

## `fc.logs`

| Method                                | Description                                          |
| ------------------------------------- | ---------------------------------------------------- |
| `inject_anomaly(req)`                 | Seed a synthetic anomaly for ListAnomalies/UpdateAnomaly |
| `get_delivery_config()`               | Persisted CloudWatch Logs delivery configurations    |
| `get_field_indexes(log_group_name)`   | Parsed `Fields` from index policies on a log group   |

## `fc.organizations`

Called as a method on the main client: `fc.organizations()`.

| Method            | Description                                                                 |
| ----------------- | --------------------------------------------------------------------------- |
| `get_accounts()`  | List member accounts with lifecycle state, parent OU, tags, attached SCPs   |

## `fc.ses`

| Method                                  | Description                                          |
| --------------------------------------- | ---------------------------------------------------- |
| `get_emails()`                          | List all sent emails                                 |
| `simulate_inbound(req)`                 | Simulate an inbound email (receipt rules)            |
| `get_metrics()`                         | Send/bounce/complaint metrics                        |
| `set_mail_from_status(identity, status)`| Set MAIL FROM verification status for an identity    |
| `get_dkim_public_key(identity)`         | Public DKIM key for an identity                      |
| `set_sandbox(sandbox)`                  | Enable/disable account-level sandbox                 |
| `get_bounces()`                         | List recorded bounces                                |
| `get_message_insights(message_id)`      | Message insights record for a sent message           |
| `get_smtp_submissions()`                | List SMTP-submission events                          |
| `get_event_destination_deliveries()`    | Deliveries fanned out to event destinations          |

## `fc.sns`

| Method                          | Description                                               |
| ------------------------------- | --------------------------------------------------------- |
| `get_messages()`                | List all published messages                               |
| `get_pending_confirmations()`   | List subscriptions pending confirmation                   |
| `confirm_subscription(req)`     | Confirm a pending subscription                            |
| `get_cert_pem()`                | Signing cert as a PEM string (`application/x-pem-file`)   |
| `get_sms()`                     | List SMS messages accepted by the SNS fake                |

## `fc.sqs`

| Method                  | Description                           |
| ----------------------- | ------------------------------------- |
| `get_messages()`        | List all messages across queues       |
| `tick_expiration()`     | Tick the message-expiration processor |
| `force_dlq(queue_name)` | Force all messages to the queue's DLQ |

## `fc.events`

| Method            | Description                       |
| ----------------- | --------------------------------- |
| `get_history()`   | Get EventBridge event history     |
| `fire_rule(req)`  | Fire an EventBridge rule manually |

## `fc.scheduler`

| Method                            | Description                            |
| --------------------------------- | -------------------------------------- |
| `get_schedules()`                 | List scheduler schedules               |
| `fire_schedule(group, name)`      | Manually fire a schedule by group/name |

## `fc.glue`

| Method                          | Description                                  |
| ------------------------------- | -------------------------------------------- |
| `get_jobs()`                    | List Glue jobs                               |
| `get_job_runs(job_name=None)`   | List job runs, optionally filtered by job    |

## `fc.s3`

| Method                            | Description                                            |
| --------------------------------- | ------------------------------------------------------ |
| `get_notifications()`             | List S3 notification events                            |
| `tick_lifecycle()`                | Tick the lifecycle processor                           |
| `get_access_points()`             | List S3 access points                                  |
| `get_object_lambda_responses()`   | List recorded Object Lambda responses                  |

## `fc.dynamodb`

| Method        | Description            |
| ------------- | ---------------------- |
| `tick_ttl()`  | Tick the TTL processor |
| `save_snapshot(data_path=None)` | Save a DynamoDB snapshot on demand (`None` -> configured store) |

## `fc.secretsmanager`

| Method             | Description                 |
| ------------------ | --------------------------- |
| `tick_rotation()`  | Tick the rotation scheduler |

## `fc.cognito`

| Method                                               | Description                                                |
| ---------------------------------------------------- | ---------------------------------------------------------- |
| `get_user_codes(pool_id, username)`                  | Confirmation codes for a specific user                     |
| `get_confirmation_codes()`                           | List all pending confirmation codes                        |
| `confirm_user(req)`                                  | Force-confirm a user                                       |
| `get_tokens()`                                       | List active tokens                                         |
| `expire_tokens(req)`                                 | Expire tokens for a pool/user                              |
| `get_auth_events()`                                  | List auth events                                           |
| `get_pre_token_gen_invocations()`                    | PreTokenGeneration Lambda trigger invocation log           |
| `mint_authorization_code(req)`                       | Mint an OAuth authorization code                           |
| `set_compromised_passwords(req)`                     | Seed compromised-password records                          |
| `get_webauthn_credentials()`                         | List stored WebAuthn credentials                           |

## `fc.apigatewayv2`

| Method                              | Description                                                  |
| ----------------------------------- | ------------------------------------------------------------ |
| `get_requests()`                    | List recorded HTTP API requests                              |
| `get_connections()`                 | List active WebSocket connections                            |
| `get_mtls_info(domain_name)`        | mTLS trust-store summary for a custom domain (dict)          |
| `ws_url(api_id, stage=None)`        | Build the `ws(s)://` WebSocket URL for an API + stage        |

## `fc.stepfunctions`

| Method                          | Description                                                          |
| ------------------------------- | -------------------------------------------------------------------- |
| `get_executions()`              | List all executions                                                  |
| `get_sync_executions()`         | List sync (Express) executions                                       |
| `get_execution_tree(arn)`       | Nested execution tree for a parent execution                         |
| `enqueue_activity_task(req)`    | Insert a pending task into an activity-worker queue                  |

## `fc.bedrock`

| Method                                  | Description                                              |
| --------------------------------------- | -------------------------------------------------------- |
| `get_invocations()`                     | List recorded Bedrock runtime invocations                |
| `set_model_response(model_id, text)`    | Configure a single canned response for a model           |
| `set_response_rules(model_id, rules)`   | Replace prompt-conditional response rules                |
| `clear_response_rules(model_id)`        | Clear all prompt-conditional response rules for a model  |
| `queue_fault(rule)`                     | Queue a fault rule for the next N matching calls         |
| `get_faults()`                          | List currently queued fault rules                        |
| `clear_faults()`                        | Clear all queued fault rules                             |

## `fc.bedrock_agent`

| Method          | Description                                |
| --------------- | ------------------------------------------ |
| `get_agents()`  | List Bedrock Agents (control plane)        |

## `fc.bedrock_agent_runtime`

| Method               | Description                                              |
| -------------------- | -------------------------------------------------------- |
| `get_invocations()`  | List Bedrock Agent runtime invocations (data plane)      |

## Error handling

Methods raise `FakeCloudError` on non-2xx responses:

```python
import asyncio
from fakecloud import FakeCloudError

async def main():
    try:
        await fc.cognito.confirm_user(req)
    except FakeCloudError as err:
        print(err.status)  # 404
        print(err.body)

asyncio.run(main())
```

## Example: pytest fixture

```python
import pytest
import boto3
from fakecloud import FakeCloudSync

@pytest.fixture
def fc():
    client = FakeCloudSync()
    yield client
    client.reset()

@pytest.fixture
def sqs():
    return boto3.client(
        "sqs",
        endpoint_url="http://localhost:4566",
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )

def test_app_publishes_to_sqs(fc, sqs):
    sqs.send_message(
        QueueUrl="http://localhost:4566/000000000000/my-queue",
        MessageBody="hello",
    )

    messages = fc.sqs.get_messages()
    assert len(messages.messages) == 1
    assert messages.messages[0].body == "hello"
```

## Source

- [`sdks/python`](https://github.com/faiscadev/fakecloud/tree/main/sdks/python)
- [Source README](https://github.com/faiscadev/fakecloud/blob/main/sdks/python/README.md) — always-current method list
