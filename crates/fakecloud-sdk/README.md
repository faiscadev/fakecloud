# fakecloud-sdk

Rust client SDK for [fakecloud](https://github.com/faiscadev/fakecloud), a local AWS cloud emulator.

The crate wraps fakecloud's introspection and simulation API (`/_fakecloud/*`) so Rust tests can inspect emulator state, reset services, and trigger time-based processors without going through raw HTTP calls.

As of v0.15.0 the Rust SDK covers all 111 introspection routes across 31 sub-clients, matching the TypeScript / Python / Go / Java / PHP SDKs.

## Installation

```bash
cargo add fakecloud-sdk
```

Rust 1.75+. Async-only, built on `reqwest` + `tokio`.

## Quick Start

```rust
use fakecloud_sdk::FakeCloud;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let fc = FakeCloud::new("http://localhost:4566");

    let health = fc.health().await?;
    println!("{}", health.version);

    fc.reset().await?;

    let emails = fc.ses().get_emails().await?;
    println!("sent {} emails", emails.emails.len());

    Ok(())
}
```

## Top-level

| Method                                              | Description                              |
| --------------------------------------------------- | ---------------------------------------- |
| `health().await`                                    | Server health check                      |
| `reset().await`                                     | Reset all service state                  |
| `reset_service(service).await`                      | Reset a single service                   |
| `reset_service_for_account(service, account).await` | Reset one service in one account         |
| `credentials().await`                               | Fetch container/instance credentials (`GET /_fakecloud/credentials`) |
| `instance_identity_document().await`                | EC2 instance identity document (`/latest/dynamic/instance-identity/document`) |
| `create_admin(req).await`                           | Create an admin user in another account  |

## Sub-clients

### `fc.acm()`

| Method                                          | Description                                                  |
| ----------------------------------------------- | ------------------------------------------------------------ |
| `set_certificate_status(arn_or_id, req).await`  | Flip a stored certificate's status (and optional failure reason) |
| `get_certificate_chain_info(arn_or_id).await`   | Inspect PEM block counts and byte sizes of a stored chain    |
| `approve_certificate(arn_or_id).await`          | Approve a `PENDING_VALIDATION` certificate                   |

### `fc.apigatewayv2()`

| Method                    | Description                                          |
| ------------------------- | ---------------------------------------------------- |
| `get_requests().await`    | List recorded HTTP API requests                      |
| `connections().await`     | List WebSocket connections                           |
| `mtls_info(name).await`   | Inspect mTLS trust-store status for a domain name    |
| `ws_url(api_id, stage)`   | Build a `ws://` URL for a WebSocket API (sync)       |

### `fc.application_autoscaling()`

| Method                   | Description                                                   |
| ------------------------ | ------------------------------------------------------------- |
| `tick().await`           | Re-evaluate target-tracking policies and emit scaling actions |
| `scheduled_tick().await` | Fire any scheduled actions whose time has arrived             |

### `fc.athena()`

| Method                      | Description                          |
| --------------------------- | ------------------------------------ |
| `get_named_queries().await` | List recorded Athena named queries   |

### `fc.bedrock()`

| Method                                       | Description                                                  |
| -------------------------------------------- | ------------------------------------------------------------ |
| `get_invocations().await`                    | List recorded Bedrock runtime invocations                    |
| `set_model_response(model_id, text).await`   | Configure a single canned response for a model               |
| `set_response_rules(model_id, rules).await`  | Replace prompt-conditional response rules for a model        |
| `clear_response_rules(model_id).await`       | Clear all prompt-conditional response rules for a model      |
| `queue_fault(rule).await`                    | Queue a fault rule for the next N calls                      |
| `get_faults().await`                         | List currently queued fault rules                            |
| `clear_faults().await`                       | Clear all queued fault rules                                 |

### `fc.bedrock_agent()`

| Method                | Description                       |
| --------------------- | --------------------------------- |
| `get_agents().await`  | List configured Bedrock agents    |

### `fc.bedrock_agent_runtime()`

| Method                    | Description                                     |
| ------------------------- | ----------------------------------------------- |
| `get_invocations().await` | List recorded Bedrock Agent runtime invocations |

### `fc.cloudfront()`

| Method                                   | Description                                              |
| ---------------------------------------- | -------------------------------------------------------- |
| `get_distributions().await`              | List distributions with `domainName` / `enabled` / `served` |
| `set_distribution_status(id, req).await` | Override a CloudFront distribution's deployment status   |

### `fc.cognito()`

| Method                                    | Description                                                |
| ----------------------------------------- | ---------------------------------------------------------- |
| `get_user_codes(pool_id, username).await` | Pending confirmation codes for a specific user             |
| `get_confirmation_codes().await`          | List all pending confirmation codes                        |
| `confirm_user(pool_id, username).await`   | Force-confirm a user                                       |
| `get_tokens().await`                      | List currently active tokens                               |
| `expire_tokens(req).await`                | Expire tokens for a pool/user                              |
| `get_auth_events().await`                 | List recorded auth events                                  |
| `get_pre_token_gen_invocations().await`   | List Pre-Token-Generation Lambda invocations               |
| `mint_authorization_code(req).await`      | Mint an OAuth2 authorization code without a real browser   |
| `set_compromised_passwords(req).await`    | Seed the compromised-credentials list                      |
| `get_webauthn_credentials().await`        | List registered WebAuthn credentials                       |

### `fc.dynamodb()`

| Method             | Description                     |
| ------------------ | ------------------------------- |
| `tick_ttl().await` | Tick the DynamoDB TTL processor |
| `save_snapshot(data_path).await` | Save a DynamoDB snapshot on demand (`None` -> configured store) |

### `fc.ecr()`

| Method                              | Description                       |
| ----------------------------------- | --------------------------------- |
| `get_images().await`                | List stored ECR images            |
| `get_repositories().await`          | List ECR repositories             |
| `get_pull_through_rules().await`    | List pull-through cache rules     |

### `fc.ecs()`

| Method                                  | Description                                                  |
| --------------------------------------- | ------------------------------------------------------------ |
| `get_clusters().await`                  | List ECS clusters with task counts                           |
| `get_tasks(filter).await`               | List tasks, optionally filtered by cluster / family / status |
| `get_task_logs(task_id).await`          | Fetch captured stdout/stderr for a task                      |
| `force_stop_task(task_id).await`        | Force a task into `STOPPED`                                  |
| `mark_task_failed(task_id, req).await`  | Mark a task failed with a custom reason / exit code          |
| `get_events().await`                    | List ECS service / task lifecycle events                     |
| `get_metadata_by_arn(task_arn).await`   | Fetch task metadata by full ARN                              |
| `task_credentials(task_id).await`       | Fetch IAM credentials served to the task                     |
| `task_metadata_v3(task_id).await`       | Task Metadata Endpoint v3 payload                            |
| `task_metadata_v4(task_id).await`       | Task Metadata Endpoint v4 payload                            |

### `fc.elasticache()`

| Method                                  | Description                  |
| --------------------------------------- | ---------------------------- |
| `get_clusters().await`                  | List ElastiCache clusters    |
| `get_replication_groups().await`        | List replication groups      |
| `get_serverless_caches().await`         | List serverless caches       |
| `get_acls().await`                      | List RBAC ACLs               |

### `fc.elbv2()`

| Method                       | Description                          |
| ---------------------------- | ------------------------------------ |
| `get_load_balancers().await` | List ALB / NLB / GWLB load balancers |
| `get_listeners().await`      | List listeners across load balancers |
| `get_rules().await`          | List listener rules                  |
| `get_target_groups().await`  | List target groups                   |
| `flush_access_logs().await`  | Flush buffered access-log entries    |
| `waf_counts().await`         | Inspect WAF allow/block counters     |

### `fc.events()`

| Method                  | Description                            |
| ----------------------- | -------------------------------------- |
| `get_history().await`   | Get event history and delivery records |
| `fire_rule(req).await`  | Fire an EventBridge rule manually      |

### `fc.glue()`

| Method                              | Description                              |
| ----------------------------------- | ---------------------------------------- |
| `get_jobs().await`                  | List configured Glue jobs                |
| `get_job_runs(job_name).await`      | List job runs, optionally for one job    |

### `fc.kms()`

| Method            | Description                                       |
| ----------------- | ------------------------------------------------- |
| `usage().await`   | KMS key usage counters (encrypt/decrypt/sign/etc) |

### `fc.lambda()`

| Method                                          | Description                          |
| ----------------------------------------------- | ------------------------------------ |
| `get_invocations().await`                       | List recorded Lambda invocations     |
| `get_warm_containers().await`                   | List warm (cached) Lambda containers |
| `evict_container(function_name).await`          | Evict a warm container               |
| `download_function_code(function_name).await`   | Download the uploaded function zip   |
| `download_layer_content(layer_name, ver).await` | Download a layer version's content   |

### `fc.logs()`

| Method                              | Description                                       |
| ----------------------------------- | ------------------------------------------------- |
| `inject_anomaly(req).await`         | Inject a synthetic CloudWatch Logs anomaly        |
| `get_delivery_config().await`       | Inspect log-delivery configuration                |
| `get_field_indexes(group).await`    | Inspect field indexes for a log group             |

### `fc.organizations()`

| Method                  | Description                        |
| ----------------------- | ---------------------------------- |
| `get_accounts().await`  | List Organizations member accounts |

### `fc.rds()`

| Method                              | Description                                                 |
| ----------------------------------- | ----------------------------------------------------------- |
| `get_instances().await`             | List managed RDS instances with runtime metadata            |
| `lambda_invoke(req).await`          | Invoke a Lambda via the `aws_lambda` PG extension bridge    |
| `s3_import(req).await`              | Simulate the `aws_s3.table_import_from_s3` extension call   |
| `s3_export(req).await`              | Simulate the `aws_s3.query_export_to_s3` extension call     |

### `fc.route53()`

| Method                                       | Description                          |
| -------------------------------------------- | ------------------------------------ |
| `dnssec_material(hosted_zone_id).await`      | Inspect DNSSEC key-signing material  |
| `dnssec_sign(hosted_zone_id, req).await`     | Sign a payload with the zone's KSK   |
| `set_health_check_status(id, req).await`     | Override a health check's status     |

### `fc.s3()`

| Method                                  | Description                              |
| --------------------------------------- | ---------------------------------------- |
| `get_notifications().await`             | List S3 notification events              |
| `tick_lifecycle().await`                | Tick the lifecycle processor             |
| `get_access_points().await`             | List S3 access points                    |
| `get_object_lambda_responses().await`   | List Object Lambda transformed responses |

### `fc.scheduler()`

| Method                              | Description                          |
| ----------------------------------- | ------------------------------------ |
| `get_schedules().await`             | List EventBridge Scheduler schedules |
| `fire_schedule(name, group).await`  | Fire a schedule immediately          |

### `fc.secretsmanager()`

| Method                  | Description                  |
| ----------------------- | ---------------------------- |
| `tick_rotation().await` | Tick the rotation scheduler  |

### `fc.ses()`

| Method                                              | Description                                         |
| --------------------------------------------------- | --------------------------------------------------- |
| `get_emails().await`                                | List all sent emails                                |
| `simulate_inbound(req).await`                       | Simulate an inbound email (receipt rules)           |
| `get_metrics().await`                               | Aggregate send / bounce / complaint counters        |
| `get_bounces().await`                               | List recorded bounces                               |
| `set_sandbox(enabled).await`                        | Toggle SES sandbox mode                             |
| `get_event_destination_deliveries().await`          | List event-destination delivery records             |
| `get_dkim_public_key(domain).await`                 | Fetch the public DKIM key for a domain              |
| `set_mail_from_status(req).await`                   | Override a domain's MAIL-FROM verification status   |
| `get_message_insights(message_id).await`            | Fetch message-insights breakdown for a message      |
| `get_smtp_submissions().await`                      | List inbound SMTP submission records                |

### `fc.sns()`

| Method                              | Description                                       |
| ----------------------------------- | ------------------------------------------------- |
| `get_messages().await`              | List all published messages                       |
| `get_pending_confirmations().await` | List subscriptions pending confirmation           |
| `confirm_subscription(req).await`   | Confirm a pending subscription                    |
| `cert_pem().await`                  | Fetch the PEM used to sign SNS HTTP/S deliveries  |
| `sms().await`                       | List recorded SMS deliveries                      |

### `fc.sqs()`

| Method                          | Description                            |
| ------------------------------- | -------------------------------------- |
| `get_messages().await`          | List all messages across all queues    |
| `tick_expiration().await`       | Tick the message expiration processor  |
| `force_dlq(queue_name).await`   | Force all messages to the queue's DLQ  |

### `fc.ssm()`

| Method                                          | Description                                            |
| ----------------------------------------------- | ------------------------------------------------------ |
| `set_command_status(command_id, req).await`     | Override an SSM Run Command's status                   |
| `fail_command(command_id, req).await`           | Mark an SSM Run Command failed with a reason           |
| `parameter_policy_events().await`               | List parameter-policy expiration / notification events |
| `inject_session(req).await`                     | Inject a synthetic SSM Session                         |

### `fc.stepfunctions()`

| Method                                    | Description                              |
| ----------------------------------------- | ---------------------------------------- |
| `get_executions().await`                  | List standard executions                 |
| `get_sync_executions().await`             | List Express sync executions             |
| `enqueue_activity_task(req).await`        | Enqueue a task for an activity worker    |
| `get_execution_tree(execution_arn).await` | Fetch a nested execution tree            |

### `fc.wafv2()`

| Method                       | Description                                       |
| ---------------------------- | ------------------------------------------------- |
| `evaluate(body).await`       | Evaluate a request against configured WAFv2 rules |

## Testing Bedrock-calling code

```rust
use fakecloud_sdk::FakeCloud;
use fakecloud_sdk::types::{BedrockFaultRule, BedrockResponseRule};

#[tokio::test]
async fn classifier_branches_on_spam_vs_ham() {
    let fc = FakeCloud::new("http://localhost:4566");
    fc.reset().await.unwrap();

    let model_id = "anthropic.claude-3-haiku-20240307-v1:0";
    fc.bedrock()
        .set_response_rules(
            model_id,
            &[
                BedrockResponseRule {
                    prompt_contains: Some("buy now".into()),
                    response: r#"{"label":"spam"}"#.into(),
                },
                BedrockResponseRule {
                    prompt_contains: None, // catch-all
                    response: r#"{"label":"ham"}"#.into(),
                },
            ],
        )
        .await
        .unwrap();

    classify("hello friend").await;         // user code calls Bedrock
    classify("buy now cheap pills").await;

    let invs = fc.bedrock().get_invocations().await.unwrap();
    assert_eq!(invs.invocations.len(), 2);
    assert!(invs.invocations[0].output.contains("ham"));
    assert!(invs.invocations[1].output.contains("spam"));
}

#[tokio::test]
async fn retries_on_throttling() {
    let fc = FakeCloud::new("http://localhost:4566");
    fc.reset().await.unwrap();

    fc.bedrock()
        .queue_fault(&BedrockFaultRule {
            error_type: "ThrottlingException".into(),
            message: Some("Rate exceeded".into()),
            http_status: Some(429),
            count: Some(1), // only the first call faults; the retry succeeds
            ..Default::default()
        })
        .await
        .unwrap();

    classify("hello").await;

    let invs = fc.bedrock().get_invocations().await.unwrap();
    assert_eq!(invs.invocations.len(), 2);
    assert!(invs.invocations[0]
        .error
        .as_deref()
        .unwrap_or("")
        .contains("ThrottlingException"));
    assert!(invs.invocations[1].error.is_none());
}
```

## Error handling

All methods return `Result<T, fakecloud_sdk::Error>`. `Error` has two variants:

- `Error::Api { status, body }` — fakecloud returned a non-2xx response
- `Error::Http(reqwest::Error)` — transport-level failure

## Repository

- Project: <https://github.com/faiscadev/fakecloud>
- Website: <https://fakecloud.dev>
