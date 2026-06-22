+++
title = "Persistence"
description = "How fakecloud persists state to disk across restarts."
weight = 2
+++

By default fakecloud keeps all state in memory: startup is instant, shutdown is a no-op, and tests can run in parallel without cross-contamination. That's what you want for CI and most dev workflows.

For longer-running local environments where you want state to survive restarts, pass `--storage-mode=persistent --data-path=<dir>` to mirror all service state to disk.

## Enabling persistent mode

```sh
fakecloud --storage-mode persistent --data-path /var/lib/fakecloud
```

Or via environment:

```sh
FAKECLOUD_STORAGE_MODE=persistent FAKECLOUD_DATA_PATH=/var/lib/fakecloud fakecloud
```

## What's persisted

Every implemented service persists its control-plane state in this mode — a snapshot is written to disk on every mutation and reloaded on startup. The highlights below note what each captures.

- **S3** — buckets, objects, versions, delete markers, multipart uploads (resumable across restarts), and every bucket subresource: tags, lifecycle, CORS, policy, notification, logging, website, public access block, object lock, replication, ownership, inventory, encryption, ACL, accelerate. Written to disk on every mutation and reloaded on startup.
- **SQS** — queues, attributes, tags, in-flight and delayed messages.
- **SNS** — topics, subscriptions, attributes, tags, platform applications and endpoints, SMS settings.
- **EventBridge** — event buses, rules, targets, archives, replays, connections.
- **IAM / STS** — users, groups, roles, policies, instance profiles, access keys.
- **SSM Parameter Store** — parameters (String/SecureString/StringList), history.
- **Secrets Manager** — secrets, versions, rotation settings.
- **CloudWatch Logs** — log groups, streams, and log events.
- **KMS** — keys, aliases, key policies, grants.
- **DynamoDB** — tables, items, indexes, streams metadata.
- **Kinesis** — streams, shards, records.
- **SES** — identities, configuration sets, templates, contact lists and contacts, tags, suppression list, event destinations, identity policies, dedicated IP pools, tenants, receipt rule sets / rules / filters, account settings.
- **API Gateway v2** — HTTP APIs, routes, integrations, stages, deployments, authorizers.
- **CloudFormation** — stacks, templates, parameters, tags, resource listings, and notification ARNs.
- **Cognito** — user pools, user pool clients, users, groups, identity providers, resource servers, domains, import jobs, tags, UI customization, log delivery, risk and branding configuration, terms, WebAuthn credentials, refresh/access tokens and sessions. The `/_fakecloud/cognito/auth-events` introspection buffer resets on restart.
- **Lambda** — functions (code zips, configuration, resource policies), event source mappings. The `/_fakecloud/lambda/invocations` introspection buffer resets on restart; containers are rebuilt from the persisted code zip on first Invoke.
- **Step Functions** — state machines, definitions, executions, execution history events, tags.
- **RDS** — DB instances (configuration, credentials, tags), DB snapshots (including dump data), subnet groups, parameter groups.
- **ElastiCache** — cache clusters, replication groups, global replication groups, subnet groups, parameter groups, users, user groups, snapshots, serverless caches and snapshots, reserved cache nodes, tags.
- **Bedrock** — guardrails, guardrail versions, customization jobs, provisioned throughputs, logging config, async invocations, custom models, deployments, model import/copy/invocation jobs, evaluation jobs, inference profiles, prompt routers, resource policies, marketplace endpoints, foundation model agreements, automated reasoning policies/test cases/workflows, tags. The `/_fakecloud/bedrock/invocations` introspection buffer and simulation config (custom responses, response rules, fault rules) reset on restart.
- **Bedrock Agent** — agents (action groups, aliases, versions, collaborators, knowledge-base associations), data sources, flows (aliases, versions), knowledge bases, ingestion jobs, prompts and prompt versions, tags.
- **EC2** — VPCs, subnets, security groups, instances, ENIs, EBS volumes, route tables, internet/NAT gateways, NACLs, Elastic IPs, VPC endpoints, transit gateways, IPAM, and the rest of the account-partitioned control plane. Backing instance containers are reconciled on restart: a persisted `running`/`pending` instance is flipped to `pending` and a fresh container is respawned (the prior process's was removed by the reaper).
- **Route 53** — hosted zones, record sets, health checks, traffic policies and versions, traffic policy instances, DNSSEC status, key signing keys, query-logging configs, CIDR collections, reusable delegation sets, VPC authorizations, tags.
- **CloudFront** — distributions, invalidations, origin access controls/identities, cache / origin-request / response-headers / continuous-deployment policies, functions, public keys, key groups, key value stores, field-level encryption, realtime log configs, VPC origins, anycast IP lists, trust stores, resource policies, streaming distributions, connection groups, distribution tenants, tags. A distribution left `InProgress` at shutdown resumes its deploy and reaches `Deployed` after restart.
- **ELBv2** — load balancers, target groups, registered targets, listeners, rules, trust stores, resource policies. Target health is not persisted; the health prober re-derives it on startup.
- **WAF v2** — web ACLs, rule groups, IP sets, regex pattern sets, logging configs, permission policies, web-ACL associations, API keys, managed rule sets, tags. Data-plane sampled-request telemetry resets on restart.
- **ACM** — certificates (status, domains, validation, chains), tags, account config. A certificate left `PENDING_VALIDATION` (DNS) resumes auto-issue and reaches `ISSUED` after restart.
- **Glue** — databases, tables, partitions, jobs and job runs, crawlers, classifiers, connections, triggers, workflows, blueprints, schemas, security configs, sessions, and the rest of the Data Catalog.
- **Athena** — workgroups, data catalogs, named queries, prepared statements, query executions, notebooks, sessions, calculations, capacity reservations, tags.
- **Firehose** — delivery streams, destinations, tags, and server-side encryption config.
- **Organizations** — the organization, OUs, member accounts, service control policies and attachments, handshakes, enabled service access, delegated administrators, responsibility transfers, tags. A `CreateAccount` request left `IN_PROGRESS` resumes and reaches `SUCCEEDED` after restart.
- **Everything else** — API Gateway v1, ECR, ECS, EventBridge Scheduler, CloudWatch (alarms and dashboards), Application Auto Scaling, and Cognito Identity likewise persist their full control-plane state.

## Container-backed service data

The list above covers each service's **control-plane** state. Services that run real containers (RDS, ElastiCache, EC2, ECS) also have a **data plane** — the bytes inside the database, cache, or instance filesystem. In persistent mode fakecloud keeps that data durable too, by backing each container with a named volume keyed to the resource so a container recreated after a restart reattaches the same data instead of coming back empty:

- **RDS** — postgres/mysql/mariadb data directories. A row written before a restart is still there after the backing container is recovered. (Oracle/SQL Server/Db2 manage their own state and are not volume-backed.)
- **ElastiCache** — Redis/Valkey persist their `/data` RDB across restarts. Memcached is in-memory only, matching real ElastiCache (a reboot clears it).
- **EC2** — each instance's data directory (`/var/lib/fakecloud/ec2` by default, see `FAKECLOUD_EC2_INSTANCE_DATA_DIR`) survives a fakecloud restart and a stop/start, the way an EBS root volume does. This persists the instance's data directory, not a full root-filesystem snapshot. The volume is removed on `TerminateInstances`, matching a deleted EBS root volume.
- **ECS** — task storage is ephemeral, matching AWS: anonymous "Docker volumes" and `dockerVolumeConfiguration` with `scope=task` are deleted when the task stops. Host bind mounts, EFS/FSx, and `scope=shared` volumes persist independently of the task.

These data volumes default **on** under `--storage-mode=persistent` and **off** in memory mode (so test/CI runs stay ephemeral and isolated). Override per service with `FAKECLOUD_PERSIST_DB_VOLUMES` / `FAKECLOUD_PERSIST_EC2_VOLUMES`. The volumes are daemon-managed, so they work whether or not fakecloud itself runs in a container; on the Kubernetes backend, volume lifecycle is handled by the cluster.

## Version compatibility

On startup fakecloud reads `<data-path>/fakecloud.version.toml`. The file records the on-disk format version and the fakecloud version that created the directory. If the format version doesn't match the running binary, startup fails with an actionable error that points at the file.

There is no automatic migration — the intent is that you either keep using the binary that wrote the directory or start from an empty data path.

## S3 object body handling

Object bodies are streamed straight to disk in persistent mode, not held in RAM. A bounded LRU cache (`--s3-cache-size`, default 256 MiB) keeps recently read bodies available for fast re-reads. Objects larger than `cache-size / 2` bypass the cache on both read and write, so a single large upload cannot evict the entire working set.

## Introspection buffers are not persisted

The `/_fakecloud/s3/notifications` buffer — and every other `/_fakecloud/*` introspection endpoint, including `/_fakecloud/ses/emails` and `/_fakecloud/ses/inbound-emails` — is intentionally not persisted. These exist so tests can assert which events fired during the current run, not as a long-term audit log.
