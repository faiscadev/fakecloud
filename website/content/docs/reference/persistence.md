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
- **SSM Parameter Store** — parameters (String/SecureString/StringList), history, and the rest of the SSM control plane, per account and region. Snapshots written before SSM was region-partitioned load with each parameter in the region its ARN names.
- **Secrets Manager** — secrets, versions, rotation settings, replicas, per account and region. Snapshots written before Secrets Manager was region-partitioned load with each secret in the region its ARN names.
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
- **Bedrock Agent Runtime** -- sessions (metadata, status, encryption key), session invocations and invocation steps, flow executions (status, the flow snapshot captured at start), tags. The `/_fakecloud/bedrock-agent-runtime/invocations` introspection log resets on restart.
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

The list above covers each service's **control-plane** state. Services that run real containers (RDS, ElastiCache, EC2, ECS) also have a **data plane**: the bytes inside the database, cache, or instance filesystem. In persistent mode fakecloud keeps that data durable too, by backing each container with a named volume keyed to the resource so a container recreated after a restart reattaches the same data instead of coming back empty:

- **RDS**: postgres/mysql/mariadb data directories. A row written before a restart is still there after the backing container is recovered. (Oracle/SQL Server/Db2 manage their own state and are not volume-backed.) The volume is removed on `DeleteDBInstance`.
- **ElastiCache**: Redis/Valkey (cache clusters, replication groups, serverless caches) persist their `/data` RDB across restarts. Memcached is in-memory only, matching real ElastiCache (a reboot clears it). The volume is removed when the resource is deleted.
- **EC2**: each instance's data directory (`/var/lib/fakecloud/ec2` by default, see `FAKECLOUD_EC2_INSTANCE_DATA_DIR`) survives a fakecloud restart and a stop/start, the way an EBS root volume does. This persists the instance's data directory, not a full root-filesystem snapshot. The volume is removed on `TerminateInstances`, matching a deleted EBS root volume.
- **ECS**: task storage is ephemeral, matching AWS: anonymous "Docker volumes" and `dockerVolumeConfiguration` with `scope=task` are deleted when the task stops. Host bind mounts, EFS/FSx, and `scope=shared` volumes persist independently of the task.

The RDS and EC2 data volumes default **on** under `--storage-mode=persistent` and **off** in memory mode (so test/CI runs stay ephemeral and isolated). Override per service with `FAKECLOUD_PERSIST_DB_VOLUMES` / `FAKECLOUD_PERSIST_EC2_VOLUMES`. ElastiCache always backs Redis/Valkey with a volume. The volumes are daemon-managed, so they work whether or not fakecloud itself runs in a container; on the Kubernetes backend, volume lifecycle is handled by the cluster.

### Data volumes belong to one data directory

The first time fakecloud uses a data directory it writes a random id to `<data-path>/data-volume-scope`. Every volume name carries a short hash of that id and the directory's canonical path, for example `fakecloud-rds-data-d1a2b3c4d5e6f-123456789012-db-ABC123...` (RDS volumes are keyed by the instance's `DbiResourceId` and ElastiCache volumes by the resource's ARN and creation time, so a `NewDBInstanceIdentifier` rename keeps its data and a delete followed by a recreate under the same name never reuses the old volume). Restarting against the same data directory reattaches the same volumes. A different data directory (including a copy), the same path after the directory was wiped, or a second fakecloud on the same Docker daemon using another data directory never sees them, so a fresh `--data-path` that creates a DB with an identifier another data directory used starts with an empty database. Deleting the resource, or resetting the service via `/_fakecloud/reset`, removes its volume.

Because the path is part of the scope, moving or renaming a data directory starts the next run with empty data volumes (the old ones stay on the daemon until you move the directory back or remove them). Every scoped volume is labelled with the path of the data directory that created it, so you can list or clean up the ones a directory owns:

```sh
docker volume ls --filter label=fakecloud-data-path=/var/lib/fakecloud
docker volume rm $(docker volume ls -q --filter label=fakecloud-data-path=/var/lib/fakecloud)
```

An adopted pre-scoping volume (see below) keeps its old unlabelled name, so the label filter doesn't list it; remove it by name:

```sh
docker volume rm fakecloud-rds-data-123456789012-my-db
```

In memory mode (when the volumes are on: always for ElastiCache, opt-in for RDS and EC2) the volumes are scoped to the fakecloud process instead. ElastiCache volumes and containers are also keyed by account, so same-named caches in two accounts never share data. A clean shutdown removes them, and the next fakecloud start removes any left by a process that was killed.

Data directories written by builds before this scoping used unscoped volume names (`fakecloud-rds-data-<account>-<identifier>`, `fakecloud-elasticache-data-<id>`, `fakecloud-ec2-data-<account>-<instance-id>`). On the first start against such a data directory, every resource it already holds keeps the unscoped volume it was created with when that volume still exists (Docker cannot rename volumes, so it is reused in place rather than copied), and the choice is recorded in the persisted state. Deleting that resource removes the unscoped volume. Those builds named ElastiCache volumes by cache id alone, so two accounts with a same-named cache shared one volume; when an upgraded data directory holds such a cache in more than one account, none of them adopts the shared volume and each starts on its own account-scoped one (a warning names the old volume so you can recover its data by hand). Resources created afterwards always get scoped volumes, even if an unscoped volume with a matching name is lying around. If the Docker daemon can't list its volumes at startup, such a resource's container is not recreated on that start (rather than coming back on an empty volume); the next start that can list them binds and recovers it.

## Version compatibility

On startup fakecloud reads `<data-path>/fakecloud.version.toml`. The file records the on-disk format version and the fakecloud version that created the directory. If the format version doesn't match the running binary, startup fails with an actionable error that points at the file.

Except for the ARN partition migration, the regional state migration and the legacy CloudWatch Logs migration described below, there is no automatic migration. The intent is that you either keep using the binary that wrote the directory or start from an empty data path.

### ARN partition migration

fakecloud mints ARNs in the partition of their region: `arn:aws-cn:` for `cn-*` regions, `arn:aws-us-gov:` for GovCloud, and the `aws-iso*` partitions for the isolated regions. Older releases used `arn:aws:` everywhere, so a data directory written by a China, GovCloud or ISO-region server before that change holds `arn:aws:` ARNs.

The first time a newer binary opens such a directory it rewrites those ARNs once, before any service loads its state, and records `arn_partitions_migrated = true` in `fakecloud.version.toml` so it never runs again. Directories created by a current binary start with the marker set.

- A regional ARN whose region belongs to another partition (`arn:aws:kms:cn-north-1:...`) is rewritten to that partition (`arn:aws-cn:kms:cn-north-1:...`), whatever `--region` the server runs with.
- A region-less ARN (`arn:aws:iam::123456789012:role/app`, `arn:aws:s3:::bucket`) is rewritten to the partition of the server's `--region`, and only when that partition is not `aws`. AWS-managed policy ARNs (`arn:aws:iam::aws:policy/...`) keep the `aws` spelling, matching what fakecloud serves for them in every partition.
- The rewrite covers every service's state: resource fields, maps keyed by ARN (SNS topics, tag maps, ...), and ARNs inside stored documents such as IAM, bucket, queue and key policies, which are otherwise left byte-for-byte unchanged. S3 bucket configuration and object system metadata (such as the SSE-KMS key ARN) are covered too.
- Policy wildcards and variables move with the resources they match: `arn:aws:kms:*:123456789012:key/*` becomes `arn:aws-cn:kms:*:123456789012:key/*`, and `arn:aws:iam::${aws:PrincipalAccount}:role/app` becomes `arn:aws-cn:iam::${aws:PrincipalAccount}:role/app` on a China server.
- These payloads and customer-data fields are not rewritten: SQS message bodies and attributes (their MD5 digests would stop matching), ECR image manifests (addressed by digest), DynamoDB items, SSM parameter values, Secrets Manager secrets, SES email template content, SNS published messages, EventBridge published and archived events, CloudWatch dashboard bodies, S3 object keys, user metadata, object and multipart-upload tags and client-supplied object headers (`Content-Type`, `Content-Disposition`, ...), CloudWatch Logs events, S3 object bodies, and container data volumes. An item or object keyed by an ARN string stays reachable by the key your application wrote.

After the migration, responses return the partition-correct ARN for these resources, the same as for resources created afterwards, and ARN-keyed lookups (for example SNS `GetTopicAttributes`) take the new ARN.

### Regional state migration

Regional services keep a separate set of resources per account and region, so the same queue or stack name can exist in two regions at once. Older releases kept one set per account for some services, so a data directory written before a service was split by region holds all of that account's resources together.

On load, such a snapshot is split once: every resource moves to the region its ARN names, records that carry no ARN of their own follow the resource they belong to, and anything left goes to the server's `--region`. The snapshot is written back in the per-region format on the next change. Services migrated this way:

- **SQS** - each queue goes to the region of its queue ARN; message move tasks follow their source queue. Queue URLs (`<endpoint>/<account>/<name>`) carry no region, so they stay byte-identical, and the request region selects which region's queue a URL addresses.
- **CloudFormation** - each stack goes to the region of its stack ID; change sets, events, policies, exports and stack sets follow it.

## S3 object body handling

Object bodies are streamed straight to disk in persistent mode, not held in RAM. A bounded LRU cache (`--s3-cache-size`, default 256 MiB) keeps recently read bodies available for fast re-reads. Objects larger than `cache-size / 2` bypass the cache on both read and write, so a single large upload cannot evict the entire working set.

## Introspection buffers are not persisted

The `/_fakecloud/s3/notifications` buffer — and every other `/_fakecloud/*` introspection endpoint, including `/_fakecloud/ses/emails` and `/_fakecloud/ses/inbound-emails` — is intentionally not persisted. These exist so tests can assert which events fired during the current run, not as a long-term audit log.

## CloudWatch Logs event segments

CloudWatch Logs stores event bodies in append-only JSON Lines segments, rotating
at approximately 4 MiB per segment (a single event may take a segment over that
threshold). Small metadata lives in `logs/manifest.json`. A save appends only the
events written since the previous save, syncs the segments, and then atomically
commits their byte lengths in the manifest; existing event bytes are never
rewritten. After an interrupted write, uncommitted trailing bytes are ignored and
truncated on the next append. Missing or truncated committed data produces a load
error.

Retention deletes events, matching AWS. `PutRetentionPolicy` accepts only the AWS
values (1, 3, 5, 7, 14, 30, 60, 90, 120, 150, 180, 365, 400, 545, 731, 1096, 1827,
2192, 2557, 2922, 3288, 3653) and removes expired events from memory; `PutLogEvents`
rejects events older than the retention period (`expiredLogEventEndIndex`).
Retention is enforced on every Logs mutation and by a sweep every 60 seconds, so
idle groups expire too. Fully expired segments are deleted after the new manifest
commits. Mixed-age segments keep a durable expiration cutoff so removing or
extending a retention policy cannot resurrect previously deleted events; their
remaining disk space is reclaimed once every event in the segment expires. Groups
without a retention policy retain events indefinitely.

The previous `logs/snapshot.json` format is read automatically and migrated on
startup before serving requests. The legacy snapshot is removed only after the new
manifest is durable. The migration is one-way: an older fakecloud binary does not
read `logs/manifest.json` and would start with empty CloudWatch Logs state. Before
upgrading, back up the data directory while fakecloud is stopped, and restore that
backup if you need to downgrade. Do not edit or delete segment files manually.
