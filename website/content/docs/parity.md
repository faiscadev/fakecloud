+++
title = "Parity matrix"
description = "Service-by-service behavior parity: what is real, what is synthesized, and what is not yet implemented."
weight = 1
+++

fakecloud implements **41 AWS services** with **3,717 operations**. **124,255/124,255 generated Smithy conformance variants pass** on every commit — true 100% across every implemented service, no flake margin and no skipped services. Conformance checks request/response shapes, field names, and error codes against [AWS's own Smithy models](https://github.com/faiscadev/fakecloud/blob/main/conformance-baseline.json). Behavior parity varies by service — some run real infrastructure (Postgres, Redis, Docker containers), some run a real control plane but return synthesized data for complex queries, and a few have control-plane-only coverage with no data-plane enforcement.

| Service | Ops | Protocol | Control plane | Data plane | Known limitations |
| --- | --- | --- | --- | --- | --- |
| [S3](@/docs/services/s3.md) | 107 | REST-XML | Full | Full | `SelectObjectContent` returns real EventStream chunks. `WriteGetObjectResponse` stores body + metadata. Access points include data-plane routing. `PublicAccessBlock.IgnorePublicAcls` is enforced on `GetObject`. Object Lock compliance mode is enforced on single-object delete but not yet on batch delete. Multi-region access points are control-plane only. |
| [SQS](@/docs/services/sqs.md) | 23 | JSON 1.1 (Query) | Full | Full | — |
| [SNS](@/docs/services/sns.md) | 42 | JSON 1.1 (Query) | Full | Full | Email subscriptions deliver via SMTP relay when `FAKECLOUD_SMTP_RELAY_*` env is configured; otherwise they land in the introspection ledger. |
| [EventBridge](@/docs/services/eventbridge.md) | 57 | JSON 1.1 | Full | Full | — |
| [EventBridge Scheduler](@/docs/services/scheduler.md) | 12 | JSON 1.1 | Full | Full | — |
| [Lambda](@/docs/services/lambda.md) | 70 | REST-JSON | Full | Full | `UpdateFunctionCode` fetches real bytes from S3 and recomputes `CodeSha256`. Reserved concurrency is recorded but not yet enforced at invoke time. Provisioned concurrency is a roadmap item. |
| [DynamoDB](@/docs/services/dynamodb.md) | 57 | JSON 1.1 | Full | Full | — |
| [IAM](@/docs/services/iam.md) | 176 | JSON 1.1 (Query) | Full | Full | — |
| [STS](@/docs/services/sts.md) | 11 | JSON 1.1 (Query) | Full | Full | — |
| [SSM](@/docs/services/ssm.md) | 146 | JSON 1.1 | Full | Partial | `StartSession` returns the model's `TargetNotConnected` and `ResumeSession` returns `DoesNotExistException` with a documentation pointer rather than opening a real websocket. Session Manager data plane is not implemented. |
| [Secrets Manager](@/docs/services/secretsmanager.md) | 23 | JSON 1.1 | Full | Full | — |
| [CloudWatch Logs](@/docs/services/logs.md) | 113 | JSON 1.1 | Full | Full | `StartLiveTail` returns streamed results with real `GetLogObject` pointer resolution. `GetLogFields` persists and aggregates JSON keys observed across the source's events. Delivery configuration persists with standard AWS templates. Log event export to S3 and Firehose is real. Metric filters extract metrics from ingested logs. |
| [KMS](@/docs/services/kms.md) | 53 | JSON 1.1 | Full | Full | Real ECDSA P-256, P-384, and P-521 signing. |
| [CloudFormation](@/docs/services/cloudformation.md) | 90 | JSON 1.1 (Query) | Full | Full | Custom resources execute real Lambda-backed custom resource providers. |
| [SES](@/docs/services/ses.md) | 111 | JSON 1.1 | Full | Full | v2 sending + v1 inbound receipt rules are both real. DKIM signing is real. `GetMessageInsights` returns real delivery tracking data. Bounce simulator addresses are available for testing. SMTP credential issuance is implemented via IAM service-specific credentials, and an opt-in SMTP submission listener (`FAKECLOUD_SES_SMTP_PORT`) accepts mail authenticated with those credentials. Outbound SMTP relay is supported when `FAKECLOUD_SMTP_RELAY_*` env is configured. |
| [Cognito User Pools](@/docs/services/cognito.md) | 126 | JSON 1.1 | Full | Full | Real RSA-2048 RS256 JWT signing. JWKS + OIDC discovery endpoints serve real JWKs. `/oauth2/token`, `/oauth2/authorize`, `/oauth2/userInfo`, and `/oauth2/revoke` are all implemented. Refresh token rotation is supported when enabled. `PreTokenGeneration` trigger invokes the configured Lambda and merges claims. `CompromisedCredentialsRiskConfiguration` is enforced. WebAuthn `packed` attestation format is verified. `GetSigningCertificate` returns real X.509 certificates. |
| [Cognito Identity](@/docs/services/cognito.md) | 23 | JSON 1.1 | Full | Full | Identity pools, federated identities, developer identities, and real STS-style credential issuance are implemented. |
| [Kinesis](@/docs/services/kinesis.md) | 39 | JSON 1.1 | Full | Full | — |
| [RDS](@/docs/services/rds.md) | 163 | JSON 1.1 (Query) | Full | Full | Real Postgres, MySQL, MariaDB, Oracle, SQL Server, and Db2 via Docker. PostgreSQL `aws_lambda` + `aws_s3` extensions and Aurora-compatible MySQL/MariaDB `mysql.lambda_async`/`mysql.lambda_sync` invoke fakecloud Lambda and import/export S3 objects from SQL. |
| [ElastiCache](@/docs/services/elasticache.md) | 75 | JSON 1.1 (Query) | Full | Full | Real Redis, Valkey, and Memcached via Docker. `RestoreFromSnapshot` uses real RDB dump format. ACL `SETUSER` and `CONFIG SET` commands are supported. |
| [Step Functions](@/docs/services/stepfunctions.md) | 37 | JSON 1.1 | Full | Full | Full ASL interpreter with `.sync` wait patterns, `waitForTaskToken`, and generic `aws-sdk:*` integrations. |
| [API Gateway v1](@/docs/services/apigateway.md) | 124 | REST-JSON | Full | Full | Authorizer enforcement (TOKEN/REQUEST/COGNITO_USER_POOLS), request validators, VTL templates (MOCK and HTTP integrations), AWS direct service integrations, VPC_LINK integrations, and custom domain name + base path mapping routing are all implemented in the HTTP data plane. |
| [API Gateway v2](@/docs/services/apigatewayv2.md) | 103 | JSON 1.1 | Full | Full | WebSocket support (`$connect`/`$disconnect`/`$default`), JWT and Lambda authorizer enforcement, AWS service integrations, access log delivery to CloudWatch Logs, stage variables, and custom domain routing are all implemented in the HTTP data plane. |
| [Bedrock](@/docs/services/bedrock.md) | 103 | JSON 1.1 | Full | Partial | Control plane (guardrails, custom models, jobs, inference profiles, account data retention) is fully implemented. Runtime (`InvokeModel`, `Converse`, streaming) runs in echo / configurable-response mode with real token counting and fault injection, not real model inference. |
| [Bedrock Runtime](@/docs/services/bedrock.md) | 10 | JSON 1.1 | Full | Partial | Same as Bedrock runtime notes above. |
| [Bedrock Agent](@/docs/services/bedrock-agent.md) | 72 | JSON 1.1 | Full | Partial | Full Agents control plane: agents, agent versions/aliases, action groups, knowledge bases, data sources, ingestion jobs, prompt management, flows, and flow aliases/versions. Knowledge-base ingestion and retrieval are shape-correct synthetic — the embedding/foundation model itself is out of scope, see the "never implement" list below. |
| [Bedrock Agent Runtime](@/docs/services/bedrock-agent-runtime.md) | 31 | JSON 1.1 | Full | Partial | `InvokeAgent`, `Retrieve`, `RetrieveAndGenerate`, `InvokeFlow`, and the streaming variants are wired end-to-end with shape-correct synthetic chunks. No real foundation-model inference — see Bedrock runtime caveat above. |
| [ECR](@/docs/services/ecr.md) | 58 | JSON 1.1 | Full | Full | OCI v2 push/pull is real. Lifecycle policy evaluation, image scanning, pull-through cache, registry templates, and cosign signature verification are all implemented. |
| [ECS](@/docs/services/ecs.md) | 77 | JSON 1.1 (Query) | Full | Full | Real Fargate-style task execution via Docker, services with rolling deployments + blue/green lifecycle-hook pauses (`ContinueServiceDeployment`), task sets, container instances, capacity providers, and ECS Exec. Multi-container tasks, volume mounts, health checks, and `dependsOn` ordering are all implemented. |
| [ELBv2](@/docs/services/elbv2.md) | 51 | JSON 1.1 (Query) | Full | Partial | Control plane (ALB/NLB/GWLB CRUD, target groups, listeners, rules, mTLS trust stores) is fully implemented. An in-process HTTP data plane for ALBs handles rule matching, forwarding, fixed-response, redirect, and sticky sessions. WAFv2 inspection is wired into the ALB data plane. NLB and GWLB data planes are not implemented. |
| [CloudFront](@/docs/services/cloudfront.md) | 147 | REST-XML | Full | Partial | Control plane is fully implemented (distributions, policies, functions, key value stores, etc.). CloudFront Functions can be tested via `TestFunction`. There is no actual CDN edge network — distributions do not serve traffic from edge locations. |
| [Route 53](@/docs/services/route53.md) | 71 | REST-XML | Full | Partial | Control plane is fully implemented (hosted zones, RRsets, health checks, DNSSEC, traffic policies, etc.). `TestDNSAnswer` resolves routing policies and alias targets using fakecloud state. A real DNS server on UDP/TCP 53 is not implemented by default. |
| [WAFv2](@/docs/services/wafv2.md) | 55 | JSON 1.1 | Full | Control-only | Control plane is fully implemented (WebACLs, rule groups, IP sets, regex patterns, API keys, managed rules, logging). WAFv2 inspection is wired into the ELBv2 ALB data plane and API Gateway v1+v2 data planes, but CloudFront and AppSync associations are stored only. Rate-based rules and CAPTCHA/Challenge actions are not enforced against real traffic. |
| [Application Auto Scaling](@/docs/services/application-autoscaling.md) | 14 | JSON 1.1 | Full | Partial | Control plane is fully implemented (scalable targets, step/target-tracking/predictive policies, scheduled actions). Scaling actions fire and update the target service (`UpdateService` for ECS, `UpdateTable` for DynamoDB, etc.), but the actual metric-driven alarm loop is synthesized. |
| [Athena](@/docs/services/athena.md) | 70 | JSON 1.1 | Full | Control-only | Control plane is fully implemented. `StartQueryExecution` synthesizes a `SUCCEEDED` execution with a single-row `["1"]` result. fakecloud is not a SQL engine. |
| [ACM](@/docs/services/acm.md) | 17 | JSON 1.1 | Full | Partial | Control plane is fully implemented. Certificates are self-signed (`rcgen`) or imported PEM. DNS validation is auto-promoted after a configurable delay; there is no real CA or DNS validation pipeline. EMAIL validation stays `PENDING_VALIDATION` until approved via the admin endpoint. |
| [CloudWatch (Metrics & Alarms)](@/docs/services/cloudwatch.md) | 49 | JSON 1.1 (Query) | Full | Partial | Metrics, statistics, dashboards, metric/composite alarms, anomaly detectors, insight rules, metric streams, alarm mute rules, contributor insights, OTel enrichment, dataset KMS key management, and tagging are all implemented with persisted in-memory state. Alarm threshold transitions trigger SNS/AppAS/EC2 actions. `GetMetricWidgetImage` returns a deterministic image blob. Metrics do not persist across server restarts. |
| [Firehose](@/docs/services/firehose.md) | 12 | JSON 1.1 | Full | Full | Real S3 destination delivery with buffering hints honored. Other destinations (Redshift, OpenSearch, Splunk, HTTP endpoint) round-trip configuration. Server-side encryption (`Start`/`StopDeliveryStreamEncryption`) persists and surfaces in `DescribeDeliveryStream`. |
| [Glue](@/docs/services/glue.md) | 267 | JSON 1.1 | Full | Partial | Full control plane: Data Catalog (databases, tables, partitions with `GetPartitions` `Expression` pruning), jobs, crawlers, classifiers, connections, triggers, workflows, blueprints, dev endpoints, schema registry, interactive sessions, ML transforms, data quality, user-defined functions, usage profiles, column statistics, and tagging. Status transitions are real (crawler `READY`↔`RUNNING`, trigger/workflow/run lifecycles). Job/crawler/Spark *execution* itself is synthesized — fakecloud is not a Spark engine. |
| [Organizations](@/docs/services/organizations.md) | 63 | JSON 1.1 | Full | Full | Full org tree (roots, OUs, accounts), policies with SCP enforcement, handshakes, delegated administrators, service access, tagging, and a resource policy. Billing responsibility transfers ride handshake-backed records. `CreateAccount` transitions `IN_PROGRESS` -> `SUCCEEDED` after a short synthetic delay. |
| [EC2](@/docs/services/ec2.md) | 769 | ec2Query | Full | Partial | Full 769-op control plane: VPCs, subnets, security groups, route tables, gateways, ENIs, instances, EBS volumes/snapshots, AMIs (+ watermarks), network ACLs, VPC peering/endpoints, flow logs, launch templates, spot/fleet, capacity/reserved/dedicated hosts, transit gateways (+ multicast/peering/metering), VPN + Client VPN, IPAM, Verified Access, Network Insights, Outpost/local-gateway/CoIP, and Instance Connect. Instance control plane is metadata-faithful; real Docker-backed instance execution is a roadmap follow-up. A few model ops absent from the vendored SDK are validated via raw ec2Query. |

## Reading the matrix

* **Control plane** — the APIs that create, configure, and manage resources (e.g., `CreateBucket`, `PutRolePolicy`, `CreateFunction`). fakecloud implements 100% of the control plane for every service listed above.
* **Data plane** — the APIs that process, store, or move actual data (e.g., `GetObject`, `InvokeModel`, `AssumeRole`, `SendMessage`). A service marked **Full** has a real data plane. A service marked **Partial** has some real data-plane operations and some synthesized / stubbed ones. A service marked **Control-only** has no data-plane implementation.
* **Known limitations** — specific gaps that are intentionally synthesized or not yet implemented. These are usually outside the Smithy conformance boundary (the shape is correct, but the behavior is simplified). If a limitation is important for your use case, open an issue or check the [service-specific docs](@/docs/services/_index.md) for workarounds.

## What "100% conformance" means

fakecloud validates every implemented operation against AWS's own Smithy models using a generated test suite with **124,255 variants**, **all of which pass** on every commit. This guarantees that field names, types, required/optional flags, error codes, and HTTP signatures are identical to AWS. It does *not* guarantee that every operation behaves exactly like AWS in all edge cases — that is what the **Data plane** and **Known limitations** columns describe.

If you need a service that is not listed above, the issue tracker and [roadmap](https://github.com/faiscadev/fakecloud#roadmap) are the best places to request it.

## What fakecloud will never implement

A small set of features depend on real AWS infrastructure, vendor-internal data, or external networks that a local emulator fundamentally cannot replicate. fakecloud is committed to *not* faking these — we surface a clearly synthesized stand-in instead so tests are not silently wrong.

| Area | Why we cannot implement it |
| --- | --- |
| **Bedrock real model inference** (`InvokeModel`, `Converse`, `ConverseStream`) | Foundation model weights are vendor-proprietary and require real GPU + provider credentials. fakecloud Runtime returns an echo / configurable response with real token counting and fault injection. |
| **Bedrock Agent semantic responses** (`InvokeAgent`, `RetrieveAndGenerate`) | Same — depends on real foundation models. Agents return shape-correct synthetic chunks. |
| **ACM real certificate authority** | Browser-trusted certificates can only be issued by CAs in the OS trust store. fakecloud certificates are self-signed or imported PEM. Trust them locally for testing only. |
| **WAFv2 AWS Managed Rule Group content** | The actual rules inside `AWSManagedRulesCommonRuleSet`, `AWSManagedRulesAnonymousIpList`, etc. are proprietary AWS data. fakecloud accepts the rule-group references and runs structural evaluation, but the rule bodies themselves are not the real AWS content. |
| **Real public DNS resolution** (Route 53) | Authoritative public DNS requires global anycast network presence. fakecloud's `TestDNSAnswer` resolves against local state. A real DNS server on UDP/TCP 53 can be opted into for self-contained tests but it is not Internet-facing. |
| **CloudFront edge network** | The CDN is the product. Distributions in fakecloud round-trip configuration; there is no global edge serving traffic. Use `TestFunction` to exercise CloudFront Functions. |
| **Real outbound email and SMS** (SES, SNS) | Local emulators must not actually send email to inboxes or SMS to phone numbers — that crosses into spam / abuse territory. SES and SNS deliver messages into fakecloud's introspection ledger; an opt-in SMTP submission listener (`FAKECLOUD_SES_SMTP_PORT`) accepts inbound connections but does not relay outbound to the public Internet. |
| **EBS / EFS block storage** | Kernel-level storage emulation is out of scope. EFS volumes attached to ECS tasks are mounted as docker volumes with the same logical lifecycle, not real NFS. |
| **CloudFront streaming distributions (RTMP)** | Service was deprecated by AWS in 2020 and is no longer accepted by their API for new distributions. fakecloud round-trips configuration only and treats RTMP as wontfix. |

If a feature in this list blocks your use case, please open an issue describing what you are trying to test — there is often a smaller, targetable surface that fakecloud can implement instead.

## Significant projects on the roadmap

These are gaps that fakecloud *can* implement but represent significant engineering projects rather than incremental fixes. They are tracked in the public roadmap and are good places to contribute.

| Project | Scope |
| --- | --- |
| **Athena full SQL engine** | DataFusion-backed parser + executor for `SELECT` with `WHERE`, `GROUP BY`, aggregates, joins, subqueries, window functions, plus Parquet and JSON SerDes against S3 sources. |
| **WAFv2 ManagedRuleGroup framework** | Rule expansion engine + bundled OWASP-style stand-in rules + per-rule evaluation against real request headers/bodies. The framework that runs the rules is in scope; the exact AWS rule contents are not (see above). |
| **Cognito WebAuthn full attestation verification** | CTAP CBOR parser plus signature verification chains for the four common attestation formats (`packed`, `fido-u2f`, `android-key`, `tpm`). The `packed` format alone is a smaller targetable batch. |
| **ECR cross-registry image replication** | Real OCI v2 distribution copy + cross-account auth + region routing when replication rules fire on `PutImage`. |
| **Glue full Job runner** | Spark-style execution with partition-aware reads + JDBC connectors. The Glue Job control plane (`CreateJob`, `GetJob`, etc.) ships independently. |
| **API Gateway v1 full VTL evaluator** | `$util.*` functions, loops, conditionals, escape helpers, full Velocity Template Language coverage in integration request/response templates. |
| **CloudFront full edge function pipeline** | Origin shield, cache key transforms, Lambda@Edge integration. The edge network itself is out of scope; the function pipeline is on the roadmap. |
| **CloudWatch Metrics persistence layer** | Snapshot store integration so metrics, alarms, and dashboards survive server restarts. |
| **Bedrock Knowledge Base ingestion lifecycle** | Document chunking + retrieval pipeline. The embedding model itself is out of scope; the framework around it is on the roadmap. |

If you want to take one of these on, please open an issue first so we can scope it together.

For a flat listing of every AWS operation grouped by service (not implementation status — that's this page), see the [AWS operations index](@/docs/operations/_index.md).
