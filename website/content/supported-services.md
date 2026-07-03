+++
title = "AWS Service Coverage & API Conformance"
description = "fakecloud provides 100% API conformance across 4,220 operations. Explore our supported AWS services for local development."
template = "page.html"
+++

fakecloud provides 100% API conformance across 4,220 operations. Unlike mocks, fakecloud is built against official AWS Smithy models to ensure wire-protocol compatibility and deterministic behavior for local development.

## Coverage Summary
- **Total Services**: 54
- **Total Operations**: 4,220
- **Conformance Engine**: 140,394 Smithy-based test variants
- **Startup Time**: ~300ms

## Supported Services

### Compute & Containers
- **EC2**: 769 operations. The complete EC2 control plane — VPCs, subnets, security groups, route tables, gateways, instances, EBS, AMIs, the full 74-op Transit Gateway surface, Site-to-Site + Client VPN, IPAM, Verified Access, Network Insights, and Outpost / local-gateway networking. Instances run as real containers — Docker/Podman by default or native Kubernetes Pods (`FAKECLOUD_EC2_BACKEND=k8s`) — running user-data at boot, with start/stop/reboot/terminate mapped to the container lifecycle and `GetConsoleOutput` returning the container log; degrades to metadata-only when no container runtime is present.
- **Lambda**: 70 operations. Full execution environment in real Docker containers across 23 runtimes, cross-service triggers (S3, SNS, SQS, EventBridge).
- **ECR**: 58 operations. Full OCI v2 Distribution protocol support for `docker push` and `docker pull`.
- **ECS**: 77 operations. Real Fargate-style task execution via Docker, services with rolling deployments, ECS Exec.

### Storage & Databases
- **S3**: 107 operations. Bucket lifecycle, Object tagging, Multipart uploads, real `SelectObjectContent` EventStream.
- **DynamoDB**: 57 operations. TTL, GSI/LSI, and DynamoDB Streams.
- **RDS**: 163 operations. Real Postgres, MySQL, MariaDB, Oracle, SQL Server, and Db2 via Docker.
- **RDS Data API**: 6 operations. Real SQL (`ExecuteStatement`/`BatchExecuteStatement`) on the backing Postgres/MySQL container with typed parameters and results, plus transactions (`BeginTransaction`/`CommitTransaction`/`RollbackTransaction`).
- **Redshift**: 141 operations. Full control plane — clusters, snapshots, parameter/subnet/security groups, snapshot schedules and copy grants, endpoint access, per-cluster logging, cross-region snapshot-copy config, and tagging. No SQL data plane (that is the separate `redshift-data` API).
- **Aurora DSQL**: 16 operations. Serverless distributed PostgreSQL control plane. Cluster lifecycle (`CreateCluster`/`GetCluster`/`UpdateCluster`/`DeleteCluster`/`ListClusters`) with async `CREATING`->`ACTIVE` transitions, cluster resource policies, change streams to Kinesis (`CreateStream`/`GetStream`/`DeleteStream`/`ListStreams`), `GetVpcEndpointServiceName`, and tagging. Data plane (reachable container + IAM-token auth) is a follow-up.
- **ElastiCache**: 75 operations. Real Redis, Valkey, and Memcached via Docker.
- **MemoryDB**: 45 operations. Full control plane for Redis/Valkey clusters, shards, ACLs, users, parameter and subnet groups, snapshots, and multi-region clusters, with persistence. Redis/Valkey data-plane container backing is a follow-up.
- **EKS**: 65 operations (complete). Full Elastic Kubernetes Service control plane: clusters (incl. connected-cluster register/deregister), managed node groups, Fargate profiles, add-ons, access entries + access policies, OIDC identity-provider configs, pod-identity associations, upgrade insights, capabilities, encryption config, and EKS Anywhere subscriptions (create/describe/list/delete, config + version updates with tracking, cluster-version/add-on/access-policy catalogues, tagging), with persistence. Resources transition `CREATING` -> `ACTIVE` on describe. No real Kubernetes control-plane endpoint (models the AWS management API, not `kubectl`).

### AI & Machine Learning
- **Bedrock**: 214 operations across 4 APIs (Bedrock 101, Bedrock Runtime 10, Bedrock Agent 72, Bedrock Agent Runtime 31). Guardrails, Model Customization, Provisioned Throughput, Agents, Knowledge Bases.
- **Bedrock Runtime**: Deterministic `InvokeModel`, `InvokeModelWithResponseStream`, and `Converse` APIs (echo / configurable-response mode; no real inference).

### Messaging & Integration
- **SQS**: 23 operations. Standard and FIFO queues, Dead Letter Queues (DLQ).
- **SNS**: 42 operations. Topic management and fan-out to SQS/Lambda.
- **EventBridge**: 57 operations. Rules, Targets; EventBridge Scheduler (12 operations) and EventBridge Pipes (10 operations) are separate services.
- **EventBridge Pipes**: 10 operations. Point-to-point source -> filter -> Lambda enrichment -> target integrations with per-target InputTemplate transforms, driven by a real background runner.
- **Cloud Map**: 30 operations (complete). Full AWS Cloud Map (`servicediscovery`) control plane + discovery API: HTTP/public-DNS/private-DNS namespaces, services (DnsConfig/HealthCheck + attributes), instance register/deregister/get/list + health status, `DiscoverInstances`/`DiscoverInstancesRevision` lookup, and tagging — driven by the async operation model (mutations return an `OperationId` that settles `SUCCESS` on `GetOperation`); persisted.
- **Account Management**: 15 operations (complete). Full AWS Account control plane: alternate contacts (BILLING/OPERATIONS/SECURITY), primary contact information, account information + name, GovCloud account pairing, primary-email management (start/accept OTP flow), and Region opt-in control (ListRegions, GetRegionOptStatus, Enable/DisableRegion with `ENABLING` -> `ENABLED` settle-on-read over the real opt-in-region catalogue). Honors the optional `AccountId` member so an organization's management account can act on a member; persisted.
- **IAM Identity Center Identity Store**: 19 operations (complete). Full `identitystore` directory control plane: users, groups, and group memberships (create/describe/update/delete/list), the attribute-lookup helpers `GetUserId`/`GetGroupId`/`GetGroupMembershipId` (by `UniqueAttribute`), and `IsMemberInGroups`. Nested SCIM attribute bags round-trip verbatim; `@length`/`@range` constraints enforced. Account-partitioned and persisted.
- **IAM Identity Center SSO Admin**: 79 operations (complete). Full `sso-admin` control plane: IAM Identity Center instances and regions, permission sets with inline/managed/customer-managed/boundary policies, account assignments and permission-set provisioning (async status settle), applications with assignments/access-scopes/authentication-methods/grants/session config, the application-provider catalogue, trusted token issuers, access-control attribute configuration, and tagging. Nested config objects round-trip verbatim; `@length`/`@range` constraints enforced. Account-partitioned and persisted.
- **Verified Permissions**: 34 operations (complete). Full `verifiedpermissions` Cedar authorization control plane: policy stores, Cedar schemas (`PutSchema`/`GetSchema`), static and template-linked policies, policy templates, identity sources (Cognito/OIDC), policy-store aliases, and tagging. `IsAuthorized`/`IsAuthorizedWithToken`/`BatchIsAuthorized`/`BatchIsAuthorizedWithToken` compute real Cedar decisions via the official `cedar-policy` engine — policies compile into a Cedar `PolicySet` and the request is evaluated to an `ALLOW`/`DENY` with determining policies and errors. `*WithToken` resolves the principal from the JWT `sub` claim. `@length`/`@range`/enum constraints enforced. Account-partitioned and persisted.

### Security & Management
- **IAM**: 176 operations. Policy evaluation including permission boundaries, session policies, ABAC, NotPrincipal, and KMS key policies.
- **STS**: 11 operations. Local token generation and session management.
- **SSM**: 146 operations. Parameter Store; Secrets Manager (23 operations) is a separate service.

## Technical Conformance Data
fakecloud is validated against the same Smithy models used by the official AWS SDKs. This ensures that every request and response matches the expected wire format exactly, eliminating 'works on my machine' bugs caused by shallow mocks.

[View the full machine-readable API manifest on GitHub](https://github.com/faiscadev/fakecloud)
