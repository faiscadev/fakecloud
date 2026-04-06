<p align="center">
  <strong>FakeCloud</strong><br>
  <em>Local AWS cloud emulator. Free forever.</em>
</p>

<p align="center">
  <a href="https://github.com/faiscadev/fakecloud/actions"><img src="https://img.shields.io/github/actions/workflow/status/faiscadev/fakecloud/ci.yml?branch=main&label=CI" alt="CI"></a>
  <a href="https://github.com/faiscadev/fakecloud/blob/main/LICENSE"><img src="https://img.shields.io/badge/license-AGPL--3.0-blue" alt="License"></a>
  <a href="https://github.com/faiscadev/fakecloud/pkgs/container/fakecloud"><img src="https://img.shields.io/badge/ghcr.io-fakecloud-blue?logo=docker" alt="GHCR"></a>
  <a href="https://crates.io/crates/fakecloud"><img src="https://img.shields.io/crates/v/fakecloud" alt="crates.io"></a>
  <a href="https://fakecloud.dev"><img src="https://img.shields.io/badge/docs-fakecloud.dev-green" alt="Docs"></a>
</p>

---

FakeCloud is an open-source local AWS cloud emulator. It runs on a single port
(`4566`), requires no account or auth token, and aims to faithfully replicate
AWS service behavior for local development and testing.

Part of the [faisca project family](https://github.com/faiscadev).

## Why FakeCloud?

In March 2026, LocalStack replaced its open-source Community Edition with a
proprietary image that requires an account and auth token. Several open-source
alternatives have emerged since then. Here's how they compare:

### Comparison

| Feature | FakeCloud | LocalStack | [Floci](https://github.com/hectorvent/floci) | [MiniStack](https://github.com/Nahuel990/ministack) |
|---|---|---|---|---|
| License | AGPL-3.0 | Proprietary | MIT | MIT |
| Language | Rust | Python | Java (Quarkus Native) | Python |
| Auth required | No | Yes (account + token) | No | No |
| Commercial use | Free | Paid plans only | Free | Free |
| AWS services | 12 | 80+ | 25 | 38 |
| Cross-service delivery | Yes | Yes | Yes | Yes |
| Scheduled rules fire | Yes | Yes | -- | -- |

## Quick Start

### From source

```sh
# Requires Rust 1.85+
git clone https://github.com/faiscadev/fakecloud.git
cd fakecloud
cargo run --release --bin fakecloud
```

### Docker

```sh
docker run --rm -p 4566:4566 ghcr.io/faiscadev/fakecloud
```

### Docker Compose

```yaml
# docker-compose.yml
services:
  fakecloud:
    image: ghcr.io/faiscadev/fakecloud
    ports:
      - "4566:4566"
    environment:
      FAKECLOUD_LOG: info
```

```sh
docker compose up
```

FakeCloud is now listening at `http://localhost:4566`.

## Supported Services

### SQS (20 actions)

CreateQueue, DeleteQueue, ListQueues, GetQueueUrl, GetQueueAttributes,
SetQueueAttributes, SendMessage, SendMessageBatch, ReceiveMessage,
DeleteMessage, DeleteMessageBatch, PurgeQueue, ChangeMessageVisibility,
ChangeMessageVisibilityBatch, ListQueueTags, TagQueue, UntagQueue,
AddPermission, RemovePermission, ListDeadLetterSourceQueues

Key features: real MD5 hashing, long polling (WaitTimeSeconds), FIFO queues
with message group ordering and content-based deduplication, dead-letter queues,
message attributes with MD5 computation, batch operations, system attribute
filtering.

### SNS (34 actions)

**Topics:** CreateTopic, DeleteTopic, ListTopics, GetTopicAttributes,
SetTopicAttributes

**Subscriptions:** Subscribe, ConfirmSubscription, Unsubscribe,
ListSubscriptions, ListSubscriptionsByTopic, GetSubscriptionAttributes,
SetSubscriptionAttributes

**Publishing:** Publish, PublishBatch

**Tags & Permissions:** TagResource, UntagResource, ListTagsForResource,
AddPermission, RemovePermission

**Platform Applications:** CreatePlatformApplication, DeletePlatformApplication,
GetPlatformApplicationAttributes, SetPlatformApplicationAttributes,
ListPlatformApplications

**Platform Endpoints:** CreatePlatformEndpoint, DeleteEndpoint,
GetEndpointAttributes, SetEndpointAttributes, ListEndpointsByPlatformApplication

**SMS:** SetSMSAttributes, GetSMSAttributes, CheckIfPhoneNumberIsOptedOut,
ListPhoneNumbersOptedOut, OptInPhoneNumber

Key features: SQS fan-out delivery, HTTP/HTTPS endpoint delivery, subscription
filter policies (exact match, prefix, anything-but, numeric, exists), platform
application and endpoint management, SMS attributes.

### EventBridge (41 actions)

**Event Buses:** CreateEventBus, DeleteEventBus, ListEventBuses,
DescribeEventBus

**Rules:** PutRule, DeleteRule, ListRules, DescribeRule, EnableRule, DisableRule,
ListRuleNamesByTarget

**Targets:** PutTargets, RemoveTargets, ListTargetsByRule

**Events:** PutEvents

**Permissions:** PutPermission, RemovePermission

**Tags:** TagResource, UntagResource, ListTagsForResource

**Archives:** CreateArchive, DescribeArchive, ListArchives, UpdateArchive,
DeleteArchive

**Connections:** CreateConnection, DescribeConnection, ListConnections,
UpdateConnection, DeleteConnection

**API Destinations:** CreateApiDestination, DescribeApiDestination,
ListApiDestinations, UpdateApiDestination, DeleteApiDestination

**Replays:** StartReplay, DescribeReplay, ListReplays, CancelReplay

**Partner Event Sources:** CreatePartnerEventSource, DescribePartnerEventSource

Key features: pattern-based rules (nested fields, numeric comparisons, prefix,
exists, anything-but), scheduled rules (rate and cron expressions) that actually
fire on a background timer, targets deliver to SNS topics and SQS queues,
archives with event replay, connections and API destinations, partner event
sources.

### IAM / STS (135 actions)

**IAM Users:** CreateUser, GetUser, DeleteUser, ListUsers, UpdateUser, TagUser,
UntagUser, ListUserTags, CreateAccessKey, DeleteAccessKey, ListAccessKeys,
UpdateAccessKey, GetAccessKeyLastUsed, CreateLoginProfile, GetLoginProfile,
UpdateLoginProfile, DeleteLoginProfile, AttachUserPolicy, DetachUserPolicy,
ListAttachedUserPolicies, PutUserPolicy, GetUserPolicy, DeleteUserPolicy,
ListUserPolicies

**IAM Roles:** CreateRole, GetRole, DeleteRole, ListRoles, UpdateRole,
UpdateRoleDescription, UpdateAssumeRolePolicy, TagRole, UntagRole, ListRoleTags,
PutRolePermissionsBoundary, DeleteRolePermissionsBoundary, AttachRolePolicy,
DetachRolePolicy, ListAttachedRolePolicies, PutRolePolicy, GetRolePolicy,
DeleteRolePolicy, ListRolePolicies, CreateServiceLinkedRole,
DeleteServiceLinkedRole, GetServiceLinkedRoleDeletionStatus

**IAM Groups:** CreateGroup, GetGroup, DeleteGroup, ListGroups, UpdateGroup,
AddUserToGroup, RemoveUserFromGroup, ListGroupsForUser, PutGroupPolicy,
GetGroupPolicy, DeleteGroupPolicy, ListGroupPolicies, AttachGroupPolicy,
DetachGroupPolicy, ListAttachedGroupPolicies

**IAM Policies:** CreatePolicy, GetPolicy, DeletePolicy, ListPolicies, TagPolicy,
UntagPolicy, ListPolicyTags, CreatePolicyVersion, GetPolicyVersion,
ListPolicyVersions, DeletePolicyVersion, SetDefaultPolicyVersion,
ListEntitiesForPolicy

**Instance Profiles:** CreateInstanceProfile, GetInstanceProfile,
DeleteInstanceProfile, ListInstanceProfiles, AddRoleToInstanceProfile,
RemoveRoleFromInstanceProfile, ListInstanceProfilesForRole, TagInstanceProfile,
UntagInstanceProfile, ListInstanceProfileTags

**Identity Providers:** CreateSAMLProvider, GetSAMLProvider, DeleteSAMLProvider,
ListSAMLProviders, UpdateSAMLProvider, CreateOpenIDConnectProvider,
GetOpenIDConnectProvider, DeleteOpenIDConnectProvider, ListOpenIDConnectProviders,
UpdateOpenIDConnectProviderThumbprint, AddClientIDToOpenIDConnectProvider,
RemoveClientIDFromOpenIDConnectProvider, TagOpenIDConnectProvider,
UntagOpenIDConnectProvider, ListOpenIDConnectProviderTags

**Certificates:** UploadServerCertificate, GetServerCertificate,
DeleteServerCertificate, ListServerCertificates, UploadSigningCertificate,
ListSigningCertificates, UpdateSigningCertificate, DeleteSigningCertificate

**SSH Keys:** UploadSSHPublicKey, GetSSHPublicKey, ListSSHPublicKeys,
UpdateSSHPublicKey, DeleteSSHPublicKey

**MFA:** CreateVirtualMFADevice, DeleteVirtualMFADevice, ListVirtualMFADevices,
EnableMFADevice, DeactivateMFADevice, ListMFADevices

**Account:** GetAccountSummary, GetAccountAuthorizationDetails, CreateAccountAlias,
DeleteAccountAlias, ListAccountAliases, UpdateAccountPasswordPolicy,
GetAccountPasswordPolicy, DeleteAccountPasswordPolicy, GenerateCredentialReport,
GetCredentialReport

**STS:** GetCallerIdentity, AssumeRole, AssumeRoleWithWebIdentity,
AssumeRoleWithSAML, GetSessionToken, GetFederationToken, GetAccessKeyInfo

### SSM (46 actions)

**Parameters:** PutParameter, GetParameter, GetParameters, GetParametersByPath,
DeleteParameter, DeleteParameters, DescribeParameters, GetParameterHistory,
LabelParameterVersion, UnlabelParameterVersion

**Tags:** AddTagsToResource, RemoveTagsFromResource, ListTagsForResource

**Documents:** CreateDocument, GetDocument, DeleteDocument, UpdateDocument,
DescribeDocument, UpdateDocumentDefaultVersion, ListDocuments,
DescribeDocumentPermission, ModifyDocumentPermission

**Commands:** SendCommand, ListCommands, GetCommandInvocation,
ListCommandInvocations, CancelCommand

**Maintenance Windows:** CreateMaintenanceWindow, DescribeMaintenanceWindows,
GetMaintenanceWindow, DeleteMaintenanceWindow, UpdateMaintenanceWindow,
RegisterTargetWithMaintenanceWindow, DeregisterTargetFromMaintenanceWindow,
DescribeMaintenanceWindowTargets, RegisterTaskWithMaintenanceWindow,
DeregisterTaskFromMaintenanceWindow, DescribeMaintenanceWindowTasks

**Patch Baselines:** CreatePatchBaseline, DeletePatchBaseline,
DescribePatchBaselines, GetPatchBaseline, RegisterPatchBaselineForPatchGroup,
DeregisterPatchBaselineForPatchGroup, GetPatchBaselineForPatchGroup,
DescribePatchGroups

Key features: String / StringList / SecureString types, automatic versioning,
parameter history, hierarchical path queries with recursive option, pagination
with NextToken, labels, version limits, parameter name normalization (leading
slash optional), tag-based filtering, document management with permissions,
maintenance windows with targets and tasks, patch baselines and patch groups.

### S3 (74 actions)

**Buckets:** ListBuckets, CreateBucket, DeleteBucket, HeadBucket,
GetBucketLocation

**Objects:** PutObject, GetObject, DeleteObject, HeadObject, CopyObject,
DeleteObjects, ListObjectsV2, ListObjects, ListObjectVersions,
GetObjectAttributes, RestoreObject

**Object Properties:** PutObjectTagging, GetObjectTagging, DeleteObjectTagging,
PutObjectAcl, GetObjectAcl, PutObjectRetention, GetObjectRetention,
PutObjectLegalHold, GetObjectLegalHold

**Bucket Configuration:** PutBucketTagging, GetBucketTagging, DeleteBucketTagging,
PutBucketAcl, GetBucketAcl, PutBucketVersioning, GetBucketVersioning,
PutBucketCors, GetBucketCors, DeleteBucketCors,
PutBucketNotificationConfiguration, GetBucketNotificationConfiguration,
PutBucketWebsite, GetBucketWebsite, DeleteBucketWebsite,
PutBucketAccelerateConfiguration, GetBucketAccelerateConfiguration,
PutPublicAccessBlock, GetPublicAccessBlock, DeletePublicAccessBlock,
PutBucketEncryption, GetBucketEncryption, DeleteBucketEncryption,
PutBucketLifecycleConfiguration, GetBucketLifecycleConfiguration,
DeleteBucketLifecycleConfiguration,
PutBucketLogging, GetBucketLogging,
PutBucketPolicy, GetBucketPolicy, DeleteBucketPolicy,
PutObjectLockConfiguration, GetObjectLockConfiguration,
PutBucketReplication, GetBucketReplication, DeleteBucketReplication,
PutBucketOwnershipControls, GetBucketOwnershipControls,
DeleteBucketOwnershipControls,
PutBucketInventoryConfiguration, GetBucketInventoryConfiguration,
DeleteBucketInventoryConfiguration

**Multipart Uploads:** CreateMultipartUpload, UploadPart, UploadPartCopy,
CompleteMultipartUpload, AbortMultipartUpload, ListParts, ListMultipartUploads

Key features: path-style addressing, nested key paths, prefix/delimiter listing
with common prefixes, pagination with continuation tokens, user metadata,
cross-bucket copy, batch delete, ETag (MD5) computation, multipart uploads with
copy support, versioning, CORS, bucket notifications (SNS/SQS delivery),
lifecycle rules with background expiration and storage class transitions, object
lock (retention and legal hold), encryption, replication, and website
configuration.

### Lambda (10 actions, stub)

**Functions:** CreateFunction, GetFunction, DeleteFunction, ListFunctions,
Invoke, PublishVersion

**Event Source Mappings:** CreateEventSourceMapping, ListEventSourceMappings,
GetEventSourceMapping, DeleteEventSourceMapping

Key features: function CRUD with config storage (runtime, handler, role,
memory, timeout, environment variables, tags, architectures), canned Invoke
response (does not execute code), event source mapping management. Uses
REST-style routing (HTTP method + URL path) with SigV4 credential-scope
routing.

### Secrets Manager (11 actions)

CreateSecret, GetSecretValue, PutSecretValue, UpdateSecret, DeleteSecret,
RestoreSecret, DescribeSecret, ListSecrets, TagResource, UntagResource,
ListSecretVersionIds

Key features: secret versioning with AWSCURRENT/AWSPREVIOUS stage tracking,
soft delete with configurable recovery window and force delete, secret
restoration, lookup by name or ARN, pagination, tag management, description
and KMS key metadata.

### CloudWatch Logs (14 actions)

**Log Groups:** CreateLogGroup, DeleteLogGroup, DescribeLogGroups

**Log Streams:** CreateLogStream, DeleteLogStream, DescribeLogStreams

**Log Events:** PutLogEvents, GetLogEvents, FilterLogEvents

**Tags:** TagLogGroup, UntagLogGroup, ListTagsLogGroup

**Retention:** PutRetentionPolicy, DeleteRetentionPolicy

Key features: log groups with log streams, event storage and retrieval,
simple substring filter pattern matching, retention policies, tagging.

### KMS (16 actions)

**Keys:** CreateKey, DescribeKey, ListKeys, EnableKey, DisableKey,
ScheduleKeyDeletion

**Encryption:** Encrypt, Decrypt, GenerateDataKey,
GenerateDataKeyWithoutPlaintext

**Aliases:** CreateAlias, DeleteAlias, ListAliases

**Tags:** TagResource, UntagResource, ListResourceTags

Key features: fake envelope encryption (base64-encoded with key ID prefix),
key enable/disable/deletion scheduling, alias resolution for all operations,
data key generation.

### CloudFormation (8 actions)

CreateStack, DeleteStack, DescribeStacks, ListStacks, ListStackResources,
DescribeStackResources, UpdateStack, GetTemplate

Key features: JSON and YAML template parsing, resource provisioning into
existing services (SQS, SNS, SSM, IAM, S3, EventBridge, DynamoDB, CloudWatch
Logs), stack update with diff-based resource create/delete, parameter and
tag support, Ref/Fn::Sub/Fn::Join intrinsic function resolution.

Supported resource types: AWS::SQS::Queue, AWS::SNS::Topic,
AWS::SNS::Subscription, AWS::SSM::Parameter, AWS::IAM::Role,
AWS::IAM::Policy, AWS::S3::Bucket, AWS::Events::Rule,
AWS::DynamoDB::Table, AWS::Logs::LogGroup.

### Cross-Service Integration

FakeCloud implements real cross-service message delivery and background
processing:

- **S3 -> SNS/SQS**: Bucket event notification configurations deliver to SNS
  topics and SQS queues when objects are created/deleted.
- **SNS -> SQS**: Publishing to an SNS topic delivers to all SQS subscriptions.
- **SNS -> HTTP/HTTPS**: Publishing to an SNS topic delivers to HTTP endpoints.
- **EventBridge -> SNS/SQS**: PutEvents and scheduled rules deliver to SNS
  topic and SQS queue targets.
- **S3 Lifecycle**: Background processor runs every 60 seconds, expiring objects
  and transitioning storage classes based on lifecycle rules.
- **EventBridge Scheduler**: Cron and rate-based rules fire on schedule,
  delivering events to configured targets.

## Configuration

FakeCloud is configured via CLI flags or environment variables.

| Flag | Env Var | Default | Description |
|---|---|---|---|
| `--addr` | `FAKECLOUD_ADDR` | `0.0.0.0:4566` | Listen address and port |
| `--region` | `FAKECLOUD_REGION` | `us-east-1` | AWS region to advertise |
| `--account-id` | `FAKECLOUD_ACCOUNT_ID` | `123456789012` | AWS account ID |
| `--log-level` | `FAKECLOUD_LOG` | `info` | Log level (trace, debug, info, warn, error) |

```sh
# Examples
fakecloud --addr 127.0.0.1:5000 --log-level debug
FAKECLOUD_LOG=trace cargo run --bin fakecloud
```

## Health Check

```sh
curl http://localhost:4566/_fakecloud/health
```

```json
{
  "status": "ok",
  "version": "0.1.0",
  "services": ["cloudformation", "dynamodb", "sqs", "sns", "events", "iam", "sts", "ssm", "lambda", "secretsmanager", "logs", "kms", "s3"]
}
```

## Architecture

FakeCloud is organized as a Cargo workspace:

| Crate | Purpose |
|---|---|
| `fakecloud` | Binary entry point (clap CLI, Axum HTTP server) |
| `fakecloud-core` | `AwsService` trait, service registry, request dispatch, protocol parsing |
| `fakecloud-aws` | Shared AWS types (ARNs, error builders, SigV4 parser) |
| `fakecloud-sqs` | SQS implementation |
| `fakecloud-sns` | SNS implementation with delivery |
| `fakecloud-eventbridge` | EventBridge implementation with scheduler |
| `fakecloud-iam` | IAM and STS implementation |
| `fakecloud-ssm` | SSM Parameter Store implementation |
| `fakecloud-dynamodb` | DynamoDB implementation |
| `fakecloud-lambda` | Lambda stub implementation |
| `fakecloud-secretsmanager` | Secrets Manager implementation |
| `fakecloud-s3` | S3 implementation |
| `fakecloud-logs` | CloudWatch Logs implementation |
| `fakecloud-kms` | KMS implementation |
| `fakecloud-cloudformation` | CloudFormation implementation |
| `fakecloud-e2e` | End-to-end tests using aws-sdk-rust |

Protocol handling:
- **Query protocol** (SQS, SNS, IAM, STS, CloudFormation): form-encoded body, `Action` parameter, XML responses
- **JSON protocol** (SSM, EventBridge, DynamoDB, Secrets Manager, CloudWatch Logs, KMS): JSON body, `X-Amz-Target` header, JSON responses
- **REST protocol** (S3, Lambda): HTTP method + path-based routing, XML/JSON responses
- SigV4 signatures are parsed for service routing but never validated

## Testing

```sh
cargo test --workspace              # unit tests
cargo build && cargo test -p fakecloud-e2e  # E2E tests (213 tests)
cargo clippy --workspace -- -D warnings     # lint
cargo fmt --check                           # format check
```

E2E tests use the official `aws-sdk-rust` crates and spawn a real FakeCloud
server per test.

## Contributing

Contributions are welcome. FakeCloud is still in early development (Phase 1).

1. Fork the repository
2. Create a feature branch: `git checkout -b feat/my-feature`
3. Follow existing conventions:
   - Conventional commits (`feat:`, `fix:`, `chore:`, `test:`, `refactor:`)
   - Per-action error enums with `thiserror` (no god enums)
   - Add E2E tests for new actions
4. Run the full test suite: `cargo test --workspace`
5. Open a pull request

### Planned services (Phase 2)

EC2, RDS, ECS, Elastic Load Balancing, CloudWatch Metrics, Route 53, API Gateway, Step Functions.

## License

FakeCloud is licensed under the [GNU Affero General Public License v3.0](https://www.gnu.org/licenses/agpl-3.0.html) (AGPL-3.0-or-later).

This means you can freely use, modify, and distribute FakeCloud, but if you run
a modified version as a network service, you must make the source code available
to users of that service.

---

<p align="center">
  Built by <a href="https://github.com/faiscadev">faiscadev</a> | <a href="https://fakecloud.dev">fakecloud.dev</a>
</p>
