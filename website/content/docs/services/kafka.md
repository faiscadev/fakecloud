+++
title = "Amazon MSK"
description = "Amazon MSK (Managed Streaming for Apache Kafka, kafka) on fakecloud: a complete 59-operation control plane (100% conformance). restJson1."
weight = 71
+++

fakecloud implements **Amazon MSK** (Managed Streaming for Apache Kafka) as a
restJson1 service. All **59 operations** ship with **100% conformance** against
AWS's own Smithy model, backed by account-partitioned state that persists across
restarts in persistent mode. MSK signs as `kafka`.

Amazon MSK on fakecloud is a faithful **control plane**: every `Create`/`Update`
is reflected by its `Describe`/`List`, every `Delete` deletes, and AWS's async
cluster lifecycle is modelled by returning the transient state and settling on
the next describe (an interrupted transition reconciles on restart).

MSK on fakecloud also has a real, Docker-backed **broker data plane**. When a
container runtime (Docker/Podman) is available, each provisioned cluster is
backed by a **real single-node Apache Kafka broker** (`apache/kafka:3.8.0`, KRaft
combined mode). `CreateCluster` background-spawns the broker container and the
cluster settles `CREATING` -> `ACTIVE` only once the broker actually serves;
`GetBootstrapBrokers` then returns a **genuinely reachable `host:port`** a real
Kafka client produces and consumes through, and topic operations
(`CreateTopic`/`DeleteTopic`/`ListTopics`/`DescribeTopic`/`UpdateTopic`/
`DescribeTopicPartitions`) are driven against the live broker with its own
`kafka-topics.sh` / `kafka-configs.sh` tools -- a topic the API creates is
genuinely created on Kafka. `RebootBroker` restarts the container in place
(preserving the topic log), `DeleteCluster` tears it down, and an `ACTIVE`
cluster reconciles on restart by re-attaching its persisted container (or
respawning if it is gone). This is exactly the Docker-backed bar the Amazon MQ
data plane meets.

**Single-broker simplification** (honestly labeled): a real MSK cluster has
three or more brokers, but fakecloud runs ONE Kafka container, which only
satisfies replication factor 1. A requested `ReplicationFactor > 1` is clamped
to 1 when the topic is created on the broker (the described value reflects what
the broker actually applied), and `ListNodes` surfaces broker node 1 as the live
node. Internal Kafka topics (`__consumer_offsets`, etc.) are hidden from
`ListTopics`.

**Degraded / control-plane-only mode.** When no container runtime is available
(and for serverless clusters, which have no broker to run), the topic operations
and bootstrap-broker strings fall back to real *control-plane* behavior with the
**same response shapes**: `CreateTopic` persists a topic that
`DescribeTopic`/`ListTopics` reflect, and `GetBootstrapBrokers` returns broker
endpoint strings synthesized from the cluster's broker nodes (well-formed
`b-N.<cluster>...kafka.<region>.amazonaws.com:9092` forms). The real broker
round trip is proven end to end by the `msk-broker` CI job (gated behind
`FAKECLOUD_E2E_MSK=1`), which produces and consumes a message through the live
broker.

## Resources

- **Clusters** - `CreateCluster` (provisioned) and `CreateClusterV2`
  (provisioned or serverless) return an
  `arn:aws:kafka:<region>:<account>:cluster/<name>/<uuid>-<n>` ARN with a
  monotonic `-N` suffix and a `State` of `CREATING`, settling to `ACTIVE` on the
  next describe. `DescribeCluster` returns the full `ClusterInfo`;
  `DescribeClusterV2` returns the `Cluster` union shape (provisioned vs
  serverless). `ListClusters`/`ListClustersV2` support `ClusterNameFilter`
  (and `ClusterTypeFilter`) plus `MaxResults`/`NextToken` pagination and are
  region-scoped. A duplicate cluster name in the same account and region is a
  `ConflictException`. `DeleteCluster` moves the cluster to `DELETING` and the
  next reconcile reaps it.
- **Cluster updates** - `UpdateBrokerCount`, `UpdateBrokerStorage`,
  `UpdateBrokerType`, `UpdateClusterConfiguration`, `UpdateClusterKafkaVersion`,
  `UpdateMonitoring`, `UpdateSecurity`, `UpdateConnectivity`, `UpdateStorage`,
  `UpdateRebalancing`, and `RebootBroker` each record a `ClusterOperation`
  (with `OperationType` and an `OperationState` that settles to
  `UPDATE_COMPLETE`), apply the change to the cluster, bump its
  `CurrentVersion`, and move it to `UPDATING`/`REBOOTING_BROKER` (settling to
  `ACTIVE` on the next describe). `ListClusterOperations`/`ListClusterOperationsV2`
  and `DescribeClusterOperation`/`DescribeClusterOperationV2` return them.
- **Cluster attributes** - `CreateCluster`/`CreateClusterV2` persist and
  `DescribeCluster`/`DescribeClusterV2` echo the full cluster sub-objects, with
  AWS defaults applied when omitted so both an explicit and a minimal create
  round-trip: `BrokerNodeGroupInfo` (`ConnectivityInfo` with `PublicAccess`
  defaulting to `DISABLED` and an all-disabled `VpcConnectivity`, `StorageInfo`/
  `EBSStorageInfo` with `VolumeSize` and an as-configured `ProvisionedThroughput`,
  `BrokerAZDistribution` `DEFAULT`), `EncryptionInfo` (`EncryptionInTransit`
  `ClientBroker` `TLS` + `InCluster` `true`, and a synthesized
  `EncryptionAtRest.DataVolumeKMSKeyId` KMS key ARN), `ClientAuthentication`,
  `LoggingInfo`, `OpenMonitoring`, `EnhancedMonitoring` (`DEFAULT`), and
  `CurrentBrokerSoftwareInfo` (Kafka version + any associated configuration
  ARN/revision). Serverless clusters synthesize a default `VpcConfig` security
  group and an IAM-enabled `ClientAuthentication`.
- **Broker nodes** - `ListNodes` synthesizes one `BrokerNodeInfo` per
  `NumberOfBrokerNodes` (broker id `1..=N`, client VPC IP, ENI, subnet,
  endpoints, and current software info). `GetBootstrapBrokers` returns exactly
  the connection-string variants the cluster's config enables, gated on its
  in-transit `ClientBroker` and client-auth schemes: plaintext (`:9092`, when
  `ClientBroker` allows `PLAINTEXT`), TLS (`:9094`, when TLS is allowed and no
  SASL scheme is on), SASL/SCRAM (`:9096`) and SASL/IAM (`:9098`) when enabled,
  the `Public*` variants (`:9194`/`:9196`/`:9198`) when public access is enabled,
  and the `VpcConnectivity*` variants when vpc-connectivity auth is enabled; the
  schemes a cluster does not enable are omitted, exactly as MSK omits them. When
  a real broker backs the cluster, its reachable `host:port` is returned as the
  plaintext string.
- **Configurations** - `CreateConfiguration` stores the base64 `ServerProperties`
  with a monotonic revision list (revision `1`, `CreationTime`, `Description`);
  `UpdateConfiguration` adds a revision; `DescribeConfigurationRevision` returns
  a revision's `ServerProperties`. `ListConfigurations`/
  `ListConfigurationRevisions` paginate.
- **SCRAM secrets** - `BatchAssociateScramSecret` / `BatchDisassociateScramSecret`
  maintain a per-cluster list of secret ARNs (returning `UnprocessedScramSecrets: []`);
  `ListScramSecrets` paginates them.
- **Cluster policy** - `PutClusterPolicy` / `GetClusterPolicy` /
  `DeleteClusterPolicy` store a JSON policy document with a `CurrentVersion`.
- **VPC connections & replicators** - full CRUD with persistence and faithful
  shapes, settling to `AVAILABLE` (VPC connections) and `RUNNING` (replicators)
  on the next read; `ListClientVpcConnections` / `RejectClientVpcConnection`
  cover the cluster-side view.
- **Topics** (control-plane this batch) - `CreateTopic` persists
  `{TopicName, PartitionCount, ReplicationFactor, Configs}`;
  `DescribeTopic`/`ListTopics`/`DescribeTopicPartitions` reflect it;
  `UpdateTopic`/`DeleteTopic` mutate it.
- **Versions** - `ListKafkaVersions` returns the real MSK supported-version
  catalog (the `1.1.1` / `2.x` lines through `3.8.x`, including the `2.8.2.tiered`
  and `3.7.x`/`3.8.x` KRaft variants) with `ACTIVE` status, so the
  `aws_msk_kafka_version` data source (which filters by an exact version + status)
  resolves; `GetCompatibleKafkaVersions` returns the strictly-newer upgrade
  targets for a source version.
- **Tags** - `TagResource` (204) / `UntagResource` (204) / `ListTagsForResource`
  maintain an ARN-keyed tag map; `CreateCluster`/`CreateConfiguration`/
  `CreateVpcConnection`/`CreateReplicator` honour inline `Tags`, and tags surface
  on the resource's describe.

## Errors

MSK-faithful error shapes: `BadRequestException` (400), `NotFoundException`
(404), `ConflictException` (409), and the service's `Unauthorized` /
`Forbidden` / `TooManyRequests` / `InternalServerError` / `ServiceUnavailable`
family, each matching the model's per-operation error list.

## CloudFormation

fakecloud provisions every `AWS::MSK::*` resource type through CloudFormation:
`AWS::MSK::Cluster`, `AWS::MSK::ServerlessCluster`, `AWS::MSK::Configuration`,
`AWS::MSK::ClusterPolicy`, `AWS::MSK::BatchScramSecret`,
`AWS::MSK::VpcConnection`, and `AWS::MSK::Replicator`. Each is written through to
the `kafka` service via the same shared record builders the direct API uses, so
a CFN-provisioned resource reads back identically on `Describe*` and persists
across a restart (it survives just like its direct-API equivalent).

`Ref` and `Fn::GetAtt` follow the AWS resource spec: a cluster / serverless
cluster / configuration / VPC connection / replicator `Ref` resolves to the
resource ARN; a cluster policy and batch SCRAM secret `Ref` resolve to the
cluster ARN. `Fn::GetAtt` resolves `Arn` (cluster, configuration, VPC
connection), `ReplicatorArn` (replicator), and `CurrentVersion` (cluster
policy). A provisioned `AWS::MSK::Cluster` is backed by the same real Apache
Kafka container the direct `CreateCluster` path spawns (the container is started
in the background so `CreateStack` never blocks on a broker boot), settling
`CREATING` -> `ACTIVE` once it serves; deleting the stack tears the container
down. Without a container runtime the cluster settles `ACTIVE` through the
in-memory control plane, the same as the direct API.

## Terraform

fakecloud passes the **entire** upstream `terraform-provider-aws` MSK acceptance
suite (`internal/service/kafka`) in the `kafka` tfacc job, with no denies:
provisioned + serverless clusters (create/describe/update/delete, tags, import,
client-auth, encryption, logging, monitoring, storage, connectivity, and
version-upgrade paths), cluster policies, configurations, SCRAM-secret
associations, standalone VPC connections, replicators, TLS client-auth with an
ACM PCA certificate-authority ARN, and the cluster / cluster-policy /
configuration / broker-nodes / bootstrap-brokers / kafka-version / vpc-connection
data sources all round-trip against the auto-settling control plane. The suite
runs with the Kafka broker data plane disabled (`FAKECLOUD_KAFKA_DISABLE_BACKEND=1`)
so the AWS-format `*.amazonaws.com` bootstrap-broker assertions don't race a real
`127.0.0.1:<port>` endpoint; the real in-transit produce/consume round trip is
proven separately by the dedicated `msk-broker` Docker E2E.

## Persistence

All MSK state is account-partitioned and persisted in persistent mode; in-flight
lifecycle transitions (clusters, operations, VPC connections, replicators,
topics) reconcile on restart.
