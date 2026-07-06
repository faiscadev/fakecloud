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

The real Apache Kafka **broker data plane** - topics created on a running Kafka
container and `GetBootstrapBrokers` returning a genuinely reachable endpoint - is
a later batch. In this release the topic operations and bootstrap-broker strings
are real *control-plane* behavior: `CreateTopic` persists a topic that
`DescribeTopic`/`ListTopics` reflect, and `GetBootstrapBrokers` returns broker
endpoint strings synthesized from the cluster's broker nodes (well-formed
`b-N.<cluster>...kafka.<region>.amazonaws.com:9092` forms).

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
- **Broker nodes** - `ListNodes` synthesizes one `BrokerNodeInfo` per
  `NumberOfBrokerNodes` (broker id `1..=N`, client VPC IP, ENI, subnet,
  endpoints, and current software info). `GetBootstrapBrokers` returns the
  plaintext (`:9092`), TLS (`:9094`), SASL/SCRAM (`:9096`), and SASL/IAM
  (`:9098`) connection strings derived from those nodes.
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
- **Versions** - `ListKafkaVersions` returns the supported MSK version catalog
  (2.8.x through 3.8.x, including KRaft variants) with `ACTIVE` status;
  `GetCompatibleKafkaVersions` returns compatible upgrade targets.
- **Tags** - `TagResource` (204) / `UntagResource` (204) / `ListTagsForResource`
  maintain an ARN-keyed tag map; `CreateCluster`/`CreateConfiguration`/
  `CreateVpcConnection`/`CreateReplicator` honour inline `Tags`, and tags surface
  on the resource's describe.

## Errors

MSK-faithful error shapes: `BadRequestException` (400), `NotFoundException`
(404), `ConflictException` (409), and the service's `Unauthorized` /
`Forbidden` / `TooManyRequests` / `InternalServerError` / `ServiceUnavailable`
family, each matching the model's per-operation error list.

## Not yet implemented

- The real Apache Kafka broker data plane (topics created on a running Kafka
  container; `GetBootstrapBrokers` returning a genuinely connectable endpoint).
  A later batch backs the control plane with a real broker container the way RDS
  and ElastiCache do.
- CloudFormation `AWS::MSK::*` resource provisioning (a later batch).

## Persistence

All MSK state is account-partitioned and persisted in persistent mode; in-flight
lifecycle transitions (clusters, operations, VPC connections, replicators,
topics) reconcile on restart.
