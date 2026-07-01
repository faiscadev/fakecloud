+++
title = "Aurora DSQL"
description = "Aurora DSQL (dsql) serverless distributed PostgreSQL control plane on fakecloud: clusters, resource policies, change streams to Kinesis, multi-region properties, and tagging with real account-partitioned, persisted state."
weight = 18
+++

fakecloud implements the **Aurora DSQL** (`dsql`) control plane, the serverless,
distributed, PostgreSQL-compatible database. The full **16-operation restJson1
control plane** ships now: cluster lifecycle, cluster resource policies, change
streams to Kinesis, VPC endpoint service lookup, and tagging. State is
account-partitioned and persists across restarts in persistent mode, so clusters
and streams created in one session are still there in the next.

## Supported features

- **Cluster lifecycle** (`CreateCluster`, `GetCluster`, `UpdateCluster`,
  `DeleteCluster`, `ListClusters`). Clusters get 26-char lowercase ids, an
  `arn:aws:dsql:<region>:<acct>:cluster/<id>` ARN, and an endpoint host of
  `<id>.dsql.<region>.on.aws`. Creation runs a real async `CREATING` -> `ACTIVE`
  lifecycle and deletion runs `DELETING` -> `DELETED`, so waiters see the same
  state transitions AWS returns.
- **Idempotency**: `clientToken` on `CreateCluster` returns the existing
  cluster instead of creating a duplicate.
- **Deletion protection**: `deletionProtectionEnabled` is enforced.
  `DeleteCluster` against a protected cluster is rejected until it is turned off
  via `UpdateCluster`.
- **Multi-region properties**: `multiRegionProperties` (witness region + linked
  cluster ARNs) round-trips through create/get/update.
- **Cluster resource policies** (`PutClusterPolicy`, `GetClusterPolicy`,
  `DeleteClusterPolicy`) store and return the IAM resource policy document
  attached to a cluster.
- **Change streams** (`CreateStream`, `GetStream`, `DeleteStream`,
  `ListStreams`) model DSQL change data capture streaming to Kinesis.
- **VPC endpoint service**: `GetVpcEndpointServiceName` returns the per-region
  endpoint service name for PrivateLink access.
- **Tagging** (`TagResource`, `UntagResource`, `ListTagsForResource`) keyed by
  cluster ARN.

100% conformance: all 491 generated Smithy probe variants pass.

The PostgreSQL-compatible **data plane** (a reachable database container plus
IAM-token authentication) is a follow-up batch. Today `dsql` is a complete
control plane; connecting to and running SQL against a DSQL endpoint is not yet
implemented.

## Example

```python
import boto3
dsql = boto3.client("dsql", endpoint_url="http://localhost:4566")

# Create a cluster (async CREATING -> ACTIVE)
cluster = dsql.create_cluster(deletionProtectionEnabled=True,
    tags={"env": "test"})
cid = cluster["identifier"]

# Read it back
resp = dsql.get_cluster(identifier=cid)
# resp["arn"] -> "arn:aws:dsql:us-east-1:123456789012:cluster/<id>"
# resp["status"] -> "CREATING" then "ACTIVE"

# Attach a resource policy
dsql.put_cluster_policy(identifier=cid, policy='{"Version":"2012-10-17","Statement":[]}')

# Start a change stream to Kinesis
stream = dsql.create_stream(identifier=cid)

# Tag the cluster
dsql.tag_resource(resourceArn=resp["arn"], tags={"team": "data"})
```
