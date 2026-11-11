+++
title = "OpenSearch Service"
description = "Amazon OpenSearch Service + Amazon Elasticsearch Service (es) on fakecloud: one shared control plane for both APIs (147 operations, 100% conformance) plus real single-node OpenSearch / Elasticsearch clusters behind every domain. restJson1."
weight = 47
+++

fakecloud implements **Amazon OpenSearch Service** and its predecessor **Amazon
Elasticsearch Service** as a single restJson1 control plane. The two APIs share
one SigV4 signing scope (`es`) and one endpoint (`es.<region>.amazonaws.com`); on
the wire they are distinguished only by the URL path version prefix
(`/2015-01-01/...` = legacy Elasticsearch Service, `/2021-01-01/...` = OpenSearch
Service). They are therefore **one crate** backed by **one shared domain store**:
a `Domain` is a single entity whether it was created through
`CreateElasticsearchDomain` (2015) or `CreateDomain` (2021), and it is visible
through either API's Describe/List operations. The 2015 API exposes it via the
`ElasticsearchDomainStatus` shape, the 2021 API via the superset `DomainStatus`
shape.

**The complete 147-operation surface** ships — **51 Elasticsearch Service
operations + 96 OpenSearch Service operations** — with **100% conformance across
both APIs**, backed by account-partitioned state that persists across restarts in
persistent mode.

## Real search clusters

With Docker or Podman available, every domain is backed by a **real
single-node search cluster**, so a client genuinely indexes and searches
against it:

| Engine version | Image |
| --- | --- |
| `OpenSearch_1.0` .. `OpenSearch_3.1` | `public.ecr.aws/opensearchproject/opensearch:<newest patch>` (e.g. `OpenSearch_2.11` -> `2.11.1`); a newer minor runs its `X.Y.0` tag |
| `Elasticsearch_6.0` .. `Elasticsearch_7.10` | `docker.elastic.co/elasticsearch/elasticsearch-oss:<newest patch>` (e.g. `Elasticsearch_7.10` -> `7.10.2`), the Apache-2.0 distribution Amazon Elasticsearch Service was built from |
| `Elasticsearch_1.5` .. `Elasticsearch_5.6` | none: these domains stay control-plane only |

`FAKECLOUD_OPENSEARCH_IMAGE_REPO` / `FAKECLOUD_ELASTICSEARCH_IMAGE_REPO`
override the repositories (a mirror or a private registry); the tag is still
chosen from the engine version.

- **Lifecycle.** `CreateDomain` / `CreateElasticsearchDomain` returns
  `Processing=true` (`DomainProcessingStatus=Creating`) and starts the cluster
  in the background; Describe reports `Processing=false` /
  `DomainProcessingStatus=Active` only once the node answers
  `_cluster/health`. `DeleteDomain` removes the container and its data volume.
- **Endpoint.** The domain keeps reporting its AWS-format `Endpoint`
  (`search-<name>-<id>.<region>.es.amazonaws.com`, or `Endpoints["vpc"]` for a
  VPC domain). fakecloud's own listener serves that name: a request to
  `http://localhost:4566` whose `Host` is the endpoint (or `EndpointV2`) is
  proxied to the domain's cluster, so pointing the name at fakecloud
  (`/etc/hosts`, a compose network alias, or a `Host` header) is enough, even
  when fakecloud runs in a container. While the cluster isn't up that name
  answers `503`. The cluster's port 9200 is also published on an ephemeral
  host port (`docker ps --filter label=fakecloud-opensearch=<domain ARN>`).
- **Firehose.** A delivery stream with a `DomainARN` destination delivers to
  the domain's cluster once it is up.
- **Single node, small heap.** `discovery.type=single-node` with a 512 MiB
  heap, whatever `ClusterConfig` says (instance counts, dedicated masters,
  zone awareness and warm/cold tiers are stored and echoed, not run).
- **Plain HTTP, no auth.** The cluster runs with the security plugin
  disabled, so the endpoint answers HTTP with no credentials.
  `AccessPolicies`, `AdvancedSecurityOptions` (fine-grained access control,
  including a master user), `DomainEndpointOptions` (`EnforceHTTPS`) and
  node-to-node / at-rest encryption are stored and echoed but not enforced on
  the data plane.
- **Configuration changes.** The `AdvancedOptions` that are node settings
  (`rest.action.multi.allow_explicit_index`, `indices.fielddata.cache.size`
  as a heap percentage, `indices.query.bool.max_clause_count`) are applied to
  the node. Changing them through `UpdateDomainConfig` (or a CloudFormation
  update), or upgrading with `UpgradeDomain`, replaces the container on the
  same data volume, so indices carry over, as AWS's blue/green deployment
  does; the domain reports `Processing=true` (`Modifying`, or
  `UpgradingEngineVersion` with `UpgradeProcessing=true`) until the new node
  is healthy. Other changes leave the running cluster alone.
  `DescribeDomainChangeProgress` reports the change `PROCESSING`
  (`ApplyingChanges`) and `GetUpgradeStatus` the upgrade `IN_PROGRESS`
  meanwhile; a new domain reports `Created=false` until its cluster is up,
  and `DeleteDomain` returns `DomainProcessingStatus=Deleting`.
- **Known difference: no blue/green overlap.** AWS keeps the old nodes
  serving until the new ones take over; fakecloud stops the old container
  before the new one is healthy, so the endpoint answers `503` (and Firehose
  `DomainARN` deliveries retry) for the length of the replacement.
- **Durability.** The data directory is a named volume. A persistent-mode
  restart (`--storage-mode persistent`) brings each domain back in a fresh
  container on its persisted volume, reporting `Processing=true` until it
  answers health again (a node on existing data counts as up even if its
  indices are red). Before a node starts, any container still mounting the
  domain's volume is removed, whichever fakecloud process started it. A
  reset removes every domain's container and volume.
- **Crashes.** A cluster that crashes is not restarted automatically: its
  endpoint stays bound to the dead port. An `UpdateDomainConfig` call (when
  the daemon reports the container gone or exited) or a fakecloud restart
  brings it back.
- **CloudFormation.** `AWS::OpenSearchService::Domain` and
  `AWS::Elasticsearch::Domain` start the same cluster; a stack update that
  changes node settings or the engine version replaces it, and a stack delete
  removes it.
- **Failure.** If the cluster can't start (no image for the platform, a
  failed pull, a node that never turns healthy), the domain settles to
  `Processing=false` without a reachable cluster (control-plane only); the
  next configuration change or restart tries again.

Without a container runtime, or with `FAKECLOUD_OPENSEARCH_DISABLE_BACKEND=1`,
domains are control-plane only: a new domain settles to
`Processing=false`/`Created=true` on first describe with its synthetic
endpoint, and nothing serves it. Search clusters always run through the
Docker/Podman CLI, also under `FAKECLOUD_CONTAINER_BACKEND=k8s`.

## Supported now (all 147 operations)

**Domains (shared across both APIs):** create, describe, delete, list domain
names, batch describe, describe/update domain config (including `DryRun`), change
progress, auto-tunes, cancel config change. `VPCOptions` (on domains and VPC
endpoints) resolve their subnets and security groups in EC2 and are returned as
`VPCDerivedInfo` with the subnets' `VPCId` and `AvailabilityZones`; without
security groups the VPC's `default` group is used (`ValidationException` for
unknown subnets or groups).

**Elasticsearch Service (2015-01-01):** `CreateElasticsearchDomain`,
`DescribeElasticsearchDomain(s)`, `DescribeElasticsearchDomainConfig`,
`UpdateElasticsearchDomainConfig`, `DeleteElasticsearchDomain`,
`DeleteElasticsearchServiceRole`, instance-type limits/list, versions, compatible
versions, upgrade domain + history/status, service software update start/cancel.

**OpenSearch Service (2021-01-01) extras:** applications + capabilities
(register/get/deregister), per-domain data sources and indices, direct-query data
sources, domain health/nodes/dry-run progress, domain maintenance, scheduled
actions, insights, `RollbackServiceSoftwareUpdate`, default application settings.

**Shared sub-resources:** tags (`AddTags`/`RemoveTags`/`ListTags` over the shared
ARN space), packages (create/update/delete/describe/associate/dissociate, version
history, per-domain and per-package listings, scope), VPC endpoints
(create/update/delete/describe/list + access authorize/revoke/list), inbound and
outbound cross-cluster (search) connections (create/accept/reject/delete/describe),
reserved instances (purchase, describe offerings/instances), and instance-type
and version catalogues.

## Design: one crate, two APIs

Because both APIs sign as `es` and hit the same endpoint, the service's
`service_name()` returns `es` and its `handle()` routes by the URL path version
prefix to the correct API's operation set, then maps the shared internal domain
struct onto that API's response shape. A domain created via OpenSearch is
therefore describable via Elasticsearch Service and vice versa — the same entity,
two views.
