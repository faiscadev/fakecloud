+++
title = "OpenSearch Service"
description = "Amazon OpenSearch Service + Amazon Elasticsearch Service (es) on fakecloud: one shared control plane for both APIs (147 operations, 100% conformance) — domains, packages, VPC endpoints, cross-cluster connections, applications, data sources. restJson1."
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

## Control-plane only

This is a control-plane emulation: no real Elasticsearch or OpenSearch cluster is
spawned (mirroring LocalStack Community, which treats these as control-plane
mocks). CRUD is real — `CreateDomain`/`CreateElasticsearchDomain` persists a
domain that its Describe/List reflects, config updates persist, and Delete
removes it. A newly created domain settles to `Processing=false`/`Created=true`
with a synthetic search endpoint (`search-<name>-<rand>.<region>.es.amazonaws.com`)
on first describe, mirroring the real control plane's eventual creation, so
provisioning waiters complete. There is no search data plane (indexing/query
traffic against the cluster itself).

## Supported now (all 147 operations)

**Domains (shared across both APIs):** create, describe, delete, list domain
names, batch describe, describe/update domain config (including `DryRun`), change
progress, auto-tunes, cancel config change.

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
