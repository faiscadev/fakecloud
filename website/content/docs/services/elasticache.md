+++
title = "ElastiCache"
description = "Real Redis, Valkey, and Memcached clusters via Docker. Replication groups, snapshots, ACL user management, configuration endpoints."
weight = 18
+++

fakecloud implements **77 of 77** ElastiCache operations at 100% Smithy conformance. Cache clusters run in **real Docker containers** — your code connects to a real Redis, Valkey, or Memcached instance.

## Supported features

- **Cache clusters** — `CreateCacheCluster`, `ModifyCacheCluster`, `DeleteCacheCluster`, `DescribeCacheClusters`, `RebootCacheCluster`
- **Real engines via Docker** — Redis, Valkey, and Memcached all run as real containers (`redis:7-alpine`, `valkey:8-alpine`, `memcached:1.6-alpine`). All commands work, including streams, pub/sub, Lua, modules, CAS, and the Memcached text/binary protocols
- **Replication groups** — `CreateReplicationGroup` with primary/replica topology (Redis/Valkey only — matches AWS). Persisted fields include `AtRestEncryptionEnabled`, `TransitEncryptionEnabled`, `TransitEncryptionMode`, `AuthTokenEnabled`, `KmsKeyId`, `LogDeliveryConfigurations`, `ClusterEnabled`, `MultiAZ`, `AutomaticFailover`, `DataTiering`, `NetworkType`, `IpDiscovery`, `SnapshotRetentionLimit`, `SnapshotWindow`, `PreferredMaintenanceWindow`, and node-group member topology — all round-tripped through `DescribeReplicationGroups`
- **Global replication groups** — cross-region global datastores (CRUD)
- **Serverless caches** — `CreateServerlessCache`, `ModifyServerlessCache` (Redis/Valkey only — matches AWS)
- **Snapshots** — `CreateSnapshot`, `CopySnapshot`, `DeleteSnapshot`, `RestoreReplicationGroupFromSnapshot`. Restore reads back the **real RDB file** dumped during snapshot creation and seeds the replacement Redis/Valkey container with `BGSAVE`-captured data, so your test fixture's keys survive snapshot/restore round-trips
- **Serverless cache snapshots** — CRUD
- **Subnet groups** — CRUD. Subnets resolve in EC2: the group reports their `VpcId` and each subnet's Availability Zone (`InvalidSubnet` for unknown subnets or ones spanning VPCs), and the `default` group is the default VPC's subnets
- **Users and user groups** — IAM-integrated auth. `CreateUser`/`ModifyUser` accept `AccessString` (RBAC rules) and `Passwords`/`NoPasswordRequired`. The container is configured via real Redis `ACL SETUSER` so the engine enforces the rules — unauthorized commands are rejected by the real engine, not stubbed
- **Parameter groups** — CRUD. `ModifyCacheParameterGroup` parameter changes that map to runtime-tunable Redis options are pushed to the live container via `CONFIG SET`, so `maxmemory-policy`, `timeout`, `tcp-keepalive`, etc. take effect immediately on the running instance
- **Security groups** — cache security group CRUD
- **Failover** — `TestFailover`, `TestMigration`
- **Tagging** — `AddTagsToResource`, `RemoveTagsFromResource`, `ListTagsForResource`
- **Engine versions** — `DescribeCacheEngineVersions`

## Configuration endpoint (Memcached)

Memcached cache clusters with `NumCacheNodes > 1` expose a real **ConfigurationEndpoint** in `DescribeCacheClusters`. The endpoint resolves to a fakecloud-managed config proxy that speaks the Memcached `config get cluster` command, returning the live node list (host:port pairs) just like AWS's Auto Discovery. The official `ElastiCache Cluster Client` libraries (`elasticache-cluster-client-memcached-for-java`, `aws-elasticache-cluster-client-libmemcached`, etc.) hit the configuration endpoint and discover the real backing nodes without any client-side configuration.

## Protocol

Query protocol. Form-encoded body, `Action` parameter, XML responses.

## How the Docker integration works

When you call `CreateCacheCluster` (or `CreateReplicationGroup` for Redis/Valkey topologies), fakecloud starts a real Docker container running the corresponding official image and reports the mapped host port. Your application connects with a normal Redis or Memcached client. Memcached gets a real `memcached:1.6-alpine` container running the full text and binary protocols — not a stub.

## Introspection

- `GET /_fakecloud/elasticache/clusters` — current cache clusters (id, engine, status, mapped port)
- `GET /_fakecloud/elasticache/replication-groups` — replication groups with node members and ports
- `GET /_fakecloud/elasticache/serverless-caches` — serverless caches with endpoints
- `GET /_fakecloud/elasticache/acls` — ACL state (users + user groups) for replication groups with one or more user groups attached

All four are exposed by the introspection SDKs (`fakecloud.elasticache.clusters()`, `getElastiCacheAcls()`, etc.) for assertions in tests.

## Gotchas

- **Requires a Docker socket.** ElastiCache needs access to `/var/run/docker.sock`.
- **First use pulls the image.** Expect a slower first run while the Redis/Valkey/Memcached image downloads.
- **Memcached is cache-cluster only.** AWS does not support replication groups or serverless caches for Memcached, and neither does fakecloud. Use `CreateCacheCluster` with `Engine=memcached`.

## Source

- [`crates/fakecloud-elasticache`](https://github.com/faiscadev/fakecloud/tree/main/crates/fakecloud-elasticache)
- [AWS ElastiCache API reference](https://docs.aws.amazon.com/AmazonElastiCache/latest/APIReference/Welcome.html)
