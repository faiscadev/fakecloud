+++
title = "Local ElastiCache for integration tests"
description = "Run local ElastiCache for integration tests with fakecloud. 75 operations, real Redis/Valkey/Memcached via Docker, replication groups, serverless caches. Free, no account required."
template = "page.html"
+++

Need local ElastiCache for integration tests? Use [fakecloud](https://github.com/faiscadev/fakecloud).

```sh
curl -fsSL https://fakecloud.dev/install.sh | bash
fakecloud
```

Point your AWS SDK at `http://localhost:4566`. Docker required because fakecloud runs **real** Redis / Valkey / Memcached.

## Why fakecloud for ElastiCache

- **75 ElastiCache operations** at 100% conformance — cache clusters, replication groups, global replication groups, serverless caches and snapshots, subnet groups, users / user groups, failover, tagging.
- **Real Redis, Valkey, and Memcached.** fakecloud pulls real Redis / Valkey / Memcached Docker images and runs them as the ElastiCache node. Your `LPUSH`, `ZADD`, `XADD`, streams, pub/sub, Lua scripts — all work because the engine is real. Memcached gets a real `memcached:1.6-alpine` container with the full text and binary protocols.
- **Real ACL enforcement.** `CreateUser` / `ModifyUser` push `ACL SETUSER` into the live Redis container, so unauthorized commands are rejected by the real engine — not by a stub.
- **Real parameter group changes.** `ModifyCacheParameterGroup` runtime-tunable parameters (e.g. `maxmemory-policy`, `timeout`) are pushed to the live container via `CONFIG SET` and take effect immediately.
- **Real Memcached Auto Discovery.** Multi-node Memcached clusters expose a real `ConfigurationEndpoint` that speaks `config get cluster`, so the official ElastiCache cluster clients discover the live nodes the same way they do against AWS.
- **Snapshot / restore round-trips real data.** `CreateSnapshot` dumps a real RDB file; `RestoreReplicationGroupFromSnapshot` seeds the replacement container with that RDB, so test fixture keys survive snapshot/restore cycles.
- **Encryption + logging persist.** `AtRestEncryptionEnabled`, `TransitEncryptionEnabled`, `TransitEncryptionMode`, `AuthTokenEnabled`, `KmsKeyId`, `LogDeliveryConfigurations`, `MultiAZ`, `AutomaticFailover`, `DataTiering`, `NetworkType`, `IpDiscovery`, snapshot / maintenance windows, and node-group topology all round-trip through `DescribeReplicationGroups`.
- **Endpoint works.** `DescribeCacheClusters` returns a real connectable host. Your application connects with a regular Redis client (redis-py, ioredis, lettuce, go-redis).
- **Paid on LocalStack; free here.** ElastiCache has always been LocalStack Pro-only.
- **No account, no auth token, no paid tier.** AGPL-3.0.

## Create a Redis cluster

```sh
aws --endpoint-url http://localhost:4566 elasticache create-cache-cluster \
  --cache-cluster-id mycache \
  --engine redis \
  --cache-node-type cache.t3.micro \
  --num-cache-nodes 1
```

Get the endpoint:

```sh
aws --endpoint-url http://localhost:4566 elasticache describe-cache-clusters \
  --cache-cluster-id mycache \
  --show-cache-node-info \
  --query 'CacheClusters[0].CacheNodes[0].Endpoint'
```

Connect:

```sh
redis-cli -h <endpoint> -p 6379
```

Real Redis — all commands work, including `CLIENT LIST`, `INFO`, `CONFIG GET`, modules.

## Valkey

```sh
aws --endpoint-url http://localhost:4566 elasticache create-cache-cluster \
  --cache-cluster-id mycache \
  --engine valkey \
  --cache-node-type cache.t3.micro \
  --num-cache-nodes 1
```

Valkey is API-compatible with Redis.

## Replication groups

```sh
aws --endpoint-url http://localhost:4566 elasticache create-replication-group \
  --replication-group-id my-rg \
  --replication-group-description "test rg" \
  --engine redis \
  --cache-node-type cache.t3.micro \
  --num-node-groups 1 \
  --replicas-per-node-group 1
```

Real primary + replica nodes via Redis replication.

## Memcached

```sh
aws --endpoint-url http://localhost:4566 elasticache create-cache-cluster \
  --cache-cluster-id mymc \
  --engine memcached \
  --cache-node-type cache.t3.micro \
  --num-cache-nodes 1
```

Real `memcached` text protocol — `set`, `get`, `delete`, `stats`, CAS, all work. AWS does not support replication groups or serverless caches for Memcached, and neither does fakecloud.

## Serverless caches

```sh
aws --endpoint-url http://localhost:4566 elasticache create-serverless-cache \
  --serverless-cache-name myserverless \
  --engine redis
```

## In tests

```ts
import { ElastiCacheClient, CreateCacheClusterCommand, DescribeCacheClustersCommand } from '@aws-sdk/client-elasticache';
import { createClient } from 'redis';

const ec = new ElastiCacheClient({ endpoint: 'http://localhost:4566' });

beforeAll(async () => {
  await ec.send(new CreateCacheClusterCommand({
    CacheClusterId: 'test',
    Engine: 'redis',
    CacheNodeType: 'cache.t3.micro',
    NumCacheNodes: 1,
  }));
  // ... poll for "available" ...
});

test('app caches via real redis behind ElastiCache emulation', async () => {
  const redis = createClient({ url: 'redis://localhost:6379' });
  await redis.connect();
  await redis.set('key', 'value');
  expect(await redis.get('key')).toBe('value');
});
```

## How it differs from alternatives

| Tool | Real Redis | Real Valkey | Real Memcached | Replication groups | Serverless caches | Price |
|---|---|---|---|---|---|---|
| fakecloud | Yes (Docker) | Yes (Docker) | Yes (Docker) | Yes | Yes | Free |
| LocalStack Pro | Yes | Yes | Yes | Yes | Partial | Paid |
| LocalStack Community | **No** | **No** | **No** | — | — | — (not available) |
| Plain `docker run redis` | Yes | N/A | N/A | Manual | N/A | Free, but no ElastiCache API |
| Moto | Stubbed | Stubbed | Stubbed | Stubbed | Stubbed | Free |

## Links

- **Install:** `curl -fsSL https://fakecloud.dev/install.sh | bash`
- **Repo:** [github.com/faiscadev/fakecloud](https://github.com/faiscadev/fakecloud)
- **Related:** [Local RDS for tests](/local-rds/), [Fake AWS server for tests](/fake-aws-server/)
