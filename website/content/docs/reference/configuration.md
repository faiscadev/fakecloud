+++
title = "Configuration"
description = "CLI flags and environment variables for fakecloud."
weight = 1
+++

fakecloud is configured via CLI flags or environment variables. Flags take precedence when both are set.

| Flag                 | Env Var                     | Default            | Description                                                                              |
| -------------------- | --------------------------- | ------------------ | ---------------------------------------------------------------------------------------- |
| `--addr`             | `FAKECLOUD_ADDR`            | `0.0.0.0:4566`     | Listen address and port                                                                  |
| `--region`           | `FAKECLOUD_REGION`          | `us-east-1`        | AWS region to advertise                                                                  |
| `--account-id`       | `FAKECLOUD_ACCOUNT_ID`      | `123456789012`     | AWS account ID                                                                           |
| `--log-level`        | `FAKECLOUD_LOG`             | `info`             | Log level (trace, debug, info, warn, error)                                              |
| `--storage-mode`     | `FAKECLOUD_STORAGE_MODE`    | `memory`           | `memory` (default, all state in RAM) or `persistent` (mirror state to `--data-path`)    |
| `--data-path`        | `FAKECLOUD_DATA_PATH`       | —                  | Directory to persist state to. Required when `--storage-mode=persistent`.                |
| `--s3-cache-size`    | `FAKECLOUD_S3_CACHE_SIZE`   | `268435456`        | In-memory LRU cache for S3 object bodies in persistent mode. Default 256 MiB.            |
|                      | `FAKECLOUD_CONTAINER_CLI`   | auto-detect        | Container CLI to use (`docker` or `podman`)                                              |
|                      | `FAKECLOUD_CONTAINER_BACKEND` | unset (docker)   | Global execution backend for all container-backed services (Lambda, ECS, RDS, ElastiCache, EC2). `k8s` runs them as native Kubernetes Pods. See [Kubernetes backend](/docs/guides/kubernetes-backend/). |
|                      | `FAKECLOUD_LAMBDA_BACKEND`  | inherits global    | Per-service override for Lambda (`k8s` or `docker`). Wins over `FAKECLOUD_CONTAINER_BACKEND`. |
|                      | `FAKECLOUD_ECS_BACKEND`     | inherits global    | Per-service override for ECS task execution (`k8s` or `docker`). |
|                      | `FAKECLOUD_RDS_BACKEND`     | inherits global    | Per-service override for RDS DB instances (`k8s` or `docker`). |
|                      | `FAKECLOUD_ELASTICACHE_BACKEND` | inherits global| Per-service override for ElastiCache (`k8s` or `docker`). |
|                      | `FAKECLOUD_EC2_BACKEND`     | inherits global    | Per-service override for EC2 instances (`k8s` or `docker`). |
|                      | `FAKECLOUD_EC2_DEFAULT_IMAGE` | `amazonlinux:2023` | Base container image EC2 instances boot from (any OS image with `tail`/`sh`). |
|                      | `FAKECLOUD_PERSIST_DB_VOLUMES` | on in persistent mode | Back RDS (postgres/mysql/mariadb) data directories with a durable named volume so database contents survive a restart. Defaults on under `--storage-mode=persistent`, off in memory mode. Set `1`/`0` to override. |
|                      | `FAKECLOUD_PERSIST_EC2_VOLUMES` | on in persistent mode | Back each EC2 instance's data directory with a durable named volume so its contents survive a restart and stop/start. Defaults on under `--storage-mode=persistent`, off in memory mode. Set `1`/`0` to override. |
|                      | `FAKECLOUD_EC2_INSTANCE_DATA_DIR` | `/var/lib/fakecloud/ec2` | In-instance directory backed by the durable EC2 volume. Point it at wherever your instance workload writes long-lived state. |
|                      | `FAKECLOUD_LAMBDA_MAX_CONCURRENCY` | `10`        | Max warm instances kept per Lambda function. Each instance serves one invocation at a time (the runtime emulator can't handle concurrent events); the pool scales up to this cap under concurrent load, then queues. Raise for higher per-function concurrency. |
|                      | `FAKECLOUD_K8S_NAMESPACE`   | `default`          | Namespace fakecloud creates Pods in. Only honored on the K8s backend. |
|                      | `FAKECLOUD_K8S_SELF_URL`    | —                  | In-cluster URL of the fakecloud Service (e.g. `http://fakecloud.fakecloud.svc.cluster.local:4566`). Required for the K8s backend — Pods fetch artifacts from and call back to this URL. |
|                      | `FAKECLOUD_K8S_ECR_URL`     | host of `_SELF_URL`| Override the host:port the K8s backend rewrites AWS private-ECR URIs to. Defaults to the host of `FAKECLOUD_K8S_SELF_URL`. |
|                      | `FAKECLOUD_K8S_PULL_SECRET` | unset              | Name of a `kubernetes.io/dockerconfigjson` Secret used as `imagePullSecrets` for Pods pulling private images. |
|                      | `FAKECLOUD_K8S_NODE_SELECTOR` | unset            | `key=value,key=value` node selector applied to every fakecloud Pod. Per-service override: `FAKECLOUD_<SERVICE>_K8S_NODE_SELECTOR` (e.g. `FAKECLOUD_LAMBDA_K8S_NODE_SELECTOR`). See [Kubernetes backend](/docs/guides/kubernetes-backend/#pod-scheduling-and-metadata). |
|                      | `FAKECLOUD_K8S_TOLERATIONS` | unset              | JSON array of k8s `Toleration` objects applied to every fakecloud Pod. Per-service override: `FAKECLOUD_<SERVICE>_K8S_TOLERATIONS`. |
|                      | `FAKECLOUD_K8S_ANNOTATIONS` | unset              | `key=value,key=value` annotations added to every fakecloud Pod. Per-service override: `FAKECLOUD_<SERVICE>_K8S_ANNOTATIONS`. |
|                      | `FAKECLOUD_MAX_REQUEST_BODY_BYTES` | `1073741824` | Max bytes a buffered request body can absorb before fakecloud returns 413. Default 1 GiB. Streaming routes (S3 `PutObject` / `UploadPart`, ECR OCI blob upload `PATCH` / `PUT`) bypass this cap entirely — they spool the raw HTTP body to disk instead of buffering it all in RAM. Raise this only when stress-testing buffered requests past 1 GiB. |

## Examples

```sh
# Bind to localhost only
fakecloud --addr 127.0.0.1:4566

# Verbose logging
fakecloud --log-level debug

# Different region and account
fakecloud --region eu-west-1 --account-id 999999999999

# Persistent storage
fakecloud --storage-mode persistent --data-path /var/lib/fakecloud
```

## Environment-only configuration

```sh
FAKECLOUD_LOG=trace fakecloud
FAKECLOUD_REGION=eu-central-1 fakecloud
```

See also [Persistence](/docs/reference/persistence/) for details on persistent storage mode.

## LocalStack and AWS URL compatibility

fakecloud decodes both LocalStack's `*.localhost.localstack.cloud` hostname convention and the real AWS `*.amazonaws.com` hostnames. Persisted URLs from either setup — queue URLs baked into dev scripts, presigned URLs in fixtures, webhook targets in response mocks — replay against fakecloud without rewriting. The following patterns are recognized on the `Host` header:

| Host pattern                                                  | Routed as                                        |
| ------------------------------------------------------------- | ------------------------------------------------ |
| `<service>.<region>.localhost.localstack.cloud[:port]`        | `<service>` in `<region>`                        |
| `<bucket>.s3.<region>.localhost.localstack.cloud[:port]`      | S3 virtual-hosted-style on `<bucket>`            |
| `<service>.<region>.amazonaws.com`                            | `<service>` in `<region>`                        |
| `s3.<region>.amazonaws.com`                                   | S3 path-style                                    |
| `<bucket>.s3.<region>.amazonaws.com`                          | S3 virtual-hosted-style on `<bucket>`            |
| `s3.amazonaws.com`                                            | S3 path-style, legacy `us-east-1` global         |
| `<bucket>.s3.amazonaws.com`                                   | S3 virtual-hosted-style on `<bucket>`, `us-east-1` |
| `s3-<region>.amazonaws.com`                                   | S3 path-style, older dash-separated form         |
| `<bucket>.s3-<region>.amazonaws.com`                          | S3 virtual-hosted-style, older dash-separated    |

The DNS wildcard `*.localhost.localstack.cloud` resolves to `127.0.0.1`, so LocalStack-shaped hostnames reach fakecloud unchanged; for AWS-shaped hostnames, point the client at fakecloud's endpoint (or add the names to `/etc/hosts`) and fakecloud parses the `Host` header to recover service, region, and (for S3) bucket. SigV4-signed requests still route by credential scope first — the hostname is a secondary signal that takes over when the request is unsigned, uses a non-standard `Authorization` header, or is being probed with `curl`.

```bash
# Unsigned SQS request — routed to SQS purely by Host header
curl -X POST \
     -H 'Host: sqs.us-east-1.amazonaws.com' \
     -d 'Action=ListQueues&Version=2012-11-05' \
     http://127.0.0.1:4566/

# Virtual-hosted-style S3 GetObject — bucket recovered from Host header
curl -H 'Host: my-bucket.s3.us-east-1.amazonaws.com' \
     http://127.0.0.1:4566/key

# Legacy global S3 endpoint — implicit us-east-1
curl -H 'Host: my-bucket.s3.amazonaws.com' http://127.0.0.1:4566/key
```

Bucket names with dots (e.g. `a.b.c`) are supported against every S3 suffix; fakecloud recognizes the `.s3.<region>` / `.s3-<region>` / `.s3` trailer and treats everything before it as the bucket label.
