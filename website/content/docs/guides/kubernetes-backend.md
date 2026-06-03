+++
title = "Kubernetes backend for Lambda"
description = "Run fakecloud Lambda functions as native Kubernetes Pods instead of Docker containers. Avoid docker-in-docker, get real resource limits, debug with kubectl."
weight = 30
+++

By default, fakecloud executes Lambda functions in Docker containers via `docker run`. When fakecloud itself runs inside Kubernetes (CI pipelines, multi-tenant test clusters), that means docker-in-docker: privileged pods, opaque resource accounting, and harder debugging.

The Kubernetes backend (issue [#1234](https://github.com/faiscadev/fakecloud/issues/1234)) replaces the Docker path with native Pods. Each Lambda function gets a Pod sized from the function's `MemorySize`, executes the AWS Runtime Interface Emulator image, and is reused across invocations exactly like a warm Docker container.

## When to use it

- fakecloud runs as a Pod in your CI / dev cluster
- You'd rather not grant fakecloud's Pod the `privileged` security context that docker-in-docker requires
- You want real Kubernetes `requests` / `limits` on Lambda Pods so the scheduler can pack them
- You want `kubectl logs` / `kubectl describe pod` on misbehaving functions

If fakecloud runs on your laptop with Docker Desktop, stick with the default Docker backend — it's faster (no init-container HTTP fetch) and needs no cluster.

## Enabling it

Set on the fakecloud Pod:

```
FAKECLOUD_LAMBDA_BACKEND=k8s
FAKECLOUD_K8S_SELF_URL=http://fakecloud.fakecloud.svc.cluster.local:4566
```

`FAKECLOUD_K8S_SELF_URL` must resolve from inside Lambda Pods — that's how init containers pull function code and layers from the fakecloud process. Use the in-cluster service DNS name, never `localhost` or `127.0.0.1`.

### Selecting the backend

Backend selection is per-service with a global fallback:

- `FAKECLOUD_<SERVICE>_BACKEND=k8s|docker` — an explicit per-service override (`FAKECLOUD_LAMBDA_BACKEND`, and the same pattern for the other container-backed services). An explicit value always wins.
- `FAKECLOUD_CONTAINER_BACKEND=k8s|docker` — a global default applied to any service whose own variable is unset. Set this once to flip the whole stack to Kubernetes.
- Unset everywhere — Docker (the default).

So `FAKECLOUD_CONTAINER_BACKEND=k8s` with `FAKECLOUD_LAMBDA_BACKEND=docker` runs everything on Kubernetes *except* Lambda, which stays on Docker.

Optional env vars:

| Variable | Default | Purpose |
| --- | --- | --- |
| `FAKECLOUD_K8S_NAMESPACE` | `default` | Namespace Lambda Pods are created in. |
| `FAKECLOUD_K8S_ECR_URL` | host of `FAKECLOUD_K8S_SELF_URL` | Override the host:port the backend rewrites AWS private-ECR URIs to. |
| `FAKECLOUD_K8S_PULL_SECRET` | unset | Name of a `kubernetes.io/dockerconfigjson` Secret attached as `imagePullSecrets`. Only needed for `PackageType=Image` Lambda functions whose image registry requires auth. |

## RBAC

The fakecloud Pod's ServiceAccount needs permission to create / list / watch / delete Pods in the configured namespace. The ElastiCache backend additionally execs into cache Pods (`redis-cli` for CONFIG/ACL and snapshot SAVE), so it needs `pods/exec` too.

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: fakecloud
  namespace: fakecloud
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: fakecloud-lambda-pods
  namespace: fakecloud
rules:
  - apiGroups: [""]
    resources: ["pods"]
    verbs: ["create", "get", "list", "watch", "delete"]
  # Required only for the ElastiCache backend (exec into cache Pods).
  - apiGroups: [""]
    resources: ["pods/exec"]
    verbs: ["create"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: fakecloud-lambda-pods
  namespace: fakecloud
subjects:
  - kind: ServiceAccount
    name: fakecloud
    namespace: fakecloud
roleRef:
  kind: Role
  name: fakecloud-lambda-pods
  apiGroup: rbac.authorization.k8s.io
```

If you set `FAKECLOUD_K8S_NAMESPACE` to a different namespace from where fakecloud itself runs, create the Role + RoleBinding in that target namespace.

## Deployment + Service

A minimal in-cluster install:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: fakecloud
  namespace: fakecloud
spec:
  replicas: 1
  selector:
    matchLabels: { app: fakecloud }
  template:
    metadata:
      labels: { app: fakecloud }
    spec:
      serviceAccountName: fakecloud
      containers:
        - name: fakecloud
          image: ghcr.io/faiscadev/fakecloud:latest
          ports:
            - containerPort: 4566
          env:
            - name: FAKECLOUD_LAMBDA_BACKEND
              value: "k8s"
            - name: FAKECLOUD_K8S_SELF_URL
              value: "http://fakecloud.fakecloud.svc.cluster.local:4566"
            - name: FAKECLOUD_K8S_NAMESPACE
              value: "fakecloud"
---
apiVersion: v1
kind: Service
metadata:
  name: fakecloud
  namespace: fakecloud
spec:
  selector: { app: fakecloud }
  ports:
    - name: http
      port: 4566
      targetPort: 4566
```

## How it works

1. Your test client calls `lambda:Invoke` against the fakecloud Service.
2. The Lambda service in fakecloud computes a deploy fingerprint (function code SHA + attached layer hashes) and looks for a matching warm Pod. If one exists, it POSTs the invocation payload to that Pod's `:8080`.
3. On a cache miss, fakecloud creates a new Pod via the Kubernetes API. The Pod has:
   - A `busybox` init container that downloads the function's code zip and a tar of its layers from fakecloud's internal `/_fakecloud/lambda/_internal/*` endpoints (bearer-token-protected, per-process token), unpacks them into shared `emptyDir` volumes mounted at `/var/task` and `/opt`.
   - A main container running the AWS RIE image for the function's runtime (`public.ecr.aws/lambda/python:3.12`, `public.ecr.aws/lambda/nodejs:20`, etc.). For `PackageType=Image` functions, the user's image is used directly; AWS private-ECR URIs are rewritten to the in-cluster fakecloud OCI registry.
   - Resource requests / limits sized from `MemorySize`, ephemeral `/tmp` as `emptyDir { medium: Memory, sizeLimit: EphemeralStorage.Size }`, `restartPolicy: Never`.
4. fakecloud watches the Pod until `status.podIP` is populated, then TCP-handshakes the RIE port and forwards the invocation.
5. Idle Pods are torn down by the same TTL loop the Docker backend uses.
6. On fakecloud startup, any Pod labeled `fakecloud-managed-by=fakecloud` whose `fakecloud-instance` label doesn't match the current process is deleted — covers crashes that left orphans behind.

## Security model

- Init containers pull code over the cluster network via a process-local bearer token. The token is generated at fakecloud startup, embedded into Pod specs at launch time, and never persisted or logged. Each fakecloud restart mints a fresh token, invalidating any in-flight artifact downloads.
- Pods carry labels `fakecloud-managed-by=fakecloud`, `fakecloud-instance=<pid>`, `fakecloud-lambda=<function>`, `fakecloud-deploy-id=<hash>` so you can `kubectl get pods -l fakecloud-managed-by=fakecloud`.
- Pods run with whatever default security context your cluster's `PodSecurityPolicy` / `PodSecurityAdmission` enforces — no `privileged`, no special capabilities. The fakecloud Pod itself also doesn't need privileged.

## ElastiCache backend

Set `FAKECLOUD_ELASTICACHE_BACKEND=k8s` (or the global `FAKECLOUD_CONTAINER_BACKEND=k8s`) to run cache clusters, replication groups, and serverless caches as native Pods instead of Docker containers.

- Each cache resource becomes one Pod with a single `redis:7-alpine` or `memcached:1.6-alpine` container; the resource's endpoint address is the Pod IP and the standard engine port (6379 / 11211).
- CONFIG / ACL changes and snapshot `SAVE` are applied by `kubectl exec`-style calls through the API server (hence the `pods/exec` RBAC rule) — no dependency on Pod-IP routability from fakecloud.
- **Snapshot restore**: a cache created from a snapshot gets a Pod whose container `wget`s the snapshot RDB from a per-process, bearer-token-guarded `/_fakecloud/elasticache/_internal/rdb/<pod>` endpoint into `/data/dump.rdb` before launching `redis-server`, so the engine loads it at startup — the k8s analogue of the Docker backend's `docker cp`.
- **Reboot** (RebootCacheCluster) recreates the Pod; for Redis the live dataset is snapshotted and reloaded across the recreate so data survives, matching the Docker backend's in-place restart. Memcached reboots flush (no persistence), as on AWS.
- Cache Pods carry `fakecloud-service=elasticache`; the startup reaper sweeps only its own service's orphans.

## RDS

Set `FAKECLOUD_RDS_BACKEND=k8s` (or the global `FAKECLOUD_CONTAINER_BACKEND=k8s`) to run DB instances as native Pods.

- Each DB instance becomes one Pod. The bridge engines (`postgres` / `mysql` / `mariadb`) use the prebuilt `ghcr.io/faiscadev/fakecloud-*` images — the cluster pulls them (there's no in-cluster image build, unlike the Docker backend; override the registry with `FAKECLOUD_POSTGRES_REGISTRY`, or supply `FAKECLOUD_K8S_PULL_SECRET` for a private one). The heavy engines (Oracle / SQL Server / Db2) use their upstream images; Db2 runs with a privileged security context.
- Bridge engines receive `FAKECLOUD_ENDPOINT` pointing at the in-cluster `FAKECLOUD_K8S_SELF_URL`, so their `aws_lambda` / `aws_s3` / UDF callbacks reach fakecloud.
- Readiness is a real connection for Postgres / MySQL / MariaDB and a Pod-log marker (then a TCP probe) for the heavy engines. Snapshot dump / restore and log-file reads run through `kubectl exec` (`pg_dump` / `mysqldump` / `psql` / `cat`) — another reason for the `pods/exec` RBAC rule.
- **Reboot** recreates the Pod; for the dumpable engines the dataset is snapshotted and reloaded across the recreate.
- Because fakecloud connects to the DB over the Pod IP, the RDS k8s backend requires fakecloud to run **in-cluster** (the standard `FAKECLOUD_K8S_SELF_URL` deployment).

## Limitations

- The Kubernetes backend covers Lambda, ElastiCache, and RDS execution. ECS task execution still shells out to Docker — it's follow-up work tracked off issue #1234.
- Container-image Lambda functions whose image registry requires auth need a manually-created `kubernetes.io/dockerconfigjson` Secret referenced via `FAKECLOUD_K8S_PULL_SECRET`. Auto-creating that secret requires `secrets` permissions that not every cluster admin wants to grant fakecloud.
- Cold-start latency adds the init container HTTP round-trip to download code + layers (typically <500ms intra-cluster), on top of image pull + RIE start. Warm-Pod reuse keeps subsequent invocations as fast as the Docker backend.
- The K8s backend requires fakecloud's process to remain reachable at `FAKECLOUD_K8S_SELF_URL` for the lifetime of each Pod's init container. If fakecloud restarts mid-init, that Pod's bootstrap fails and the facade will spawn a fresh one on the next invocation.

## Troubleshooting

- **`FAKECLOUD_LAMBDA_BACKEND=k8s but Kubernetes backend failed to initialize`** — kube client construction failed. fakecloud uses `kube::Client::try_default()`: in-cluster service account first, then `KUBECONFIG`. Make sure the Pod has a ServiceAccount mounted at `/var/run/secrets/kubernetes.io/serviceaccount`, or set `KUBECONFIG` for out-of-cluster testing.
- **Lambda Pods stuck in `Init:0/1` with `ImagePullBackOff` on busybox** — the cluster's default image registry can't reach `docker.io`. Mirror `busybox:1.36` to your internal registry, or configure a default-pull-secret that has Docker Hub credentials.
- **Init container exits non-zero with `wget: server returned error: HTTP/1.1 401`** — fakecloud restarted between Pod create and init-container start, invalidating the bearer token. The facade will retry on the next invocation.
- **`fakecloud-lambda` Pods are leaking after fakecloud crashes** — confirm fakecloud has `delete` permission on `pods` in the configured namespace. The startup reaper deletes any Pod labeled `fakecloud-managed-by=fakecloud` whose `fakecloud-instance` differs from the current process.
- **`kubectl get pods -l fakecloud-managed-by=fakecloud`** — quick health check showing every Lambda Pod fakecloud has spawned.

## Running the test suite

The K8s backend has unit tests (Pod-spec generation, helpers) that run on every workspace `cargo test`. Real-cluster integration tests are opt-in and gated behind the `k8s-integration` feature so a casual `cargo test` doesn't try to talk to a cluster that isn't there.

To run them:

```sh
kind create cluster --name fakecloud-k8s-test
FAKECLOUD_K8S_TEST=1 cargo test -p fakecloud-lambda \
    --features k8s-integration --test k8s_integration -- --test-threads=1
```

The first test hard-fails (not skips) when `FAKECLOUD_K8S_TEST` is unset, so you can't silently miss a regression. CI runs the same suite against a kind cluster on every push that touches `crates/fakecloud-lambda/**` via [`.github/workflows/lambda-k8s.yml`](https://github.com/faiscadev/fakecloud/blob/main/.github/workflows/lambda-k8s.yml).

## Status

Shipped in fakecloud 0.14.x. Beta — please open an issue or comment on [#1234](https://github.com/faiscadev/fakecloud/issues/1234) if you hit edge cases.
