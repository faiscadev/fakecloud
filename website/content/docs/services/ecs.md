+++
title = "ECS"
description = "Elastic Container Service — full API: clusters, task definitions, real Fargate-style task execution via Docker, services with rolling deployments, task sets, container instances, ECS Exec."
weight = 22
+++

fakecloud implements Amazon Elastic Container Service (ECS) with full API coverage. 88 operations.

**Status: full API.** Covers clusters, task definitions, real Fargate-style task execution, services with rolling deployments + CODE_DEPLOY blue/green task sets, daemons, ExpressGatewayService, task sets, container instances, capacity providers, attributes, task protection, ECS Exec, placement constraints / strategies, awsvpc ENI binding, and the agent-side `Submit*` / `DiscoverPollEndpoint` surface.

## Supported today (full API)

- **Clusters** — `CreateCluster`, `DescribeClusters`, `DeleteCluster`, `ListClusters`, `UpdateCluster`, `UpdateClusterSettings`, `PutClusterCapacityProviders`
- **Task definitions** — `RegisterTaskDefinition`, `DescribeTaskDefinition`, `DeregisterTaskDefinition`, `DeleteTaskDefinitions`, `ListTaskDefinitions`, `ListTaskDefinitionFamilies`
- **Tasks** — `RunTask`, `StartTask`, `StopTask`, `DescribeTasks`, `ListTasks` with real Fargate-style execution via Docker/Podman
- **Services** — `CreateService`, `UpdateService`, `DeleteService`, `DescribeServices`, `ListServices`, `ListServicesByNamespace` with desired-count enforcement and rolling deployments
- **Service deployments** — `StopServiceDeployment`, `ListServiceDeployments`, `DescribeServiceDeployments`, `DescribeServiceRevisions`
- **Task sets** — `CreateTaskSet`, `UpdateTaskSet`, `DeleteTaskSet`, `DescribeTaskSets`, `UpdateServicePrimaryTaskSet` (EXTERNAL deployment controller)
- **Container instances** — `RegisterContainerInstance`, `DeregisterContainerInstance`, `DescribeContainerInstances`, `ListContainerInstances`, `UpdateContainerAgent`, `UpdateContainerInstancesState`
- **Attributes** — `PutAttributes`, `DeleteAttributes`, `ListAttributes`
- **Capacity providers** — `CreateCapacityProvider`, `DeleteCapacityProvider`, `DescribeCapacityProviders`, `UpdateCapacityProvider`
- **Task protection** — `GetTaskProtection`, `UpdateTaskProtection`
- **ECS Exec** — `ExecuteCommand` starts the command detached in the task's running container (`docker exec` or `podman exec`; run as given rather than through a shell; stopped when the task stops, or after 20 minutes in images that ship `timeout`) and returns the session at once. Output is not streamed: the session has no `streamUrl`, so `aws ecs execute-command --interactive` cannot attach a shell.
- **Agent surface** — `SubmitContainerStateChange`, `SubmitTaskStateChange`, `SubmitAttachmentStateChanges`, `DiscoverPollEndpoint`
- **Tagging** — `TagResource`, `UntagResource`, `ListTagsForResource` (clusters and task definitions)
- **Account settings** — `PutAccountSetting`, `PutAccountSettingDefault`, `DeleteAccountSetting`, `ListAccountSettings`
- **Daemons** — `RegisterDaemonTaskDefinition`, `DescribeDaemonTaskDefinition`, `DeleteDaemonTaskDefinition`, `ListDaemonTaskDefinitions`, `CreateDaemon`, `DescribeDaemon`, `UpdateDaemon`, `DeleteDaemon`, `ListDaemons`, `DescribeDaemonDeployments`, `ListDaemonDeployments`, `DescribeDaemonRevisions`. `CreateDaemon` spawns one task per matching capacity-provider host so the daemon scales with the cluster. Daemon tasks run from the daemon task definition (its own family/revision namespace, never a regular task definition of the same family): its container definitions, `volumes[]` and `mountPoints[]` (see [Volumes + mount points](#volumes-mount-points)), and its `pidMode` / `ipcMode` (`shared` joins the host PID / IPC namespace: `--pid host` / `--ipc host` on Docker and Podman, `hostPID` / `hostIPC` on Kubernetes, the same namespace a task definition's `pidMode` / `ipcMode` `host` joins).
- **Express gateway services** — `CreateExpressGatewayService`, `DescribeExpressGatewayService`, `UpdateExpressGatewayService`, `DeleteExpressGatewayService` (2026 ECS Express deployment controller)

### Services + rolling deployments

`CreateService` spawns tasks to match `desiredCount` under the service, tagging each with `startedBy=ecs-svc/<name>` so the tasks reconcile back to the service. `UpdateService` supports two independent mutations:

- **Scale** — set a new `desiredCount`. The service spawns additional tasks when scaling up and flips excess tasks to `desiredStatus=STOPPED` (runtime kill on the container) when scaling down.
- **Rolling deployment** — pass a new `taskDefinition`. The service marks the previous PRIMARY deployment as `ACTIVE`, creates a new `PRIMARY` deployment for the target revision, and drains tasks on the old task definition while new ones come up. Deployment circuit breaker + `minimumHealthyPercent` / `maximumPercent` are honoured in `deploymentConfiguration`.

`DeleteService` refuses while `desiredCount > 0` unless `force=true`; the forced path scales to 0 and stops every running task under the service before removing it.

#### Placement constraints + strategies

Both `RunTask` and `CreateService` honour `placementConstraints[]` (`distinctInstance`, `memberOf <expression>` against container-instance attributes) and `placementStrategy[]` (`random`, `spread` by attribute, `binpack` by `cpu` / `memory`). The scheduler ranks eligible container instances per strategy before launching; tasks fail with `unable to place a task` when no instance satisfies the constraints, matching real ECS.

#### awsvpc networking

Task definitions with `networkMode=awsvpc` get a per-task ENI in the first subnet of `networkConfiguration.awsvpcConfiguration.subnets[]` (from `RunTask`, or the service's network configuration). The attachment is `PRECREATED` with its `subnetId` when the task is created and `ATTACHED` once it starts, with `networkInterfaceId`, `macAddress`, `privateDnsName` and `privateIPv4Address`. In a subnet fakecloud's EC2 knows, the ENI is a real requester-managed network interface (described by the attachment's ARN, carrying the service's security groups) that EC2 creates and allocates the private IP for, skipping the five addresses AWS reserves in every subnet and every address already in use; it appears in `DescribeNetworkInterfaces` while the task runs and is deleted when the task stops. A subnet EC2 doesn't know gets a synthetic ENI with an address from `10.0.0.0/16`. Each task runs on its own container network, and its container ports are published on ephemeral host ports, so tasks never compete for a host port. A load balancer target `<ENI IP>:<containerPort>` is routed to that published port, which is how an ALB in front of an `awsvpc` service reaches its tasks on Docker Desktop, podman and Linux alike, while `DescribeTargetHealth` and `DescribeTasks` keep reporting the ENI IP. On the Kubernetes backend the ENI attaches when the Pod runs and the target routes to the Pod IP.

#### CODE_DEPLOY blue/green task sets

Services declared with `deploymentController.type=CODE_DEPLOY` skip the in-line rolling deployment and require external task-set churn. `CreateTaskSet` registers a non-primary (`BLUE`) set, `UpdateServicePrimaryTaskSet` flips traffic, and the old set is drained on `DeleteTaskSet`. This matches the AWS deployment pattern CodeDeploy uses to drive blue/green cutovers.

Task-definition families track revisions monotonically; `DeleteTaskDefinitions` requires `DeregisterTaskDefinition` first (real AWS behaviour), and the result flips status to `DELETE_IN_PROGRESS`.

## Task execution

`RunTask` records the task synchronously and kicks off a background docker execution per spawned task:

1. `docker pull <image>` (timestamps captured on the task: `pullStartedAt` / `pullStoppedAt`). A transient registry failure (rate limiting such as `429 Too Many Requests`, common for anonymous `public.ecr.aws` pulls from a shared IP; a registry 5xx; a network timeout) is retried with backoff and falls back to a copy of the image already cached locally. A refused pull (the image or tag no longer exists, or access is denied) stops the task with `TaskFailedToStart` even when a stale copy is cached, as does a transient failure with nothing cached.
2. `docker run -d <image>` (container ID recorded on the task's container)
3. `docker wait <id>` (blocks on container exit; exit code → `containers[].exitCode`)
4. `docker logs <id>` (captured stdout/stderr stored on the task + exposed via the introspection endpoint)
5. `docker rm <id>` (cleanup)

Environment variables from the task definition are forwarded with `localhost` / `127.0.0.1` rewritten to `host.docker.internal` so containers reach fakecloud itself the same way Lambda does. `ECS_CONTAINER_METADATA_URI` and `ECS_CONTAINER_METADATA_URI_V4` are also injected, pointing at fakecloud's task-metadata endpoint (`/_fakecloud/ecs/v3/{task_id}` and `/_fakecloud/ecs/v4/{task_id}`) so SDKs and sidecars that read task metadata work out of the box.

### Container definition fidelity

The runtime translates the following task-definition fields straight into `docker run` flags so containers see the same shape they would on real ECS:

- `linuxParameters.capabilities.add[]` / `drop[]` -> `--cap-add` / `--cap-drop`
- `linuxParameters.initProcessEnabled=true` -> `--init`
- `linuxParameters.sharedMemorySize` -> `--shm-size <MiB>m`
- `linuxParameters.tmpfs[]` -> `--tmpfs <containerPath>:size=<size>,...`
- `ulimits[]` -> `--ulimit <name>=<soft>:<hard>`
- `user` -> `--user`
- `readonlyRootFilesystem=true` -> `--read-only`
- `pseudoTerminal=true` -> `--tty`
- `privileged=true` -> `--privileged`
- task-definition `pidMode=host` / `ipcMode=host` -> `--pid host` / `--ipc host` (`hostPID` / `hostIPC` on Kubernetes)
- On Kubernetes, `privileged`, `hostPID` and `hostIPC` are refused by Pod Security levels below `privileged`; such a task stops with `TaskFailedToStart` and the admission error as its reason
- `systemControls[]` -> `--sysctl <namespace>=<value>`
- `logConfiguration.logDriver`, while a daemon with a log driver volume is active in the task's cluster and the engine server is Docker Engine: `json-file` -> `--log-driver json-file` plus its `options` as `--log-opt`; any other driver -> the `local` driver (rotating at 1 GiB x 100 files), so fakecloud still reads the container's output for `awslogs` forwarding but it writes no json-file log. On Podman (including a Docker CLI on Podman's compat socket) only an explicit `json-file` is passed, which Podman maps to `k8s-file`. With no such daemon, or an engine that doesn't answer, the container keeps the engine's default logging. The driver is chosen when a container starts, so a container started before a log driver daemon became active keeps the engine default (json-file on Docker) and its log is in the volume even if its `logConfiguration` names another driver
- `stopTimeout` -> `--stop-timeout` (30 seconds when unset, the ECS default), and the SIGTERM-to-SIGKILL grace every stop uses: `StopTask`, a scale-down, the sidecars left running after an essential container exits. `StopTask` returns while the containers are still stopping, as on ECS, and they stop in reverse `dependsOn` order. On Kubernetes the Pod's `terminationGracePeriodSeconds` is the longest `stopTimeout` in the task
- `volumeConfigurations[]` (per-task EBS attachments) -> volume per task with size, encryption, and FS type recorded on the task's `attachments[]` and bind-mounted at the declared `mountPoint`

### Container restart policies

A container definition's `restartPolicy` (regular and daemon task definitions alike) restarts the container in place when it exits, without replacing the task, following the ECS agent's rules:

- Any exit code is restarted, `0` included, unless it is listed in `ignoredExitCodes`.
- The container must have run for `restartAttemptPeriod` seconds (default 300) since it last started or restarted; a container that exits sooner is not restarted.
- Nothing is restarted while the task is stopping.
- A container that is not restarted exits as it would without a policy: an essential one stops the task.

`RegisterTaskDefinition` and `RegisterDaemonTaskDefinition` reject a policy without `enabled`, more than 50 `ignoredExitCodes`, or a `restartAttemptPeriod` outside 60-1800 seconds with `ClientException`.

On Docker and Podman the exited container is started again (`start` on the same container), so it keeps its container ID (`runtimeId`), mounts, network namespace and published ports, and the task's ENI IP and load balancer targets stay valid; an `awsvpc` container that publishes its own ports has its ENI targets pointed at the ports it gets on restart. The task metadata endpoint v4 reports the container's `RestartCount` (only for containers with a restart policy, as on ECS); `DescribeTasks` has no restart count field, and shows the container `RUNNING` throughout.

On Kubernetes, where a single container in a Pod can only be restarted by the kubelet, a container with a restart policy becomes a native sidecar (an initContainer with `restartPolicy: Always`) when the task also has an essential long-running container without a policy and the cluster runs native sidecars (Kubernetes 1.29 or later, detected from the API server version at startup; assumed when the version can't be read, and turned off if the API server refuses a sidecar Pod, which is then recreated without them). Otherwise (every long-running container has a policy, the only ones without are non-essential, or the cluster is older) the Pod's own `restartPolicy` is `Always`. The kubelet restarts such containers on every exit, with its own backoff; fakecloud applies the ECS rules to each exit it observes, counting the restarts ECS would make in `RestartCount` and treating an exit ECS would not restart (an ignored exit code, one before `restartAttemptPeriod`, or any exit of a container without a policy) as the container stopping, which stops the task when the container is essential. A non-essential container is still restarted by the kubelet after such an exit, though ECS considers it stopped; under a Pod `restartPolicy` of `Always` this includes a non-essential one-shot container without a policy, which ECS runs once. A container that is the target of a `COMPLETE`/`SUCCESS` dependency runs once as an initContainer and is never restarted; when the Pod's `restartPolicy` is `Always`, its first non-zero exit stops the task with the dependency's reason (the kubelet would otherwise retry it).

### Stopping tasks

A stopping task goes through the ECS task lifecycle: `DEACTIVATING` while it leaves its load balancer target groups (deregistered before any container is stopped; the target group's deregistration delay is not waited out), `STOPPING` while its containers stop, `DEPROVISIONING` while its ENI is released, then `STOPPED`. A task stopped by `StopTask` keeps stop code `UserInitiated` and its reason; one the service scheduler stops (a scale-down, `DeleteService`) gets `ServiceSchedulerInitiated` with "Scaling activity initiated by (deployment <id>)". Containers stop in reverse `dependsOn` order, each with its own `stopTimeout`, and containers with no dependency between them stop in parallel. Services' `runningCount` counts only `RUNNING` tasks, while a stopping task still keeps a deleted service `DRAINING` and blocks `DeleteCluster`. The introspection force-stop and a reset don't wait out any `stopTimeout`: they request the stop (so no restart policy brings a container back), kill the containers (on Kubernetes, delete the Pod with no grace period), and answer once the task is finalized: the force-stop waits for that (up to 60 seconds), a reset a few seconds at most, after which the task still finalizes and releases its load balancer targets and ENI in the background. A CloudFormation stack deletion that removes a service or cluster scales its services to zero (so the scheduler doesn't replace them) and starts stopping its running tasks first (deregistering their load balancer targets while the service still says which they are), then stops their containers gracefully in the background, and drops the task records only once each task's ENI is released.

### Container dependencies and startTimeout

`dependsOn[]` conditions gate each container's start: `START` once the upstream has started, `COMPLETE` once it exited, `SUCCESS` once it exited 0, `HEALTHY` once its health check passes (immediately when it has none). As on ECS, a dependent waiting on `COMPLETE`, `SUCCESS` or `HEALTHY` gives up when the upstream has been running for the upstream's `startTimeout`: the dependent never starts and the task stops with `TaskFailedToStart` and a `stoppedReason` naming both containers, the condition and the timeout. `RegisterTaskDefinition` rejects a negative `startTimeout` or `stopTimeout`, and one outside 2-120 seconds when `requiresCompatibilities` includes `FARGATE`, with `ClientException`. Without a `startTimeout` on the upstream there is no time limit (AWS documents no default for it, unlike the 30 second `stopTimeout`): the dependent waits until the condition is met, can no longer be met (a `SUCCESS` upstream that exited non-zero, a `HEALTHY` upstream that exited, which stop the task the same way), or the task is stopped. On Kubernetes a `COMPLETE`/`SUCCESS` upstream is an initContainer, and one still running past its `startTimeout` stops the task the same way; `START`/`HEALTHY` ordering between long-running containers is best-effort there.

### awslogs flush before STOPPED

Before a task transitions to `STOPPED`, the runtime drains any buffered `awslogs` events for that task synchronously, so `GetLogEvents` immediately after a `DescribeTasks` STOPPED response sees every line the container emitted. No probabilistic delay, no flake-prone polling in tests.

Without a container runtime (docker/podman missing), `RunTask` still returns tasks but they immediately transition to `STOPPED` with `stopCode=TaskFailedToStart`. This keeps the API surface shape-correct so tests on CI agents without Docker can still drive the control-plane surface.

### Pulling from fakecloud ECR

Tasks that reference AWS private-ECR URIs (`<account>.dkr.ecr.<region>.amazonaws.com/<repo>:<tag>`) are resolved against fakecloud's own OCI v2 endpoint. The runtime pulls from `127.0.0.1:<port>/<repo>:<tag>`, retags to the AWS URI, and runs the container under the user-visible image name. The pull is performed by the container engine on the host, so it uses loopback even when fakecloud itself runs in a container (`FAKECLOUD_IN_CONTAINER=1`) -- the host alias containers use does not resolve on a Linux host, and Docker Desktop and OrbStack only accept a plain-HTTP registry over loopback. Podman on macOS or Windows pulls inside its machine VM, where only `host.containers.internal` reaches the host, so that is used there. Set `FAKECLOUD_ECR_REGISTRY_HOST` to override the registry host (for example when a containerized fakecloud's port is published under another address). The registry serves plain HTTP. Docker treats `127.0.0.1` as an insecure registry on its own. Podman does not, so under the podman backend (including a `docker` CLI that is really podman, such as the `podman-docker` shim) fakecloud pulls its own registry with `--tls-verify=false` (only for rewritten ECR URIs; upstream registries keep TLS verification).

Same resolution path applies to Lambda functions deployed with `PackageType=Image`: `Code.ImageUri` pointing at a fakecloud ECR URI is pulled and run on invoke.

### awslogs -> CloudWatch Logs

Container definitions that declare the `awslogs` log driver get their captured stdout/stderr forwarded to fakecloud's CloudWatch Logs service:

```json
"logConfiguration": {
  "logDriver": "awslogs",
  "options": {
    "awslogs-group": "/ecs/my-service",
    "awslogs-stream-prefix": "app",
    "awslogs-region": "us-east-1",
    "awslogs-create-group": "true"
  }
}
```

The runtime creates the log group on demand when `awslogs-create-group=true`, creates a stream named `<prefix>/<container-name>/<task-id>`, and appends one `LogEvent` per captured line. The usual fakecloud-logs API (`DescribeLogStreams`, `GetLogEvents`, subscription filters) then sees the container's output with no additional wiring.

### loadBalancers -> ELBv2 RegisterTargets

Services declared with `loadBalancers[]` register each task's primary IP (awsvpc) or container-instance/container-port pair (bridge/host) into the matching ELBv2 target group via `RegisterTargets` when the task transitions to `RUNNING`, and `DeregisterTargets` when it drains. The cutover happens inline as part of the rolling deployment so health-check and target-state assertions on the target group match what real ECS would produce.

### EventBridge task state change events

Task state transitions fire `aws.ecs` / `ECS Task State Change` events on the default EventBridge bus. Event `detail` carries the task ARN, cluster ARN, last status, stop code / reason on STOPPED, and a summary of each container including exit code. Rules matching `source: aws.ecs` receive these events and can route to SQS, SNS, Lambda, Step Functions — the standard target fan-out.

### Task role credentials

Tasks whose task definition (or a `RunTask` `overrides.taskRoleArn`) names a `taskRoleArn` get the credentials the way they do on ECS: every container gets `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI=/v2/credentials/<task-id>`, and `http://169.254.170.2` inside the task's network answers it. An unmodified AWS SDK or CLI in the container resolves the task role through its default credential chain, and so do scripts that call `curl 169.254.170.2$AWS_CONTAINER_CREDENTIALS_RELATIVE_URI`. Tasks without a task role get no credentials URI, as on AWS. The endpoint returns the container-credentials JSON the ECS agent serves:

```json
{
  "AccessKeyId": "FSIA...",
  "SecretAccessKey": "...",
  "Token": "...",
  "Expiration": "2026-04-24T12:00:00Z",
  "RoleArn": "arn:aws:iam::123456789012:role/app-task-role"
}
```

The credentials are a real session for the task role, named after the task ID like on AWS: `GetCallerIdentity` returns `arn:aws:sts::<account>:assumed-role/<role>/<task-id>`. They are minted and registered like an `AssumeRole` session, so they verify under `--verify-sigv4` and, under `--iam`, are evaluated against the role's policies. Refetches return the same set until it nears expiry, then a fresh one. When the task stops its credentials are revoked, and the endpoint answers a stopped, unknown or role-less task the way the ECS agent does: HTTP 400 with `{"code":"InvalidIdInRequest","message":"CredentialsV2Request: Credentials not found","HTTPErrorCode":400}`.

A container that falls back to `AWS_CONTAINER_CREDENTIALS_FULL_URI` also gets `AWS_CONTAINER_AUTHORIZATION_TOKEN`, a per-task token the AWS SDKs send as the `Authorization` header. Under `--iam strict` the full-URI endpoint (`/_fakecloud/ecs/creds/<task-id>`) requires it, so knowing a task ID is not enough to obtain its role's credentials there; a request without the task's token gets HTTP 401 `{"code":"AccessDenied","message":"CredentialsV2Request: Authorization token missing or invalid for this task","HTTPErrorCode":401}`. The agent's relative-URI surface (`169.254.170.2/v2/credentials/<task-id>`) takes no token, as on ECS, where only the task's own network reaches that address (and several SDKs never send a token with a relative URI). Under `--iam strict` fakecloud reproduces that boundary instead: on its main port it answers that surface only to peers on a container network (private bridge / pod addresses such as `172.17.0.0/12`, `10.0.0.0/8`, `192.168.0.0/16`, `100.64.0.0/10`, IPv6 ULA / link-local), which is where a task's NAT'd requests come from, and refuses other clients (the host's own loopback, for one) with the same 401. The `--imds-link-local` listener is unaffected. In the default and `soft` modes the token is not required. The introspection SDKs' task-credentials helpers call the full-URI endpoint without a token, so under `--iam strict` they receive that 401.

How `169.254.170.2` reaches fakecloud: before a task-role container starts, its network namespace gets a NAT rule (nftables, or iptables) that sends `169.254.170.2:80` to fakecloud's listener, which serves the agent's `/v2/credentials/<task-id>` path to requests addressed to that host. Nothing listens on port 80 inside the task, so the app keeps every port to itself.

- **Network modes:** `bridge`, `host` and `awsvpc` tasks all get the route (fakecloud's Docker backend emulates `host` on a bridge whose published ports are the task's host ports, never in the host's own namespace). A `none`-mode task has no network, so as on ECS it gets the relative URI with nothing reachable behind it.
- **Docker / Podman:** a small holder container (the counterpart of the ECS agent's pause container), started with `NET_ADMIN`, owns each task-role container's network namespace. It carries the container's network settings (task network, published ports, host alias, `net.*` sysctls), installs the rule with `NET_ADMIN`, and the app container joins it (`--network container:<holder>`), so the rule is in place before the app starts. The holder runs a helper image fakecloud builds once per host (Alpine + `nftables`, tagged `fakecloud-ecs-creds-helper:<hash>`) and is removed with the task.
- **Kubernetes:** a first initContainer (`fakecloud-ecs-creds`, with `NET_ADMIN`) installs the rule in the Pod's network namespace, pointed at `FAKECLOUD_K8S_SELF_URL`, and exits before any task container runs. Its default image is `public.ecr.aws/docker/library/alpine:3.20`, which installs `nftables` at start (so the cluster needs egress to the Alpine package mirror, or use the override below).

Set `FAKECLOUD_ECS_CREDS_HELPER_IMAGE` to use your own helper image on either backend (for a private mirror or an air-gapped cluster); it needs `sh`, `getent`, `awk`, and `nft` or `iptables`.

If the namespace can't be set up (no helper image, `NET_ADMIN` refused by the runtime or by a Pod Security admission policy, no NAT support), fakecloud logs a warning and runs the task anyway with `AWS_CONTAINER_CREDENTIALS_FULL_URI=http://host.docker.internal:<port>/_fakecloud/ecs/creds/<task-id>` (the in-cluster fakecloud URL on Kubernetes) instead. Code that fetches that URL directly still gets the credentials, but the AWS SDKs only accept a plain-HTTP full URI on a loopback or link-local host, so an SDK credential chain refuses it.

`RegisterTaskDefinition` (and `RunTask` role overrides, and CloudFormation `AWS::ECS::TaskDefinition`) refuses a `taskRoleArn` or `executionRoleArn` whose trust policy does not let `ecs-tasks.amazonaws.com` assume it with ECS's `ClientException` ("ECS was unable to assume the role '...' that was provided for this task. ..."). Under `--iam strict` a role from another account is refused the same way; `--iam soft` logs it to the IAM audit target and allows it.

### Volumes + mount points

Task-definition `volumes[]` and per-container `mountPoints[]` translate into real `docker run -v` flags so containers see actual files at the paths they expect. Supported volume kinds:

- **Host bind** — `volume.host.sourcePath` is bind-mounted directly. A file written by the container shows up on the host path after the task stops.
- **EFS** — `efsVolumeConfiguration.fileSystemId` resolves to a host-side stub directory under `/tmp/fakecloud/efs/<filesystemId>[/<rootDirectory>]`. Multiple tasks targeting the same filesystem id share the stub, so a writer task and a reader task can exchange data the same way they would on real EFS.
- **FSx for Windows** — `fsxWindowsFileServerVolumeConfiguration.fileSystemId` resolves to an analogous stub under `/tmp/fakecloud/fsx/<filesystemId>/<rootDirectory>`.
- **Docker named volume** — `dockerVolumeConfiguration` passes the volume name through verbatim; docker creates the named volume on first reference.
- **Log driver volume** (daemon task definitions) — `logDriverVolumeConfiguration` with `driver=json-file` mounts the container engine's json-file log directory, which the engine reports itself, so it is right when fakecloud runs in a container or the engine in a VM. On Docker that is `<DockerRootDir>/containers`: one directory per container, named with the container ID, holding `<container-id>-json.log` and its rotated files, so a daemon reads the logs of the other tasks' containers that use the json-file driver (or no `logConfiguration`, since json-file is the engine default) and not of those using another driver. That `<id>/<id>-json.log` layout is Docker Engine only. Podman has no separate json-file format (`json-file` is an alias for `k8s-file`), so on Podman the volume holds Podman's own layout, `<GraphRoot>/<driver>-containers`, whose per-container directories hold `userdata/ctr.log`. If the engine doesn't answer, the daemon task fails to start rather than mount a guessed directory.

`mountPoints[].readOnly` is honoured by appending `:ro` to the rendered `-v` flag. On the Kubernetes backend every volume (host binds, EFS/FSx, and log driver volumes included) is a Pod-local `emptyDir` shared by the task's containers, since a Pod gets no host path on the node.

### Secrets injection

Container definitions can pull secrets from SecretsManager or SSM Parameter Store via the standard `secrets[]` field:

```json
"secrets": [
  { "name": "DB_PASSWORD", "valueFrom": "arn:aws:secretsmanager:us-east-1:123456789012:secret:db-password-AbCdEf" },
  { "name": "DB_USER",     "valueFrom": "arn:aws:secretsmanager:us-east-1:123456789012:secret:db-creds-AbCdEf:username::" },
  { "name": "OLD_TOKEN",   "valueFrom": "arn:aws:secretsmanager:us-east-1:123456789012:secret:token:value:AWSPREVIOUS:" },
  { "name": "API_KEY",     "valueFrom": "arn:aws:ssm:us-east-1:123456789012:parameter/app/api-key" },
  { "name": "FEATURE",     "valueFrom": "/app/feature-flag:2" }
]
```

The runtime resolves each reference at task start, the way the ECS container agent does, and injects the values as environment variables:

- **Secrets Manager**: `valueFrom` is the secret's full ARN or partial ARN (without the random 6-character suffix), resolved in the account and region the ARN names. Append `:json-key:version-stage:version-id` (all three positions, empty ones unset) to pick a field out of a JSON secret and/or a version by staging label (`AWSCURRENT`, `AWSPREVIOUS`, custom) or version ID. A non-string JSON field is injected the way the agent renders it (`5432`, `true`, `1e+08`). A secret in another account is readable when its resource policy allows the task's execution role (a session of it named after the task: the policy can name the role, or the task's account); a task without an execution role reads as its account.
- **SSM Parameter Store**: `valueFrom` is a parameter name (in the task's account) or ARN (in the ARN's account; a parameter owned by another account must be shared with a resource policy allowing the task's execution role, as for secrets), optionally with a `:version` or `:label` selector. `SecureString` values are injected decrypted.

A reference that does not resolve (missing secret, version or JSON key, missing parameter, access denied) fails the task with `stopCode=TaskFailedToStart` and a `stoppedReason` in ECS's form, for example `ResourceInitializationError: unable to pull secrets or registry auth: execution resource retrieval failed: unable to retrieve secret from asm: retrieved secret from Secrets Manager did not contain json key password`.

## Protocol

JSON protocol over `POST /`, with `X-Amz-Target: AmazonEC2ContainerServiceV20141113.<Action>`. Request + response bodies are JSON; tags use lowercase `key` / `value` (matches AWS SDK serialization).

## Introspection

Endpoints bypass the public AWS API so tests can assert deterministic state without pagination or role-assumption noise.

| Endpoint | Method | Purpose |
|---|---|---|
| `/_fakecloud/ecs/clusters` | GET | Dump every cluster across all accounts |
| `/_fakecloud/ecs/tasks` | GET | Dump every task; filter with `?cluster=` / `?status=` |
| `/_fakecloud/ecs/tasks/{taskId}` | GET | Single-task deep detail |
| `/_fakecloud/ecs/tasks/{taskId}/logs` | GET | Captured docker stdout/stderr + exit code |
| `/_fakecloud/ecs/tasks/{taskId}/force-stop` | POST | Stop the task (recorded like `StopTask`, so a restart policy doesn't restart it), killing its containers at once, and answer once it is `STOPPED` (waiting up to 60 seconds) |
| `/_fakecloud/ecs/tasks/{taskId}/mark-failed` | POST | Flip to STOPPED without killing the container (inject exit code + reason) |
| `/_fakecloud/ecs/events` | GET | Replay the lifecycle event log |
| `/_fakecloud/ecs/metadata/{taskArn}` | GET | Aggregated v4 metadata dump keyed by full task ARN (URL-encode it) |

### Task metadata by ARN

`GET /_fakecloud/ecs/metadata/{taskArn}` returns the same shape a container would see at `ECS_CONTAINER_METADATA_URI_V4`, but addressable from outside the container by full task ARN -- handy when a test holds the `RunTask` response and wants to assert what the in-container SDK metadata loader would observe.

The path segment must be URL-encoded (`arn:aws:ecs:us-east-1:000000000000:task/cluster/<id>` -> `arn%3Aaws%3Aecs%3Aus-east-1%3A000000000000%3Atask%2Fcluster%2F<id>`). SDK helpers (`getEcsTaskMetadata` / `get_task_metadata`) handle the encoding for you.

```json
{
  "task": {
    "cluster": "prod",
    "taskArn": "arn:aws:ecs:us-east-1:000000000000:task/prod/abc123",
    "family": "web",
    "revision": 4,
    "desiredStatus": "RUNNING",
    "knownStatus": "RUNNING",
    "containers": [
      {
        "name": "app",
        "image": "nginx:1.27",
        "imageId": "sha256:...",
        "ports": [{"containerPort": 8080, "hostPort": 32768, "protocol": "tcp"}],
        "labels": {},
        "desiredStatus": "RUNNING",
        "knownStatus": "RUNNING",
        "limits": {"cpu": 0.25, "memory": 512},
        "createdAt": "2026-05-11T10:00:00Z",
        "startedAt": "2026-05-11T10:00:02Z"
      }
    ],
    "pullStartedAt": "2026-05-11T10:00:00Z",
    "pullStoppedAt": "2026-05-11T10:00:01Z",
    "availabilityZone": "us-east-1a",
    "launchType": "FARGATE",
    "vpcId": "vpc-fakecloud",
    "eniId": "eni-..."
  }
}
```

All endpoints are sorted deterministically (by ARN for clusters/tasks, by timestamp for events) so test assertions don't flake on map iteration order.

### Clusters dump

```json
{
  "clusters": [
    {
      "clusterName": "prod",
      "clusterArn": "arn:aws:ecs:us-east-1:111122223333:cluster/prod",
      "status": "ACTIVE",
      "runningTasksCount": 0,
      "pendingTasksCount": 0,
      "activeServicesCount": 0,
      "registeredContainerInstancesCount": 0,
      "capacityProviders": ["FARGATE"],
      "tags": [{"key": "env", "value": "prod"}],
      "createdAt": "2026-04-23T23:00:00+00:00"
    }
  ]
}
```

## SDK usage (testing helper)

The fakecloud client SDKs ship typed wrappers for every introspection endpoint. Use them instead of poking `/_fakecloud/*` paths by hand.

```go
// Go
clusters, _ := fakecloud.New("http://localhost:4566").ECS().GetClusters(ctx)
```

```python
# Python
async with FakeCloud() as fc:
    clusters = await fc.ecs.get_clusters()
```

```typescript
// TypeScript
const fc = new FakeCloud();
const { clusters } = await fc.ecs.getClusters();
```

```rust
// Rust
let fc = FakeCloud::new("http://localhost:4566");
let clusters = fc.ecs().get_clusters().await?;
```

## Source

- [`crates/fakecloud-ecs`](https://github.com/faiscadev/fakecloud/tree/main/crates/fakecloud-ecs)
- [AWS ECS API reference](https://docs.aws.amazon.com/AmazonECS/latest/APIReference/Welcome.html)
