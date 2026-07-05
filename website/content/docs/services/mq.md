+++
title = "Amazon MQ"
description = "Amazon MQ (mq) on fakecloud: a complete 25-operation control plane (100% conformance) plus a real, connectable ActiveMQ/RabbitMQ broker data plane backed by real containers. restJson1."
weight = 69
+++

fakecloud implements **Amazon MQ** as a restJson1 service. All **25 operations**
ship with **100% conformance** against AWS's own Smithy model, backed by
account-partitioned state that persists across restarts in persistent mode.

Amazon MQ on fakecloud is a faithful control plane **and a real data plane**:
`CreateBroker` spawns a genuine message-broker container - `apache/activemq-classic`
for ActiveMQ, `rabbitmq:3-management` for RabbitMQ - and the broker settles to
`RUNNING` only once that container actually accepts connections. `DescribeBroker`
then returns the broker's REAL reachable host and mapped ports, so a client
application genuinely connects and exchanges messages over OpenWire / AMQP /
STOMP / MQTT (ActiveMQ) or AMQP (RabbitMQ). The broker's users are injected into
the live broker's authentication and a user-supplied configuration is applied to
the running container, exactly the way RDS and ElastiCache back their control
planes with real engines.

When no container runtime (Docker/Podman) is available, MQ degrades to a
control-plane-only service: brokers still reach `RUNNING` through the in-memory
lifecycle, but no real broker container is spawned.

## Resources

- **Brokers** - `CreateBroker` returns a `b-`-prefixed id and its
  `arn:aws:mq:<region>:<account>:broker:<name>:<id>` ARN with a `brokerState` of
  `CREATION_IN_PROGRESS`, settling to `RUNNING` on the next `DescribeBroker` (an
  interrupted transition reconciles on restart). `creatorRequestId` is honoured
  as the idempotency token. `DescribeBroker` reports the engine, deployment
  mode, security groups, auto-assigned subnets, encryption options,
  maintenance window, logs, current/pending configuration, the derived per-user
  summary, and - once `RUNNING` - the `brokerInstances` list with the broker's
  REAL, connectable endpoints projected from the live backing container's host
  and mapped ports:
  - **ActiveMQ**: OpenWire (`tcp://<host>:<port>`), AMQP, STOMP, MQTT, WS, and
    the web console (`http://<host>:<port>`) - each pointing at a genuinely
    listening socket on the `apache/activemq-classic` container.
  - **RabbitMQ**: `amqp://<host>:<port>` plus the management console, backed by
    the `rabbitmq:3-management` container.

  (In the control-plane-only fallback with no container runtime, these fall back
  to the well-formed cosmetic `*.amazonaws.com` forms - identical response shape,
  synthetic values.)
- **Broker lifecycle** - `RebootBroker` moves the broker to
  `REBOOT_IN_PROGRESS`, restarts the real container applying every staged
  pending change (engine version, host instance type, security groups,
  authentication strategy, logs, and the pending configuration - the old current
  configuration is pushed to `history`, and the injected users / uploaded config
  are re-applied to the fresh container), then returns to `RUNNING`.
  `UpdateBroker` stages those pending changes. `DeleteBroker` moves the broker to
  `DELETION_IN_PROGRESS` and stops + removes its backing container. `Promote` is
  accepted for a broker (the cross-region-data-replication promotion). A broker
  persisted as `RUNNING` reconciles its backing container on restart (respawning
  it if it is gone), so its endpoint is never advertised dead.
- **Configurations** - `CreateConfiguration` returns a `c-`-prefixed id and its
  ARN with revision 1; `UpdateConfiguration` appends a new revision carrying the
  base64 `Data` and description. `DescribeConfiguration`,
  `DescribeConfigurationRevision` (returns the exact stored base64 `Data`),
  `ListConfigurations`, `ListConfigurationRevisions`, and `DeleteConfiguration`
  round-trip the engine type, authentication strategy, and revision history. An
  ActiveMQ broker created without an explicit configuration gets an
  auto-generated default one, mirroring AWS. For a running ActiveMQ broker, the
  associated configuration revision's `activemq.xml` is `docker cp`-ed into the
  container's conf dir on create/reboot so the uploaded config genuinely
  configures the live broker; RabbitMQ configuration is applied as
  `rabbitmq.conf`.
- **Users** - the broker's users take real effect on the live broker: for
  ActiveMQ they are injected as a `simpleAuthenticationPlugin` (with a permissive
  `authorizationPlugin`) so clients authenticate against them, and
  `CreateUser` / `UpdateUser` / `DeleteUser` are staged with a `pendingChange` of
  `CREATE` / `UPDATE` / `DELETE` applied on the next reboot (exactly as AWS defers
  ActiveMQ user mutations). For RabbitMQ the changes are applied immediately via
  `rabbitmqctl` (matching AWS, which applies RabbitMQ user changes without a
  reboot). `DescribeUser` and `ListUsers` report console access, groups, and any
  pending change.
- **Tags** - `CreateTags` / `DeleteTags` / `ListTags` key resource tags by broker
  or configuration ARN.
- **Metadata** - `DescribeBrokerEngineTypes` and `DescribeBrokerInstanceOptions`
  return the ActiveMQ / RabbitMQ engine-version and host-instance-option
  catalogues (filterable by engine type, host instance type, and storage type).

## Protocol

Amazon MQ uses the **restJson1** protocol: operations are routed by HTTP method
plus the `@http` URI path (`POST /v1/brokers`, `GET /v1/brokers/{BrokerId}`,
`PUT /v1/configurations/{ConfigurationId}`, ...), path labels are captured
positionally, and `@httpQuery` parameters (`maxResults`, `nextToken`,
`tagKeys`) are read from the raw query string so repeated multi-value keys
survive. The SigV4 signing name is `mq`.

## Persistence

State is account-partitioned and, in persistent mode, snapshotted to disk and
restored on startup. Any broker left mid-transition (`CREATION_IN_PROGRESS`,
`REBOOT_IN_PROGRESS`, `DELETION_IN_PROGRESS`) is reconciled on load so an
interrupted lifecycle never wedges. When a container runtime is configured, a
broker persisted as `RUNNING` is re-driven through its backing container on
restart: the persisted container is **re-attached** (preserving its message
data) if it still exists, or a fresh one is spawned if it is gone, with bounded
retries so a transient bring-up hiccup never terminally fails a healthy broker.

## CloudFormation

`AWS::AmazonMQ::Broker`, `AWS::AmazonMQ::Configuration`, and
`AWS::AmazonMQ::ConfigurationAssociation` are provisioned as real records in the
mq service state (they read back through `DescribeBroker` /
`DescribeConfiguration`). When a container runtime is configured, an
`AWS::AmazonMQ::Broker` is backed by a REAL ActiveMQ/RabbitMQ container the same
way the direct `CreateBroker` API is: the stack inserts the record
`CREATION_IN_PROGRESS` and the broker settles to `RUNNING` once its container
accepts connections (a stack delete stops + removes the container). `Ref`
resolves to the broker / configuration id, and `Fn::GetAtt` exposes the broker's
`Arn`, `IpAddresses`, `OpenWireEndpoints`, `AmqpEndpoints`, `StompEndpoints`,
`MqttEndpoints`, `WssEndpoints`, `ConfigurationId`, and `ConfigurationRevision`,
and the configuration's `Arn`, `Id`, and `Revision`. (The `Fn::GetAtt` endpoint
lists are resolved synchronously at provision time and so carry the cosmetic
forms; `DescribeBroker` returns the real connectable endpoints once the broker is
`RUNNING`.)

## Known limitations

The backing container requires a container runtime (Docker or Podman); without
one, MQ degrades to a control-plane-only service (brokers still reach `RUNNING`
via the in-memory lifecycle, but no real broker is spawned and the endpoints are
the cosmetic `*.amazonaws.com` forms). `ACTIVE_STANDBY_MULTI_AZ` ActiveMQ brokers
are backed by a single container (the endpoint list reflects it) rather than two
independent instances. TLS transport variants are served over the mapped
plaintext ports.
