+++
title = "Amazon MQ"
description = "Amazon MQ (mq) on fakecloud: a complete 25-operation control plane (100% conformance) - brokers with a real ActiveMQ/RabbitMQ lifecycle, configurations, users, and tagging. restJson1."
weight = 69
+++

fakecloud implements **Amazon MQ** as a restJson1 service. All **25 operations**
ship with **100% conformance** against AWS's own Smithy model, backed by
account-partitioned state that persists across restarts in persistent mode.

Amazon MQ on fakecloud is a faithful **control plane**: every `Create`/`Update`
is reflected by its `Describe`/`List`, every `Delete` deletes, and AWS's
asynchronous broker lifecycle is modelled - not faked. No real message-broker
process is spawned (fakecloud models the AWS management API, not the
AMQP/OpenWire/STOMP/MQTT data plane).

## Resources

- **Brokers** - `CreateBroker` returns a `b-`-prefixed id and its
  `arn:aws:mq:<region>:<account>:broker:<name>:<id>` ARN with a `brokerState` of
  `CREATION_IN_PROGRESS`, settling to `RUNNING` on the next `DescribeBroker` (an
  interrupted transition reconciles on restart). `creatorRequestId` is honoured
  as the idempotency token. `DescribeBroker` reports the engine, deployment
  mode, security groups, auto-assigned subnets, encryption options,
  maintenance window, logs, current/pending configuration, the derived per-user
  summary, and - once `RUNNING` - the `brokerInstances` list with real per-engine
  wire endpoints:
  - **ActiveMQ**: OpenWire (`ssl://<id>-1.mq.<region>.amazonaws.com:61617`),
    AMQP (`amqp+ssl://...:5671`), STOMP (`stomp+ssl://...:61614`), MQTT
    (`mqtt+ssl://...:8883`), WSS (`wss://...:61619`), and the web console
    (`https://...:8162`). `ACTIVE_STANDBY_MULTI_AZ` yields two instances.
  - **RabbitMQ**: `amqps://<id>.mq.<region>.amazonaws.com:5671` plus the console.
- **Broker lifecycle** - `RebootBroker` moves the broker to
  `REBOOT_IN_PROGRESS` and, on the next describe, applies every staged pending
  change (engine version, host instance type, security groups, authentication
  strategy, logs, and the pending configuration - the old current configuration
  is pushed to `history`) before returning to `RUNNING`. `UpdateBroker` stages
  those pending changes. `DeleteBroker` moves the broker to
  `DELETION_IN_PROGRESS` and it is gone on the next describe. `Promote` is
  accepted for a broker (the cross-region-data-replication promotion).
- **Configurations** - `CreateConfiguration` returns a `c-`-prefixed id and its
  ARN with revision 1; `UpdateConfiguration` appends a new revision carrying the
  base64 `Data` and description. `DescribeConfiguration`,
  `DescribeConfigurationRevision` (returns the exact stored base64 `Data`),
  `ListConfigurations`, `ListConfigurationRevisions`, and `DeleteConfiguration`
  round-trip the engine type, authentication strategy, and revision history. An
  ActiveMQ broker created without an explicit configuration gets an
  auto-generated default one, mirroring AWS.
- **Users** - `CreateUser` / `UpdateUser` / `DeleteUser` are staged per broker
  with a `pendingChange` of `CREATE` / `UPDATE` / `DELETE` that is applied on the
  next reboot (exactly as AWS defers user mutations). `DescribeUser` and
  `ListUsers` report console access, groups, and the pending change.
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
interrupted lifecycle never wedges.

## CloudFormation

`AWS::AmazonMQ::Broker`, `AWS::AmazonMQ::Configuration`, and
`AWS::AmazonMQ::ConfigurationAssociation` are provisioned as real records in the
mq service state (they read back through `DescribeBroker` /
`DescribeConfiguration`). `Ref` resolves to the broker / configuration id, and
`Fn::GetAtt` exposes the broker's `Arn`, `IpAddresses`, `OpenWireEndpoints`,
`AmqpEndpoints`, `StompEndpoints`, `MqttEndpoints`, `WssEndpoints`,
`ConfigurationId`, and `ConfigurationRevision`, and the configuration's `Arn`,
`Id`, and `Revision`.

## Known limitations

No real message broker runs - fakecloud models the AWS management API, so the
broker endpoints are well-formed but do not accept live AMQP/OpenWire/STOMP/MQTT
connections. Serving a real backing ActiveMQ / RabbitMQ container is a possible
follow-on, mirroring how RDS and ElastiCache back their control planes with real
engines.
