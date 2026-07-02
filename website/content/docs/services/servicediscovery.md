+++
title = "Cloud Map"
description = "AWS Cloud Map (servicediscovery) on fakecloud: namespace control plane over HTTP, public-DNS, and private-DNS namespaces, driven by the async operation model. awsJson1.1."
weight = 47
+++

fakecloud implements **AWS Cloud Map** (`servicediscovery`), the service-discovery
and application resource registry, as an awsJson1.1 control plane. The
**namespace** control plane, **operation** tracking, and the **service** control
plane ship now, backed by account-partitioned state that persists across restarts
in persistent mode.

## Supported now (19 of 30 operations)

- **Namespaces** — `CreateHttpNamespace`, `CreatePrivateDnsNamespace`,
  `CreatePublicDnsNamespace`, `GetNamespace`, `ListNamespaces`,
  `DeleteNamespace`, `UpdateHttpNamespace`, `UpdatePrivateDnsNamespace`,
  `UpdatePublicDnsNamespace`. Each namespace carries an `Id` (`ns-...`), an ARN,
  its `Type` (`HTTP` / `DNS_PUBLIC` / `DNS_PRIVATE`), `Properties`
  (`HttpProperties.HttpName`; DNS types also get a synthesized
  `DnsProperties.HostedZoneId` and default SOA), a `ServiceCount`, and tags.
  `CreatePrivateDnsNamespace` requires a `Vpc`.
- **Operations** — `GetOperation`, `ListOperations`. Cloud Map's create/delete/
  update calls are **asynchronous**: they return an `OperationId` rather than the
  resource. fakecloud mints an `Operation` (`SUBMITTED`) whose status settles to
  `SUCCESS` when you call `GetOperation` (deterministic, no background timer), with
  a `Targets` map pointing at the affected namespace. The namespace itself is
  visible immediately via `GetNamespace` / `ListNamespaces`.
- **Services** — `CreateService`, `GetService`, `ListServices`, `UpdateService`,
  `DeleteService`, plus `GetServiceAttributes`, `UpdateServiceAttributes`,
  `DeleteServiceAttributes`. Each service carries an `Id` (`srv-...`), an ARN, its
  `NamespaceId`, `DnsConfig` (RoutingPolicy + DnsRecords), `HealthCheckConfig` /
  `HealthCheckCustomConfig`, an `InstanceCount`, and a string-map of service
  attributes. `CreateService`/`DeleteService` are synchronous (and adjust the
  parent namespace's `ServiceCount`); `UpdateService` is asynchronous (returns an
  `OperationId`). `ListServices` filters by `NAMESPACE_ID`.

`ListNamespaces`, `ListServices`, and `ListOperations` paginate with
`MaxResults` / `NextToken` and honor `Filters`. 100% conformance across every
shipped operation: all 655 generated Smithy probe variants for these 19
operations pass.

## In progress (roadmap)

The remaining Cloud Map surface lands in later batches: **instances**
(`RegisterInstance` / `DeregisterInstance` / `GetInstance` / `ListInstances` +
`GetInstancesHealthStatus` / `UpdateInstanceCustomHealthStatus`), the discovery
data plane (`DiscoverInstances` / `DiscoverInstancesRevision`), and resource
**tagging** (`TagResource` / `UntagResource` / `ListTagsForResource`).

## Example

```python
import boto3
sd = boto3.client("servicediscovery", endpoint_url="http://localhost:4566")

op = sd.create_http_namespace(Name="my-app")
# create returns an operation id; poll it to completion
status = sd.get_operation(OperationId=op["OperationId"])["Operation"]["Status"]
print(status)  # SUCCESS

ns_id = sd.get_operation(OperationId=op["OperationId"])["Operation"]["Targets"]["NAMESPACE"]
print(sd.get_namespace(Id=ns_id)["Namespace"]["Name"])  # my-app
```
