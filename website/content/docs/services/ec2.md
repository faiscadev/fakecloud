+++
title = "EC2"
description = "Amazon EC2 — the full 769-operation control plane. VPCs, subnets, security groups, instances, EBS, AMIs, transit gateways, VPN, IPAM, Verified Access, and the entire networking long tail at 100% Smithy conformance."
weight = 41
+++

fakecloud implements **769 of 769** AWS EC2 operations at 100% Smithy conformance — the complete control plane for the largest service surface in AWS. Request/response shapes, flattened `ec2Query` XML lists, field names, enum validation, and integer/length bounds are checked against AWS's own Smithy model on every commit.

## Supported features

- **Core networking** — VPCs (+ secondary CIDRs, tenancy), DHCP option sets, subnets (+ CIDR reservations), security groups (rules, references, VPC associations), route tables, internet / egress-only / NAT gateways, and elastic IPs with transfer/move flows.
- **Compute** — `RunInstances` and the full instance lifecycle (start/stop/reboot/terminate/monitor), instance attributes, credit specifications, metadata + maintenance options, instance types, and topology. Key pairs and placement groups. **Instances are backed by real Docker/Podman containers** — `RunInstances` boots a container per instance, runs your user-data at boot, and maps start/stop/reboot/terminate onto the container lifecycle (falling back to a metadata-only control plane when no container runtime is present).
- **Storage** — EBS volumes (+ modifications, recycle bin), snapshots (+ copy, tier, lock, fast restores, block-public-access), AMIs (register/copy/deprecate/deregistration-protection), and EBS encryption defaults.
- **Interfaces** — elastic network interfaces with attachments, permissions, IPv4/IPv6 address assignment, and prefix lists.
- **Edge / advanced networking** — network ACLs, VPC peering, VPC endpoints + PrivateLink (services, connection notifications), flow logs, launch templates (+ versions), spot requests / fleets / EC2 fleets, capacity reservations (+ fleets), reserved instances, dedicated hosts.
- **Transit Gateway** — the complete 74-op surface: gateways, attachments, route tables (+ associations / propagations / prefix-list refs), peering, Connect + Connect peers, policy tables, route-table announcements, multicast domains (+ group members/sources), metering policies, and Client-VPN attachments.
- **Site-to-Site & Client VPN** — customer gateways, virtual private gateways, VPN connections (+ routes / tunnels / device configs), VPN concentrators, and full Client VPN (endpoints, routes, authorization rules, target networks, connections, certificate/config export-import).
- **IPAM** — IPAMs, scopes, pools, pool CIDRs + allocations, resource CIDRs, address history, resource discovery (+ associations, discovered getters), BYOASN, BYOIP-to-IPAM, external resource verification tokens, policies (+ allocation rules / org targets), and prefix-list resolvers (+ targets / rules / versions).
- **Verified Access** — instances, trust providers (+ attach/detach), groups, endpoints, policies (group + endpoint), logging configuration, and client-config export.
- **Network Insights** — reachability paths + analyses and access scopes + scope analyses (content, findings).
- **Outpost / hybrid** — carrier gateways, CoIP pools + CIDRs, local-gateway route tables, routes, VPC + virtual-interface-group associations, virtual interfaces, and groups.
- **Access & diagnostics** — EC2 Instance Connect endpoints, fast launch, serial-console access, console output / screenshot, and password data. `GetConsoleOutput` returns the instance container's real console log (its combined stdout/stderr, including anything user-data printed at boot), base64-encoded as on AWS.
- **Cross-cutting** — tag specifications on create, `CreateTags` / `DeleteTags` / `DescribeTags`, `Filter.N` filtering, and `MaxResults` / `NextToken` pagination across every `Describe*`.

## Protocol

EC2 uses the `ec2Query` protocol: form-encoded requests and flattened-XML responses (no `<member>` wrapper, lower-camel element names, lowercase `<requestId>`, no `<Result>` envelope). Because EC2 declares no per-operation error shapes, fakecloud is lenient on not-found and validates only wire-observable negatives — missing required scalars, invalid enum values, out-of-range integers / `MaxResults`, and bad lengths.

## Introspection

- `GET /_fakecloud/ec2/instances` — list every fakecloud-managed EC2 instance across all account partitions with its control-plane metadata (`instanceId`, `imageId`, `instanceType`, `state`, `privateIp`, `publicIp`, `subnetId`, `vpcId`, `keyName`, `securityGroupIds`, `availabilityZone`, `launchTime`) plus `containerId` — the backing Docker container id or Kubernetes Pod name, or `null` when the instance runs metadata-only (no container runtime). Lets tests assert on instance state (and which container backs it) without re-parsing `DescribeInstances` XML. Exposed through every fakecloud introspection SDK — `fc.ec2().getInstances()` (Go/Java/TS/PHP), `fc.ec2.get_instances()` (Python), `fc.ec2().get_instances()` (Rust).

## Known limitations

- **Instances run as containers, not VMs** — `RunInstances` boots a real container per instance (Amazon Linux by default, overridable via `FAKECLOUD_EC2_DEFAULT_IMAGE`) and runs user-data at boot, but it is a container, not a full virtual machine: no kernel modules, no nested virtualization, and the instance type / EBS sizing is metadata only. The container runs as a local Docker/Podman container by default, or as a **native Kubernetes Pod** when `FAKECLOUD_EC2_BACKEND=k8s` (or the global `FAKECLOUD_CONTAINER_BACKEND=k8s`) is set — mirroring the Lambda/ECS/RDS/ElastiCache backends, so fakecloud running inside Kubernetes needs no Docker daemon. On the k8s backend a stopped instance's Pod is deleted and recreated on start (instances are not persistent disks). When no container runtime is available at all, the control plane degrades to a metadata-only instance so every API call still succeeds.
- **A handful of model operations are absent from the vendored AWS SDK** (`DescribeIpamPoolAllocations`, `ModifyIpamPoolAllocation`, the capacity-reservation cancellation-quote pair, and the `AttachImageWatermark`/`DetachImageWatermark` AMI watermark pair). They are implemented and conformance-probed via raw `ec2Query`, and graduate to typed SDK calls on the next SDK refresh.
- Security-group rules and network ACLs are stored and returned faithfully but are **not enforced** against instance traffic (there is no data plane to police).
