+++
title = "Route 53 Resolver"
description = "Amazon Route 53 Resolver — resolver endpoints, resolver rules and VPC associations, query logging, and DNS Firewall. Real control plane validated against EC2 VPC subnets and security groups. JSON 1.1 protocol."
weight = 70
+++

fakecloud implements Amazon Route 53 Resolver's full JSON 1.1 API: all 72 operations covering resolver endpoints, resolver rules and their VPC associations, query-log configurations, DNS Firewall (rule groups, domain lists, rules, and associations), the per-VPC firewall / resolver / DNSSEC configuration singletons, Outpost resolvers, resource-based policies, and tags. 100% Smithy conformance.

This is the **hybrid-DNS control plane** for a VPC — distinct from [Route 53](@/docs/services/route53.md) (public/private hosted zones, a REST-XML service). Route 53 Resolver is `route53resolver`, an awsJson1_1 service with the target prefix `Route53Resolver`.

**Status: 100% coverage of a real control plane. DNS query forwarding/filtering at endpoints (the Resolver data plane) is not implemented — see [Known limitations](#known-limitations).**

## Supported today

- **Resolver endpoints** — `CreateResolverEndpoint` provisions an `INBOUND` or `OUTBOUND` endpoint against **real EC2 VPC subnets and security groups**: every `IpAddresses[].SubnetId` and every `SecurityGroupIds[]` is validated against fakecloud's EC2 state, all subnets must belong to the same VPC (which becomes the endpoint's `HostVPCId`), and unknown subnets/security groups are rejected with the AWS error shape. A new endpoint is reported `CREATING` and settles to `OPERATIONAL` on a fast background task (its IP addresses settle to `ATTACHED`), mirroring AWS's async create. `GetResolverEndpoint`, `ListResolverEndpoints`, `UpdateResolverEndpoint`, and `DeleteResolverEndpoint` are implemented, along with `ListResolverEndpointIpAddresses`, `AssociateResolverEndpointIpAddress`, and `DisassociateResolverEndpointIpAddress` (which enforce the two-IP-address minimum). Endpoint ids match AWS: `rslvr-in-*` / `rslvr-out-*`.
- **Resolver rules + associations** — `CreateResolverRule` supports `FORWARD` / `SYSTEM` / `RECURSIVE` / `DELEGATE` rules; a `FORWARD` rule must reference a real `OUTBOUND` resolver endpoint (validated) and carries its `TargetIps` (Ip/Ipv6/Port/Protocol). `AssociateResolverRule` binds a rule to a VPC (validated against EC2 state) and settles `CREATING` -> `COMPLETE`; `DisassociateResolverRule`, `GetResolverRuleAssociation`, and `ListResolverRuleAssociations` complete the set. A rule with live associations cannot be deleted (`ResourceInUseException`). Rule ids: `rslvr-rr-*`; association ids: `rslvr-rrassoc-*`.
- **Query logging** — `CreateResolverQueryLogConfig`, `GetResolverQueryLogConfig`, `DeleteResolverQueryLogConfig`, and `ListResolverQueryLogConfigs`, plus `AssociateResolverQueryLogConfig` / `DisassociateResolverQueryLogConfig` (which track `AssociationCount`), `GetResolverQueryLogConfigAssociation`, and `ListResolverQueryLogConfigAssociations`. A configuration with live associations cannot be deleted.
- **DNS Firewall** — full CRUD for firewall **rule groups** (`rslvr-frg-*`), **domain lists** (`rslvr-fdl-*`, with `ImportFirewallDomains` / `UpdateFirewallDomains` ADD/REMOVE/REPLACE + `ListFirewallDomains`), and **firewall rules** (single `Create/Update/Delete/ListFirewallRules` plus the `Batch*FirewallRule` batch ops) with real `ALLOW` / `BLOCK` / `ALERT` actions, `BlockResponse` (`NODATA`/`NXDOMAIN`/`OVERRIDE`), priority, and domain-list references. Rules are keyed within a group by domain-list id + Qtype (duplicates rejected). `AssociateFirewallRuleGroup` binds a rule group to a VPC with a priority and settles to `COMPLETE`; `Disassociate`/`Get`/`Update`/`ListFirewallRuleGroupAssociations` are implemented. `ListFirewallRuleTypes` returns the rule-type catalog. Deletion guards match AWS (a rule group with rules or associations, or a domain list referenced by a rule, cannot be deleted).
- **Per-VPC configuration** — `Get/Update/ListResolverConfig(s)` (autodefined-reverse `ENABLE`/`DISABLE` with `ENABLING`->`ENABLED` settle), `Get/Update/ListResolverDnssecConfig(s)` (DNSSEC validation `ENABLE`/`DISABLE` with settle), and `Get/Update/ListFirewallConfig(s)` (`FirewallFailOpen`). All key on a real VPC id.
- **Outpost resolvers** — `Create/Get/Update/Delete/ListOutpostResolvers` with `CREATING` -> `OPERATIONAL` settle (`rslvr-op-*`).
- **Resource policies** — `Put`/`Get` for `FirewallRuleGroupPolicy`, `ResolverQueryLogConfigPolicy`, and `ResolverRulePolicy` (the RAM cross-account share policies).
- **Tags** — `TagResource`, `UntagResource`, and `ListTagsForResource` keyed by resource ARN, applied at create time from each resource's `Tags`.

Real AWS id and ARN formats are used throughout (`rslvr-in-*`, `rslvr-out-*`, `rslvr-rr-*`, `rslvr-rrassoc-*`, `rslvr-qlc-*`, `rslvr-frg-*`, `rslvr-fdl-*`, `rslvr-frgassoc-*`; `arn:aws:route53resolver:<region>:<account>:<kind>/<id>`). Smithy `@length` / `@range` / enum constraints are enforced on every operation, state machines and deletion guards match AWS, and every resource type is account-partitioned, persisted, and restored across restarts.

## CloudFormation

The provisioner supports `AWS::Route53Resolver::ResolverEndpoint`, `ResolverRule`, `ResolverRuleAssociation`, `ResolverQueryLoggingConfig`, `ResolverQueryLoggingConfigAssociation`, `FirewallDomainList`, `FirewallRuleGroup`, `FirewallRuleGroupAssociation`, `FirewallConfig`, `ResolverConfig`, and `ResolverDNSSECConfig`, with `GetAtt` support (`Arn`, `ResolverEndpointId`, `IpAddressCount`, `Id`, `RuleCount`, etc.). CloudFormation-provisioned resources persist and survive a restart.

## Known limitations

- **DNS query forwarding / filtering (data plane)** — Route 53 Resolver's data plane is the actual resolution of VPC DNS queries at inbound/outbound endpoints and the enforcement of DNS Firewall rules against live queries. fakecloud implements the complete control plane (everything terraform-provider-aws and typical clients exercise), but it does **not** run a DNS forwarder that resolves or filters real traffic through resolver endpoints. Endpoints, rules, and firewall rules are stored and validated, not applied to a live query path. This is a future data-plane item; the control-plane behavior above is real and not stubbed.
