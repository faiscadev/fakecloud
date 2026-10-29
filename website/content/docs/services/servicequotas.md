+++
title = "Service Quotas"
description = "Service Quotas on fakecloud: all 26 operations, the default quotas AWS publishes for every service it lists, increase requests that raise the applied value, the Organizations quota request template, tags, automatic management and utilization reports. Opt-in enforcement (globally or per quota), applied values settable below the AWS default, and manual request approval through the introspection API."
weight = 81
+++

fakecloud implements **Service Quotas** (`servicequotas`). All **26 operations**
from the AWS Smithy model ship, backed by account-partitioned state that
persists across restarts in persistent mode. The wire protocol is awsJson1.1
(x-amz-target `ServiceQuotasV20190624.<Op>`), signing as `servicequotas`.

Quotas are not just reported. Once you switch enforcement on, EC2, IAM,
DynamoDB, KMS, S3, Lambda and Firehose read the applied value of the quotas they check
(for EC2 and VPC: security groups and their rules, VPCs, subnets, gateways,
route tables, network ACLs, network interfaces, endpoints, peering, Elastic IPs,
VPN connections and the On-Demand and Spot vCPU quotas) from Service Quotas, so
raising an adjustable one with `RequestServiceQuotaIncrease` changes what the service accepts.
Enforcement is
**opt-in**: by default nothing is enforced, so a local test suite that creates
more resources than a fresh AWS account allows keeps working. See
[Enforcement](#enforcement).

## Quota catalog

fakecloud carries every quota AWS publishes: **every service** `ListServices`
returns and **all of its default quotas** from `ListAWSDefaultServiceQuotas`,
dumped from AWS in `us-east-1`. Each quota keeps its real code, name,
description, default value, unit, adjustability, global flag, and the
`Period`, CloudWatch usage metric and quota context (per-resource quotas)
whenever AWS reports them, exactly as AWS returns them, for example `vpc`/`L-F678F1CE` VPCs per Region
(5), `ec2`/`L-1216C47A` Running On-Demand Standard instances (5 vCPUs, with its
`AWS/Usage` `ResourceCount` metric) or `iam`/`L-FE177D64` Roles per account
(1000, global).

The dump is vendored in the `fakecloud-servicequotas` crate
(`data/quotas.json.gz`) and decoded once, on first use. To refresh it from a
newer AWS listing, run `scripts/gen-service-quotas-catalog.py` (`dump` calls
the AWS CLI with your profile, `build` writes the vendored file
deterministically; see the script header). Two things the API does not
publish are kept by hand on top of the dump: which quotas a fakecloud service
can enforce (see [Enforceable quotas](#enforceable-quotas)) and the documented
maximum an increase request can be approved for (VPC and IAM quotas, see
[Increase requests](#increase-requests)).

`ListServices` returns every service and `ListAWSDefaultServiceQuotas` /
`ListServiceQuotas` every quota of a service, paginated (`MaxResults` up to
100; `ec2` alone has well over a thousand). An unknown service code or quota code
returns `NoSuchResourceException`.

## Supported features

### Quotas

- **`GetAWSDefaultServiceQuota`** / **`ListAWSDefaultServiceQuotas`** return
  the AWS default, with a default-quota ARN
  (`arn:<partition>:servicequotas:<region>::<service>/<quota>`).
- **`GetServiceQuota`** / **`ListServiceQuotas`** return the account's applied
  value, with an applied-quota ARN
  (`arn:<partition>:servicequotas:<region>:<account>:<service>/<quota>`).
  The partition follows the region (`aws-cn` for `cn-*`, `aws-us-gov` for
  `us-gov-*`), and global quotas (IAM, S3 general purpose buckets and the
  other quotas AWS marks `GlobalQuota`) leave the region empty in both forms.
  `QuotaCode` and `QuotaAppliedAtLevel` filter the list.
- Both forms carry the quota's `Description`, and `Period`, `UsageMetric` and
  `QuotaContext` when AWS publishes them.
- Every applied value is the account-level one (`QuotaAppliedAtLevel`
  `ACCOUNT`), so the `RESOURCE` filter matches nothing. A `ContextId` is an
  `IllegalArgumentException`: for an account-only quota because the quota is
  applied at the account level, and for a quota whose `QuotaContext` is
  `RESOURCE` because fakecloud does not support resource-level applied values
  (see [Known limitations](#known-limitations)).

### Increase requests

- **`RequestServiceQuotaIncrease`** returns the request as `PENDING`. By
  default it is decided straight away (with `--quota-requests manual` it waits
  for the introspection API, see [Manual request approval](#manual-request-approval)).
  It is `APPROVED` and the applied value raised,
  unless it asks for more than AWS allows. Quotas with a documented maximum
  carry it (16 security groups per network interface, 1,000 routes per route
  table, 25 managed policies per role, 10,000 roles, 700 OIDC providers, for
  example). For the two
  security-group quotas, AWS also requires that security groups per interface
  multiplied by rules per group stays at or below 1000. A request above
  either limit is `NOT_APPROVED`, and the applied value stays. A value at or
  below the current one, or a non-adjustable quota, is an
  `IllegalArgumentException`.
- **`GetRequestedServiceQuotaChange`**,
  **`ListRequestedServiceQuotaChangeHistory`** (filter by service, status and
  level) and **`ListRequestedServiceQuotaChangeHistoryByQuota`** read the
  request history, newest first. Requests are regional; global quotas' requests
  are visible from every region.
- **`CreateSupportCase`** opens a case for a `PENDING` request, moving it to
  `CASE_OPENED` (`InvalidResourceStateException` for any other status). Under
  automatic approval requests are decided on submission, so only manual
  approval leaves one to open a case for.

### Quota request template

Templates are used from the AWS Organizations management account, in
`us-east-1` only (`TemplatesNotAvailableInRegionException` elsewhere). Calls
from an account outside an organization return
`NoAvailableOrganizationException`, and calls from a member account return
`AccessDeniedException`.

- **`PutServiceQuotaIncreaseRequestIntoTemplate`**,
  **`GetServiceQuotaIncreaseRequestFromTemplate`**,
  **`DeleteServiceQuotaIncreaseRequestFromTemplate`** and
  **`ListServiceQuotaIncreaseRequestsInTemplate`** manage up to 10 entries
  (`QuotaExceededException` past that).
- **`AssociateServiceQuotaTemplate`** requires an organization with all
  features enabled (`OrganizationNotInAllFeaturesModeException` otherwise) and
  enables trusted access for `servicequotas.amazonaws.com`.
  **`GetAssociationForServiceQuotaTemplate`** reports `ASSOCIATED` or
  `DISASSOCIATED`, and **`DisassociateServiceQuotaTemplate`** detaches it.
- An associated template is applied to every account **created** in the
  organization after the association, when the account is created: each entry
  becomes an increase request in that account and region, decided on the same
  terms as `RequestServiceQuotaIncrease` (left `PENDING` under manual
  approval). Accounts that were already members, or
  that joined by invitation, are left alone.

Firehose's three throughput quotas are non-adjustable and cannot be included
in an increase request template.

### Tags

**`TagResource`**, **`UntagResource`** and **`ListTagsForResource`** tag the
caller's applied quotas by quota ARN (up to 50 tags; `aws:` keys are
reserved).

### Automatic management and utilization reports

- **`StartAutoManagement`**, **`GetAutoManagementConfiguration`**,
  **`UpdateAutoManagement`** and **`StopAutoManagement`** store the opt-in
  level and type, notification ARN and exclusion list per region.
- **`StartQuotaUtilizationReport`** / **`GetQuotaUtilizationReport`** report
  real usage for the quotas fakecloud can count, as a percentage of the applied
  value: from EC2 state (VPCs, internet gateways, egress-only internet
  gateways, security groups, network interfaces and Elastic IPs), IAM (users,
  roles, groups, customer managed policies, instance profiles, server
  certificates, OIDC providers), DynamoDB tables per region, KMS customer
  managed keys per region, S3 general purpose buckets, and Lambda function and
  layer storage per region (in GB).

## Enforcement

fakecloud enforces a quota only when you switch it on. A quota is enforced
when, in order of precedence:

1. the account has an override for that quota (`enforce` or `ignore`), else
2. there is a server-wide override for that quota, else
3. the global switch is on.

| Flag | Env var | Effect |
|---|---|---|
| `--enforce-quotas` | `FAKECLOUD_ENFORCE_QUOTAS` | Enforce every enforceable quota at its applied value |
| `--enforce-quota SERVICE/QUOTA` | `FAKECLOUD_ENFORCE_QUOTA` (comma-separated) | Enforce one quota, e.g. `--enforce-quota vpc/L-0EA8095F`. Repeatable |
| `--ignore-quota SERVICE/QUOTA` | `FAKECLOUD_IGNORE_QUOTA` (comma-separated) | Never enforce one quota, even with `--enforce-quotas`. Repeatable |
| `--quota-requests auto\|manual` | `FAKECLOUD_QUOTA_REQUESTS` | How increase requests are decided (default `auto`) |

The server refuses to start on an unknown quota code, or on `--enforce-quota`
for a quota no fakecloud service checks. The server-wide settings (the global
switch, server-wide overrides and the approval mode) come from these flags at
every start: changes made at runtime through the introspection API last until
a restart or `POST /_fakecloud/reset`, which puts them back to the flags.
Applied values, per-account overrides and requests are account data and
persist across restarts in persistent mode.

Firehose's three automatically scaled throughput quotas additionally require an
explicit applied value for the producer's account and region. Their reported AWS
defaults never throttle, even with global enforcement enabled; introspection then
reports `enforced: false` and `enforcementSource: "auto_scaling_default"`. An
explicit value equal to the default still counts as explicit. This local default
bypass does not simulate AWS's scaling latency. Usage and burst debt are not
persisted, unlike applied values and account overrides.

### Enforceable quotas

| Quota | Enforced by |
|---|---|
| Security groups per network interface (`vpc`/`L-2AFB9258`) | `CreateNetworkInterface`, `ModifyNetworkInterfaceAttribute` (`SecurityGroupsPerInterfaceLimitExceeded`); `RunInstances`, `ModifyInstanceAttribute`, Auto Scaling and CloudFormation launches (`SecurityGroupsPerInstanceLimitExceeded`) |
| Inbound or outbound rules per security group (`vpc`/`L-0EA8095F`) | `AuthorizeSecurityGroupIngress`, `AuthorizeSecurityGroupEgress`, `ModifySecurityGroupRules`, `ModifyManagedPrefixList`, CloudFormation security groups (`RulesPerSecurityGroupLimitExceeded`) |
| VPCs per Region (`vpc`/`L-F678F1CE`) | `CreateVpc`, `CreateDefaultVpc` when it re-creates a deleted default VPC (`VpcLimitExceeded`) |
| Internet gateways per Region (`vpc`/`L-A4707A72`) | `CreateInternetGateway` (`InternetGatewayLimitExceeded`) |
| Subnets per VPC (`vpc`/`L-407747CB`) | `CreateSubnet`, `CreateDefaultSubnet` when it creates one (`SubnetLimitExceeded`) |
| VPC security groups per Region (`vpc`/`L-E79EC296`) | `CreateSecurityGroup` (`SecurityGroupLimitExceeded`) |
| Route tables per VPC (`vpc`/`L-589F43AA`) | `CreateRouteTable` (`RouteTableLimitExceeded`) |
| Routes per route table (`vpc`/`L-93826ACB`) | `CreateRoute` (`RouteLimitExceeded`); `ReplaceRoute` only replaces an existing route |
| Network ACLs per VPC (`vpc`/`L-B4A6D682`) | `CreateNetworkAcl` (`NetworkAclLimitExceeded`) |
| Rules per network ACL (`vpc`/`L-2AEEBF1A`) | `CreateNetworkAclEntry`, `ReplaceNetworkAclEntry` when it adds a rule (`NetworkAclEntryLimitExceeded`) |
| NAT gateways per Availability Zone (`vpc`/`L-FE5A380F`) | `CreateNatGateway` (`NatGatewayLimitExceeded`) |
| Network interfaces per Region (`vpc`/`L-DF5E4CA3`) | `CreateNetworkInterface`, the secondary interfaces `RunInstances` creates (`NetworkInterfaceLimitExceeded`) |
| IPv4 CIDR blocks per VPC (`vpc`/`L-83CA0A9D`) | `AssociateVpcCidrBlock` (`CidrLimitExceeded`) |
| Active VPC peering connections per VPC (`vpc`/`L-7E9ECCDB`) | `AcceptVpcPeeringConnection`, for both of the connection's VPCs (`ActiveVpcPeeringConnectionPerVpcLimitExceeded`) |
| Outstanding VPC peering connection requests (`vpc`/`L-DC9F7029`) | `CreateVpcPeeringConnection` (`OutstandingVpcPeeringConnectionLimitExceeded`) |
| Gateway VPC endpoints per Region (`vpc`/`L-1B52E74A`), Interface VPC endpoints per VPC (`vpc`/`L-29B6F2EB`) | `CreateVpcEndpoint` (`VpcEndpointLimitExceeded`) |
| EC2-VPC Elastic IPs (`ec2`/`L-0263D0A3`) | `AllocateAddress` (`AddressLimitExceeded`) |
| VPN connections per region (`ec2`/`L-3E6EC3A3`) | `CreateVpnConnection` (`VpnConnectionLimitExceeded`) |
| Running On-Demand Standard, F, G and VT, Inf, P, X and High Memory instances (`ec2`/`L-1216C47A`, `L-74FC7D96`, `L-DB2E81BA`, `L-1945791B`, `L-417A185B`, `L-7295265B`, `L-43DA4232`) | `RunInstances`, `StartInstances`, Auto Scaling and CloudFormation launches (`VcpuLimitExceeded`) |
| All Standard Spot Instance Requests (`ec2`/`L-34B43A08`) | `RunInstances` with `InstanceMarketOptions.MarketType=spot`, `RequestSpotInstances` (`MaxSpotInstanceCountExceeded`) |
| Users per account (`iam`/`L-F55AF5E4`) | `CreateUser` (`LimitExceeded`, `Cannot exceed quota for UsersPerAccount: N`) |
| Roles per account (`iam`/`L-FE177D64`) | `CreateRole`, `CreateServiceLinkedRole` (`LimitExceeded`, `RolesPerAccount`); the service-linked roles an account starts with count |
| Groups per account (`iam`/`L-F4A5425F`) | `CreateGroup` (`LimitExceeded`, `GroupsPerAccount`) |
| Customer managed policies per account (`iam`/`L-E95E4862`) | `CreatePolicy` (`LimitExceeded`, `PoliciesPerAccount`) |
| Managed policies per role / user / group (`iam`/`L-0DA4ABF3`, `L-4019AD8B`, `L-384571C4`) | `AttachRolePolicy`, `AttachUserPolicy`, `AttachGroupPolicy` (`LimitExceeded`, `PoliciesPerRole` / `PoliciesPerUser` / `PoliciesPerGroup`); AWS managed and customer managed policies both count, and re-attaching an attached policy is not a new attachment |
| Server certificates per account (`iam`/`L-BF35879D`) | `UploadServerCertificate` (`LimitExceeded`, `ServerCertificatesPerAccount`) |
| OpenId connect providers per account (`iam`/`L-858F3967`) | `CreateOpenIDConnectProvider` (`LimitExceeded`, `OpenIdConnectProvidersPerAccount`) |
| Instance profiles per account (`iam`/`L-6E65F664`) | `CreateInstanceProfile` (`LimitExceeded`, `InstanceProfilesPerAccount`) |
| Role trust policy length (`iam`/`L-C07B4B0D`) | `CreateRole`, `UpdateAssumeRolePolicy` (`LimitExceeded`, `ACLSizePerRole`); characters of the trust policy, not counting white space |
| Maximum number of tables (`dynamodb`/`L-F98FE922`) | `CreateTable`, `RestoreTableFromBackup`, `RestoreTableToPointInTime`, `ImportTable`, and `UpdateTable` adding a replica (counted in the replica's region) (`LimitExceededException`) |
| Customer Master Keys (`kms`/`L-C2F1777E`) | `CreateKey`, `ReplicateKey` (counted in the replica's region) (`LimitExceededException`); customer managed keys in any key state count, including pending deletion, AWS managed keys do not |
| General purpose buckets (`s3`/`L-DC2B2D3D`) | `CreateBucket` (`TooManyBuckets`); a global quota: one applied value per account, counting buckets across all regions |
| Function and layer storage (`lambda`/`L-2ACBD22F`) | `CreateFunction`, `UpdateFunctionCode`, `PublishVersion`, `PublishLayerVersion` (`CodeStorageExceededException`); the code of every function's `$LATEST`, published version and layer version counts, container images do not |
| Firehose requests, records and MiB per second per stream (`firehose`/`L-7681C790`, `L-17D4EADE`, `L-937C879B`) | Public `PutRecord` and `PutRecordBatch` share independent per-stream budgets. Request exhaustion returns whole-call HTTP 500 `ServiceUnavailableException`; record/byte exhaustion produces ordered HTTP 200 batch-entry failures (HTTP 500 for `PutRecord`), with `Slow down.` messages. Positive record/byte allowances tolerate one boundary-crossing record and carry excess forward. Non-adjustable through AWS Service Quotas; use the local setter. See [Firehose throughput limits](/docs/services/firehose/#opt-in-direct-put-throughput-limits) |

IAM refusals are HTTP 409 with IAM's `Cannot exceed quota for <Name>: <limit>`
message. Every count quota refuses the request that would take the count past
the applied value, so with a value of N the Nth resource is created and the
next one is refused. CloudFormation stacks (and Cloud Control API) creating
these resources hit the same limits, failing the resource with the service's
error.

As on AWS, the rules quota applies to each direction separately and counts
IPv4 and IPv6 rules separately. A rule that references a security group counts
toward both. A rule that references a customer-managed prefix list counts as
the list's maximum number of entries, and one that references an AWS-managed
prefix list counts as the list's published weight (55 for the CloudFront
origin-facing list, 1 for S3 and DynamoDB), toward the list's address family.
`DescribeManagedPrefixLists` lists the AWS-managed prefix lists (owner `AWS`)
alongside the account's own.
`ModifyManagedPrefixList` honours this too: a larger `MaxEntries` that would
push a referencing group over the quota leaves the list at its old size in
`modify-failed`, with the group ids in `StateMessage`.

Each count follows the AWS quota's scope and rules:

- The VPC, security group and network ACL counts include the defaults every
  account and VPC ships with: the default VPC, each VPC's default security
  group and default network ACL, and its main route table. A new VPC's
  default security group counts toward VPC security groups per Region usage,
  but `CreateVpc` itself is only held to VPCs per Region.
- Routes per route table is enforced separately for IPv4 and IPv6 routes.
  It covers non-propagated routes (fakecloud does not propagate routes into VPC
  route tables); the table's implicit `local` route is not counted, and a route
  to a prefix list counts as the list's maximum entries (or an AWS-managed
  list's published weight).
- Rules per network ACL is enforced separately for inbound and outbound rules;
  the default deny rule (`*`) does not count.
- NAT gateways are counted per Availability Zone of their subnet, in the
  `pending`, `available` and `deleting` states. Network interfaces are also
  counted per Availability Zone: the quota is named per Region, but AWS
  enforces it per zone.
- IPv4 CIDR blocks per VPC counts the primary block and every associated
  secondary block.
- Gateway endpoints count per Region; interface and Gateway Load Balancer
  endpoints share the per-VPC quota.
- Elastic IPs count addresses from Amazon's pool; BYOIP addresses do not.
- The vCPU quotas sum the default vCPUs of `pending` and `running` instances
  of the quota's families (`DescribeInstanceTypes` reports the same vCPU
  counts), leaving out instances on a Dedicated Host. A launch takes as many
  instances as fit, down to `MinCount`, and is refused when not even
  `MinCount` fits. The Spot quota adds open and active Spot requests.

A quota scoped to a resource (per VPC, per zone, per route table, per network
ACL) reports its busiest resource as its `usage` and in utilization reports.

`ValidateSecurityGroupQuotasForInterface` exists to ask whether groups fit the
quotas, so it always answers against the applied values, enforced or not.

Every other quota is reported with `enforceable: false`; an override
on one (on or off) is refused rather than silently doing nothing.

## Introspection

`/_fakecloud/service-quotas/*` reads and changes quota state without going
through the AWS API, and is wrapped as the `serviceQuotas` sub-client in every
[SDK](/docs/sdks/). JSON is camelCase; errors are `{"error": "..."}` with 400
(bad input), 404 (unknown quota, service or request) or 409 (request already
decided). An omitted `accountId` or `region` means the server's.

| Endpoint | What it does |
|---|---|
| `GET /_fakecloud/service-quotas/quotas?accountId=&region=&serviceCode=` | With `serviceCode`, every quota of that service; without it, only the quotas a fakecloud service can enforce, the ones a usage source measures, and the ones the account or server changed (an applied value in any region, an enforcement override, or an open increase request). Each with `defaultValue`, `appliedValue`, `usage` (when fakecloud counts it), `enforceable`, `enforced` and `enforcementSource` (`not_enforceable`, `account_override`, `override`, `global`) |
| `PUT /_fakecloud/service-quotas/quotas/{service}/{quota}` | Body `{accountId?, region?, value?, enforce?}`. Sets the applied value, **even below the AWS default** (AWS never lowers a quota; a test can, to hit a limit without creating the default number of resources). `enforce`: `true` enforces, `false` ignores, `null` clears the override, absent leaves it. With `accountId` the override is for that account only, otherwise server-wide |
| `DELETE /_fakecloud/service-quotas/quotas/{service}/{quota}?accountId=&region=` | Back to the AWS default, and drops the override (the account's when `accountId` is given, else the server-wide one) |
| `GET` / `PUT /_fakecloud/service-quotas/enforcement` | `{enforceAll, overrides, accountOverrides}`; `PUT` takes `{enforceAll?, overrides?: [{serviceCode, quotaCode, accountId?, enforce}]}` (`null` clears) and applies the batch only if every entry is valid |
| `GET` / `PUT /_fakecloud/service-quotas/request-approval` | `{mode: "auto" \| "manual"}` |
| `GET /_fakecloud/service-quotas/requests?accountId=&status=` | Increase requests across accounts, newest first |
| `POST /_fakecloud/service-quotas/requests/{id}/approve` | Approve a `PENDING` or `CASE_OPENED` request, raising the applied value |
| `POST /_fakecloud/service-quotas/requests/{id}/deny` | Body `{status?}`: `DENIED` (default), `NOT_APPROVED`, `CASE_CLOSED` or `INVALID_REQUEST` |

For example, to test how an app handles `SecurityGroupsPerInterfaceLimitExceeded`
with a single group:

```sh
curl -X PUT localhost:4566/_fakecloud/service-quotas/quotas/vpc/L-2AFB9258 \
  -H 'content-type: application/json' -d '{"value": 1, "enforce": true}'
```

### Manual request approval

With `--quota-requests manual` (or `PUT .../request-approval {"mode":"manual"}`),
`RequestServiceQuotaIncrease` and template entries stay `PENDING`, so code that
polls `GetRequestedServiceQuotaChange` can be tested through every status:
open a support case with `CreateSupportCase`, then approve or deny the request
through the endpoints above. A value AWS would never approve (past the quota's
maximum or the security-group product limit) is still `NOT_APPROVED` on
submission, and approving a request that stopped being approvable while it
waited returns 409; deny it instead.

## IAM account summary and Lambda account settings

IAM `GetAccountSummary` reads its `UsersQuota`, `GroupsQuota`, `RolesQuota`,
`PoliciesQuota`, `InstanceProfilesQuota`, `ServerCertificatesQuota`,
`AttachedPoliciesPer{Role,User,Group}Quota` and `AssumeRolePolicySizeQuota`
entries from the account's applied `iam` quotas, so a quota raised here shows
up there. The other entries (policy sizes, access keys per user, ...) are fixed
AWS limits.

Lambda `GetAccountSettings` reports the applied "Concurrent executions"
(`L-B99A9384`) as `AccountLimit.ConcurrentExecutions` and the applied
"Function and layer storage" (`L-2ACBD22F`) as `AccountLimit.TotalCodeSize`,
and `PutFunctionConcurrency` keeps the unreserved pool at 100 or more against
that concurrency limit, as Lambda always does.

## Known limitations

- Only the quotas listed under [Enforceable quotas](#enforceable-quotas) are
  enforceable. Other
  quotas are reported and can be raised or lowered, but fakecloud does not
  refuse requests that go past them. Among the EC2 and VPC quotas, egress-only
  internet gateways per Region and transit gateways per account are counted
  but not enforced, as AWS documents no error code for them, and IPv6 CIDR
  blocks per VPC is not enforced because fakecloud keeps one IPv6 block per
  VPC.
- `RunInstances` does not create a network interface record for an instance's
  primary interface, so only the secondary interfaces it creates count toward
  network interfaces. Interfaces other services place in a VPC (an EFS mount
  target's, for example) count toward the quota but those services do not
  refuse a create past it.
- vCPU counts come from a table of the instance types EC2 offered when it was
  generated. While a vCPU quota is enforced, launching or requesting as Spot
  an instance type missing from the table, such as one newer than the table,
  is refused with `InvalidParameterValue`; while it is not enforced, such a
  type is accepted and counts 0 vCPUs, also when it is started later. The
  Trn, DL, HPC and Mac families' quotas are reported but not enforced.
- Lambda "Concurrent executions" (`L-B99A9384`) is not enforceable: invocations
  from event source mappings, SNS, S3, EventBridge and other services run the
  function without passing the `Invoke` concurrency gate, so fakecloud cannot
  count in-flight executions account-wide. Reserved concurrency per function is
  still enforced on `Invoke`.
- Quotas whose `QuotaContext` is `RESOURCE` (per transit gateway, per
  Connect instance, per web ACL, ...) have only their account-level applied
  value: resource-level applied values are not supported, so
  `GetServiceQuota` and `RequestServiceQuotaIncrease` refuse a `ContextId`
  (saying so) and `ListServiceQuotas` with `QuotaAppliedAtLevel` `RESOURCE`
  returns nothing.
- Default values are the ones AWS publishes in `us-east-1`, except for the
  three [Firehose throughput quotas](/docs/services/firehose/#opt-in-direct-put-throughput-limits),
  which report regional defaults. For other region-dependent defaults,
  fakecloud reports the `us-east-1` value in every region.
