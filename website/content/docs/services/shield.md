+++
title = "AWS Shield"
description = "AWS Shield / Shield Advanced on fakecloud: the full 36-operation control plane -- protections and protection groups, the annual Shield Advanced subscription, emergency contacts, DRT access, proactive engagement, application-layer automatic response, health-check association, and tags -- with account-partitioned persistence and honest (non-synthetic) attack surfacing."
weight = 74
+++

fakecloud implements **AWS Shield** and **Shield Advanced** (`shield`), the AWS
managed DDoS protection service. All **36 operations** from the AWS Smithy model
ship now, backed by account-partitioned state that persists across restarts in
persistent mode. The wire protocol is awsJson1.1 (x-amz-target
`AWSShield_20160616.<Op>`), signing as `shield`. Shield is a global service, so
its ARNs carry an empty region field (`arn:aws:shield::<account>:...`).

This is a faithful **control plane**: every operation is real, validated,
persisted CRUD -- no stubbed success responses. Requests are validated against
the model's `required` / `length` / `range` / `enum` constraints before any
handler runs, and errors are AWS-shaped and drawn from each operation's declared
Smithy error set (`ResourceNotFoundException`, `InvalidParameterException`,
`ResourceAlreadyExistsException`, `LockedSubscriptionException`,
`InvalidResourceException`, `LimitsExceededException`,
`NoAssociatedRoleException`, ...).

Attack surfacing is **honest**: with no real DDoS traffic to observe,
`ListAttacks` returns an empty attack list and `DescribeAttackStatistics`
returns a zeroed time-series over the requested window. No synthetic attacks are
fabricated.

## Supported features

- **Protections** (`CreateProtection`, `DescribeProtection`, `DeleteProtection`,
  `ListProtections`). `CreateProtection` registers a protection for a supported
  resource ARN (CloudFront distribution, Route 53 hosted zone, Elastic IP,
  Classic / Application load balancer, Global Accelerator), mints a 36-char
  `ProtectionId` and an `arn:aws:shield::<account>:protection/<id>` ARN, and
  round-trips via `Describe` (by `ProtectionId` or `ResourceArn`) and `List`
  (with `InclusionFilters` on resource ARN / name / type). A resource may carry
  at most one protection (`ResourceAlreadyExistsException`); a value that is not
  an ARN is rejected with `InvalidResourceException`.
- **Protection groups** (`CreateProtectionGroup`, `UpdateProtectionGroup`,
  `DeleteProtectionGroup`, `DescribeProtectionGroup`, `ListProtectionGroups`,
  `ListResourcesInProtectionGroup`) with `Aggregation` (`SUM` / `MEAN` / `MAX`),
  `Pattern` (`ALL` / `ARBITRARY` / `BY_RESOURCE_TYPE`), an optional
  `ResourceType`, and member resource ARNs.
- **Subscription** (`CreateSubscription`, `DescribeSubscription`,
  `UpdateSubscription`, `DeleteSubscription`, `GetSubscriptionState`).
  `CreateSubscription` starts a one-year auto-renewing Shield Advanced
  subscription; `GetSubscriptionState` returns `ACTIVE` / `INACTIVE`;
  `DescribeSubscription` returns the full `SubscriptionLimits` (per-resource-type
  protection limits + protection-group limits) and the subscription time range.
  `UpdateSubscription` toggles `AutoRenew`. The subscription is locked by its
  one-year commitment, so `DeleteSubscription` returns
  `LockedSubscriptionException` (or `ResourceNotFoundException` when the account
  was never subscribed), matching the live service.
- **Emergency contacts** (`UpdateEmergencyContactSettings`,
  `DescribeEmergencyContactSettings`) -- the email / phone / notes list the DDoS
  Response Team (DRT) can reach during an incident.
- **DRT access** (`AssociateDRTRole`, `DisassociateDRTRole`,
  `AssociateDRTLogBucket`, `DisassociateDRTLogBucket`, `DescribeDRTAccess`). A
  DRT role must be associated before granting log-bucket access
  (`NoAssociatedRoleException`), and at most 10 log buckets are allowed
  (`LimitsExceededException`).
- **Proactive engagement** (`AssociateProactiveEngagementDetails`,
  `EnableProactiveEngagement`, `DisableProactiveEngagement`) -- the status is
  reflected onto the stored subscription.
- **Application-layer automatic response**
  (`EnableApplicationLayerAutomaticResponse`,
  `UpdateApplicationLayerAutomaticResponse`,
  `DisableApplicationLayerAutomaticResponse`) with the `Block` / `Count`
  response action, persisted on the protection for the resource.
- **Health-check association** (`AssociateHealthCheck`,
  `DisassociateHealthCheck`) -- attaches Route 53 health-check ids to a
  protection.
- **Attacks** (`ListAttacks`, `DescribeAttack`, `DescribeAttackStatistics`) --
  honest empty surfacing (see above).
- **Tags** (`TagResource`, `UntagResource`, `ListTagsForResource`), keyed by
  resource ARN.

## Example

```python
import boto3

shield = boto3.client("shield", endpoint_url="http://localhost:8080")

# Shield Advanced subscription.
shield.create_subscription()
print(shield.get_subscription_state()["SubscriptionState"])  # ACTIVE

# Protect a CloudFront distribution.
resp = shield.create_protection(
    Name="web-frontend",
    ResourceArn="arn:aws:cloudfront::123456789012:distribution/E1EXAMPLE",
)
protection_id = resp["ProtectionId"]

protection = shield.describe_protection(ProtectionId=protection_id)["Protection"]
print(protection["ProtectionArn"])

# Group protections for aggregated detection.
shield.create_protection_group(
    ProtectionGroupId="frontend",
    Aggregation="SUM",
    Pattern="ALL",
)

# No real DDoS traffic -> empty attack list.
print(shield.list_attacks()["AttackSummaries"])  # []

shield.delete_protection(ProtectionId=protection_id)
```
