+++
title = "AWS Resource Access Manager"
description = "AWS Resource Access Manager (ram) on fakecloud: a complete 35-operation implementation (100% conformance) — resource shares, principal/resource associations, cross-account share invitations, managed + customer permissions with versions, resource-type catalogue, and tagging. restJson1."
weight = 61
+++

fakecloud implements **AWS Resource Access Manager (RAM)** as a restJson1
service. All **35 operations** ship with **100% conformance** against AWS's
own Smithy model, backed by account-partitioned state that persists across
restarts in persistent mode.

RAM lets an account share its resources with other AWS accounts, organizational
units, or an entire organization. fakecloud models the whole control plane:
resource shares and their associations, the cross-account invitation flow,
and the managed/customer permission lifecycle.

## Resource shares

`CreateResourceShare` mints a share ARN
(`arn:aws:ram:<region>:<account>:resource-share/<uuid>`) and can attach
resource ARNs, principals, and permission ARNs in the same call — each becomes
a `ResourceShareAssociation`. `GetResourceShares` honours the `resourceOwner`
filter (`SELF` vs `OTHER-ACCOUNTS`), `UpdateResourceShare` changes the name and
`allowExternalPrincipals`, and `DeleteResourceShare` tears the share down.
Shares are created `ACTIVE`.

## Associations settle immediately

`AssociateResourceShare` / `DisassociateResourceShare` add or remove resource
and principal associations, which settle straight from `ASSOCIATING` to
`ASSOCIATED` (mirroring AWS's eventual consistency without the wait).
`GetResourceShareAssociations` returns them filtered by `associationType`
(`RESOURCE` / `PRINCIPAL`), share ARN, or associated entity.

## Cross-account invitations

When a share adds a principal that is an external account, RAM raises a
`PENDING` `ResourceShareInvitation`
(`arn:aws:ram:<region>:<account>:resource-share-invitation/<uuid>`).
`GetResourceShareInvitations` lists them; `AcceptResourceShareInvitation` and
`RejectResourceShareInvitation` move them to `ACCEPTED` / `REJECTED`, and
`ListPendingInvitationResources` enumerates the resources on a pending
invitation.

## Managed and customer permissions

RAM ships a seeded catalogue of **AWS-managed default permissions**
(`AWSRAMDefaultPermissionSubnet`, `AWSRAMDefaultPermissionTransitGateway`,
`AWSRAMDefaultPermissionResolverRule`, `AWSRAMDefaultPermissionPrefixList`,
`AWSRAMDefaultPermissionLicenseConfiguration`, ...), each `ATTACHABLE` and
`AWS_MANAGED`. Customer-managed permissions have a full version lifecycle:
`CreatePermission`, `CreatePermissionVersion`, `ListPermissionVersions`,
`SetDefaultPermissionVersion`, `DeletePermissionVersion`, `DeletePermission`,
`GetPermission`. `AssociateResourceSharePermission` /
`DisassociateResourceSharePermission` / `ReplacePermissionAssociations` bind
permissions to shares, and `ListResourceSharePermissions` /
`ListPermissionAssociations` / `ListReplacePermissionAssociationsWork` report
the bindings.

## Discovery + organization sharing

`ListResourceTypes` returns the shareable resource types with their service
names and regional scope; `ListPrincipals` and `ListResources` enumerate the
principals and resources reachable through your shares (with the `resourceOwner`
filter); `GetResourcePolicies` returns the policy documents.
`EnableSharingWithAwsOrganization` returns `returnValue: true`, and
`PromotePermissionCreatedFromPolicy` / `PromoteResourceShareCreatedFromPolicy`
model the policy-to-managed promotion path.

## Validation + persistence

Model-derived `@required` / `@length` / `@range` / `@enum` constraints are
enforced, so malformed requests get the same `InvalidParameterException` /
`MalformedPolicyDocumentException` / `UnknownResourceException` AWS returns.
All state is account-partitioned and persisted across restarts in persistent
mode.
