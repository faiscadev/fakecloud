+++
title = "IAM Identity Center Identity Store"
description = "AWS IAM Identity Center Identity Store (identitystore) on fakecloud: users, groups, group memberships, attribute lookups, and IsMemberInGroups. awsJson1.1."
weight = 49
+++

fakecloud implements the **AWS IAM Identity Center Identity Store**
(`identitystore`) as an awsJson1.1 control plane. **The complete 19-operation
directory surface** ships — users, groups, and the memberships that link them —
backed by account-partitioned state that persists across restarts in persistent
mode.

An Identity Store is a per-account directory keyed by `IdentityStoreId`
(`d-xxxxxxxxxx`). Real AWS provisions the store when an IAM Identity Center
instance is enabled; fakecloud creates it lazily on the first write so the
directory API is usable immediately. Nested SCIM attribute bags (`Name`,
`Emails`, `Addresses`, `PhoneNumbers`, `Photos`, `Roles`, ...) are stored as
submitted and round-trip verbatim on describe. The model's `@length` and
`@range` constraints (IdentityStoreId 1-36, ResourceId 1-47, UserName 1-128,
free-form profile attributes 1-1024, `MaxResults` 1-100, `NextToken` 1-65535)
are enforced with `ValidationException`.

## Supported now (all 19 operations)

- **Users** — `CreateUser`, `DescribeUser`, `UpdateUser`, `DeleteUser`,
  `ListUsers`, `GetUserId`. `UserName` is unique per store (duplicate ->
  `ConflictException`). `UpdateUser` applies SCIM `AttributeOperations`
  (add/replace/remove) over dotted attribute paths. `ListUsers` supports the
  legacy equality `Filters` shape and `MaxResults`/`NextToken` pagination.
- **Groups** — `CreateGroup`, `DescribeGroup`, `UpdateGroup`, `DeleteGroup`,
  `ListGroups`, `GetGroupId`. `DisplayName` is unique per store. Deleting a
  group also removes its memberships.
- **Group memberships** — `CreateGroupMembership`, `DescribeGroupMembership`,
  `DeleteGroupMembership`, `GetGroupMembershipId`, `ListGroupMemberships`,
  `ListGroupMembershipsForMember`. A membership links a `MemberId` (`{UserId}`)
  into a group; the referenced user and group must exist, and a duplicate pair
  returns `ConflictException`.
- **Attribute lookups** — `GetUserId` / `GetGroupId` resolve an
  `AlternateIdentifier`'s `UniqueAttribute` (e.g. `UserName` / `DisplayName`) to
  the resource id; unknown identifiers return `ResourceNotFoundException`.
- **Membership test** — `IsMemberInGroups` returns, for each queried group id,
  whether the member belongs to it.

## Not implemented

- `ExternalId`-based `AlternateIdentifier` lookups (no external identity
  provider is modeled), and the SCIM external-id/attribute provisioning that a
  real Sync engine would drive. The paired **SSO Admin** control plane
  (instances, permission sets, account assignments, applications) is a separate
  service.
