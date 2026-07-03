+++
title = "IAM Identity Center SSO Admin"
description = "AWS IAM Identity Center SSO Admin (sso-admin) on fakecloud: instances, permission sets, account assignments, applications, trusted token issuers, regions, and tagging. awsJson1.1."
weight = 50
+++

fakecloud implements the **AWS IAM Identity Center SSO Admin** (`sso-admin`) as
an awsJson1.1 control plane. **The complete 79-operation surface** ships,
backed by account-partitioned state that persists across restarts in persistent
mode. It signs with the `sso` SigV4 service name and speaks the
`SWBExternalService` awsJson1.1 target prefix, exactly like the real API.

Nested configuration objects (`PortalOptions`, `TrustedTokenIssuerConfiguration`,
`InstanceAccessControlAttributeConfiguration`, `Grant`, `AuthenticationMethod`,
`AuthorizedTargets`, ...) are stored as submitted and round-trip verbatim on
describe. The model's `@length`, `@range`, and enum constraints are enforced
with `ValidationException`.

## Supported now (all 79 operations)

- **Instances** — `CreateInstance`, `DescribeInstance`, `UpdateInstance`,
  `DeleteInstance`, `ListInstances`. Instance ARNs are
  `arn:aws:sso:::instance/ssoins-<16hex>`, each with an `IdentityStoreId`
  (`d-<10hex>`) and owner account.
- **Permission sets** — `CreatePermissionSet`, `DescribePermissionSet`,
  `UpdatePermissionSet`, `DeletePermissionSet`, `ListPermissionSets`, plus their
  policies: managed (`Attach`/`Detach`/`ListManagedPoliciesInPermissionSet`),
  inline (`Put`/`Get`/`DeleteInlinePolicyFromPermissionSet` — an unset inline
  policy reads back as the empty string), permissions boundary
  (`Put`/`Get`/`DeletePermissionsBoundaryFromPermissionSet`), and customer
  managed policy references
  (`Attach`/`Detach`/`ListCustomerManagedPolicyReferencesInPermissionSet`).
- **Account assignments** — `CreateAccountAssignment` /
  `DeleteAccountAssignment` return an async operation status that settles from
  `IN_PROGRESS` to `SUCCEEDED` on
  `Describe{Creation,Deletion}Status`. `List` variants (`ListAccountAssignments`,
  `ListAccountAssignmentsForPrincipal`, `ListAccountsForProvisionedPermissionSet`,
  `ListPermissionSetsProvisionedToAccount`, and the status lists) resolve real
  stored assignments.
- **Provisioning** — `ProvisionPermissionSet` returns a
  `PermissionSetProvisioningStatus` that settles to `SUCCEEDED` on describe;
  `DescribePermissionSetProvisioningStatus` /
  `ListPermissionSetProvisioningStatus`.
- **Applications** — `CreateApplication`, `DescribeApplication`,
  `UpdateApplication`, `DeleteApplication`, `ListApplications`, plus application
  assignments, access scopes, authentication methods, grants, and session /
  assignment configuration (`Put`/`Get`/`Delete`/`List` families).
- **Application providers** — `ListApplicationProviders` /
  `DescribeApplicationProvider` return the fixed AWS managed-provider catalogue.
- **Trusted token issuers** — `Create`/`Describe`/`Update`/`Delete`/`List`
  with `OIDC_JWT` configuration round-tripped verbatim.
- **Regions** — `AddRegion`, `RemoveRegion`, `DescribeRegion`, `ListRegions`
  per instance, tracking the primary region.
- **Access-control attribute configuration** —
  `Create`/`Describe`/`Update`/`DeleteInstanceAccessControlAttributeConfiguration`.
- **Tagging** — `TagResource`, `UntagResource`, `ListTagsForResource` keyed by
  `ResourceArn`.

## Not implemented

- Cross-service enforcement of assignments (fakecloud does not gate real STS /
  console access on account assignments), and the KMS-backed instance encryption
  status fields are surfaced structurally rather than driving real key
  operations.
