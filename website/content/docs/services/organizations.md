+++
title = "Organizations"
description = "AWS Organizations control plane — accounts, OUs, SCPs, tag policies, handshakes, delegated administrators. Real SCP enforcement across services."
weight = 31
+++

fakecloud implements **63 of 63** AWS Organizations operations at 100% Smithy conformance. SCPs (Service Control Policies) are **really enforced** as a permission ceiling across every IAM-evaluated service.

## Supported features

- **Organization lifecycle** — `CreateOrganization`, `DescribeOrganization`, `DeleteOrganization`, `EnableAllFeatures`. The management account is the only caller allowed to mutate org state, matching AWS.
- **Accounts**
  - `CreateAccount` / `CreateGovCloudAccount` run a **real async lifecycle**: the call returns immediately with a `CreateAccountStatus` whose `State` starts `IN_PROGRESS`, then transitions to `SUCCEEDED` after the background task provisions the member account, attaches the default IAM admin role (`OrganizationAccountAccessRole`), and assigns the account to the requested OU (or to the root). `DescribeCreateAccountStatus` and `ListCreateAccountStatus` reflect the real status.
  - `CloseAccount` moves the account to `SUSPENDED` and blocks subsequent control-plane calls from that account ID — the management account is protected and cannot be closed.
  - `RemoveAccountFromOrganization` detaches a member account from the org; subsequent describes flip to `NotFound`, mirroring AWS's behaviour where the account becomes a standalone payer.
  - `LeaveOrganization` lets the **calling** member account remove itself; the management account is refused with `MasterCannotLeaveOrganizationException`, and a caller that isn't a member gets `AccountNotFoundException`.
  - `DescribeAccount`, `ListAccounts`, `ListAccountsForParent` paginate the directory.
  - `InviteAccountToOrganization` + `AcceptHandshake` / `DeclineHandshake` / `CancelHandshake` / `DescribeHandshake` + `ListHandshakesForAccount` / `ListHandshakesForOrganization` run the real handshake state machine — only the invited account can accept/decline, only the inviter can cancel, and a handshake past the 15-day `ExpirationTimestamp` flips to `EXPIRED` on the next Organizations call, after which it can no longer be accepted. A responsibility transfer riding an expired handshake ends with it.
- **Organizational Units** — `CreateOrganizationalUnit`, `UpdateOrganizationalUnit`, `DeleteOrganizationalUnit`, `DescribeOrganizationalUnit`, `ListOrganizationalUnitsForParent`, `ListChildren`, `ListParents`, `ListRoots`, `MoveAccount`. The hierarchy is enforced — non-empty OUs cannot be deleted, and `MoveAccount` validates both source and destination parents.
- **StackSets auto-deployment** — a membership change (an account created, invited, moved between OUs, removed or closed) reconciles every service-managed CloudFormation stack set that has `AutoDeployment.Enabled`, so an account that joins a targeted OU is provisioned with that stack set's stacks before the Organizations call returns. See [CloudFormation](/docs/services/cloudformation/#stack-sets).
- **Policies** — `CreatePolicy`, `UpdatePolicy`, `DeletePolicy`, `DescribePolicy`, `ListPolicies`, `ListPoliciesForTarget`, `ListTargetsForPolicy`, `AttachPolicy`, `DetachPolicy`, `EnablePolicyType`, `DisablePolicyType`, `DescribeEffectivePolicy`. The full `PolicyType` enum is accepted on the list filters; the four types fakecloud manages (`SERVICE_CONTROL_POLICY`, `TAG_POLICY`, `BACKUP_POLICY`, `AISERVICES_OPT_OUT_POLICY`) can be created — others return `PolicyTypeNotAvailableForOrganizationException`, an out-of-enum value returns `InvalidInputException`. Policy documents are JSON-validated on create/update — malformed content is rejected with `MalformedPolicyDocumentException`.
- **Effective-policy validation** — `ListAccountsWithInvalidEffectivePolicy` and `ListEffectivePolicyValidationErrors` return the honest empty result: fakecloud stores only well-formed policies, so no account ever has an invalid effective policy.
- **Billing responsibility transfers** — `InviteOrganizationToTransferResponsibility` opens a handshake-backed `BILLING` transfer; `DescribeResponsibilityTransfer`, `UpdateResponsibilityTransfer` (rename), `TerminateResponsibilityTransfer` (-> `WITHDRAWN`), and `ListInboundResponsibilityTransfers` / `ListOutboundResponsibilityTransfers` operate over the transfer records, filtered by direction. One live offer per target: a second invitation to the same account, by id or by address, returns `DuplicateHandshakeException`. The handshake reports the transfer it carries as a nested `RESPONSIBILITY_TRANSFER` resource (with `TRANSFER_TYPE`, `TRANSFER_START_TIMESTAMP`, `MANAGEMENT_ACCOUNT` and `MANAGEMENT_EMAIL`), so the invited account can read what it is being offered without a second call.
- **Resource policies** — `PutResourcePolicy`, `DescribeResourcePolicy`, `DeleteResourcePolicy` for the org-wide delegation policy.
- **Service access** — `EnableAWSServiceAccess`, `DisableAWSServiceAccess`, `ListAWSServiceAccessForOrganization`, `RegisterDelegatedAdministrator`, `DeregisterDelegatedAdministrator`, `ListDelegatedAdministrators`, `ListDelegatedServicesForAccount`. Delegated-admin registration is gated on the service having `EnableAWSServiceAccess` first, matching AWS error ordering. A registered delegated administrator can run the organization's read operations on the management account's behalf -- `ListHandshakesForOrganization`, `ListAWSServiceAccessForOrganization`, `ListDelegatedAdministrators`, `ListDelegatedServicesForAccount` and `DescribeResourcePolicy` -- while every mutating operation stays management-only.
- **Tagging** — `TagResource`, `UntagResource`, `ListTagsForResource` on accounts, OUs, roots, and policies.

## SCP enforcement

Service Control Policies aren't just stored — they're a real permission ceiling. When `FAKECLOUD_IAM=strict` is on, every IAM evaluation walks up from the calling account's parent OU(s) through the root, collects the SCPs that apply at each level, and intersects them with the identity-based policy decision. The semantics match AWS:

- Same-target SCPs are **unioned** (multiple SCPs attached to the same OU/account can each grant a subset).
- Cross-level SCPs are **intersected** (a permission must be allowed at every level — root, parent OU, account — to survive).
- The management account and service-linked roles are exempt, just like AWS.
- An explicit `Deny` at any level wins.

This means a parent OU with `Deny: s3:DeleteBucket` actually blocks the call in a member account even when the member's IAM policy grants `s3:*`. The same machinery enforces TAG_POLICY constraints on `TagResource` calls when strict mode is on.

## Protocol

JSON 1.1. `X-Amz-Target: AWSOrganizationsV20161128.<Action>`.

## Bootstrap

Because Organizations sits above IAM, fakecloud exposes
`POST /_fakecloud/iam/create-admin` to seed a management-account admin
without needing existing credentials. Once the management account is
bootstrapped, the rest of the org lifecycle uses normal SigV4'd
calls.

The bootstrapped account is standalone — it joins no organization,
matching AWS. Add `"organizationId": "o-..."` to the body to enroll it
into an organization you already created, the shortcut equivalent of an
`InviteAccountToOrganization` + `AcceptHandshake` pair.

## Smoke test

```sh
fakecloud &

# Bootstrap a management-account admin.
curl -fsS -X POST http://localhost:4566/_fakecloud/iam/create-admin \
  -H 'content-type: application/json' \
  -d '{"accountId":"123456789012","userName":"admin"}'

aws --endpoint-url http://localhost:4566 organizations create-organization \
  --feature-set ALL

aws --endpoint-url http://localhost:4566 organizations create-account \
  --email dev@example.com --account-name Dev

aws --endpoint-url http://localhost:4566 organizations list-create-account-status \
  --states SUCCEEDED IN_PROGRESS

aws --endpoint-url http://localhost:4566 organizations create-policy \
  --type SERVICE_CONTROL_POLICY \
  --name DenyBucketDelete \
  --description "block deletes" \
  --content '{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Action":"s3:DeleteBucket","Resource":"*"}]}'
```

## Introspection

`GET /_fakecloud/organizations/accounts` returns every member account in the org with lifecycle state, parent OU, tags, and directly-attached SCPs — useful for asserting org shape from tests without management-account credentials (the endpoint bypasses IAM).

```sh
curl -fsS http://localhost:4566/_fakecloud/organizations/accounts | jq
```

Response shape:

```json
{
  "accounts": [
    {
      "id": "111111111111",
      "arn": "arn:aws:organizations::111111111111:account/o-abc/111111111111",
      "email": "111111111111@example.com",
      "name": "Account 111111111111",
      "status": "ACTIVE",
      "joinedMethod": "INVITED",
      "joinedTimestamp": "2026-05-11T00:00:00Z",
      "parentOuId": "r-1234",
      "organizationId": "o-abc",
      "tags": [],
      "scpAttached": []
    }
  ],
  "managementAccountId": "111111111111",
  "masterAccountId": "111111111111",
  "organizations": [
    {
      "organizationId": "o-abc",
      "arn": "arn:aws:organizations::111111111111:organization/o-abc",
      "managementAccountId": "111111111111",
      "rootId": "r-1234",
      "featureSet": "ALL"
    }
  ]
}
```

`scpAttached` lists SCPs attached directly to the account only — to resolve the full inherited set walk up the OU tree or call `DescribeEffectivePolicy`. `accounts` is empty (and the account-id fields `null`) when no organization has been created yet. `masterAccountId` mirrors `managementAccountId` for back-compat with the AWS field renamed in 2020.

`accounts` spans **every** organization in the process, each entry carrying its own `organizationId`, and `organizations` lists one entry per organization. The flat `managementAccountId`/`masterAccountId` are set only when exactly one organization exists, so a caller written against the single-organization shape keeps working; with several, read `organizations` (or each account's `organizationId`) rather than getting one arbitrary organization's answer.

Note that this overloads `null` on those two fields: before multi-organization support it meant "no organization has been created yet", and it now also means "more than one exists, so there is no single answer". Test `organizations` instead -- it is empty only when no organization exists.

The first-party SDKs wrap this:

- Rust: `fakecloud_sdk::FakeCloud::new(url).organizations().get_accounts()`
- Go: `fakecloud.New(url).Organizations().GetAccounts(ctx)`
- Python: `await fc.organizations.get_accounts()` (async) or `fc.organizations.get_accounts()` (sync)
- TypeScript: `await fc.organizations.getAccounts()`
- Java: `fc.organizations().getAccounts()`
- PHP: `$fc->organizations()->getAccounts()`

`GET /_fakecloud/organizations/responsibility-transfers` returns every billing responsibility transfer in the org with direction (INBOUND/OUTBOUND), lifecycle status, source/target management accounts, and the active handshake id.

```sh
curl -fsS http://localhost:4566/_fakecloud/organizations/responsibility-transfers | jq
```

Response shape:

```json
{
  "responsibilityTransfers": [
    {
      "id": "rt-0123456789abcdef0123456789abcdef",
      "arn": "arn:aws:organizations::111111111111:responsibilitytransfer/o-abc/rt-0123456789abcdef0123456789abcdef",
      "name": "my-billing-transfer",
      "type": "BILLING",
      "status": "REQUESTED",
      "direction": "OUTBOUND",
      "sourceManagementAccountId": "111111111111",
      "sourceManagementAccountEmail": "admin@example.com",
      "targetManagementAccountId": "222222222222",
      "targetManagementAccountEmail": "222222222222@example.com",
      "startTimestamp": "2026-05-29T00:00:00+00:00",
      "endTimestamp": null,
      "activeHandshakeId": "h-0123456789abcdef0123456789abcdef"
    }
  ]
}
```

`endTimestamp` and `activeHandshakeId` are `null` when not set; the list is empty when no organization exists. Transfers are sorted by id.

The first-party SDKs wrap this:

- Rust: `fakecloud_sdk::FakeCloud::new(url).organizations().get_responsibility_transfers()`
- Go: `fakecloud.New(url).Organizations().GetResponsibilityTransfers(ctx)`
- Python: `await fc.organizations.get_responsibility_transfers()` (async) or `fc.organizations.get_responsibility_transfers()` (sync)
- TypeScript: `await fc.organizations.getResponsibilityTransfers()`
- Java: `fc.organizations().getResponsibilityTransfers()`
- PHP: `$fc->organizations()->getResponsibilityTransfers()`

## Gotchas

- **Management account only.** Mutating calls (`CreateAccount`, `AttachPolicy`, `EnableAWSServiceAccess`, etc.) must originate from the management account. Calls from member accounts return `AccessDeniedException`, matching AWS.
- **SCP enforcement is opt-in.** SCPs are stored and `DescribeEffectivePolicy` works in all modes, but enforcement only kicks in under `FAKECLOUD_IAM=strict` (or `soft` for log-only). See [SigV4 verification and IAM enforcement](@/docs/reference/security.md).
- **CreateAccount is async.** The state moves through `IN_PROGRESS` -> `SUCCEEDED` over a short interval; tests should poll `DescribeCreateAccountStatus` rather than assume the account is usable immediately.

## Source

- [`crates/fakecloud-organizations`](https://github.com/faiscadev/fakecloud/tree/main/crates/fakecloud-organizations)
- [AWS Organizations API reference](https://docs.aws.amazon.com/organizations/latest/APIReference/Welcome.html)
