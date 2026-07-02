+++
title = "Account Management"
description = "AWS Account Management (account) on fakecloud: alternate contacts, primary contact information, account information, primary-email OTP flow, and Region opt-in control. restJson1."
weight = 48
+++

fakecloud implements **AWS Account Management** (`account`) as a restJson1
control plane. **The complete 15-operation surface** ships — alternate contacts,
primary contact information, account information, primary-email management, and
Region opt-in control — backed by account-partitioned state that persists across
restarts in persistent mode.

Every operation honors the optional `AccountId` member, so an organization's
management account can read and write a member account's settings exactly as it
would against real AWS; absent `AccountId`, the operation targets the caller's
own account. (The primary-email operations mark `AccountId` as required, matching
the AWS Smithy model.)

## Supported now (all 15 operations)

- **Alternate contacts** — `PutAlternateContact`, `GetAlternateContact`,
  `DeleteAlternateContact`. Each account exposes three independent contact slots
  — `BILLING`, `OPERATIONS`, and `SECURITY` — storing `Name`, `Title`,
  `EmailAddress`, and `PhoneNumber`. Field length limits from the model are
  enforced (Name <= 64, Title <= 50, EmailAddress <= 254, PhoneNumber <= 25), and
  reading or deleting an unset slot returns `ResourceNotFoundException`.
- **Primary contact information** — `PutContactInformation`,
  `GetContactInformation`. The full `ContactInformation` object round-trips
  (`FullName`, `AddressLine1`-`3`, `City`, `CountryCode`, `PhoneNumber`,
  `PostalCode`, `CompanyName`, `DistrictOrCounty`, `StateOrRegion`, `WebsiteUrl`);
  the model's required members are validated on write.
- **Account information** — `GetAccountInformation` (returns `AccountId`,
  `AccountName`, `AccountCreatedDate`, `AccountState`), `PutAccountName`
  (`AccountName` <= 50), and `GetGovCloudAccountInformation` (returns a
  deterministically-paired `GovCloudAccountId` for a standard account id).
- **Primary email** — `GetPrimaryEmail`, `StartPrimaryEmailUpdate`,
  `AcceptPrimaryEmailUpdate`. Starting an update records a pending change and
  returns `Status: PENDING`; accepting it with the matching one-time password
  commits the new address and returns `Status: ACCEPTED`. A wrong OTP or email
  returns `ValidationException`.
- **Region opt-in** — `ListRegions`, `GetRegionOptStatus`, `EnableRegion`,
  `DisableRegion`. Backed by the real AWS Region catalogue: always-on regions
  report `ENABLED_BY_DEFAULT`, opt-in regions default to `DISABLED`. `EnableRegion`
  / `DisableRegion` move an opt-in region to `ENABLING` / `DISABLING`, which
  settles to `ENABLED` / `DISABLED` on the next `GetRegionOptStatus` or
  `ListRegions` read (mirroring the real service's eventual transition).
  `ListRegions` supports the `RegionOptStatusContains` filter and
  `MaxResults` / `NextToken` pagination; toggling an always-on region returns
  `ValidationException`.

## Persistence

All account data — contacts, contact information, account name, the pending and
committed primary email, and per-region opt overrides — is account-partitioned
and written through to the persistence snapshot, so it survives a restart in
persistent mode.

## Not implemented

`CloseAccount` is not part of the current AWS Account Smithy model surface and is
therefore not implemented. There is no billing or organization-membership side
effect beyond the contact/region metadata this API manages.
