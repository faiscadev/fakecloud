+++
title = "Verified Permissions"
description = "AWS Verified Permissions (verifiedpermissions) on fakecloud: policy stores, Cedar schemas, static and template-linked policies, policy templates, identity sources, aliases, tagging, and real Cedar authorization. awsJson1.0."
weight = 51
+++

fakecloud implements **AWS Verified Permissions** (`verifiedpermissions`) as an
awsJson1.0 control plane with a **real Cedar authorization engine**. **The
complete 34-operation surface** ships, backed by account-partitioned state that
persists across restarts in persistent mode. It signs with the
`verifiedpermissions` SigV4 service name and speaks the `VerifiedPermissions`
awsJson1.0 target prefix, exactly like the real API.

The four authorization operations are not stubs: fakecloud embeds the official
[`cedar-policy`](https://crates.io/crates/cedar-policy) crate — the same Cedar
engine AWS uses. A policy store's static and template-linked policies are
compiled into a Cedar `PolicySet`, the request's principal, action, resource,
context and entities are translated from the Verified Permissions wire shapes
into Cedar values, and the returned `decision` (`ALLOW`/`DENY`),
`determiningPolicies` and `errors` are the genuine Cedar evaluation result.

Nested configuration objects (`Configuration`, `EntityIdentifier`,
`ValidationSettings`, ...) round-trip verbatim on describe. The model's
`@length`, `@range`, and enum constraints are enforced with
`ValidationException`.

## Supported now (all 34 operations)

- **Policy stores** — `CreatePolicyStore`, `GetPolicyStore`,
  `UpdatePolicyStore`, `DeletePolicyStore` (honors `deletionProtection` with
  `InvalidStateException`), `ListPolicyStores`. ARNs are
  `arn:aws:verifiedpermissions::<account>:policy-store/<id>`, each carrying its
  `ValidationSettings` (`OFF`/`STRICT`).
- **Schemas** — `PutSchema` / `GetSchema` store the Cedar schema JSON for a
  store and echo back its namespaces.
- **Policies** — `CreatePolicy`, `GetPolicy`, `UpdatePolicy`, `DeletePolicy`,
  `ListPolicies`, `BatchGetPolicy`. Both `static` policies (a Cedar statement,
  validated on write) and `templateLinked` policies (a template plus
  principal/resource bindings) are supported; `ListPolicies` honors the
  principal/resource/policyType/policyTemplateId filter.
- **Policy templates** — `CreatePolicyTemplate`, `GetPolicyTemplate`,
  `UpdatePolicyTemplate`, `DeletePolicyTemplate`, `ListPolicyTemplates` store a
  parameterized Cedar body (`?principal` / `?resource` slots).
- **Identity sources** — `CreateIdentitySource`, `GetIdentitySource`,
  `UpdateIdentitySource`, `DeleteIdentitySource`, `ListIdentitySources` link a
  Cognito user pool or OIDC provider; the `Configuration` round-trips verbatim.
- **Policy-store aliases** — `CreatePolicyStoreAlias`, `GetPolicyStoreAlias`,
  `DeletePolicyStoreAlias`, `ListPolicyStoreAliases`.
- **Authorization (real Cedar)** — `IsAuthorized`, `IsAuthorizedWithToken`,
  `BatchIsAuthorized`, `BatchIsAuthorizedWithToken`. `*WithToken` decodes the
  supplied identity/access JWT and resolves the principal entity from the `sub`
  claim per the identity source's `principalEntityType`.
- **Tagging** — `TagResource`, `UntagResource`, `ListTagsForResource` keyed by
  the policy-store `ResourceArn`.

## Not implemented

- Cedar **schema-based validation** of policies on write in `STRICT` mode is
  best-effort (statements are parsed for Cedar syntactic validity, but not
  type-checked against the store schema). Identity-source token verification is
  claim-extraction only — JWT signatures are not cryptographically verified,
  matching fakecloud's stub-external-PKI posture.
