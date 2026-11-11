# AWS-managed key recordings

Recorded with the AWS CLI against a real account (us-east-1, 2026-11-11),
read-only: no resource was created. The account id is normalized to
`123456789012`; key ids, ARNs and the caller (`user/admin`) are as AWS
returned them.

- `list-aliases.json`: the `alias/aws/*` entries of `aws kms list-aliases`.
  AWS lists the predefined AWS-managed aliases of services the account never
  used without a `TargetKeyId`; the key behind one is created the first time
  its service uses it. Five keys existed: `aws/acm`, `aws/ebs`, `aws/lambda`,
  `aws/s3` and `aws/secretsmanager`.
- `<service>.json` for those five: `describe_key` is
  `aws kms describe-key --key-id alias/aws/<service>` (`KeyMetadata`), and
  `policy` is `aws kms get-key-policy --key-id <key id> --policy-name default`
  parsed from its JSON string. `crate::aws_managed::key_policy` reproduces
  these policies (unit test `recorded_policies_match_the_fixtures`).
- `direct-crypto.txt`: `aws kms encrypt` and `aws kms generate-data-key`
  called directly on `alias/aws/s3`, `alias/aws/secretsmanager` and
  `alias/aws/ebs`, all refused with HTTP 400 `AccessDeniedException` naming
  the key the alias resolves to; and `get-key-policy` with an alias as the key
  id, refused with `InvalidArnException`. `--debug` showed the wire body
  (`__type` / `Message`).

The policies follow one template, varied per service: a statement letting
`"AWS": "*"` use the key for the cryptographic operations (and, for most,
`CreateGrant` / `DescribeKey`) under `StringEquals` on
`kms:CallerAccount = <account>` and `kms:ViaService =
<service>.<region>.amazonaws.com` (`ec2.` for `aws/ebs`), plus "Allow direct
access to key metadata to the account" granting the account root
`Describe*` / `Get*` / `List*` (and `RevokeGrant`). Variations recorded:
`aws/s3` has no `CreateGrant` and no `RevokeGrant`; `aws/secretsmanager`
splits `GenerateDataKey*` into its own statement with `StringLike`
`secretsmanager.*.amazonaws.com`; `aws/lambda` adds a direct `Decrypt` for
ciphertexts bound to the account's functions and service-principal
statements; `aws/acm` only allows grants and `Decrypt` through ACM and denies
re-encryption to another key. Services without a recording here get the
`aws/ebs` form with their own name, `kms:ViaService` and `Id`
(`auto-<service>-2`); their `Sid` wording and `Id` are inferred from the
recorded ones, not recorded.

`../aws-managed-key-refusals.txt` holds the key-management refusals recorded
against `alias/aws/lambda`'s key.
