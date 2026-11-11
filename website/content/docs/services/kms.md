+++
title = "KMS"
description = "Encryption, key management, aliases, grants, real ECDH, key import."
weight = 12
+++

fakecloud implements **54 of 54** KMS operations at 100% Smithy conformance.

## Supported features

- **Symmetric keys** — CreateKey, Encrypt, Decrypt, GenerateDataKey, ReEncrypt. Real AES-256-GCM envelope encryption with AWS-shaped ciphertext blobs (key id header + IV + ciphertext + tag).
- **Asymmetric keys** — Sign, Verify, GetPublicKey across:
  - **RSA** — RSA_2048, RSA_3072, RSA_4096 with real RSA Sign/Verify/GetPublicKey
  - **ECC** — ECC_NIST_P256, ECC_NIST_P384, ECC_NIST_P521, and ECC_SECG_P256K1 with real ECDSA Sign/Verify/GetPublicKey
- **Key management** — DescribeKey, EnableKey, DisableKey, ScheduleKeyDeletion, CancelKeyDeletion
- **Aliases** — CRUD with `alias/` prefix validation. Aliases are regional, as in AWS: the same alias name can exist independently in every region, `ListAliases` pages through the request region's aliases only, and an alias must point at a key in its own region. An alias name resolves in the caller's region and an alias ARN in the region it names, anywhere a key id/ARN is accepted (Encrypt, Decrypt, Sign, Verify, GenerateDataKey, ReEncrypt, grants, key policies, cross-service KmsKeyId, which resolves in the resource's region).
- **Grants** — CreateGrant, RetireGrant, RevokeGrant, ListGrants
- **Key rotation** — automatic rotation flag (tracked), on-demand rotation
- **Key policies** — PutKeyPolicy, GetKeyPolicy, ListKeyPolicies
- **Tags** — on keys
- **Real ECDH** — DeriveSharedSecret performs actual Elliptic Curve Diffie-Hellman
- **Key import** — GetParametersForImport, ImportKeyMaterial with real key material handling
- **Custom key stores** — CRUD (records only)
- **Key replica** — ReplicateKey
- **Cross-service KMS hook** — S3 (SSE-KMS), SQS, SNS, DynamoDB, Secrets Manager, and SSM SecureString call into KMS for real encrypt/decrypt under the hood. Every call is recorded at `/_fakecloud/kms/usage` so test code can assert which service principal triggered which operation on which key with which encryption context. AWS-managed aliases (`aws/s3`, `aws/sqs`, `aws/sns`, `aws/dynamodb`, `aws/secretsmanager`, `aws/ssm`, ...) auto-provision on first use, one AWS-managed key per account and region, which that region's `alias/aws/<service>` names. As on AWS, AWS-managed keys cannot be managed: `PutKeyPolicy`, `ScheduleKeyDeletion`, `CancelKeyDeletion`, `EnableKey` / `DisableKey`, the rotation operations, `TagResource` / `UntagResource`, `UpdateKeyDescription`, `CreateGrant`, `CreateAlias` / `UpdateAlias` targeting one, `ReplicateKey`, `UpdatePrimaryRegion` and the imported-material operations answer `AccessDeniedException` ("... because no resource-based policy allows the kms:<Action> action").
- **AWS-managed key policies** - every `aws/<service>` key carries the key policy AWS gives it (recorded from AWS for `aws/s3`, `aws/ebs`, `aws/lambda`, `aws/secretsmanager` and `aws/acm`; other services follow the same template with their own name), readable with `GetKeyPolicy` by key id or ARN (an alias is `InvalidArnException`, as on AWS). The policy grants `Encrypt`, `Decrypt`, `ReEncrypt*` and `GenerateDataKey*` only to principals of the owning account calling through the owning service (`kms:ViaService` = `<service>.<region>.amazonaws.com`, `kms:CallerAccount` = the account), and fakecloud evaluates it on every use: a direct `Encrypt`, `Decrypt`, `GenerateDataKey*` or `ReEncrypt` on `alias/aws/s3` is refused with `AccessDeniedException` ("User: ... is not authorized to perform: kms:Encrypt on resource: <key ARN> because no resource-based policy allows the kms:Encrypt action") whatever the IAM mode, while S3, SQS, SNS, DynamoDB, Secrets Manager and SSM keep encrypting and decrypting under their own `aws/*` keys through the KMS hook. Each hook call is made via the calling service, so one service cannot use another service's `aws/*` key. The `aws/lambda` key also lets account principals decrypt ciphertexts whose encryption context names one of the account's functions, as its policy says. Under `--iam strict`, key policies see `kms:CallerAccount` and the request's `kms:EncryptionContext:*` keys.
- **Default keys on encrypted resources** - a resource encrypted at rest without a named key reports the ARN of its service's AWS-managed key, a real key minted once per account and region that `DescribeKey alias/aws/<service>` resolves to: `aws/rds` for RDS instances and clusters, DocumentDB and Neptune clusters, `aws/ebs` for EBS volumes and snapshots (or the account's EBS default key when `ModifyEbsDefaultKmsKeyId` customized it), and `aws/elasticfilesystem`, `aws/redshift`, `aws/dms`, `aws/kafka`, `aws/timestream`, `aws/codeartifact`, `aws/codecommit` for their services. A key named by alias (resolved in the resource's region) or key id is reported as the ARN of the key it names. CloudFormation-provisioned resources report the same keys.
- **Service Quotas enforcement (opt-in)**: with "Customer Master Keys (CMKs)" (`L-C2F1777E`) enforced in [Service Quotas](@/docs/services/servicequotas.md#enforceable-quotas), `CreateKey` and `ReplicateKey` are refused with `LimitExceededException` once the region holds that many customer managed keys (any key state, pending deletion included; AWS managed keys do not count). Off by default

## Introspection

- `GET /_fakecloud/kms/usage` — list every cross-service KMS call (operation, service principal, key ARN, encryption context). Useful for asserting that your code path actually triggered KMS encrypt/decrypt under the hood.

## Protocol

JSON protocol. `X-Amz-Target` header, JSON body, JSON responses.

## Gotchas

- **Real AES-256-GCM, AWS-shaped ciphertext.** Encrypt/Decrypt round-trip across process restarts as long as the same key material is available. Ciphertext layout mimics AWS (key id header, IV, ciphertext, GCM tag) so length and shape match what real callers expect, but blobs are not interchangeable with real AWS KMS.

## Source

- [`crates/fakecloud-kms`](https://github.com/faiscadev/fakecloud/tree/main/crates/fakecloud-kms)
- [AWS KMS API reference](https://docs.aws.amazon.com/kms/latest/APIReference/Welcome.html)
