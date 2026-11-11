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
- **Cross-service KMS hook** - S3 (SSE-KMS), SQS, SNS, DynamoDB, Secrets Manager, SSM SecureString and the SES receipt-rule S3 action call into KMS for real encrypt/decrypt under the hood. Every call is recorded at `/_fakecloud/kms/usage` so test code can assert which service principal triggered which operation on which key with which encryption context. AWS-managed keys (`aws/s3`, `aws/sqs`, `aws/sns`, `aws/dynamodb`, `aws/secretsmanager`, `aws/ssm`, ...) exist from first use, one per account and region, which that region's `alias/aws/<service>` names. As on AWS, AWS-managed keys cannot be managed: `PutKeyPolicy`, `ScheduleKeyDeletion`, `CancelKeyDeletion`, `EnableKey` / `DisableKey`, the rotation operations, `TagResource` / `UntagResource`, `UpdateKeyDescription`, `CreateGrant`, `CreateAlias` / `UpdateAlias` targeting one, `ReplicateKey`, `UpdatePrimaryRegion` and the imported-material operations answer `AccessDeniedException` ("... because no resource-based policy allows the kms:<Action> action").
- **AWS-managed keys** - the `alias/aws/<service>` keys of every service AWS has one for (`acm`, `backup`, `codeartifact`, `codecommit`, `dms`, `dynamodb`, `ebs`, `ecr`, `elasticache`, `elasticfilesystem`, `es`, `glue`, `kafka`, `kinesis`, `kinesisvideo`, `lambda`, `rds`, `redshift`, `s3`, `secretsmanager`, `ses`, `sns`, `sqs`, `ssm`, `timestream`, `xray`; another name such as `alias/aws/logs` is `NotFoundException` "Alias <alias ARN> is not found.", as on AWS) carry the key policy and description AWS gives them, recorded from a real account. As on AWS, `ListAliases` lists the predefined aliases (`aws/dynamodb`, `aws/ebs`, `aws/elasticfilesystem`, `aws/es`, `aws/glue`, `aws/kinesisvideo`, `aws/rds`, `aws/redshift`, `aws/s3`, `aws/ssm`, `aws/xray`) without a `TargetKeyId` until first used; the key is created the first time it is used, directly (`DescribeKey`, `Encrypt`, ...) or by its service. `GetKeyPolicy` takes a key id or ARN (an alias is `InvalidArnException`). The policy grants `Encrypt`, `Decrypt`, `ReEncrypt*` and `GenerateDataKey*` only to principals of the owning account calling through the owning service (`kms:ViaService` = `<service>.<region>.amazonaws.com`, `kms:CallerAccount` = the account), and fakecloud evaluates it on every use: a direct `Encrypt`, `Decrypt`, `GenerateDataKey*` or `ReEncrypt` on `alias/aws/s3` is refused with `AccessDeniedException` ("User: ... is not authorized to perform: kms:Encrypt on resource: <key ARN> because no resource-based policy allows the kms:Encrypt action") whatever the IAM mode, while each service keeps using its own key through the KMS hook, which calls KMS through the calling service (so one service cannot use another's `aws/*` key) or, for SES, as the SES service principal. The `aws/lambda` key also lets account principals decrypt ciphertexts whose encryption context names one of the account's functions, and `aws/ses` lets them decrypt the mail SES sealed, as their policies say. Under `--iam strict`, key policies see `kms:CallerAccount` and the request's `kms:EncryptionContext:*` keys.
- **Service ciphertexts** - ciphertexts the hook seals bind the service's encryption context like KMS ciphertexts (S3 `aws:s3:arn`, SQS `aws:sqs:arn`, SNS `aws:sns:topicArn`, Secrets Manager `SecretARN` + `SecretVersionId`, SSM `PARAMETER_ARN`, DynamoDB `aws:dynamodb:tableName` + `aws:dynamodb:subscriberId`, Lambda `aws:lambda:FunctionArn`), so the KMS API decrypts one only with that context (`InvalidCiphertextException` otherwise). Context-free service ciphertexts persisted before this binding still decrypt after an upgrade. A failed KMS call is reported by each service with its own declared error (SQS `KMS.AccessDeniedException` / `KMS.DisabledException` / `KMS.InvalidKeyUsageException` / `KMS.InvalidStateException` / `KMS.NotFoundException`, SNS `KMSAccessDenied` / `KMSDisabled` / `KMSInvalidState` / `KMSNotFound`, S3 `AccessDenied` / `KMS.<code>`, SSM `AccessDeniedException` / `InvalidKeyId`, Secrets Manager `AccessDeniedException` / `InvalidParameterException` / `EncryptionFailure` / `DecryptionFailure`, DynamoDB `CreateTable` / `UpdateTable` `AccessDeniedException` / `ValidationException`), relaying KMS's message. A refusal names the identity the service acted for (the IAM user or assumed role that called it, the account root when the service acts for the account). Services that deliver into SQS as their own service principal (SNS subscriptions, EventBridge rule targets, S3 event notifications) need the queue key's policy to name that principal: a queue under the AWS-managed `aws/sqs` key refuses them, as on AWS, so SNS delivers nothing and counts `AWS/SNS` `NumberOfNotificationsFailed` (and uses the subscription's dead-letter queue), EventBridge delivers nothing and counts `AWS/Events` `FailedInvocations`, and S3 refuses the notification configuration with `InvalidArgument` "Unable to validate the following destination configurations"; a customer key whose policy grants the principal works, and the account itself (or a role a service acts as: Scheduler, Pipes, Step Functions, Lambda destinations) still uses `aws/sqs`. Secrets Manager encrypts every secret, one created without `KmsKeyId` under the region's `aws/secretsmanager` key (also when CloudFormation creates it); secrets stored in plaintext by an older fakecloud still read back.
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
