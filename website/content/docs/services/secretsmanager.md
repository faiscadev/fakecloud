+++
title = "Secrets Manager"
description = "Secrets, versioning, rotation via Lambda, replication."
weight = 10
+++

fakecloud implements **23 of 23** Secrets Manager operations at 100% Smithy conformance.

## Supported features

- **Secrets** — CRUD, tags, resource-based policies
- **Versioning** — stages (AWSCURRENT, AWSPREVIOUS, AWSPENDING), version IDs, explicit version retrieval
- **Soft delete** — DeleteSecret with recovery window, RestoreSecret
- **Rotation** — RotateSecret invokes a Lambda function through all 4 steps (createSecret, setSecret, testSecret, finishSecret)
- **Automatic rotation scheduling** — via `/_fakecloud/secretsmanager/rotation-scheduler/tick`
- **Regions** — secrets are regional: the same secret name can hold different values in two regions, `ListSecrets` only lists the request region, and a secret ARN of another region is not found from this region's endpoint (as on AWS). ECS `secrets[].valueFrom` and CodeBuild `SECRETS_MANAGER` variables resolve a name in the task's or build's region and a full ARN in the region it names.
- **Replication** — `ReplicateSecretToRegions` (or `AddReplicaRegions` on `CreateSecret`) creates a read-only replica in each replica region, with the primary's name and random suffix and an ARN naming the replica region, re-encrypted with the replica's KMS key. Writes to the primary (values, staging labels, description, tags, resource policy, rotation) reach the replicas; writes to a replica fail with `InvalidRequestException`. A replica region already holding a secret of that name reports `Failed` unless `ForceOverwriteReplicaSecret` is set. `RemoveRegionsFromReplication` deletes the replica, `StopReplicationToReplica` (called in the replica's region) promotes it to a standalone secret, and a primary with replicas can't be deleted until they are removed.
- **Random password generation** — GetRandomPassword with full character class support
- **Real KMS encryption** - when `KmsKeyId` is set on a secret, `CreateSecret` / `PutSecretValue` call `kms:GenerateDataKey` (for `SecretString` and `SecretBinary` alike) and `GetSecretValue` calls `kms:Decrypt` with the encryption context Secrets Manager uses, `{SecretARN: <arn>, SecretVersionId: <version>}` (`UpdateSecret` with a new value too). A secret created without `KmsKeyId` is encrypted under the region's AWS-managed `aws/secretsmanager` key, created on that first use (and `DescribeSecret` reports no key, as on AWS); secrets an older fakecloud stored in plaintext still read back, and a rotated version is re-encrypted for its own version id. `UpdateSecret` with a new `KmsKeyId` re-encrypts the `AWSCURRENT`, `AWSPENDING` and `AWSPREVIOUS` versions under it (refused by KMS, it changes nothing; on a secret with no value yet the key is still checked, so a missing or disabled key is refused as `CreateSecret` refuses it), as does a CloudFormation `AWS::SecretsManager::Secret` update changing `KmsKeyId`, and `ListSecretVersionIds` reports each version's `KmsKeyIds` (`DefaultEncryptionKey` for the AWS-managed key). A key KMS refuses fails the write or read (a `BatchGetSecretValue` entry lands in `Errors`, an SSM `/aws/reference/secretsmanager/` parameter fails with `AccessDeniedException`, a CodeBuild `SECRETS_MANAGER` variable fails the build; `RotateSecret` fails before changing anything, rotation configuration included; replication fails, a replica turning `Failed`, rather than copying the ciphertext). A replica needs the versions holding `AWSCURRENT`, `AWSPENDING` or `AWSPREVIOUS`: a version holding only other labels, which a key change leaves under its old key, keeps the copy the replica already has (or is left out of a new replica) once that key is refused: `AccessDeniedException` "Access to KMS is not allowed", `InvalidParameterException` for a missing key, `EncryptionFailure` / `DecryptionFailure` ("Secrets Manager can't decrypt the secret value: <KMS message>") for a disabled or deleted one. All KMS calls land in `/_fakecloud/kms/usage` so test code can assert encryption ran.

## Protocol

JSON protocol. `X-Amz-Target` header, JSON body, JSON responses.

## Introspection

- `POST /_fakecloud/secretsmanager/rotation-scheduler/tick` — trigger rotation for secrets whose schedule is due
- `GET /_fakecloud/kms/usage` — list every KMS call triggered by service-side encryption (Secrets Manager, and the rest of the services as the KMS hook rolls out), with operation, service principal, key ARN, and encryption context

## Cross-service delivery

- **Secrets Manager -> Lambda** — Rotation invokes the configured Lambda for all 4 rotation steps
- **Secrets Manager -> KMS** — Encrypt on Create / PutSecretValue, Decrypt on GetSecretValue when `KmsKeyId` is set

## Source

- [`crates/fakecloud-secretsmanager`](https://github.com/faiscadev/fakecloud/tree/main/crates/fakecloud-secretsmanager)
- [AWS Secrets Manager API reference](https://docs.aws.amazon.com/secretsmanager/latest/apireference/Welcome.html)
