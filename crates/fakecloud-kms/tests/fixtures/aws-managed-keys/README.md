# AWS-managed key recordings

Recorded with the AWS CLI against a real account (us-east-1, 2026-11-10/11).
The account id is normalized to `123456789012`; key ids, ARNs and the caller
(`user/admin`) are as AWS returned them.

## AWS-managed keys

- `list-aliases-before.json`: the `alias/aws/*` entries of
  `aws kms list-aliases` before any of the calls below. AWS lists the
  predefined AWS-managed aliases of every account (`aws/dynamodb`,
  `aws/elasticfilesystem`, `aws/es`, `aws/glue`, `aws/kinesisvideo`,
  `aws/rds`, `aws/redshift`, `aws/ssm`, `aws/xray`; `aws/s3` and `aws/ebs`
  are in the same set and were already in use) without a `TargetKeyId`; keys
  existed only for `aws/acm`, `aws/ebs`, `aws/lambda`, `aws/s3` and
  `aws/secretsmanager`.
- `unmaterialized.txt`: direct `aws kms encrypt` / `generate-data-key` on
  `alias/aws/dynamodb` and `alias/aws/rds` (listed, no target),
  `alias/aws/sns` (not listed), `alias/aws/nosuchservice` and a missing
  customer alias. The first three created the key (the refusal names a new
  key ARN) and were refused with `AccessDeniedException`; the last two are
  `NotFoundException` "Alias <alias ARN> is not found.".
- `<service>.json` (one per AWS-managed key, 26 in all): `describe_key` is
  `aws kms describe-key --key-id alias/aws/<service>` (`KeyMetadata`) and
  `get_key_policy` is `aws kms get-key-policy --key-id <key id>
  --policy-name default`, the policy text exactly as returned. `DescribeKey`
  on an alias AWS has a key for creates that key on first use (free, owned
  by AWS, nothing to delete): this is how the keys of `backup`,
  `codeartifact`, `codecommit`, `dms`, `dynamodb`, `ecr`, `elasticache`,
  `elasticfilesystem`, `es`, `glue`, `kafka`, `kinesis`, `kinesisvideo`,
  `rds`, `redshift`, `ses`, `sns`, `sqs`, `ssm`, `timestream` and `xray`
  came to exist for the recording.
- `describe-key-not-found.json`: `alias/aws/cloudtrail`, `alias/aws/logs`
  and `alias/aws/sagemaker` have no AWS-managed key (`NotFoundException`).
- `list-aliases-after.json`: the listing after the calls above.
- `direct-crypto.txt`: `encrypt` / `generate-data-key` called directly on
  `alias/aws/s3`, `alias/aws/secretsmanager` and `alias/aws/ebs`, all HTTP 400
  `AccessDeniedException` naming the key the alias resolves to, and
  `get-key-policy` with an alias, `InvalidArnException`.

`scripts/gen-kms-aws-managed-keys.py` turns the `<service>.json` recordings
into `crates/fakecloud-kms/data/aws_managed_keys.json`, which
`fakecloud_kms::aws_managed` reads (unit test
`catalog_matches_every_recording` checks it reproduces every recording).

## KMS failures through other services

`services.txt` and `services-wire.txt` (the `--debug` status and body of the
same calls) record what each service answers when KMS refuses it: an SQS
queue and an SNS topic whose `KmsMasterKeyId` is `alias/aws/s3` (refused by
that key's policy) or a missing alias, then `SendMessage` / `Publish`;
`PutObject` with SSE-KMS under `alias/aws/sqs`, a missing alias and a
missing key ARN; SSM `PutParameter` (SecureString), Secrets Manager
`CreateSecret` and DynamoDB `CreateTable` (SSE KMS) under `alias/aws/s3` and
a missing alias. Every resource created for the recording (two queues, two
topics, one bucket) was deleted afterwards and its absence confirmed; the
parameters, secrets and tables were refused and never created.

`../aws-managed-key-refusals.txt` holds the key-management refusals recorded
against `alias/aws/lambda`'s key.
