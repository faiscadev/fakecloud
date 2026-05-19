+++
title = "KMS emulator for tests"
description = "Run AWS KMS locally for integration tests with fakecloud. 53 KMS operations, real ECDH, encryption/decryption, aliases, grants, key import, multi-region keys. Any AWS SDK, free."
template = "page.html"
+++

Need a KMS emulator for integration tests? Use [fakecloud](https://github.com/faiscadev/fakecloud). Not a mock library — a real server that speaks the KMS wire protocol with real crypto.

```sh
curl -fsSL https://fakecloud.dev/install.sh | bash
fakecloud
```

Point your AWS SDK at `http://localhost:4566`.

## Why fakecloud for KMS

- **53 KMS operations** at 100% conformance — symmetric and asymmetric keys, encrypt/decrypt, aliases, grants, data key generation, real ECDH, key import, multi-region keys, key policies.
- **Real cryptography.** Symmetric Encrypt/Decrypt/GenerateDataKey use real AES-256-GCM envelope encryption with AWS-shaped ciphertext blobs. Asymmetric Sign/Verify/GetPublicKey use real RSA (RSA_2048/3072/4096) and real ECDSA (ECC_NIST_P256/P384/P521, ECC_SECG_P256K1). DeriveSharedSecret performs real ECDH. Output ciphertexts and signatures are cryptographically meaningful, not opaque stubs.
- **Aliases everywhere.** `alias/...` works anywhere a key id is accepted — Encrypt, Decrypt, Sign, Verify, GenerateDataKey, ReEncrypt, grants, key policies, and cross-service `KmsKeyId` parameters.
- **Cross-service KMS hook.** S3 (SSE-KMS), SQS, SNS, DynamoDB, Secrets Manager, and SSM SecureString call into KMS for real encrypt/decrypt. Every call is recorded at `/_fakecloud/kms/usage` so tests can assert which service triggered which KMS operation on which key.
- **Any AWS SDK in any language.** Real HTTP server on port 4566.
- **KMS key policies enforced.** Opt-in `--iam strict` mode validates key-policy Principal/Condition semantics with AWS's cross-account combining rules.
- **No account, no auth token, no paid tier.** AGPL-3.0.

## Quick examples

### Python (boto3)

```python
import boto3
kms = boto3.client('kms',
    endpoint_url='http://localhost:4566',
    aws_access_key_id='test',
    aws_secret_access_key='test',
    region_name='us-east-1')

key = kms.create_key(Description='test-key')
key_id = key['KeyMetadata']['KeyId']

ct = kms.encrypt(KeyId=key_id, Plaintext=b'secret data')
pt = kms.decrypt(KeyId=key_id, CiphertextBlob=ct['CiphertextBlob'])
assert pt['Plaintext'] == b'secret data'
```

### AWS CLI

```sh
aws --endpoint-url http://localhost:4566 kms create-key --description "test-key"
aws --endpoint-url http://localhost:4566 kms encrypt \
  --key-id <key-id> --plaintext "hello world" --query CiphertextBlob --output text
```

## Aliases

```sh
aws --endpoint-url http://localhost:4566 kms create-alias \
  --alias-name alias/my-key --target-key-id <key-id>

aws --endpoint-url http://localhost:4566 kms encrypt \
  --key-id alias/my-key --plaintext "hello"
```

## Data keys

```python
dk = kms.generate_data_key(KeyId=key_id, KeySpec='AES_256')
# dk['Plaintext']      -> raw 32-byte key for local encryption
# dk['CiphertextBlob'] -> encrypted version to store with data
```

Used in envelope-encryption flows. Real AES-GCM throughout.

## Asymmetric + ECDH

```python
# Real RSA Sign/Verify (RSA_2048 / RSA_3072 / RSA_4096)
rsa = kms.create_key(KeySpec='RSA_2048', KeyUsage='SIGN_VERIFY')

# Real ECDSA Sign/Verify (P-256, P-384, P-521, secp256k1)
ec = kms.create_key(KeySpec='ECC_NIST_P521', KeyUsage='SIGN_VERIFY')

# Real ECDH key agreement
asym = kms.create_key(KeySpec='ECC_NIST_P256', KeyUsage='KEY_AGREEMENT')
```

## SSE-KMS on S3

```sh
aws --endpoint-url http://localhost:4566 s3 cp file.txt s3://bucket/file.txt \
  --sse aws:kms --sse-kms-key-id alias/my-key
```

Object encrypted at rest with the specified KMS key. Retrieval decrypts transparently.

## Secrets Manager + KMS

Secrets Manager secrets are encrypted with KMS by default. Rotation via Lambda works end-to-end (Lambda runs real code, calls KMS to re-encrypt).

## How it differs from alternatives

| Tool | Multi-language | Real crypto | SSE-KMS on S3 | Key policies |
|---|---|---|---|---|
| fakecloud | Any | Yes (AES-GCM, RSA, ECDSA, ECDH) | Yes | Yes (Principal/Condition) |
| LocalStack Community | Any (auth required) | Partial | Yes | Partial |
| Moto (mock_kms) | Python only | Partial | Stubbed | Partial |
| aws-encryption-sdk | N/A | N/A | N/A (client-side only) | N/A |

## Links

- **Install:** `curl -fsSL https://fakecloud.dev/install.sh | bash`
- **Repo:** [github.com/faiscadev/fakecloud](https://github.com/faiscadev/fakecloud)
- **Related:** [Fake AWS server for tests](/fake-aws-server/), [Local S3 for integration tests](/local-s3-for-tests/)
