+++
title = "ACM PCA"
description = "AWS Certificate Manager Private CA — create private CAs, issue and revoke real X.509 certificates, audit reports, permissions, and resource policies. JSON 1.1 protocol."
weight = 69
+++

fakecloud implements AWS Certificate Manager Private CA's full JSON 1.1 API: 23 operations covering the private CA hierarchy, real certificate issuance, revocation, audit reports, resource-share permissions, and resource policies. 100% Smithy conformance.

**Status: 100% coverage with a real certificate data plane.**

## Supported today

- **Certificate authorities** — `CreateCertificateAuthority` mints a genuine CA key pair from the requested `KeyAlgorithm` (RSA 2048/3072/4096, EC P-256/P-384). As with real AWS, a CA is `PENDING_CERTIFICATE` almost immediately and serves a real PEM CSR from `GetCertificateAuthorityCsr`; its key material is generated in the background, so `CreateCertificateAuthority` returns fast even for slow RSA-4096 keys (the key-dependent calls wait briefly for it). A `ROOT` CA is activated by self-signing that CSR with the `RootCACertificate` template via `IssueCertificate` and installing the result with `ImportCertificateAuthorityCertificate`; a `SUBORDINATE` CA is activated by importing its parent-signed certificate chain. Either import flips the CA to `ACTIVE`. `DescribeCertificateAuthority` returns the full `CertificateAuthority` including `Type`, `Status`, `Serial`, validity window, `CertificateAuthorityConfiguration`, `RevocationConfiguration`, and `UsageMode`. `ListCertificateAuthorities`, `UpdateCertificateAuthority` (enable/disable + revocation config), `DeleteCertificateAuthority` (with a restorable window), and `RestoreCertificateAuthority` are all implemented.
- **Real certificate issuance** — `IssueCertificate` parses the caller's PEM CSR and signs a real end-entity (or subordinate-CA) certificate with the CA's private key, honoring the requested `Validity` (`DAYS`/`MONTHS`/`YEARS`/`ABSOLUTE`/`END_DATE`) and `TemplateArn` semantics. `GetCertificate` returns the signed PEM plus the CA chain. The issued certificate genuinely verifies against the CA certificate (`rcgen`) — the chain is real, not cosmetic. CA private keys are persisted, so certificates issued before a restart still verify afterward.
- **Revocation + audit reports** — `RevokeCertificate` tracks revoked serials with a reason. `CreateCertificateAuthorityAuditReport` produces a real report object (JSON or CSV) listing issued and revoked certificates, and `DescribeCertificateAuthorityAuditReport` returns its status and S3 location.
- **Resource sharing** — `CreatePermission` / `ListPermissions` / `DeletePermission` manage the ACM service-linked permissions used for RAM sharing.
- **Resource policies** — `PutPolicy` / `GetPolicy` / `DeletePolicy` manage the resource-based policy attached to a CA.
- **Tags** — `TagCertificateAuthority` upserts tags by key, `UntagCertificateAuthority` removes them, and `ListTags` returns the tag set.

> Key generation always produces a genuine key of the requested algorithm and size in every build (no substitution). Because real RSA-4096 generation can take tens of seconds, the CA's *status* is decoupled from keygen: `CreateCertificateAuthority` reports `PENDING_CERTIFICATE` right away while the real key is generated on a background task, and `GetCertificateAuthorityCsr` / `IssueCertificate` / `ImportCertificateAuthorityCertificate` wait (bounded) for the key to be ready. Key generation state is persisted, so a CA whose key was still generating when the process exited resumes generation on restart.

## Smoke test

```sh
fakecloud &
E=http://localhost:4566

CA_ARN=$(aws --endpoint-url $E acm-pca create-certificate-authority \
  --certificate-authority-type ROOT \
  --certificate-authority-configuration '{
    "KeyAlgorithm":"EC_prime256v1",
    "SigningAlgorithm":"SHA256WITHECDSA",
    "Subject":{"CommonName":"Example Root CA","Organization":"Example"}
  }' \
  --query CertificateAuthorityArn --output text)

# The CA starts CREATING then PENDING_CERTIFICATE; wait for its CSR.
aws --endpoint-url $E acm-pca wait certificate-authority-csr-created \
  --certificate-authority-arn "$CA_ARN" 2>/dev/null || sleep 1

# Activate the ROOT CA: self-sign its own CSR, then import the certificate.
aws --endpoint-url $E acm-pca get-certificate-authority-csr \
  --certificate-authority-arn "$CA_ARN" --output text > /tmp/ca.csr

CERT_ARN=$(aws --endpoint-url $E acm-pca issue-certificate \
  --certificate-authority-arn "$CA_ARN" \
  --csr fileb:///tmp/ca.csr \
  --signing-algorithm SHA256WITHECDSA \
  --template-arn arn:aws:acm-pca:::template/RootCACertificate/V1 \
  --validity Value=3650,Type=DAYS \
  --query CertificateArn --output text)

aws --endpoint-url $E acm-pca get-certificate \
  --certificate-authority-arn "$CA_ARN" --certificate-arn "$CERT_ARN" \
  --query Certificate --output text > /tmp/ca.crt

aws --endpoint-url $E acm-pca import-certificate-authority-certificate \
  --certificate-authority-arn "$CA_ARN" --certificate fileb:///tmp/ca.crt

# The CA is now ACTIVE.
aws --endpoint-url $E acm-pca describe-certificate-authority \
  --certificate-authority-arn "$CA_ARN"
```
