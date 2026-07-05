+++
title = "ACM PCA"
description = "AWS Certificate Manager Private CA — create private CAs, issue and revoke real X.509 certificates, audit reports, permissions, and resource policies. JSON 1.1 protocol."
weight = 69
+++

fakecloud implements AWS Certificate Manager Private CA's full JSON 1.1 API: 23 operations covering the private CA hierarchy, real certificate issuance, revocation, audit reports, resource-share permissions, and resource policies. 100% Smithy conformance.

**Status: 100% coverage with a real certificate data plane.**

## Supported today

- **Certificate authorities** — `CreateCertificateAuthority` mints a genuine CA key pair from the requested `KeyAlgorithm` (RSA 2048/3072/4096, EC P-256/P-384) and self-signs a real X.509 certificate for a `ROOT` CA, which lands `ACTIVE` and immediately usable. A `SUBORDINATE` CA starts in `PENDING_CERTIFICATE` and serves a real PEM CSR from `GetCertificateAuthorityCsr` until its parent-signed certificate chain is installed via `ImportCertificateAuthorityCertificate`, which flips it to `ACTIVE`. `DescribeCertificateAuthority` returns the full `CertificateAuthority` including `Type`, `Status`, `Serial`, validity window, `CertificateAuthorityConfiguration`, `RevocationConfiguration`, and `UsageMode`. `ListCertificateAuthorities`, `UpdateCertificateAuthority` (enable/disable + revocation config), `DeleteCertificateAuthority` (with a restorable window), and `RestoreCertificateAuthority` are all implemented.
- **Real certificate issuance** — `IssueCertificate` parses the caller's PEM CSR and signs a real end-entity (or subordinate-CA) certificate with the CA's private key, honoring the requested `Validity` (`DAYS`/`MONTHS`/`YEARS`/`ABSOLUTE`/`END_DATE`) and `TemplateArn` semantics. `GetCertificate` returns the signed PEM plus the CA chain. The issued certificate genuinely verifies against the CA certificate (`rcgen`) — the chain is real, not cosmetic. CA private keys are persisted, so certificates issued before a restart still verify afterward.
- **Revocation + audit reports** — `RevokeCertificate` tracks revoked serials with a reason. `CreateCertificateAuthorityAuditReport` produces a real report object (JSON or CSV) listing issued and revoked certificates, and `DescribeCertificateAuthorityAuditReport` returns its status and S3 location.
- **Resource sharing** — `CreatePermission` / `ListPermissions` / `DeletePermission` manage the ACM service-linked permissions used for RAM sharing.
- **Resource policies** — `PutPolicy` / `GetPolicy` / `DeletePolicy` manage the resource-based policy attached to a CA.
- **Tags** — `TagCertificateAuthority` upserts tags by key, `UntagCertificateAuthority` removes them, and `ListTags` returns the tag set.

> RSA key generation is intentionally substituted with a fast elliptic-curve key in unoptimized (debug/test) builds, where real RSA-4096 generation can take tens of seconds; release builds generate a genuine RSA key of the requested size. Issued certificates are real and verify against the CA in every build.

## Smoke test

```sh
fakecloud &

CA_ARN=$(aws --endpoint-url http://localhost:4566 acm-pca create-certificate-authority \
  --certificate-authority-type ROOT \
  --certificate-authority-configuration '{
    "KeyAlgorithm":"EC_prime256v1",
    "SigningAlgorithm":"SHA256WITHECDSA",
    "Subject":{"CommonName":"Example Root CA","Organization":"Example"}
  }' \
  --query CertificateAuthorityArn --output text)

aws --endpoint-url http://localhost:4566 acm-pca describe-certificate-authority \
  --certificate-authority-arn "$CA_ARN"

aws --endpoint-url http://localhost:4566 acm-pca get-certificate-authority-certificate \
  --certificate-authority-arn "$CA_ARN"
```
