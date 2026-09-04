+++
title = "ACM"
description = "AWS Certificate Manager — request / import / export / revoke certificates, tags, account configuration. JSON 1.1 protocol."
weight = 26
+++

fakecloud implements AWS Certificate Manager's full JSON 1.1 control plane: 36 operations covering certificate lifecycle, import/export, tags, account-wide expiry events, and the ACME surface (endpoints, external account bindings, pre-validated domains, and registered ACME accounts). 100% Smithy conformance.

**Status: 100% control-plane coverage.**

## Supported today

- **Public certificate lifecycle** — `RequestCertificate` accepts `DomainName`, `SubjectAlternativeNames`, `ValidationMethod` (DNS / EMAIL), `IdempotencyToken`, `KeyAlgorithm`, `Options`, `CertificateAuthorityArn`, `ManagedBy`, `Tags`. The certificate lands at `PENDING_VALIDATION` with `Type = AMAZON_ISSUED`. **DNS-validated** certs are auto-promoted to `ISSUED` after a configurable delay (default 5 seconds, override with the env var `FAKECLOUD_ACM_AUTO_ISSUE_SECS`), simulating ACM's async validation pipeline. **EMAIL-validated** certs stay `PENDING_VALIDATION` until the admin `/approve` endpoint flips them — matching real ACM, which waits for the user to click the validation link. Tests can also flip status to any state synchronously via the `/status` admin endpoint below. Idempotency: a request re-issued with the same `IdempotencyToken` + `DomainName` + SANs returns the same `CertificateArn`. `DescribeCertificate` returns the full `CertificateDetail` including domain validation, options, key usages, ARN, status, validity window, `RenewalEligibility`, `RenewalSummary` (once issued), and `FailureReason` when present. `GetCertificate` returns the real self-signed PEM + chain stored at `RequestCertificate` time. `ListCertificates` supports `MaxItems` + `NextToken` + `CertificateStatuses` + `Includes.keyTypes`. `SearchCertificates` honors `FilterStatement.Filter.KeyTypes` (And/Or/Not composition is parsed but ignored). `DeleteCertificate` rejects with `ResourceInUseException` while `InUseBy` is non-empty.
- **Imported certificates** — `ImportCertificate` accepts the PEM cert + private key + optional chain (base64-encoded over the wire), stores them, and flips `Status` to `ISSUED` with `Type = IMPORTED`. Passing an existing `CertificateArn` re-imports in place (the cert must already be `IMPORTED`). `ExportCertificate` returns the stored cert + chain + private key; when `Passphrase` is supplied, the private key is wrapped in a PKCS#8 v2 `BEGIN ENCRYPTED PRIVATE KEY` envelope (PBES2 / PBKDF2-HMAC-SHA256, 2048 iterations, AES-256-CBC) so callers can round-trip via `openssl pkcs8 -in key.pem -passin pass:...` or any modern PKCS#8 decoder. Omitting `Passphrase` returns the plain PEM. Imported certs are not eligible for `RevokeCertificate` (`InvalidParameterException`) or `RenewCertificate`.
- **Renewal + revocation** — `RenewCertificate` (AMAZON_ISSUED only) bumps `NotBefore`/`NotAfter` by 13 months, marks every domain validation `SUCCESS`, flips status to `ISSUED`, and refreshes `RenewalSummary` with `RenewalStatus = SUCCESS` and a fresh `UpdatedAt`. `RevokeCertificate` requires `RevocationReason`, sets `Status = REVOKED` and stamps `RevokedAt`.
- **Email validation** — `ResendValidationEmail` is only valid when `ValidationMethod = EMAIL`; DNS-validated certs return `InvalidParameterException`.
- **Tags** — `AddTagsToCertificate` upserts tags by key, `RemoveTagsFromCertificate` deletes by key (optionally also matching value), `ListTagsForCertificate` returns the tag set sorted by key for deterministic test output.
- **Account configuration** — `PutAccountConfiguration` accepts `IdempotencyToken` + `ExpiryEvents.DaysBeforeExpiry`; `GetAccountConfiguration` returns it back.
- **Certificate options** — `UpdateCertificateOptions` updates `CertificateTransparencyLoggingPreference` and `Export`.
- **Domain validation gating** — DNS-validated certificates are auto-issued by the background tick once the configured delay elapses; EMAIL-validated certificates stay `PENDING_VALIDATION` until a test drives the synchronous `/_fakecloud/acm/certificates/{arn-or-id}/approve` admin endpoint. The validation `ResourceRecord` returned by `DescribeCertificate` is deterministic per `(DomainName, ValidationMethod)` so tests can assert on its shape, and `ResendValidationEmail` is rejected for DNS-validated certs to match real ACM.
- **`ImportCertificate` KMS policy enforcement** — when a CMK is in scope (account-default or explicitly referenced from the import flow), the KMS hook is exercised end-to-end: the calling identity's IAM grants and the CMK's key policy / grants are evaluated against `kms:Encrypt` / `kms:Decrypt` / `kms:GenerateDataKey` before the cert bytes are persisted. Denied calls land in `/_fakecloud/kms/usage` with the rejection reason, so tests can assert that an import truly failed at the policy boundary rather than silently storing the cert. See [IAM policy enforcement](@/docs/reference/security.md) for the full model.

## Smoke test

```sh
fakecloud &

ARN=$(aws --endpoint-url http://localhost:4566 acm request-certificate \
  --domain-name api.example.com \
  --validation-method DNS \
  --query CertificateArn --output text)

aws --endpoint-url http://localhost:4566 acm describe-certificate \
  --certificate-arn "$ARN"

aws --endpoint-url http://localhost:4566 acm add-tags-to-certificate \
  --certificate-arn "$ARN" \
  --tags Key=env,Value=prod

aws --endpoint-url http://localhost:4566 acm list-tags-for-certificate \
  --certificate-arn "$ARN"
```

## Admin endpoints

- `POST /_fakecloud/acm/certificates/{arn-or-id}/status` — flip a stored certificate's status synchronously. Body `{"status": "ISSUED"}` promotes it to `ISSUED` (and stamps `IssuedAt`); `{"status": "FAILED", "reason": "validation declined"}` records the failure surfaced as `FailureReason` on `DescribeCertificate`; `{"status": "VALIDATION_TIMED_OUT"}` is also accepted. The `{arn-or-id}` segment is the trailing UUID of the certificate ARN (everything after `certificate/`). Returns `204 No Content` on success and `404 Not Found` for an unknown id. Available in every fakecloud SDK as `acm.setCertificateStatus(arn_or_id, ...)` (or the language-idiomatic equivalent).
- `POST /_fakecloud/acm/certificates/{arn-or-id}/approve` — synchronous equivalent of "the user clicked the validation link in the email". Flips a `PENDING_VALIDATION` cert to `ISSUED`, stamps `IssuedAt`, marks every domain validation entry `SUCCESS`, and populates `RenewalSummary`. Idempotent — calling against an already-issued cert is a no-op success. Primarily used to drive the EMAIL validation flow in tests, since EMAIL certs do not auto-issue. Returns `204` on success and `404` for an unknown id. SDK alias: `acm.approveCertificate(arn_or_id)`.
- `GET /_fakecloud/acm/certificates/{arn-or-id}/chain-info` — introspection endpoint that reports the PEM block count and byte length of the stored certificate and its chain, plus the cert `status`, `cert_type`, and a constant `external_ca_validated: false` marker documenting that fakecloud does not run a real X.509 verifier. Useful for `ImportCertificate` tests that need to confirm the chain they uploaded round-trips intact without re-fetching and parsing the cert. Returns `200` with the JSON document on success and `404` for an unknown id. SDK alias: `acm.getCertificateChainInfo(arn_or_id)`.

## Caveats

fakecloud does not run the real X.509 validation pipeline. The auto-issue tick flips DNS-validated certs from `PENDING_VALIDATION` to `ISSUED` after a fixed delay regardless of whether the synthesized validation `ResourceRecord` was actually published; the record is deterministic per domain but never observed by a real validator. EMAIL-validated certs never auto-flip — drive their issuance through the `/approve` admin endpoint. `ImportCertificate` does not parse the input X.509 cert — it stores the bytes verbatim and uses the cheap `CN=` substring scan to extract `DomainName`. `ExportCertificate` returns the imported cert as-is when one exists or a placeholder PEM otherwise. When a `Passphrase` is supplied, the stored private key must be a valid PKCS#8 PEM (`BEGIN PRIVATE KEY`); legacy PKCS#1 (`BEGIN RSA PRIVATE KEY`) imports cannot be encrypted. `KeyUsages` and `ExtendedKeyUsages` reported by `DescribeCertificate` are constants (`DIGITAL_SIGNATURE` + `KEY_ENCIPHERMENT`, TLS server + client auth) — fakecloud does not extract them from imported certs.
