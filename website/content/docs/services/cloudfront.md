+++
title = "CloudFront"
description = "CloudFront control plane — distributions, invalidations, web ACL/alias association, tags. Full DistributionConfig round-trip with ETag/If-Match concurrency."
weight = 24
+++

fakecloud implements CloudFront's REST-XML control plane focused on the operations real applications and Terraform stacks rely on: distribution lifecycle, invalidations, alias and web ACL association, tags, the full policy resource surface (OAC + Cache/OriginRequest/ResponseHeaders/ContinuousDeployment), CloudFront Functions, public keys + key groups, key value stores, legacy origin access identities, per-distribution monitoring subscriptions, the legacy RTMP streaming distributions, field-level encryption configs + profiles, realtime log configs, VPC origins, anycast IP lists, trust stores, resource policies, connection groups, domain association + DNS verification, managed certificate details, and the promote-staging distribution swap. 167 operations.

**Status: Batches 1-6b shipped, plus ConnectionFunctions, DistributionTenants, and real `boa_engine`-backed `TestFunction` / `TestConnectionFunction` execution.** 167 of 167 operations.

## Supported today

- **Distributions** — `CreateDistribution`, `CreateDistributionWithTags`, `GetDistribution`, `GetDistributionConfig`, `UpdateDistribution`, `DeleteDistribution`, `ListDistributions`, `CopyDistribution`. Returns `ETag` headers; `UpdateDistribution` and `DeleteDistribution` enforce `If-Match`.
- **Distributions-by-X listings** — `ListDistributionsByCachePolicyId`, `ListDistributionsByOriginRequestPolicyId`, `ListDistributionsByResponseHeadersPolicyId`, `ListDistributionsByKeyGroup`, `ListDistributionsByWebACLId`, `ListDistributionsByVpcOriginId`, `ListDistributionsByAnycastIpListId`, `ListDistributionsByConnectionMode`, `ListDistributionsByConnectionFunction`, `ListDistributionsByOwnedResource`, `ListDistributionsByTrustStore`, `ListDistributionsByRealtimeLogConfig`.
- **Invalidations** — `CreateInvalidation` (returns `Completed` immediately for deterministic tests), `GetInvalidation`, `ListInvalidations`.
- **Tags** — `TagResource`, `UntagResource`, `ListTagsForResource` keyed by distribution ARN.
- **Aliases / Web ACL** — `AssociateAlias`, `ListConflictingAliases`, `AssociateDistributionWebACL`, `DisassociateDistributionWebACL`.
- **Origin Access Control** — `CreateOriginAccessControl`, `GetOriginAccessControl`, `GetOriginAccessControlConfig`, `UpdateOriginAccessControl`, `DeleteOriginAccessControl`, `ListOriginAccessControls`. Full ETag/If-Match concurrency.
- **Cache Policy** — `CreateCachePolicy`, `GetCachePolicy`, `GetCachePolicyConfig`, `UpdateCachePolicy`, `DeleteCachePolicy`, `ListCachePolicies` with `?Type=managed|custom`. AWS-managed `Managed-CachingOptimized`, `Managed-CachingDisabled`, `Managed-CachingOptimizedForUncompressedObjects`, `Managed-Elemental*` are pre-seeded by their well-known IDs and reject `Update`/`Delete`.
- **Origin Request Policy** — `CreateOriginRequestPolicy`, `GetOriginRequestPolicy`, `GetOriginRequestPolicyConfig`, `UpdateOriginRequestPolicy`, `DeleteOriginRequestPolicy`, `ListOriginRequestPolicies`. Managed `Managed-CORS-S3Origin`, `Managed-CORS-CustomOrigin`, `Managed-AllViewer`, `Managed-UserAgentRefererHeaders`, `Managed-AllViewerExceptHostHeader` pre-seeded.
- **Response Headers Policy** — `CreateResponseHeadersPolicy`, `GetResponseHeadersPolicy`, `GetResponseHeadersPolicyConfig`, `UpdateResponseHeadersPolicy`, `DeleteResponseHeadersPolicy`, `ListResponseHeadersPolicies`. Managed `Managed-SimpleCORS`, `Managed-CORS-With-Preflight`, `Managed-SecurityHeadersPolicy` pre-seeded.
- **Continuous Deployment Policy** — `CreateContinuousDeploymentPolicy`, `GetContinuousDeploymentPolicy`, `GetContinuousDeploymentPolicyConfig`, `UpdateContinuousDeploymentPolicy`, `DeleteContinuousDeploymentPolicy`, `ListContinuousDeploymentPolicies`.
- **CloudFront Functions** — `CreateFunction`, `DescribeFunction`, `GetFunction` (returns raw source bytes), `UpdateFunction`, `DeleteFunction`, `ListFunctions`, `PublishFunction` (DEVELOPMENT -> LIVE), `TestFunction`. `TestFunction` executes the user's `function handler(event) { ... }` against the supplied `EventObject` via an embedded `boa_engine` JavaScript runtime: the handler return value is JSON-encoded into `<FunctionOutput>`, thrown errors land in `<FunctionErrorMessage>`, and `console.log` / `console.error` lines are captured into `<FunctionExecutionLogs>`. `<ComputeUtilization>` reports a 0..=100 share of the per-request CPU budget on success and saturates past 100 on failure. `Stage` (DEVELOPMENT | LIVE) picks which version's source to run: DEVELOPMENT is the latest CreateFunction / UpdateFunction body, LIVE is the snapshot frozen at the most recent PublishFunction call (so post-publish updates don't leak into the published behaviour). Execution is capped by boa's loop-iteration and recursion limits plus a 250ms compute budget, measured as CPU time on the executing thread like AWS's ~1ms production budget (looser, so ordinary handlers never trip it unoptimized, while a CPU-bound handler still fails with the time-limit error); because it counts CPU rather than elapsed time, a loaded host that stalls the thread does not fail a handler that did little work, and a 5s wall-clock safety net abandons a run that never returns.
- **Connection Functions** — `CreateConnectionFunction`, `DescribeConnectionFunction`, `GetConnectionFunction` (raw source), `UpdateConnectionFunction`, `DeleteConnectionFunction`, `ListConnectionFunctions`, `PublishConnectionFunction`, `TestConnectionFunction`. `TestConnectionFunction` runs the same `boa_engine` runtime against the supplied `ConnectionObject` and emits the parallel `<ConnectionFunctionOutput>` / `<ConnectionFunctionErrorMessage>` / `<ConnectionFunctionExecutionLogs>` / `<ComputeUtilization>` shape, with the same DEVELOPMENT-vs-LIVE stage selection as `TestFunction`.
- **Public Keys + Key Groups** — full CRUD with `CallerReference` immutability on update.
- **Key Value Stores** — `CreateKeyValueStore` (with optional `ImportSource`), `DescribeKeyValueStore`, `UpdateKeyValueStore`, `DeleteKeyValueStore`, `ListKeyValueStores`.
- **Legacy Origin Access Identities** — `CreateCloudFrontOriginAccessIdentity`, `Get`, `GetConfig`, `Update`, `Delete`, `List`.
- **Monitoring Subscriptions** — `CreateMonitoringSubscription`, `GetMonitoringSubscription`, `DeleteMonitoringSubscription` keyed by distribution id.
- **Streaming Distributions (legacy RTMP)** — `CreateStreamingDistribution`, `CreateStreamingDistributionWithTags`, `GetStreamingDistribution`, `GetStreamingDistributionConfig`, `UpdateStreamingDistribution`, `DeleteStreamingDistribution`, `ListStreamingDistributions`. ETag/If-Match concurrency. `DeleteStreamingDistribution` enforces the AWS rule that the distribution must be `Enabled = false` before deletion (`StreamingDistributionNotDisabled`).
- **Field-Level Encryption** — `CreateFieldLevelEncryptionConfig`, `GetFieldLevelEncryption`, `GetFieldLevelEncryptionConfig`, `UpdateFieldLevelEncryptionConfig`, `DeleteFieldLevelEncryptionConfig`, `ListFieldLevelEncryptionConfigs`. ETag/If-Match concurrency, `CallerReference` immutability on update, duplicate `CallerReference` rejected with `FieldLevelEncryptionConfigAlreadyExists`.
- **Field-Level Encryption Profiles** — `CreateFieldLevelEncryptionProfile`, `GetFieldLevelEncryptionProfile`, `GetFieldLevelEncryptionProfileConfig`, `UpdateFieldLevelEncryptionProfile`, `DeleteFieldLevelEncryptionProfile`, `ListFieldLevelEncryptionProfiles`. Same concurrency + idempotency model as FLE configs.
- **Realtime Log Configs** — `CreateRealtimeLogConfig`, `GetRealtimeLogConfig` (by `Name` or `ARN`), `UpdateRealtimeLogConfig`, `DeleteRealtimeLogConfig` (by `Name` or `ARN`), `ListRealtimeLogConfigs`. Endpoint round-trip preserves `KinesisStreamConfig` `RoleARN`/`StreamARN` exactly.
- **VPC Origins** — `CreateVpcOrigin`, `GetVpcOrigin`, `UpdateVpcOrigin`, `DeleteVpcOrigin`, `ListVpcOrigins`. ETag/If-Match concurrency, duplicate `Name` rejected with `EntityAlreadyExists`. `DeleteVpcOrigin` returns the deleted resource and `ETag`. Status seeded as `Deployed` immediately for deterministic tests.
- **Anycast IP Lists** — `CreateAnycastIpList`, `GetAnycastIpList`, `UpdateAnycastIpList`, `DeleteAnycastIpList`, `ListAnycastIpLists`. `IpCount` validated to AWS allowed values (3 or 21). Synthesized deterministic `AnycastIps` payload returned on every read.
- **Trust Stores** — `CreateTrustStore`, `GetTrustStore` (by `identifier`), `UpdateTrustStore`, `DeleteTrustStore`, `ListTrustStores`. ETag/If-Match concurrency. `UpdateTrustStore` accepts the `httpPayload` `CaCertificatesBundleSource` body shape AWS uses (no name).
- **Resource Policies** — `PutResourcePolicy`, `GetResourcePolicy`, `DeleteResourcePolicy`. Policy documents are stored verbatim per resource ARN and round-tripped on get.
- **Connection Groups** — `CreateConnectionGroup`, `GetConnectionGroup`, `GetConnectionGroupByRoutingEndpoint`, `UpdateConnectionGroup`, `DeleteConnectionGroup`, `ListConnectionGroups`. ETag/If-Match concurrency. Routing endpoint synthesized as `<id>.cloudfront.net`. `Delete` enforces the AWS rule that the group must be `Enabled = false` first (`ResourceInUse`). Duplicate `Name` rejected with `EntityAlreadyExists`.
- **Domain ops** — `ListDomainConflicts` (returns empty conflicts in fakecloud since there is no global DNS namespace), `UpdateDomainAssociation` (round-trips `Domain` + target `DistributionId`/`DistributionTenantId`), `VerifyDnsConfiguration` (returns a deterministic `valid-configuration` status).
- **Managed Certificate Details** — `GetManagedCertificateDetails` returns a synthesized ACM certificate ARN + `issued` status keyed by the supplied identifier.
- **Promote-staging** — `UpdateDistributionWithStagingConfig` swaps the distribution's `ETag` against the configured `StagingDistributionId` and rejects unknown staging ids with `NoSuchDistribution`.
- **Distribution Tenants** — `CreateDistributionTenant`, `GetDistributionTenant`, `GetDistributionTenantByDomain`, `UpdateDistributionTenant`, `DeleteDistributionTenant`, `ListDistributionTenants`, `ListDistributionTenantsByCustomization`, `VerifyDnsConfiguration` (tenant-scoped). ETag/If-Match concurrency, per-tenant `Customizations` (web ACL, certificate, geo restrictions, origin overrides) round-trip element-for-element, duplicate `Name` rejected with `EntityAlreadyExists`.

### Concurrency semantics

CloudFront's `ETag` model is preserved. Every successful `Create`/`Get`/`Update` returns the current `ETag` header, and `UpdateDistribution`/`DeleteDistribution` reject requests whose `If-Match` does not match the in-memory revision with `412 PreconditionFailed`. `DeleteDistribution` also enforces the AWS rule that the distribution must be `Enabled = false` before deletion (`DistributionNotDisabled`).

### `DistributionConfig` round-trip

`DistributionConfig` is parsed into typed Rust structs that cover the full standard surface: origins (S3, custom, VPC, OAC fields), cache behaviors (forwarded values, allowed methods, cached methods, function associations, lambda function associations, gRPC config), custom error responses, viewer certificates, geo restrictions, logging config, origin groups, tenant config, and is serialized back element-for-element. `GetDistributionConfig` returns what `CreateDistribution`/`UpdateDistribution` accepted, with the members a request omitted filled in as CloudFront does (`Quantity` 0 envelopes such as `OriginGroups`, `Restrictions` of `none`, the default `ViewerCertificate`, `http2`). A standard distribution also gets an empty `Aliases`, `PriceClass_All`, `IsIPV6Enabled` true, `Staging` false and an empty `ContinuousDeploymentPolicyId`. A multi-tenant (`tenant-only`) distribution's config leaves those standard-only members out, along with `TrustedSigners` and `SmoothStreaming`; `ListDistributions` still reports them in its summary as real AWS does (`Aliases` `Quantity` 0, `PriceClass_All`, `IsIPV6Enabled` false, `Staging` false). Summaries spell `HttpVersion` in enum case (`HTTP1_1`, `HTTP2`, `HTTP2and3`) where configs have it lower case. A standard distribution's config has no `ConnectionMode`; its summary reports `direct`. Values a request supplied are kept, except that the deprecated `Certificate`/`CertificateSource` pair always follows the certificate in use and the default CloudFront certificate on a standard distribution always reports `TLSv1` and an `SSLSupportMethod` of `vip`. On a multi-tenant distribution it reports `sni-only`, with `TLSv1`, or `TLSv1.2_2021` if the request gave `sni-only` and no security policy. Putting the returned config back is a no-op that leaves the ETag unchanged.

Cache settings are checked as CloudFront checks them, on the default behavior and every entry in `CacheBehaviors`:

- On `CreateDistribution` and `CreateDistributionWithTags`, a behavior without a cache policy must set `MinTTL` and `ForwardedValues` (`InvalidArgument`). `DefaultTTL` then defaults to 86400 held between `MinTTL` and `MaxTTL`, and `MaxTTL` to 31536000 or `DefaultTTL` if higher, so `MinTTL` 0 with `MaxTTL` 600 reads back as 0/600/600. TTLs out of order fail with `InvalidTTLOrder`.
- `UpdateDistribution` differs: such a behavior must set all three TTLs, nothing is derived, and TTLs out of order are stored as given.
- A behavior with a cache policy can't set any TTL or `ForwardedValues` (`InvalidArgument`), and carries no TTLs. An empty `CachePolicyId` counts as none and reads back absent.
- A multi-tenant distribution must set `CachePolicyId`, and can't set `IsIPV6Enabled`, `Staging`, `PriceClass`, `Aliases`, `ContinuousDeploymentPolicyId`, `AnycastIpListId`, `SmoothStreaming`, `TrustedSigners`, `ForwardedValues` or any TTL (`InvalidArgument`), even to empty or default values. `AssociateAlias` refuses one too, as does an `SSLSupportMethod` of `vip` or a `WebACLId` that isn't a WAF V2 ARN.
- `TenantConfig` is only for a multi-tenant distribution (`InvalidArgument` on a standard one).
- On both modes, an origin access identity, enabled trusted key group, field-level encryption config or real-time log config the account doesn't have is refused with CloudFront's error for it. IAM certificates, Lambda@Edge functions and logging buckets aren't checked, because they live in other services.
- A negative TTL fails with `InvalidArgument`, and a `ConnectionMode` other than `direct` or `tenant-only` with `MalformedXML`. An explicit `direct` is accepted and not echoed.
- `UpdateDistribution`, `DeleteDistribution`, `CopyDistribution` and `UpdateDistributionWithStagingConfig` (which takes `<primary ETag>, <staging ETag>`) refuse an `If-Match` that isn't shaped like an ETag with `InvalidIfMatchVersion`, unless it is exactly the current ETag, as one issued by an older version may be, and a well-shaped but superseded one with `PreconditionFailed`. On an update the config's rules run between those two checks, so a superseded ETag with an invalid config reports the config. `UpdateDistribution` can't change a distribution's mode, its `CallerReference` or its `Staging` status (`IllegalUpdate`). Sending back the config `GetDistributionConfig` returned always succeeds, including one stored by an older version.
- `UpdateDomainAssociation` refuses a domain nothing in the account holds with `EntityNotFound`, ahead of the target and `If-Match` checks.

`AWS::CloudFront::Distribution` fills omitted TTLs with its schema defaults independently (`MinTTL` 0, `DefaultTTL` 86400, `MaxTTL` 31536000), so unlike the API, `MaxTTL` 600 alone fails as out of order. The create rules apply to templates otherwise, and the update rules to stack updates, reported with the `Invalid request provided: AWS::CloudFront::Distribution: ` prefix. A template's omitted `HttpVersion` is `http1.1`, not the API's `http2`. A multi-tenant template must name a `MinimumProtocolVersion` in its `ViewerCertificate`.

### Idempotency

`CreateDistribution` rejects a second call with the same `CallerReference` with `DistributionAlreadyExists` (matches AWS). `CallerReference` is unique per account.

### Accounts

CloudFront is global (no region), but every resource belongs to the account that created it, as in AWS. Distributions, invalidations, policies, functions, key value stores, OACs, OAIs, tags and the rest are visible to and changeable by their owning account only; another account gets the usual `NoSuch*` error and does not see them in its listings. AWS-managed cache, origin request and response headers policies are visible to every account. Names (functions, policies, connection groups, ...) are unique per account. Identifiers that double as a global domain (a distribution's `<id>.cloudfront.net`) and alternate domain names stay unique across all accounts: `CreateDistribution`, `UpdateDistribution`, `AssociateAlias`, `CreateDistributionTenant` and `UpdateDistributionTenant` reject a domain (compared case-insensitively) held by any other distribution or tenant with `CNAMEAlreadyExists`, and `UpdateDomainAssociation` only moves a domain between resources of the caller's own account (`AccessDenied` otherwise). `CopyDistribution` does not copy the primary's alternate domain names, requires a `Staging` header of true, refuses a multi-tenant primary or one whose TTLs are out of order, leaves the copy's `ContinuousDeploymentPolicyId` empty, and promoting a staging config keeps the primary's alternate domain names and continuous deployment policy. `POST /_fakecloud/reset/cloudfront/{account_id}` clears one account's CloudFront state. Persistence snapshots written before resources were account-scoped are split by owner on load: a resource whose ARN names an account (CloudFormation-provisioned ones) returns to that account, and the rest load into the server's default account (`--account-id`) with their ARNs rewritten to it.

## Smoke test

```sh
fakecloud &

# Create a distribution
DIST=$(aws --endpoint-url http://localhost:4566 cloudfront create-distribution \
  --distribution-config '{
    "CallerReference": "smoke-1",
    "Comment": "smoke",
    "Enabled": true,
    "Origins": {
      "Quantity": 1,
      "Items": [
        { "Id": "primary", "DomainName": "origin.example.com",
          "CustomOriginConfig": { "HTTPPort": 80, "HTTPSPort": 443, "OriginProtocolPolicy": "https-only" } }
      ]
    },
    "DefaultCacheBehavior": {
      "TargetOriginId": "primary",
      "ViewerProtocolPolicy": "redirect-to-https",
      "ForwardedValues": { "QueryString": false, "Cookies": { "Forward": "none" } },
      "MinTTL": 0
    }
  }')

ID=$(echo "$DIST" | jq -r '.Distribution.Id')

# Invalidate something
aws --endpoint-url http://localhost:4566 cloudfront create-invalidation \
  --distribution-id "$ID" \
  --invalidation-batch '{"Paths":{"Quantity":1,"Items":["/*"]},"CallerReference":"inv-1"}'

# List
aws --endpoint-url http://localhost:4566 cloudfront list-invalidations --distribution-id "$ID"
```

## Admin endpoints

- `GET /_fakecloud/cloudfront/distributions` — list stored distributions across all accounts with `{id, domainName, enabled, served}`. `domainName` is the distribution's `<id>.cloudfront.net` domain — send it as the `Host` header to fakecloud's main endpoint to reach the distribution (see below). `served` is `true` when the distribution is enabled and the data plane is active (`false` when disabled, or when the data plane is turned off via `FAKECLOUD_CLOUDFRONT_DISABLE_DATAPLANE`).
- `POST /_fakecloud/cloudfront/distributions/{id}/status` — flip a stored distribution's reported `Status` (e.g. between `InProgress` and `Deployed`) without waiting on the auto-deploy tick. Body: `{"status": "Deployed"}`. Returns `204 No Content` on success and `404 Not Found` for an unknown id. Useful for tests that assert behavior gated on the post-deploy status.

## Local data plane

fakecloud runs a single-node, in-process HTTP data plane for CloudFront. Distributions are served **on fakecloud's own main `--addr` listener**, routed by the request `Host` header — there is no separate per-distribution port. Viewer requests carry no account, so a distribution is served by its domain whichever account owns it. A viewer request whose `Host` matches an **enabled** distribution's `DomainName` (`<id>.cloudfront.net`) or one of its alternate domain names (`Aliases` / CNAMEs) is served by the data plane; every other request (the AWS API, `/_fakecloud/*`) is dispatched normally.

Because distributions share the main port, a distribution is reachable from outside a container whenever that port is published — no second listener to expose. This matches real CloudFront, where a distribution is reached by its domain, not a port:

```bash
docker run -d -p 4566:4566 ghcr.io/faiscadev/fakecloud:latest
# ... create a distribution, note its DomainName (e.g. e1abc.cloudfront.net) ...
# Reach it from the host by sending the distribution domain as Host:
curl -s -H 'Host: e1abc.cloudfront.net' http://localhost:4566/
```

Viewer requests are served by:

- **Host routing** — the `Host` header selects the distribution (its `<id>.cloudfront.net` domain or an alias CNAME; case-insensitive, port ignored).
- **Path-pattern routing** — the request path is matched against the ordered `CacheBehaviors` (`*` / `?` globs, e.g. `/api/*`), falling back to the `DefaultCacheBehavior`, to pick the target origin.
- **Origin fetch** — the origin is reverse-proxied. An S3 origin, whether a REST endpoint (`<bucket>.s3.<region>.amazonaws.com`, what CDK's `S3BucketOrigin` emits) or a website endpoint (`<bucket>.s3-website-<region>.amazonaws.com` or `<bucket>.s3-website.<region>.amazonaws.com`), under any partition's DNS suffix, is served by this same process and fetched in-process, dispatched straight to the S3 service for that bucket, never from real AWS. Every viewer path is an object key in the origin bucket: `/_fakecloud/...` or `/latest/...` through a distribution names an object, never one of fakecloud's own endpoints, and viewer headers such as `X-Amz-Target` cannot send the fetch to another service. A custom origin that points at fakecloud's own address is fetched over HTTP as configured. A custom origin honors its `CustomOriginConfig`: an `https-only` protocol policy is fetched over HTTPS (otherwise HTTP), and the configured `HTTPPort` / `HTTPSPort` is used unless the origin `DomainName` already carries an explicit `host:port`.
- **Private S3 origins (OAC / OAI)**: an S3 REST origin with an `OriginAccessControlId` is fetched as the `cloudfront.amazonaws.com` service principal, with `aws:SourceArn` = the distribution ARN and `aws:SourceAccount` = its account, following the origin access control's `SigningBehavior`: `always` signs every request (replacing a viewer `Authorization` header), `no-override` signs unless the viewer sent its own `Authorization` header (forwarded as is), `never` does not sign. An origin with a legacy `S3OriginConfig.OriginAccessIdentity` (`origin-access-identity/cloudfront/<id>`) is fetched as that OAI. Other origins, website endpoints, and origins naming an OAC or OAI the distribution's account does not hold are fetched anonymously. With `--iam soft|strict` the bucket policy then decides as in AWS: the policy `S3BucketOrigin.withOriginAccessControl` generates (`Principal: {"Service": "cloudfront.amazonaws.com"}` conditioned on `AWS:SourceArn`) lets that distribution read a private bucket and no other, and an OAI is granted by its principal ARN or its `CanonicalUser` id. See [IAM enforcement](@/docs/reference/security.md) for how these principals are evaluated. Without `--iam` nothing is enforced and every origin serves.
- **CustomErrorResponses** — an origin status matching a configured rule with a response page path is served from the default origin with the rule's `ResponseCode` (e.g. `404 -> /index.html` returned as `200`, the SPA deep-link fallback).

Disable the data plane with `FAKECLOUD_CLOUDFRONT_DISABLE_DATAPLANE=1` to run control-plane only (distributions then report `served: false`).

## Caveats

The data plane is a single local node, not the global CDN: there is no geo/edge distribution or per-PoP behavior. In-path CloudFront Functions / Lambda@Edge and real TTL caching / invalidation are not implemented: the data plane forwards uncached. Origin access controls authorize S3 origin fetches through the bucket policy; the object's SSE-KMS key policy is not consulted for them (fakecloud does not evaluate key policies for S3's own decrypts). CloudFront Functions can still be exercised out-of-band via `TestFunction` (this does not apply to Lambda@Edge).
