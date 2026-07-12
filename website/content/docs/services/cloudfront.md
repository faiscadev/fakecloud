+++
title = "CloudFront"
description = "CloudFront control plane — distributions, invalidations, web ACL/alias association, tags. Full DistributionConfig round-trip with ETag/If-Match concurrency."
weight = 24
+++

fakecloud implements CloudFront's REST-XML control plane focused on the operations real applications and Terraform stacks rely on: distribution lifecycle, invalidations, alias and web ACL association, tags, the full policy resource surface (OAC + Cache/OriginRequest/ResponseHeaders/ContinuousDeployment), CloudFront Functions, public keys + key groups, key value stores, legacy origin access identities, per-distribution monitoring subscriptions, the legacy RTMP streaming distributions, field-level encryption configs + profiles, realtime log configs, VPC origins, anycast IP lists, trust stores, resource policies, connection groups, domain association + DNS verification, managed certificate details, and the promote-staging distribution swap. 147 operations.

**Status: Batches 1-6b shipped, plus ConnectionFunctions, DistributionTenants, and real `boa_engine`-backed `TestFunction` / `TestConnectionFunction` execution.** 147 of 147 operations.

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
- **CloudFront Functions** — `CreateFunction`, `DescribeFunction`, `GetFunction` (returns raw source bytes), `UpdateFunction`, `DeleteFunction`, `ListFunctions`, `PublishFunction` (DEVELOPMENT -> LIVE), `TestFunction`. `TestFunction` executes the user's `function handler(event) { ... }` against the supplied `EventObject` via an embedded `boa_engine` JavaScript runtime: the handler return value is JSON-encoded into `<FunctionOutput>`, thrown errors land in `<FunctionErrorMessage>`, and `console.log` / `console.error` lines are captured into `<FunctionExecutionLogs>`. `<ComputeUtilization>` reports a 0..=100 share of the per-request CPU budget on success and saturates past 100 on failure. `Stage` (DEVELOPMENT | LIVE) picks which version's source to run: DEVELOPMENT is the latest CreateFunction / UpdateFunction body, LIVE is the snapshot frozen at the most recent PublishFunction call (so post-publish updates don't leak into the published behaviour). Execution is capped by boa's loop-iteration and recursion limits plus a 250ms wall-clock guard (looser than AWS's ~1ms production budget so CI runners with noisy neighbours don't false-alarm; tests rely on the cap to kill `while(1){}`).
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

`DistributionConfig` is parsed into typed Rust structs that cover the full standard surface — origins (S3, custom, VPC, OAC fields), cache behaviors (forwarded values, allowed methods, cached methods, function associations, lambda function associations, gRPC config), custom error responses, viewer certificates, geo restrictions, logging config, origin groups, tenant config — and is serialized back element-for-element. `GetDistributionConfig` returns exactly what `CreateDistribution`/`UpdateDistribution` accepted.

### Idempotency

`CreateDistribution` rejects a second call with the same `CallerReference` with `DistributionAlreadyExists` (matches AWS).

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

- `GET /_fakecloud/cloudfront/distributions` — list stored distributions with `{id, domainName, enabled, served}`. `domainName` is the distribution's `<id>.cloudfront.net` domain — send it as the `Host` header to fakecloud's main endpoint to reach the distribution (see below). `served` is `true` when the distribution is enabled and the data plane is active (`false` when disabled, or when the data plane is turned off via `FAKECLOUD_CLOUDFRONT_DISABLE_DATAPLANE`).
- `POST /_fakecloud/cloudfront/distributions/{id}/status` — flip a stored distribution's reported `Status` (e.g. between `InProgress` and `Deployed`) without waiting on the auto-deploy tick. Body: `{"status": "Deployed"}`. Returns `204 No Content` on success and `404 Not Found` for an unknown id. Useful for tests that assert behavior gated on the post-deploy status.

## Local data plane

fakecloud runs a single-node, in-process HTTP data plane for CloudFront. Distributions are served **on fakecloud's own main `--addr` listener**, routed by the request `Host` header — there is no separate per-distribution port. A viewer request whose `Host` matches an **enabled** distribution's `DomainName` (`<id>.cloudfront.net`) or one of its alternate domain names (`Aliases` / CNAMEs) is served by the data plane; every other request (the AWS API, `/_fakecloud/*`) is dispatched normally.

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
- **Origin fetch** — the origin is reverse-proxied. An S3-website origin (`*.s3-website-<region>.amazonaws.com`) is reached on fakecloud's own port with the website `Host` preserved (as real CloudFront treats an S3-website endpoint as a custom origin). A custom origin honors its `CustomOriginConfig`: an `https-only` protocol policy is fetched over HTTPS (otherwise HTTP), and the configured `HTTPPort` / `HTTPSPort` is used unless the origin `DomainName` already carries an explicit `host:port`.
- **CustomErrorResponses** — an origin status matching a configured rule with a response page path is served from the default origin with the rule's `ResponseCode` (e.g. `404 -> /index.html` returned as `200`, the SPA deep-link fallback).

Disable the data plane with `FAKECLOUD_CLOUDFRONT_DISABLE_DATAPLANE=1` to run control-plane only (distributions then report `served: false`).

## Caveats

The data plane is a single local node, not the global CDN: there is no geo/edge distribution or per-PoP behavior. In-path CloudFront Functions / Lambda@Edge, real TTL caching / invalidation, and OAC/SigV4 to private-S3 origins are not implemented — the MVP forwards uncached and serves public S3-website origins. CloudFront Functions can still be exercised out-of-band via `TestFunction` (this does not apply to Lambda@Edge).
