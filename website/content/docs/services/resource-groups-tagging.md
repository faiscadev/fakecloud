+++
title = "Resource Groups Tagging API"
description = "AWS Resource Groups Tagging API (tagging) on fakecloud: cross-service tag reads (GetResources / GetTagKeys / GetTagValues), tag writes (TagResources / UntagResources), compliance summary, and tag reports, backed by a real cross-service tag index."
weight = 44
+++

fakecloud implements the **Resource Groups Tagging API** (`tagging`), the
service that reads and writes resource tags **across every service** in an
account and region from one endpoint. All **9 operations** from the AWS Smithy
model ship now, backed by account-partitioned state that persists across
restarts in persistent mode.

## Supported features

- **Cross-service tag reads** (`GetResources`, `GetTagKeys`, `GetTagValues`).
  `GetResources` returns every resource with its tags, honoring
  `ResourceARNList`, `ResourceTypeFilters` (`service` or `service:type`),
  `TagFilters` (key present, optionally value-in-set), and `ResourcesPerPage`
  pagination. `GetTagKeys` / `GetTagValues` enumerate the distinct keys and the
  values for a key.
- **Tag writes** (`TagResources`, `UntagResources`) apply or remove tags on any
  ARN. Failures come back in `FailedResourcesMap`, matching AWS's per-resource
  partial-success shape.
- **Compliance** (`GetComplianceSummary`, `ListRequiredTags`) report against
  Organizations tag policies. With no tag policy in effect nothing is
  non-compliant and nothing is required, so both return empty — the same as a
  fresh AWS account.
- **Tag reports** (`StartReportCreation`, `DescribeReportCreation`) drive the
  async account-wide tag report to an S3 bucket.

100% conformance: all 260 generated Smithy probe variants pass.

## The cross-service tag index

Reads aggregate a shared **tag-provider registry**: each service exposes its
live tagged resources through a provider, so the tagging API reflects current
service state instead of a second, drifting copy. On top of that, tags applied
directly through `TagResources` — including to arbitrary ARNs no modelled
service owns, which AWS also allows — are stored and merged into results.

Per-service tag providers are wired in incrementally (the same staged rollout
used for persistence across services). Today the index reflects tags applied
through this API; each service that gets a provider then also surfaces its
native tags here and, in turn, lights up Resource Groups tag-query membership
resolution.

## Example

```python
import boto3
tagging = boto3.client("resourcegroupstaggingapi", endpoint_url="http://localhost:4566")

tagging.tag_resources(
    ResourceARNList=["arn:aws:custom:us-east-1:123456789012:thing/abc"],
    Tags={"stage": "prod", "team": "web"},
)

# All resources carrying stage=prod.
resp = tagging.get_resources(
    TagFilters=[{"Key": "stage", "Values": ["prod"]}],
)
for m in resp["ResourceTagMappingList"]:
    print(m["ResourceARN"], m["Tags"])

print(tagging.get_tag_keys()["TagKeys"])
print(tagging.get_tag_values(Key="stage")["TagValues"])
```
