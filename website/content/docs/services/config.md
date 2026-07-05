+++
title = "Config"
description = "AWS Config — configuration recorder, real cross-service config-item recording, config rules with genuine managed-rule and custom-Lambda evaluation, compliance, conformance packs, and aggregators. JSON 1.1 protocol."
weight = 70
+++

fakecloud implements AWS Config's full JSON 1.1 API: 97 operations covering the configuration recorder, configuration items, config rules with real evaluation, compliance, remediation, conformance packs, organization rules, aggregators, retention, stored queries, and resource evaluations. 100% Smithy conformance.

**Status: 100% coverage with a real recording + rule-evaluation data plane.**

## Supported today

- **Configuration recorder** — `PutConfigurationRecorder`, `Describe*`, `Delete*`, `Start`/`StopConfigurationRecorder`, `ListConfigurationRecorders`, recorder status, service-linked recorders (`PutServiceLinkedConfigurationRecorder`), and `Associate`/`DisassociateResourceTypes`. When a recorder is running, Config snapshots the live state of other fakecloud services into genuine configuration-item history.
- **Real configuration items** — with a running recorder, Config discovers and records real resources that exist in other fakecloud services (`AWS::S3::Bucket`, `AWS::EC2::Instance`, `AWS::EC2::SecurityGroup`, `AWS::EC2::VPC`, `AWS::IAM::User`, `AWS::IAM::Role`, `AWS::IAM::Policy`) by reading their actual state — not fabricated placeholders. A new `ConfigurationItem` is appended only when a resource is new or its configuration changed, so `GetResourceConfigHistory` returns true history. `PutResourceConfig` records external/custom resource types, `DeleteResourceConfig` marks them deleted, and `BatchGetResourceConfig`, `ListDiscoveredResources`, and `GetDiscoveredResourceCounts` return the recorded items.
- **Config rules with real evaluation** — `PutConfigRule` creates AWS-managed, `CUSTOM_LAMBDA`, or `CUSTOM_POLICY` rules. AWS **managed** rules run genuine evaluation logic against the recorded config items and produce real `COMPLIANT` / `NON_COMPLIANT` results: `S3_BUCKET_VERSIONING_ENABLED`, `S3_BUCKET_SERVER_SIDE_ENCRYPTION_ENABLED`, `S3_BUCKET_PUBLIC_READ_PROHIBITED`, `S3_BUCKET_PUBLIC_WRITE_PROHIBITED`, `IAM_USER_NO_POLICIES_CHECK`, `EC2_INSTANCE_NO_PUBLIC_IP`, `VPC_DEFAULT_SECURITY_GROUP_CLOSED`, `INCOMING_SSH_DISABLED`, `RESTRICTED_INCOMING_TRAFFIC`, and `REQUIRED_TAGS`. **Custom** (`CUSTOM_LAMBDA`) rules actually invoke the referenced Lambda function via fakecloud-lambda with a Config invoking event and record the returned evaluations. `PutEvaluations` / `PutExternalEvaluation` record results from a custom or external rule. Managed rules that are not implemented evaluate as `INSUFFICIENT_DATA` rather than a fabricated `COMPLIANT`.
- **Compliance** — `DescribeComplianceByConfigRule`, `DescribeComplianceByResource`, `GetComplianceDetailsByConfigRule`, `GetComplianceDetailsByResource`, `GetComplianceSummaryByConfigRule`, and `GetComplianceSummaryByResourceType` all reflect the real evaluation results.
- **Advanced query** — `SelectResourceConfig` runs a real subset of the Config query language (`SELECT <fields> [WHERE <conditions>]`, with `=` and `IN`, dotted `configuration.*` and `tags.*` paths) over the recorded configuration items. `SelectAggregateResourceConfig` runs the same over the aggregated items.
- **Remediation** — `Put`/`Describe`/`DeleteRemediationConfiguration`, remediation exceptions, `StartRemediationExecution`, and execution status.
- **Conformance packs** — `PutConformancePack`, `Describe*`, `Delete*`, status, and compliance (`DescribeConformancePackCompliance`, `GetConformancePackComplianceSummary`, `GetConformancePackComplianceDetails`, `ListConformancePackComplianceScores`) aggregated over the pack's rules.
- **Organization rules & packs** — organization config rules and organization conformance packs with statuses and detailed status.
- **Aggregators** — configuration aggregators, aggregation authorizations, aggregate compliance queries, and aggregate discovered-resource listings/counts.
- **Retention, stored queries, resource evaluations, tags** — retention configurations, stored queries (`Put`/`Get`/`Delete`/`List`), `StartResourceEvaluation` + summary + list, and `Tag`/`Untag`/`ListTagsForResource`.

> Request members are validated against the Smithy model's `required` / `length` / `range` / `enum` constraints and rejected with `ValidationException` before any business logic runs, exactly as AWS does.

## Smoke test

```sh
fakecloud &
E=http://localhost:4566

# Start recording and create a managed rule.
aws --endpoint-url $E configservice put-configuration-recorder \
  --configuration-recorder name=default,roleARN=arn:aws:iam::123456789012:role/config

aws --endpoint-url $E configservice start-configuration-recorder \
  --configuration-recorder-name default

aws --endpoint-url $E configservice put-config-rule --config-rule '{
  "ConfigRuleName": "s3-versioning",
  "Source": { "Owner": "AWS", "SourceIdentifier": "S3_BUCKET_VERSIONING_ENABLED" }
}'

# Create an S3 bucket with versioning off, then evaluate.
aws --endpoint-url $E s3api create-bucket --bucket demo-bucket
aws --endpoint-url $E configservice start-config-rules-evaluation \
  --config-rule-names s3-versioning

aws --endpoint-url $E configservice describe-compliance-by-config-rule \
  --config-rule-names s3-versioning
# -> NON_COMPLIANT (versioning is not enabled)
```

## CloudFormation

`AWS::Config::ConfigurationRecorder`, `AWS::Config::DeliveryChannel`, `AWS::Config::ConfigRule`, `AWS::Config::ConfigurationAggregator`, `AWS::Config::AggregationAuthorization`, `AWS::Config::ConformancePack`, and `AWS::Config::OrganizationConfigRule` are provisioned into real Config state (and persist across restarts).

## Not implemented

- Managed rules outside the set listed above evaluate as `INSUFFICIENT_DATA` rather than returning a fabricated verdict.
- `SelectResourceConfig` implements a tractable subset of the Config query language (equality / `IN` filters and field projection); `GROUP BY`, aggregate functions, and `LIKE` are not yet supported.
- Multi-account / multi-region aggregation reflects the local account's recorded state (single-account deployment).
