+++
title = "AWS Serverless Application Repository"
description = "AWS Serverless Application Repository (serverlessrepo) on fakecloud: a complete 14-operation control plane (100% conformance). restJson1."
weight = 76
+++

fakecloud implements the **AWS Serverless Application Repository** (SAR) as a
restJson1 service. All **14 operations** ship with **100% conformance** against
AWS's own Smithy model, backed by account-partitioned state that persists across
restarts in persistent mode. SAR signs SigV4 with the `serverlessrepo` scope and
is routed by HTTP method plus `@http` URI path under the `/applications` prefix
(`POST /applications`, `GET /applications/{ApplicationId}`,
`PUT /applications/{ApplicationId}/versions/{SemanticVersion}`,
`GET /applications/{ApplicationId}/policy`, ...).

AWS Serverless Application Repository on fakecloud is a faithful **publishing
control plane**: every `CreateApplication` / `CreateApplicationVersion` /
`PutApplicationPolicy` is reflected by its `Get*` / `List*`, `DeleteApplication`
deletes, and a stored SAM/CloudFormation template is really parsed so the
`Version` block returns the same `parameterDefinitions` a real publish would.

## Resources

- **Applications** - `CreateApplication` (`POST /applications`) validates the
  required `author`, `description`, and `name`, then mints an
  `arn:aws:serverlessrepo:<region>:<account>:applications/<name>` ARN — which, in
  SAR, *is* the `applicationId`. It stores `homePageUrl`, `labels`,
  `licenseBody`/`licenseUrl`, `readmeBody`/`readmeUrl`, `spdxLicenseId`, and
  `sourceCodeUrl`. When a `semanticVersion` (plus `templateBody` / `templateUrl`)
  is supplied, an initial version is seeded. `GetApplication`
  (`GET /applications/{ApplicationId}`, optional `?semanticVersion=`) returns the
  application plus its `Version` block; `ListApplications` paginates with a
  round-tripping `nextToken`; `UpdateApplication` (`PATCH`) patches
  `author`/`description`/`homePageUrl`/`labels`/`readmeBody`-or-`readmeUrl`; and
  `DeleteApplication` (`DELETE`) removes it. A duplicate application name is
  rejected with `ConflictException`; an unknown application id is
  `NotFoundException`.
- **Versions** - `CreateApplicationVersion`
  (`PUT /applications/{ApplicationId}/versions/{SemanticVersion}`) stores a
  version with its `templateBody`/`templateUrl`, `sourceCodeUrl`, and
  `sourceCodeArchiveUrl`, parsing `parameterDefinitions`, `requiredCapabilities`,
  and `resourcesSupported` from the template. `ListApplicationVersions` paginates
  the semantic versions.
- **Parameter definitions** - the template's `Parameters` section is parsed into
  the `ParameterDefinition` list SAR surfaces on the `Version` block: `name`,
  `type`, `defaultValue`, `description`, `allowedValues`, `allowedPattern`,
  `constraintDescription`, `min`/`maxLength`, `min`/`maxValue`, `noEcho`, and the
  required `referencedByResources` (the logical resource ids that `Ref` the
  parameter). `requiredCapabilities` is derived from the template's resource types
  (`CAPABILITY_IAM` / `CAPABILITY_NAMED_IAM` for IAM resources,
  `CAPABILITY_RESOURCE_POLICY`, `CAPABILITY_AUTO_EXPAND` for the SAM transform /
  nested applications). Both JSON and YAML (the usual SAM authoring format)
  templates are parsed, including YAML short-form intrinsic tags (`!Ref`,
  `!Sub`, `!GetAtt`, ...), which SAM templates use heavily.
- **Sharing policy** - `PutApplicationPolicy`
  (`PUT /applications/{ApplicationId}/policy`) stores the sharing statements
  (`principals`, `actions`, `principalOrgIDs`), assigning a `statementId` to any
  statement that omits one; `GetApplicationPolicy` returns them; and
  `UnshareApplication` (`POST /applications/{ApplicationId}/unshare`) removes an
  organisation (`organizationId`) from every statement's `principalOrgIDs`.
- **CloudFormation templates** - `CreateCloudFormationTemplate`
  (`POST /applications/{ApplicationId}/templates`) mints a `templateId`, a
  creation + expiration time, and a `templateUrl` that points back at this
  fakecloud host, with status `PREPARING`. `GetCloudFormationTemplate` settles the
  status to `ACTIVE` on the first read.
- **Dependencies** - `ListApplicationDependencies`
  (`GET /applications/{ApplicationId}/dependencies`, optional `?semanticVersion=`)
  returns the nested-application dependencies parsed from the template's
  `AWS::Serverless::Application` resources (an empty list when there are none),
  paginated.

## Validation and errors

Model-derived validation rejects contract violations with the error codes SAR
declares: a missing required body member or an omitted required path label ->
`BadRequestException`; an unknown application / version / template ->
`NotFoundException`; a duplicate application name -> `ConflictException`. The
full declared set is `BadRequestException`, `NotFoundException`,
`ConflictException`, `ForbiddenException`, `InternalServerErrorException`, and
`TooManyRequestsException`.

## Honest gaps (documented, not stubbed)

- **`CreateCloudFormationChangeSet`** returns well-formed `changeSetId`,
  `stackId`, `applicationId`, and `semanticVersion` values, but fakecloud does
  **not** drive the CloudFormation service to materialise a real stack or change
  set. There is no clean in-process seam to do so, and fabricating a stack the
  CloudFormation service does not know about would be worse than an honest,
  correctly-shaped identifier. Clients that call the operation and read back the
  returned identifiers behave as against AWS; a subsequent `ExecuteChangeSet`
  against the CloudFormation service would not find the change set.
- **`templateUrl`** (returned by the `Version` block and by
  `CreateCloudFormationTemplate` / `GetCloudFormationTemplate`) is a well-formed,
  deterministic URL pointing back at this fakecloud host. The SAR control plane
  stores the template body in memory, but fakecloud does not currently serve the
  raw template bytes at that URL.

Everything else — application, version, policy, and template storage, plus the
`parameterDefinition` parsing — works in memory and persists across restarts.
