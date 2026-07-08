+++
title = "AWS Support"
description = "AWS Support on fakecloud: the full 16-operation surface -- support cases, communications, attachment sets, severity levels, the service/category catalogue, and the Trusted Advisor check catalogue + refresh state machine -- with account-partitioned persistence. The Trusted Advisor analysis engine and the live support agent are documented gaps."
weight = 77
+++

fakecloud implements **AWS Support** (`support`), the programmatic interface to
AWS Support cases and AWS Trusted Advisor. All **16 operations** from the AWS
Smithy model ship now, backed by account-partitioned state that persists across
restarts in persistent mode. The wire protocol is awsJson1.1 (x-amz-target
`AWSSupport_20130415.<Op>`), signing as `support`.

This is the **control plane**: every case, communication, attachment set, and
Trusted Advisor refresh status is real, validated, persisted state -- no stubbed
success responses. Requests are validated against the model's `required` /
`length` / `range` constraints before any handler runs, and an operation that
dereferences a case that does not exist returns Support's `CaseIdNotFound`; an
unknown attachment id returns `AttachmentIdNotFound`, and an unknown attachment
set id returns `AttachmentSetIdNotFound`.

## Supported features

### Support cases

- **`CreateCase`** mints an AWS-shaped case id (`case-{account}-{year}-{hex}`)
  and a numeric `displayId`, opens the case with `status: "opened"`, records the
  `subject` / `serviceCode` / `severityCode` / `categoryCode` /
  `ccEmailAddresses` / `language` / `issueType`, and seeds the case's
  communication thread with the initial `communicationBody`. A supplied
  `attachmentSetId` is resolved onto the first communication (an unknown set id
  returns `AttachmentSetIdNotFound`).
- **`DescribeCases`** filters by `caseIdList` (an unknown id returns
  `CaseIdNotFound`), `displayId`, `afterTime` / `beforeTime`,
  `includeResolvedCases` (resolved cases are hidden by default), and
  `includeCommunications` (the recent-communications thread is embedded by
  default), and paginates with a round-tripping `nextToken`.
- **`AddCommunicationToCase`** appends a communication to the thread and returns
  `result: true`.
- **`DescribeCommunications`** returns the case's communication thread, filtered
  by time window and paginated with a `nextToken`.
- **`ResolveCase`** flips the case to `resolved` and returns both the
  `initialCaseStatus` and the `finalCaseStatus`.

### Attachment sets

- **`AddAttachmentsToSet`** mints a new `attachmentSetId` (or extends an existing
  one) and returns it with an `expiryTime` one hour out. Each attachment is
  stored under a generated `attachmentId`.
- **`DescribeAttachment`** returns a stored attachment (`fileName` + base64
  `data`) by id.

### Severity levels + case-creation reference data

- **`DescribeSeverityLevels`** returns the five real severity levels (`low`,
  `normal`, `high`, `urgent`, `critical`).
- **`DescribeServices`** returns the support service/category catalogue,
  optionally filtered by `serviceCodeList`.
- **`DescribeCreateCaseOptions`** and **`DescribeSupportedLanguages`** return
  well-formed case-creation option and supported-language data.

### Trusted Advisor

- **`DescribeTrustedAdvisorChecks`** returns the vendored catalogue of the
  well-known Trusted Advisor checks (id / name / description / category /
  metadata column headers) across all five categories -- cost optimising,
  security, fault tolerance, performance, and service limits. This is faithful
  static AWS reference data, not fabricated findings.
- **`DescribeTrustedAdvisorCheckResult`** and
  **`DescribeTrustedAdvisorCheckSummaries`** return well-formed result / summary
  shapes (status, timestamp, `resourcesSummary`, `categorySpecificSummary`,
  `flaggedResources`) reporting an all-clear account (zero flagged resources).
- **`RefreshTrustedAdvisorCheck`** enqueues a refresh and
  **`DescribeTrustedAdvisorCheckRefreshStatuses`** advances the per-check state
  machine one step on each read: `none` -> `enqueued` -> `processing` ->
  `success`, with `millisUntilNextRefreshable`.

## Honest gap: no Trusted Advisor engine, no live support agent

AWS Support's value is a human/automated support agent working your case and the
Trusted Advisor analysis engine inspecting your account. fakecloud runs
**neither**. Cases are real records with a real communication thread, but no
automated agent reply is generated -- a case stays exactly as you and your own
`AddCommunicationToCase` calls leave it. Trusted Advisor returns the real check
**catalogue** and a real refresh **state machine**, but
`DescribeTrustedAdvisorCheckResult` / `DescribeTrustedAdvisorCheckSummaries`
report a structurally correct **all-clear** result (zero flagged resources)
rather than inspecting your account and inventing findings. Everything around
the analysis -- cases, communications, attachment sets, severity levels, the
service catalogue, the check catalogue, the refresh lifecycle, validation, and
persistence -- is faithful. If your workload needs the Support contract (open a
case, thread communications, resolve it; enumerate checks and drive a refresh),
fakecloud is a faithful stand-in; if it needs an actual agent reply or real
account findings, that is out of scope.

## Validation

Every request is validated against the Support Smithy model's `required` /
`length` / `range` constraints before any handler runs. (`@pattern` is not
enforced: the only patterned member, `serviceCode`, is taken by operations that
declare no generic validation exception, so surfacing a pattern rejection would
fall outside their Smithy error contract.) The declared exceptions
`CaseIdNotFound`, `AttachmentIdNotFound`, `AttachmentSetIdNotFound`,
`AttachmentSetExpired`, `AttachmentSetSizeLimitExceeded`,
`AttachmentLimitExceeded`, `DescribeAttachmentLimitExceeded`,
`CaseCreationLimitExceeded`, and `InternalServerError` model the service's error
surface.

## Persistence

Support state is account-partitioned and, in persistent mode, snapshotted to
disk and restored on restart. Cases, communication threads, attachment sets,
individual attachments, and Trusted Advisor refresh statuses all survive a
restart.
