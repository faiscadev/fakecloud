+++
title = "Amazon Textract"
description = "Amazon Textract (textract) on fakecloud: a complete 25-operation surface (100% conformance). awsJson1_1."
weight = 73
+++

fakecloud implements **Amazon Textract** as an awsJson1_1 service (target
prefix `Textract.<Operation>`, SigV4 signing name `textract`). All **25
operations** ship with **100% conformance** against AWS's own Smithy model,
backed by account-partitioned state that persists across restarts in persistent
mode.

Textract on fakecloud is a faithful **API surface**: the request/response
shapes, model-driven validation, asynchronous job lifecycle, custom-adapter
CRUD, and tagging are all real and persisted. The one honest gap is the OCR / ML
inference itself.

**Honest gap: no OCR/ML model.** fakecloud does not run a real text-extraction
or document-understanding model, so the analysis operations return a
*structurally correct* response derived from the input (a `DocumentMetadata`
with the page count taken from the request, and well-formed but **empty**
`Blocks` / `ExpenseDocuments` / `IdentityDocuments` / lending `Results`
collections) rather than fabricated extracted text. This is the same posture as
other ML services on fakecloud: real API + validation + persistence, with the
inference documented as a gap instead of stubbed with invented data.

## Operations

### Synchronous analysis

- **`DetectDocumentText`** - validates the required `Document` (which must carry
  either inline `Bytes` or an `S3Object`) and returns `DocumentMetadata`, an
  empty `Blocks` list, and `DetectDocumentTextModelVersion`.
- **`AnalyzeDocument`** - validates `Document` + the required `FeatureTypes`
  (`TABLES` / `FORMS` / `QUERIES` / `SIGNATURES` / `LAYOUT`) and returns the
  `Blocks` / `AnalyzeDocumentModelVersion` shape.
- **`AnalyzeExpense`** - validates `Document` and returns `ExpenseDocuments`.
- **`AnalyzeID`** - validates the required `DocumentPages` (1-2 documents) and
  returns `IdentityDocuments` with the page count reflected in
  `DocumentMetadata`.

An empty or malformed `Document` is rejected with `InvalidParameterException`,
matching the live service.

### Asynchronous document processing

- **`StartDocumentTextDetection`**, **`StartDocumentAnalysis`**,
  **`StartExpenseAnalysis`**, **`StartLendingAnalysis`** - each validates the
  required `DocumentLocation` (`S3Object`), accepts and persists the optional
  `ClientRequestToken` (idempotent - a repeated token returns the same job),
  `JobTag`, `NotificationChannel`, `OutputConfig`, and `KMSKeyId`, and mints a
  real 64-character `JobId`.
- **`GetDocumentTextDetection`**, **`GetDocumentAnalysis`**,
  **`GetExpenseAnalysis`**, **`GetLendingAnalysis`**,
  **`GetLendingAnalysisSummary`** - look up the job, settle its status from
  `IN_PROGRESS` to `SUCCEEDED` on the first read, and return the per-kind
  result shape (`Blocks` / `ExpenseDocuments` / lending `Results` / `Summary`)
  plus `DocumentMetadata`, `JobStatus`, `Warnings`, and the model-version field.
  An unknown or wrong-kind job id returns `InvalidJobIdException`.

### Custom adapters

- **`CreateAdapter`** - validates the required `AdapterName` +`FeatureTypes`,
  accepts `Description` / `AutoUpdate` / `Tags` / `ClientRequestToken`
  (idempotent), and mints an `AdapterId`.
- **`GetAdapter`** / **`UpdateAdapter`** / **`DeleteAdapter`** /
  **`ListAdapters`** - full CRUD; a missing adapter returns
  `ResourceNotFoundException`. `ListAdapters` paginates with
  `MaxResults` / `NextToken`.
- **`CreateAdapterVersion`** - requires an existing `AdapterId` plus the
  `DatasetConfig` + `OutputConfig`, mints a monotonic `AdapterVersion`, and
  starts it `CREATION_IN_PROGRESS`.
- **`GetAdapterVersion`** / **`DeleteAdapterVersion`** /
  **`ListAdapterVersions`** - full read/delete/list; `GetAdapterVersion`
  settles a version `CREATION_IN_PROGRESS` -> `ACTIVE` on the first read and
  reports `EvaluationMetrics`.

### Tagging

- **`TagResource`** / **`UntagResource`** / **`ListTagsForResource`** - keyed by
  the adapter (or adapter-version) ARN
  (`arn:aws:textract:<region>:<account>:adapter/<id>`); an unknown ARN returns
  `ResourceNotFoundException`.

## Validation

Every request runs model-driven validation (`required` / `length` / `range` /
enum) against the embedded Smithy model before any handler logic, surfaced as
`InvalidParameterException` - the client-error shape every Textract operation
declares. Resource-dereference failures return `ResourceNotFoundException`
(adapters / versions) or `InvalidJobIdException` (jobs).

## Persistence

All Textract state - adapters, adapter versions, asynchronous jobs, tags, and
idempotency tokens - is account-partitioned and, in persistent mode, snapshotted
to disk so it survives a restart.
