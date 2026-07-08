+++
title = "Amazon Translate"
description = "Amazon Translate on fakecloud: the full 19-operation control plane -- text and document translation, asynchronous batch translation jobs, parallel data, custom terminologies, the supported-language catalogue, and tagging -- with account-partitioned persistence. Neural machine translation itself is a documented gap."
weight = 73
+++

fakecloud implements **Amazon Translate** (`translate`), the neural machine
translation service. All **19 operations** from the AWS Smithy model ship now,
backed by account-partitioned state that persists across restarts in persistent
mode. The wire protocol is awsJson1.1 (x-amz-target
`AWSShineFrontendService_20170701.<Op>`), signing as `translate`.

This is the **control plane**: every terminology, parallel-data record, batch
job, and tag is real, validated, persisted CRUD -- no stubbed success
responses. Requests are validated against the model's `required` / `length` /
`range` / `enum` / `pattern` constraints before any handler runs, and an
operation that dereferences a resource that does not exist returns Translate's
`ResourceNotFoundException`; a duplicate parallel-data name returns
`ConflictException`. Because each operation declares its own error set, a
validation failure is surfaced as the exact exception that operation admits
(`InvalidParameterValueException` on the control-plane ops,
`InvalidRequestException` on `TranslateText` / `TranslateDocument`).

## Honest gap: machine translation is not run

Translate's value is the neural MT model that rewrites text from one language
into another. fakecloud does **not** run machine translation. `TranslateText`
and `TranslateDocument` return a structurally correct response that **echoes the
input content verbatim** as the `TranslatedText` / `TranslatedDocument`, with
the requested `TargetLanguageCode` applied and any requested (existing)
terminologies listed in `AppliedTerminologies`. No fabricated translation is
presented as if a real model produced it -- the passthrough is deliberate and
documented. Everything around translation is faithful: terminology files are
parsed (source/target languages and term counts are read out of the uploaded
CSV/TSV), batch-job records settle through their status lifecycle, parallel data
settles to `ACTIVE`, tags round-trip, validation runs, and all state persists.
If your workload only needs the Translate control-plane contract (import a
terminology, submit a batch job and poll it to `COMPLETED`, manage parallel data
and tags, list supported languages) or a deterministic text passthrough,
fakecloud is a faithful stand-in; if it needs the actual translated text, that
is out of scope.

## Supported features

- **Synchronous translation** (`TranslateText`, `TranslateDocument`). The input
  `Text` / `Document.Content` is echoed back as the translated output with the
  requested `SourceLanguageCode` and `TargetLanguageCode` reflected. A requested
  terminology that does not exist returns `ResourceNotFoundException`; existing
  ones are reported in `AppliedTerminologies`. Any `Settings` supplied are
  echoed as `AppliedSettings`.
- **Batch translation jobs** (`StartTextTranslationJob`,
  `DescribeTextTranslationJob`, `ListTextTranslationJobs`,
  `StopTextTranslationJob`). A started job is minted with a 32-character
  `JobId` at `SUBMITTED`, persisting its `InputDataConfig`, `OutputDataConfig`,
  `DataAccessRoleArn`, `SourceLanguageCode`, `TargetLanguageCodes`,
  `TerminologyNames`, `ParallelDataNames`, and `Settings`. On the next
  `Describe`/`List` the job settles to `COMPLETED`, stamping `EndTime` and a
  `JobDetails` count. `StopTextTranslationJob` moves an in-flight job to
  `STOP_REQUESTED`, which settles to `STOPPED`. `ListTextTranslationJobs`
  honors the `JobName` / `JobStatus` filter. A referenced terminology or
  parallel-data name that does not exist returns `ResourceNotFoundException`.
- **Parallel data** (`CreateParallelData`, `GetParallelData`,
  `UpdateParallelData`, `ListParallelData`, `DeleteParallelData`). A new record
  is `CREATING` and settles to `ACTIVE` on the next read, carrying its
  `ParallelDataConfig`, `EncryptionKey`, and an
  `arn:aws:translate:<region>:<account>:parallel-data/<name>` ARN. An update
  records `LatestUpdateAttemptStatus` `UPDATING`, which settles back to
  `ACTIVE`. `Get` returns the record plus a `DataLocation`.
- **Custom terminologies** (`ImportTerminology`, `GetTerminology`,
  `ListTerminologies`, `DeleteTerminology`). Import parses the uploaded CSV/TSV
  `TerminologyData.File` for its byte size, term count, and source/target
  language columns, storing a `TerminologyProperties` with the derived
  `Directionality`, `Format`, and an
  `arn:aws:translate:<region>:<account>:terminology/<name>` ARN. `MergeStrategy`
  is `OVERWRITE`, so re-importing replaces the terminology while preserving its
  original `CreatedAt`. `Get` returns the properties plus a
  `TerminologyDataLocation`.
- **Supported languages** (`ListLanguages`) returns the published Amazon
  Translate language catalogue (`LanguageCode` + `LanguageName`) with the
  requested `DisplayLanguageCode` echoed. Display names are returned in English;
  localizing them to every display language is out of scope.
- **Tagging** (`TagResource`, `UntagResource`, `ListTagsForResource`), keyed by
  the resource ARN (`arn:aws:translate:<region>:<account>:<type>/<name>`, where
  `<type>` is `terminology` or `parallel-data`). Tagging a resource that does
  not exist returns `ResourceNotFoundException`.

## Resource ARNs

Translate resources are addressed by ARNs of the form
`arn:aws:translate:<region>:<account>:<type>/<name>`, where `<type>` is
`terminology` or `parallel-data`. Batch translation jobs are addressed by their
minted `JobId`.

## Persistence

All state is account-partitioned and, in persistent mode, snapshotted to disk
and restored on startup. Any lifecycle transition that was in flight when the
process stopped is reconciled on load, so an interrupted job or parallel-data
record never wedges.
