+++
title = "Amazon Transcribe"
description = "Amazon Transcribe on fakecloud: the full 43-operation control plane -- transcription, medical-transcription, call-analytics and medical-scribe jobs, custom and medical vocabularies, vocabulary filters, custom language models, call-analytics categories, and tagging -- with account-partitioned persistence. Speech recognition itself is a documented gap."
weight = 73
+++

fakecloud implements **Amazon Transcribe** (`transcribe`), the automatic
speech-recognition (ASR) service. All **43 operations** from the AWS Smithy
model ship now, backed by account-partitioned state that persists across
restarts in persistent mode. The wire protocol is awsJson1.1 (x-amz-target
`Transcribe.<Op>`), signing as `transcribe`.

This is the **control plane**: every job, vocabulary, filter, model, and
category is real, validated, persisted CRUD -- no stubbed success responses.
Requests are validated against the model's `required` / `length` / `range` /
`enum` / `pattern` constraints before any handler runs, and an operation that
dereferences a resource that does not exist returns Transcribe's
`NotFoundException`; a duplicate name returns `ConflictException`.

## Honest gap: speech recognition is not run

Transcribe's value is the ASR model that turns audio into a transcript.
fakecloud does **not** run speech recognition. A job you start settles to
`COMPLETED` and its `Transcript.TranscriptFileUri` points at the output location
you requested (your `OutputBucketName`/`OutputKey`, or a service-managed
location when you don't supply one), but **no transcript JSON is produced at
that URI** -- there is no audio decode and no recognition step. Everything
around recognition is faithful: job records, `Media` / `Settings` /
`ContentRedaction` / `ChannelDefinitions` echoing, status lifecycle, output-URI
synthesis, vocabularies, filters, models, categories, tags, validation, and
persistence. If your workload only needs the Transcribe control-plane contract
(create a job, poll it to `COMPLETED`, list/delete, manage vocabularies and
tags), fakecloud is a faithful stand-in; if it needs the actual recognized
text, that is out of scope.

## Supported features

- **Transcription jobs** (`StartTranscriptionJob`, `GetTranscriptionJob`,
  `ListTranscriptionJobs`, `DeleteTranscriptionJob`). A started job is minted at
  `QUEUED` with its `Media`, `LanguageCode`, `MediaFormat`,
  `MediaSampleRateHertz`, `Settings`, `ModelSettings`, `JobExecutionSettings`,
  `ContentRedaction`, language-identification flags, and `Subtitles` persisted
  verbatim. On the next `Get`/`List` the job settles to `COMPLETED`, stamping
  `CompletionTime` and a well-formed `Transcript` (plus `SubtitlesOutput` and a
  `RedactedTranscriptFileUri` when those were requested). `ListTranscriptionJobs`
  honors the `Status` and `JobNameContains` filters and reports each summary's
  `OutputLocationType`.
- **Medical transcription jobs** (`StartMedicalTranscriptionJob`,
  `GetMedicalTranscriptionJob`, `ListMedicalTranscriptionJobs`,
  `DeleteMedicalTranscriptionJob`) -- same lifecycle, carrying `Specialty`,
  `Type`, and `ContentIdentificationType`.
- **Call-analytics jobs** (`StartCallAnalyticsJob`, `GetCallAnalyticsJob`,
  `ListCallAnalyticsJobs`, `DeleteCallAnalyticsJob`) with `CallAnalyticsJobSettings`
  and `ChannelDefinitions`.
- **Call-analytics categories** (`CreateCallAnalyticsCategory`,
  `GetCallAnalyticsCategory`, `UpdateCallAnalyticsCategory`,
  `ListCallAnalyticsCategories`, `DeleteCallAnalyticsCategory`) with `Rules` and
  `InputType`.
- **Medical-scribe jobs** (`StartMedicalScribeJob`, `GetMedicalScribeJob`,
  `ListMedicalScribeJobs`, `DeleteMedicalScribeJob`). The settled job exposes a
  `MedicalScribeOutput` with both a `TranscriptFileUri` and a
  `ClinicalDocumentUri`, and reports whether a `MedicalScribeContext` was
  provided.
- **Custom vocabularies** (`CreateVocabulary`, `GetVocabulary`,
  `UpdateVocabulary`, `ListVocabularies`, `DeleteVocabulary`) and **medical
  vocabularies** (`CreateMedicalVocabulary`, `GetMedicalVocabulary`,
  `UpdateMedicalVocabulary`, `ListMedicalVocabularies`,
  `DeleteMedicalVocabulary`). A new vocabulary is `PENDING` and settles to
  `READY` on the next read; `Get` returns a `DownloadUri`.
- **Vocabulary filters** (`CreateVocabularyFilter`, `GetVocabularyFilter`,
  `UpdateVocabularyFilter`, `ListVocabularyFilters`, `DeleteVocabularyFilter`).
- **Custom language models** (`CreateLanguageModel`, `DescribeLanguageModel`,
  `ListLanguageModels`, `DeleteLanguageModel`). A new model is `IN_PROGRESS` and
  settles to `COMPLETED` on the next read, carrying its `InputDataConfig`,
  `BaseModelName`, and `LanguageCode`.
- **Tagging** (`TagResource`, `UntagResource`, `ListTagsForResource`), keyed by
  the resource ARN (`arn:aws:transcribe:<region>:<account>:<type>/<name>`).
  Tagging a resource that does not exist returns `NotFoundException`.

## Resource ARNs

Transcribe resources are addressed by ARNs of the form
`arn:aws:transcribe:<region>:<account>:<type>/<name>`, where `<type>` is one of
`transcription-job`, `medical-transcription-job`, `call-analytics-job`,
`medical-scribe-job`, `vocabulary`, `medical-vocabulary`, `vocabulary-filter`,
`language-model`, or `call-analytics-category`.

## Persistence

All state is account-partitioned and, in persistent mode, snapshotted to disk
and restored on startup. Any lifecycle transition that was in flight when the
process stopped is reconciled on load, so an interrupted job/vocabulary/model
never wedges.
