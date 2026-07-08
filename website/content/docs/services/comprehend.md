+++
title = "Amazon Comprehend"
description = "Amazon Comprehend on fakecloud: the full 85-operation surface -- synchronous and batch text detection, nine async analysis-job families, custom document classifiers and entity recognizers, real-time endpoints, flywheels, datasets, resource policies, model import, and tagging -- with account-partitioned persistence. The NLP inference itself is a documented gap."
weight = 76
+++

fakecloud implements **Amazon Comprehend** (`comprehend`), the natural-language
processing (NLP) service. All **85 operations** from the AWS Smithy model ship
now, backed by account-partitioned state that persists across restarts in
persistent mode. The wire protocol is awsJson1.1 (x-amz-target
`Comprehend_20171127.<Op>`), signing as `comprehend`.

This is the **control plane plus a structurally faithful inference plane**:
every job, classifier, recognizer, endpoint, flywheel, dataset, policy, and tag
is real, validated, persisted CRUD -- no stubbed success responses. Requests are
validated against the model's `required` / `length` / `range` / `enum` /
`pattern` constraints before any handler runs, and an operation that
dereferences a resource that does not exist returns Comprehend's
`ResourceNotFoundException` (or `JobNotFoundException` for analysis jobs); a
duplicate name returns `ResourceInUseException`.

## Honest gap: no NLP inference is run

Comprehend's value is the natural-language model that turns text into entities,
sentiment, syntax, key phrases, PII spans, and language probabilities. fakecloud
does **not** run any NLP inference. The synchronous detection operations return
well-formed, structurally correct result shapes with **empty analysis lists** --
no entities, key phrases, syntax tokens, PII spans, or detected languages are
fabricated. `DetectSentiment` returns the model's **neutral default**
(`NEUTRAL`, with a `SentimentScore` weighted to neutral) rather than an invented
judgement. Everything around inference is faithful: job records,
`InputDataConfig` / `OutputDataConfig` echoing, status lifecycle, ARN synthesis,
classifiers, recognizers, endpoints, flywheels, datasets, policies, tags,
validation, and persistence. If your workload only needs the Comprehend contract
(start a job, poll it to `COMPLETED`, list/stop, manage models/endpoints/tags),
fakecloud is a faithful stand-in; if it needs the actual analyzed text, that is
out of scope.

## Supported features

- **Synchronous detection** (`DetectDominantLanguage`, `DetectEntities`,
  `DetectKeyPhrases`, `DetectSyntax`, `DetectSentiment`,
  `DetectTargetedSentiment`, `DetectPiiEntities`, `ContainsPiiEntities`,
  `DetectToxicContent`, `ClassifyDocument`). Each validates its `Text` /
  `LanguageCode` (and other model constraints) and returns the well-formed
  result shape -- empty analysis lists, or the neutral default for sentiment.
- **Batch detection** (`BatchDetectDominantLanguage`, `BatchDetectEntities`,
  `BatchDetectKeyPhrases`, `BatchDetectSyntax`, `BatchDetectSentiment`,
  `BatchDetectTargetedSentiment`). Each mirrors its single-document counterpart
  over a `TextList`, emitting one indexed `ResultList` entry per input and an
  empty `ErrorList`.
- **Asynchronous analysis jobs** -- nine families: dominant-language, entities,
  key-phrases, sentiment, targeted-sentiment, PII, events, topics, and
  document-classification. Each `Start*` mints a `JobId` and job ARN, persists
  `InputDataConfig` / `OutputDataConfig` / `DataAccessRoleArn` (and family
  members such as `EntityRecognizerArn`, `RedactionConfig`, `TargetEventTypes`,
  `NumberOfTopics`, `DocumentClassifierArn`), and starts at `SUBMITTED`. On the
  next `Describe*` / `List*` the job settles to `COMPLETED`, stamping `EndTime`.
  The seven stoppable families expose `Stop*` (`SUBMITTED`/`IN_PROGRESS` ->
  `STOP_REQUESTED` -> `STOPPED`). `List*` honors the `JobName` / `JobStatus`
  filters.
- **Custom document classifiers** (`CreateDocumentClassifier`,
  `DescribeDocumentClassifier`, `DeleteDocumentClassifier`,
  `StopTrainingDocumentClassifier`, `ListDocumentClassifiers`,
  `ListDocumentClassifierSummaries`) and **entity recognizers**
  (`CreateEntityRecognizer`, `DescribeEntityRecognizer`,
  `DeleteEntityRecognizer`, `StopTrainingEntityRecognizer`,
  `ListEntityRecognizers`, `ListEntityRecognizerSummaries`). A new model is
  `SUBMITTED` and settles to `TRAINED` on the next read; versioned ARNs
  (`.../name/version/<v>`) are supported, and the `*Summaries` ops collapse all
  versions of a name into one summary.
- **Real-time endpoints** (`CreateEndpoint`, `DescribeEndpoint`,
  `UpdateEndpoint`, `DeleteEndpoint`, `ListEndpoints`). A new endpoint is
  `CREATING` and settles to `IN_SERVICE`, publishing its
  `DesiredInferenceUnits` as `CurrentInferenceUnits`.
- **Flywheels + iterations** (`CreateFlywheel`, `DescribeFlywheel`,
  `UpdateFlywheel`, `DeleteFlywheel`, `ListFlywheels`, `StartFlywheelIteration`,
  `DescribeFlywheelIteration`, `ListFlywheelIterationHistory`). A flywheel is
  `CREATING` and settles to `ACTIVE`; iterations start `TRAINING` and settle to
  `COMPLETED`.
- **Datasets** (`CreateDataset`, `DescribeDataset`, `ListDatasets`), addressed
  under their flywheel, settling `CREATING` -> `COMPLETED`.
- **Model import + resource policies** (`ImportModel`, `PutResourcePolicy`,
  `DescribeResourcePolicy`, `DeleteResourcePolicy`).
- **Tagging** (`TagResource`, `UntagResource`, `ListTagsForResource`), keyed by
  the resource ARN. Tagging a resource that does not exist returns
  `ResourceNotFoundException`.

## Resource ARNs

Comprehend resources are addressed by ARNs of the form
`arn:aws:comprehend:<region>:<account>:<type>/<tail>`, where `<type>` is one of
`document-classifier`, `entity-recognizer`, `document-classifier-endpoint`,
`entity-recognizer-endpoint`, `flywheel`, or one of the nine
`<family>-detection-job` / `document-classification-job` job types. Model
versions append `/version/<v>`, and datasets append `/dataset/<name>` to their
flywheel ARN.

## Persistence

All state is account-partitioned and, in persistent mode, snapshotted to disk
and restored on startup. Any lifecycle transition that was in flight when the
process stopped is reconciled on load, so an interrupted
job / classifier / endpoint / flywheel / dataset never wedges.
