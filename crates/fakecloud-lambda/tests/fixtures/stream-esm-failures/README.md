# Stream event source mapping failure recordings

`aws-recordings.json` holds what real AWS (us-east-1, 2026-11-10) did when
DynamoDB Streams and Kinesis event source mappings fed functions that fail.
Recorded with the AWS CLI: each handler logged the records of every batch it
was handed, and the mapping's OnFailure SQS destination was drained after.
Account IDs are replaced by `123456789012`. Every resource was deleted after
recording.

What the recordings show:

- A failing batch is invoked `MaximumRetryAttempts + 1` times, then its
  records go to the OnFailure destination (condition
  `RetryAttemptsExhausted`) and the shard moves past them.
- `BisectBatchOnFunctionError` splits a failing batch in half and retries the
  halves, earlier half first; splits do not count against the retries, which
  only start once a batch is down to one record.
- With `ReportBatchItemFailures`, the shard checkpoints up to the first
  reported failure and the remainder becomes a new batch with a fresh retry
  count; when that batch runs out of retries, the whole remainder (including
  records the function handled) goes to the destination, with
  `functionError: null`.
- With `MaximumRetryAttempts` -1 and `MaximumRecordAgeInSeconds` 60, retries
  back off exponentially (about 0.2s, doubling) until the oldest record is
  older than the limit; the destination record has condition
  `RecordAgeExceeded`, no `responseContext`, and an `approximateInvokeCount`
  one above the invocations made.
- Destination records carry `KinesisBatchInfo` / `DDBStreamBatchInfo` with the
  failed batch's first and last sequence numbers, size and arrival times
  (milliseconds for Kinesis, whole seconds for DynamoDB).
