# Durable execution recordings

Responses recorded from AWS Lambda (us-east-1, 2026-11-10) that the
`crate::durable` implementation follows. All resources were created for the
recording and deleted afterwards; the three KMS keys (created for rounds 3,
3c and 3d) are scheduled for deletion with the minimum 7-day window, which is
how KMS deletes keys.

- `aws-round1.jsonl`: Invoke (unqualified, sync, async, named and duplicate
  names), `GetDurableExecution`, `GetDurableExecutionHistory` (with and without
  data, paginated), `GetDurableExecutionState` / `CheckpointDurableExecution`
  with a bad token, `StopDurableExecution` (running, finished, unknown),
  callback success / failure / heartbeat (open, completed, unknown),
  `ListDurableExecutionsByFunction`.
- `aws-round2.jsonl`: step retries, chained invokes (plain, failing,
  unqualified, durable child), callback timeout and heartbeat timeout,
  `parallel`, a handler answering `PENDING` with nothing pending, an invalid
  `Status`, a crashing invocation, execution timeout (async and sync), name
  reuse across functions, list filters.
- `aws-round3.jsonl`: `DurableConfig` validation on `CreateFunction` /
  `UpdateFunctionConfiguration` (missing `ExecutionTimeout`, bounds, KMS key
  pattern, a missing key, a key whose policy does not admit Lambda, adding a
  config to a plain function, partial updates), stopping a parent while its
  durable child waits (the child keeps running), stopping the child (the
  parent records `ChainedInvokeStopped` and fails), and a synchronous invoke
  outliving the function timeout.
- `aws-round3-sync-over-900s.jsonl`: a synchronous invoke of an execution
  with `ExecutionTimeout` 1200 that never finishes: the request stayed open
  for the full 1200 s (past the documented 15 minutes) and answered the
  timeout error.
- `aws-round3-kms-disabled.jsonl`, `aws-round3-kms-pending-deletion.jsonl`:
  a customer managed key whose policy admits Lambda, then disabled, then
  scheduled for deletion, probed every 1.5 to 2 minutes for 17 minutes.

## Customer managed keys (`DurableConfig.KMSKeyArn`)

The key's behavior is observable, so fakecloud implements it against its own
KMS state (key existence, key state, and key policy evaluated for the
`lambda.amazonaws.com` service principal):

- `CreateFunction` / `UpdateFunctionConfiguration` check the key when it is
  set or changed (an update that keeps a now-disabled key is accepted): a
  missing key, a disabled key and a key pending deletion are
  `InvalidParameterValueException`; a key policy that does not let the
  Lambda service principal use `kms:GenerateDataKey` and `kms:Decrypt` is
  `AccessDeniedException` (HTTP 403). The default key policy (account root
  only) is refused.
- Once the key is disabled or pending deletion, the operations that read or
  write an execution's data answer `KMSDisabledException` /
  `KMSInvalidStateException` (HTTP 502): `GetDurableExecution` and
  `GetDurableExecutionHistory` with data, `StopDurableExecution`,
  `SendDurableExecutionCallbackSuccess`. Metadata reads
  (`IncludeExecutionData=false`, `ListDurableExecutionsByFunction`) keep
  working.
- Invocations kept working: new executions and replays succeeded for the
  whole 17 minutes after the key was disabled and after it was scheduled for
  deletion, and suspended executions stayed `RUNNING`. The documentation says
  executions fail at their next checkpoint once Lambda's data-key cache
  (up to 15 minutes) expires; that was not observed, so fakecloud does not
  fail executions.
- Not emulated: the data-key cache itself. On AWS, reads with data kept
  working for about 4.5 minutes after the key was disabled (none after
  scheduling deletion); fakecloud applies the key state at once. The KMS
  messages for a deleted key and for a key policy that later stops admitting
  the caller were not recorded; fakecloud follows the recorded template.

- `aws-round*-function-logs.txt`: what the function saw, parsed from its
  CloudWatch logs: the invocation payload (`EVENT`), its answer (`OUTPUT`),
  and the raw `CheckpointDurableExecution` / `GetDurableExecutionState`
  exchanges (`WIRE`, tokens elided). Round 2 also drives the checkpoint API
  directly with the invocation's token to record validation errors (`PROBE`).
- `recorder/`: the scripts and handlers that produced them
  (`@aws/durable-execution-sdk-js` 3.0.0, `nodejs22.x`).

Each JSONL line is one HTTP exchange: `label`, `method`, `path`, `query`,
`reqHeaders`, `reqBody`, `status`, `headers`, `body`; `note: "error"` lines
are the SDK's parsed error for the exchange before them.
