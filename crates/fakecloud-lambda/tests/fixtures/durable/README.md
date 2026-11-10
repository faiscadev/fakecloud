# Durable execution recordings

Responses recorded from AWS Lambda (us-east-1, 2026-11-10) that the
`crate::durable` implementation follows. All resources were created for the
recording and deleted afterwards.

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
