+++
title = "Firehose"
description = "Amazon Data Firehose delivery streams and buffered S3 delivery with uncompressed, GZIP, or Xerial Snappy output, JSON 1.1 protocol."
weight = 32
+++

fakecloud implements Amazon Data Firehose's JSON 1.1 control plane and buffered delivery to [S3](/docs/services/s3/) with uncompressed, GZIP, or Xerial Snappy output. Valid records from separate `PutRecord` and `PutRecordBatch` calls share a stream buffer. A background worker delivers objects when the decoded-byte size threshold or the oldest record's interval is reached, including when ingestion has gone idle.

**Status: control-plane parity and buffered S3 delivery. Unsupported destination and pipeline configurations are stored, but record admission rejects them rather than acknowledging delivery that cannot run.**

## Supported today

- **Delivery streams**: `CreateDeliveryStream` / `DescribeDeliveryStream` / `ListDeliveryStreams` / `DeleteDeliveryStream` / `UpdateDestination`. Both `DirectPut` and `KinesisStreamAsSource` source types round-trip; Kinesis-source polling is not implemented. Created streams are immediately `ACTIVE` and deletion removes them immediately.
- **Destinations**: `ExtendedS3DestinationConfiguration`, `RedshiftDestinationConfiguration`, `ElasticsearchDestinationConfiguration`, `AmazonopensearchserviceDestinationConfiguration`, `SplunkDestinationConfiguration`, `HttpEndpointDestinationConfiguration`, `SnowflakeDestinationConfiguration`, `IcebergDestinationConfiguration`. Configuration round-trips verbatim through `UpdateDestination`.
- **Buffering hints**: omitted hints default to **5 MiB / 300 seconds**. If `BufferingHints` on `CreateDeliveryStream` or `UpdateDestination` supplies either member, it must supply both. An empty `BufferingHints` object is accepted: it uses the defaults on create and keeps the current hints on update.
  - `SizeInMBs`: 1-128, each unit is 1,048,576 decoded bytes.
  - `IntervalInSeconds`: 0-900, including short intervals such as 2 seconds.

  Incomplete pairs, non-integers, and out-of-range values return `InvalidArgumentException`. CloudFormation also accepts quoted integers such as `"60"`, because `Ref` parameters resolve to strings; it coerces them, and every other quoted modeled boolean or number in the destination block (including inside `S3BackupConfiguration`), before validating. A value that does not match its modeled type makes the resource fail, naming the member's path: a quoted string that does not parse exactly (such as `IntervalInSeconds: "soon"`, or `" 60 "` with surrounding whitespace), an integer outside the model's 32-bit range, or a JSON value of the wrong type (such as a number for `Enabled`). Interval zero makes nonempty input eligible at admission; writing is still asynchronous, so poll S3 for completion rather than assuming an acknowledgment means the object exists.
- **Destination strings**: the AWS model's length limits apply: `Prefix` and `ErrorOutputPrefix` allow at most 1,024 characters, `CustomTimeZone` at most 50, `RoleARN` 1-512, and `BucketARN` 1-2,048. ARN and time-zone patterns are not enforced here. Other values return `InvalidArgumentException` from `CreateDeliveryStream` and `UpdateDestination`, and make a CloudFormation resource fail.
- **Records**: `PutRecord` / `PutRecordBatch` assign per-record `RecordId`s after supported S3 admission. Binary bytes and producer-supplied delimiters are preserved exactly before compression; no separators are added unless the `AppendDelimiterToRecord` processor is enabled. Without it, a delivery window holding only empty records produces no object. Public API and internal Logs/SES delivery use the same buffering engine.
  Each API record is limited to 1,024,000 decoded bytes; a batch contains 1-500 records and at most 4,194,304 decoded bytes. Request-wide limit violations reject the whole call before admission. Missing or null `Data` in either API rejects the whole call with HTTP 400 `InvalidArgumentException`; non-string `Data` or malformed base64 rejects it with HTTP 400 `SerializationException`. No entries are admitted and no quota capacity is consumed by these validation failures. Opt-in throughput limits can instead produce partial batch failures, as described below.
- **New line delimiter**: when `ProcessingConfiguration` has `Enabled: true` and every entry in its `Processors` is `AppendDelimiterToRecord`, a newline (`\n`) is written after every record, including the last one and empty records, before any compression; a delivery window holding only empty records becomes an object of newlines. This applies to API deliveries, to internal CloudWatch Logs and SES deliveries, and to streams created or updated through CloudFormation, which store `ProcessingConfiguration` like `CreateDeliveryStream`. The optional `Delimiter` parameter is stored but not interpreted: AWS documents only a new line delimiter. Appended newlines do not count toward the size threshold. Lambda and other processors are not run; enabled processing that includes them, or lists no processors, rejects record admission with HTTP 400 `InvalidArgumentException` naming the unsupported setting (see [Caveats](#caveats)).
- **S3 output**: static prefixes, UTC date hierarchy, stream/version/time/UUID object names, and `FileExtension` overrides. Objects have `Content-Type: application/octet-stream` and the encoded body's MD5 as their `ETag`. GZIP output has `Content-Encoding: gzip`; Snappy output has `Content-Encoding: snappy-java`; uncompressed output has no content encoding. Completed objects' bytes and metadata survive persistent-mode restart, including internal adapter delivery. Bucket versioning, default Object Lock retention, bucket-default encryption (including SSE-KMS), replication, and S3 event notifications apply through the shared S3 write path as for ordinary S3 writes, including for compressed objects. `EncryptionConfiguration.KMSEncryptionConfig` writes objects with SSE-KMS under the configured key (see [Caveats](#caveats)). Pending buffers and retry queues are process-local and are not persisted.
- **Tags**: `ListTagsForDeliveryStream` / `TagDeliveryStream` / `UntagDeliveryStream`. Keyed by stream ARN.
- **Server-side encryption**: `StartDeliveryStreamEncryption` / `StopDeliveryStreamEncryption`. Start sets the stream's `DeliveryStreamEncryptionConfiguration` to `Status=ENABLED` with the requested `KeyType` (defaults `AWS_OWNED_CMK`; stores the `KeyARN` for `CUSTOMER_MANAGED_CMK`); Stop flips it to `DISABLED`. The config is surfaced on `DescribeDeliveryStream`. Unknown streams return `ResourceNotFoundException`.

## Buffering and destination updates

Size accounting uses decoded record bytes, before compression or base64 transport encoding; newlines added by `AppendDelimiterToRecord` are not counted. Delivery becomes eligible at **greater than or equal to** the configured threshold. All accepted records in one API call enter the buffer together: a batch crossing the threshold stays whole, so the resulting object can overshoot the hint. Entries refused by an opt-in throughput limit never enter the buffer. Highly compressible input can trigger delivery even when the encoded object is much smaller than the threshold.

The interval starts with the oldest arrival in a new buffer. After that buffer is detached for delivery, subsequent records start a fresh size/time window. Buffers are isolated by account, region, and delivery-stream instance.

`UpdateDestination` affects data still in the active buffer: when that data becomes eligible, it uses the latest supported destination, hints, output settings, and stream version. Once a batch has detached, its destination, version, codec, destination encryption key, timestamps, and object key are captured and reused for every retry. An unsupported update pauses already accepted active data until the configuration becomes supported again; it does not discard that data. Detached batches continue with their captured configuration. Deleting a stream or resetting the server discards its pending data; recreating the same stream name does not recover the old buffer.

## Opt-in Direct PUT throughput limits

Ingestion quotas control whether public `PutRecord` and `PutRecordBatch` calls are accepted. They are separate from `BufferingHints`, which control when accepted data becomes eligible for S3 delivery. [`DirectPutSourceConfiguration.ThroughputHintInMBs`](https://docs.aws.amazon.com/firehose/latest/APIReference/API_DirectPutSourceConfiguration.html) is informational, not an enforcement control.

The existing [Service Quotas controls](/docs/services/servicequotas/#enforcement) support three `firehose` quotas:

| Quota code | Budget per stream per second | `us-east-1`, `us-west-2`, `eu-west-1` default | All other regions default |
| --- | --- | --- | --- |
| `L-7681C790` | Combined `PutRecord` and `PutRecordBatch` requests | 2,000 | 1,000 |
| `L-17D4EADE` | Records | 500,000 | 100,000 |
| `L-937C879B` | MiB of decoded record data | 5 | 1 |

One MiB is 1,048,576 bytes. Both public operations share all three budgets: a validated batch costs one request, plus its accepted records and their decoded bytes. A batch whose entries all fail throughput admission still consumes one request. Malformed base64 and request-wide validation failures consume no capacity. Producer-supplied delimiters count as input bytes; base64 transport encoding, output-added newlines, and compression do not affect the byte budget.

Applied values are configured per account and region, but consumption is **independent for each delivery-stream instance**, not pooled across the region. Streams in the same account and region each receive the full configured budget. Destination updates, buffer flushes, and delivery retries do not reset or consume ingestion capacity. Deleting and recreating a stream starts a new budget. Usage and burst debt are process-local and are not persisted; applied values and account enforcement overrides survive persistent-mode restarts. Server-wide enforcement settings are restored from startup flags on restart or reset, as with other Service Quotas controls.

### Enable and change limits

**Enforcement requires both an explicit applied value and an enabled enforcement switch.** AWS defaults are reported but never imposed, even with `--enforce-quotas`; introspection reports `enforced: false` and `enforcementSource: "auto_scaling_default"` until an explicit account/region value exists. This is a local convenience, not a simulation of AWS's automatic scaling latency.

Use `--enforce-quotas` globally, `--enforce-quota firehose/L-937C879B` for one quota, or the existing introspection API and its Service Quotas SDK wrappers. `--ignore-quota` and per-account overrides retain their existing precedence: account override, then server-wide override, then the global switch. The corresponding `FAKECLOUD_ENFORCE_QUOTAS`, `FAKECLOUD_ENFORCE_QUOTA`, and `FAKECLOUD_IGNORE_QUOTA` environment variables are also supported.

For a running local server, set a 0.5 MiB/second budget for every stream in account `000000000000`, region `us-east-1`, and enforce that quota for the account:

```sh
curl -fsS -X PUT \
  http://localhost:4566/_fakecloud/service-quotas/quotas/firehose/L-937C879B \
  -H 'Content-Type: application/json' \
  -d '{"accountId":"000000000000","region":"us-east-1","value":0.5,"enforce":true}'
```

Use the account and region of your producer's AWS client. Omitting `accountId` or `region` selects the server's configured account or region for the applied value, not the caller's AWS identity or signing region. There is an additional scope distinction: with `accountId`, `enforce` changes that account's override across regions; without `accountId`, it changes the server-wide override across accounts and regions. The example changes only the byte quota; requests and records remain unenforced unless separately enabled.

Setting `value` alone leaves enforcement unchanged: on a default server it **does not turn throttling on**. `enforce: false` ignores the quota, `enforce: null` clears the override, and omitting `enforce` preserves it. No new Firehose-specific administration endpoint is required.

The local setter accepts any finite, nonnegative value, including values below the AWS default, zero, and fractional MiB. Fractional request limits do not round up to a whole request. Positive record/byte limits allow a boundary-crossing record, with excess carried forward. A zero request limit refuses every metered call; zero records permits no records, and zero MiB permits only zero decoded bytes (so an empty record can pass the byte check). All enabled dimensions must allow each record.

Limits are read on each call. Raising a value applies to the next call without clearing usage already consumed in the current window; lowering it can block further admission until replenishment. Previously accepted buffers, delivery retries, and shutdown drains are unaffected. These Firehose quotas keep AWS's non-adjustable Service Quotas metadata: `RequestServiceQuotaIncrease` still refuses them. The local setter is a testing control, not a simulated AWS Support quota-increase workflow.

### Burst recovery and partial failures

fakecloud uses monotonic **one-second accounting windows**, anchored by the first metered request admitted under the request quota. Request capacity resets at each boundary. Record and byte usage instead subtract one allowance for each elapsed second, retaining any excess and never accumulating idle credits. Rejected requests do not move the boundary; rejected records consume neither records nor bytes.

Records are considered in input order. If both record and byte budgets have room, the whole record is accepted, even if it crosses a positive allowance. Later records wait for that burst to be repaid. For example, a five-byte record under a two-byte/second limit is accepted initially; one second later usage is three bytes and nonempty records are refused; after two seconds usage is one byte and another byte can be accepted. An empty record needs record capacity but no byte capacity, including while byte debt remains. Configured capacity does not automatically scale.

**Request-rate exhaustion** rejects the whole call with HTTP 500 `ServiceUnavailableException`, message `Slow down.`, before any records enter a buffer. **Record/byte exhaustion** gives `PutRecord` the same error, but `PutRecordBatch` returns HTTP 200 with ordered `RequestResponses`: accepted entries have `RecordId`, refused entries have `ErrorCode: "ServiceUnavailableException"` and `ErrorMessage: "Slow down."` without a record ID. `FailedPutCount` counts refused entries, even when every entry fails. Existing input validation and unsupported-destination errors precede quota consumption.

With the official AWS SDK for Rust, whole-request retries can delay the result or succeed after capacity replenishes. If retryable whole-call failures continue until the configured retry attempts are exhausted, `.send().await` returns `Err`. See the [Rust SDK retry configuration](https://docs.rs/aws-sdk-firehose/latest/aws_sdk_firehose/config/retry/struct.RetryConfig.html).

Inspect `FailedPutCount` and `RequestResponses`: the Rust SDK does **not** automatically resubmit failed entries from a successful HTTP 200 batch response. Back off and explicitly resubmit only failed entries, not records that already received IDs. Request retries and per-entry results are separate contracts; neither an API acknowledgment nor successful retry means S3 delivery is durable.

This increment meters only the public Direct PUT APIs, including their configuration-only non-S3 acknowledgments. Internal CloudWatch Logs and SES handoffs still share the buffering engine but are explicitly **unmetered** by these throughput controls. `KinesisStreamAsSource` streams still reject Direct PUT calls; Kinesis-source polling is not implemented.

### AWS references and local differences

**Compatibility change:** malformed batch entries no longer produce individual `InvalidArgumentException` results alongside accepted records. Both public PUT APIs validate every record before admission and reject the entire call for missing, wrong-type, or invalid-base64 data. Per-entry batch errors are reserved for throughput refusal; callers relying on partial acceptance of malformed batches must correct the input and retry the whole rejected call.

The [Firehose quotas guide](https://docs.aws.amazon.com/firehose/latest/dev/limits.html) and [regional quota reference](https://docs.aws.amazon.com/general/latest/gr/fh.html) define the regional defaults and shared per-stream Direct PUT limits. AWS describes proportional quota increases and automatic scaling with unspecified timing; fakecloud does not reproduce either workflow or AWS's internal admission algorithm. Local controls can change each dimension independently.

The [`PutRecord` reference](https://docs.aws.amazon.com/firehose/latest/APIReference/API_PutRecord.html) documents HTTP 500 `ServiceUnavailableException`, but its introductory 5,000-records/second figure conflicts with the 500,000 figure in the quota references for the three high-throughput regions. The table above follows the quota references, not that introductory number. The [`PutRecordBatch` reference](https://docs.aws.amazon.com/firehose/latest/APIReference/API_PutRecordBatch.html) distinguishes whole-call errors from ordered per-record results; it does not specify the exact threshold-allocation algorithm. One-second accounting, burst carry-over, and the dimension-to-error mapping here are explicit local testing policies, not claims about AWS's unpublished throttling algorithm.

There is also an HTTP-status discrepancy: both API references document HTTP 500 for `ServiceUnavailableException`, while the vendored Smithy model (`aws-models/firehose.json`) specifies HTTP 503. Throttling follows the API references and returns HTTP 500; official-SDK tests check both that status and the modeled error type. Closed admission during local shutdown is a distinct condition and retains HTTP 503 `ServiceUnavailableException` with message `Firehose admission is closed`, not `Slow down.`.

## S3 object bytes and names

Create the destination bucket before sending records. Records `abc`, `def\n`, and the binary bytes `00 ff` in one delivery window become exactly `abcdef\n` followed by `00 ff`, whether submitted in one batch or separate calls. For compressed output, those concatenated bytes are encoded in one GZIP or Xerial Snappy stream. Producers must supply their own delimiters, such as a newline for each JSON record, or enable `AppendDelimiterToRecord`, which writes the same records as exactly `abc\ndef\n\n` followed by `00 ff 0a` before any compression.

CloudWatch Logs subscription records arrive as GZIP-compressed bytes, as on AWS: each delivery is one record holding one GZIP-compressed `DATA_MESSAGE` envelope, with no base64 layer. An object that buffers several deliveries is their GZIP members concatenated with no separator; a multi-member GZIP reader such as `gunzip` or `zcat` returns the JSON envelopes back to back. Choosing `GZIP` compression on the stream compresses those members again, as on AWS, which documents that this data needs no Firehose compression; decompress such an object once to recover the members.

For a static prefix, the key format is:

```text
<prefix>yyyy/MM/dd/HH/<stream>-<version>-yyyy-MM-dd-HH-mm-ss-<uuid><extension>
```

The prefix's date hierarchy uses the **oldest record's arrival time in UTC**. The suffix timestamp uses the time the batch is detached for delivery, also UTC. Retries keep that captured key rather than creating new timestamps or UUIDs. The version comes from the configuration at detachment and increments on `UpdateDestination`. The UUID is a full hyphenated UUID.

An absent or empty prefix starts at `yyyy/MM/dd/HH/`. A prefix of `events/` produces `events/yyyy/MM/dd/HH/`; `events` produces `eventsyyyy/MM/dd/HH/`, without an inserted slash.

| `CompressionFormat` | Default extension | `Content-Encoding` |
| --- | --- | --- |
| `UNCOMPRESSED` (also the default when omitted) | None | None |
| `GZIP` | `.gz` | `gzip` |
| `Snappy` | `.snappy` | `snappy-java` |

Uncompressed output has no default extension. `FileExtension: ".custom.log"` replaces the default extension with `.custom.log`; an absent or empty override keeps the codec's default. Naming an object `.log` does not disable compression. `FileExtension` must be empty or a dot followed by lowercase letters, digits, or `!-_.*'()`, with at most 128 characters; other values return `InvalidArgumentException` from `CreateDeliveryStream` and `UpdateDestination`, and make a CloudFormation `AWS::KinesisFirehose::DeliveryStream` resource fail. `Content-Length` is the encoded object's byte length. S3 reads return the stored compressed bytes; decompress them to recover the original record bytes.

Use the exact enum spelling `Snappy` in `CompressionFormat`. Its output uses the Xerial/snappy-java container: a 16-byte version header followed by length-prefixed raw Snappy blocks, each covering up to 32 KiB of input. Record boundaries do not end a block, and each block is compressed independently. Read it with an Xerial-aware decoder; this is distinct from the standard Snappy framed-stream format and Hadoop-Snappy.

## Smoke test

```sh
fakecloud &

aws --endpoint-url http://localhost:4566 s3api create-bucket --bucket my-bucket

aws --endpoint-url http://localhost:4566 firehose create-delivery-stream \
  --delivery-stream-name events \
  --delivery-stream-type DirectPut \
  --extended-s3-destination-configuration '{
    "RoleARN": "arn:aws:iam::000000000000:role/firehose",
    "BucketARN": "arn:aws:s3:::my-bucket",
    "BufferingHints": {"SizeInMBs": 5, "IntervalInSeconds": 0},
    "CompressionFormat": "UNCOMPRESSED",
    "Prefix": "events/",
    "FileExtension": ".json"
  }'

aws --endpoint-url http://localhost:4566 firehose describe-delivery-stream \
  --delivery-stream-name events

aws --endpoint-url http://localhost:4566 firehose put-record \
  --delivery-stream-name events \
  --record Data=$(echo -n '{"id":"abc"}' | base64)

# Zero interval makes delivery eligible immediately, but the writer is async.
# Poll at most 20 times, sleeping 0.5 seconds after each empty result.
# CLI request latency and retries add time; this is not a wall-clock timeout.
# jq is used to inspect the object count.
for attempt in $(seq 1 20); do
  objects=$(aws --endpoint-url http://localhost:4566 s3api list-objects-v2 --bucket my-bucket)
  [ "$(printf '%s' "$objects" | jq '.Contents | length')" -gt 0 ] && break
  sleep 0.5
done
printf '%s\n' "$objects"
# Read the returned key with s3api get-object to inspect the original bytes.
```

## Introspection

`GET /_fakecloud/firehose/delivery-streams` is an IAM-bypass admin endpoint that returns every delivery stream across all accounts and regions, so tests can assert stream state without round-tripping through `DescribeDeliveryStream` (and without credentials):

```sh
curl -fsS http://localhost:4566/_fakecloud/firehose/delivery-streams | jq
```

Response shape:

```json
{
  "deliveryStreams": [
    {
      "accountId": "123456789012",
      "name": "fh-intro",
      "arn": "arn:aws:firehose:us-east-1:123456789012:deliverystream/fh-intro",
      "streamType": "DirectPut",
      "status": "ACTIVE",
      "encryption": { "status": "ENABLED", "keyType": "AWS_OWNED_CMK" },
      "destinationCount": 1,
      "createTimestamp": "2026-05-29T00:00:00+00:00",
      "lastUpdateTimestamp": "2026-05-29T00:00:00+00:00"
    }
  ]
}
```

`encryption` is derived from the persisted SSE config: `{ "status": "DISABLED" }` when no encryption is configured, or `{ "status": "ENABLED", "keyType": ..., "keyArn": ... }` after `StartDeliveryStreamEncryption` (the `keyArn` is present only for customer-managed keys). Streams are sorted by account, then name.

All first-party SDKs ship a `firehose` sub-client wrapping this endpoint (`getDeliveryStreams()`):

- Rust: `fakecloud_sdk::FakeCloud::new(url).firehose().get_delivery_streams()`
- Go: `fakecloud.New(url).Firehose().GetDeliveryStreams(ctx)`
- Python: `await fc.firehose.get_delivery_streams()` (async) or `fc.firehose.get_delivery_streams()` (sync)
- TypeScript: `await fc.firehose.getDeliveryStreams()`
- Java: `fc.firehose().getDeliveryStreams()`
- PHP: `$fc->firehose()->getDeliveryStreams()`

See [`reference/introspection`](/docs/reference/introspection/) for the full endpoint catalog.

## Caveats

Only S3 delivery with `UNCOMPRESSED`, `GZIP`, or `Snappy`, static prefixes, and UTC is implemented. Unsupported codecs (`ZIP` and `HADOOP_SNAPPY`), prefix expressions (`!{...}`), non-UTC time zones, enabled processing other than `AppendDelimiterToRecord` alone (a Lambda or other processor, even alongside `AppendDelimiterToRecord`, or no processors), enabled format conversion (including an omitted `Enabled` member), enabled dynamic partitioning, S3 backup pipelines, and non-S3 destinations return **HTTP 400 `InvalidArgumentException`** with a message naming the unsupported setting before `PutRecord` or `PutRecordBatch` acknowledges records. All modeled codec values and these destination configurations still round-trip through `CreateDeliveryStream`, `UpdateDestination`, and CloudFormation; they do not silently deliver unprocessed bytes. Disabled backup, `NoEncryption`, and other disabled capabilities remain usable. `ErrorOutputPrefix` and `CloudWatchLoggingOptions` are accepted and stored but do not change delivery.

Internal CloudWatch Logs subscription and SES event deliveries have no caller to receive an error. A record for a missing stream, for a stream with one of the unsupported settings above, or arriving after shutdown has closed admission is dropped and logged as a warning.

S3 backup pipelines and non-S3 destinations are accepted as configuration but rejected at record admission as described above. The destination bucket's default encryption (including SSE-KMS), replication, and S3 event notifications really apply to encoded objects through the shared S3 write path.

A destination's `EncryptionConfiguration.KMSEncryptionConfig` writes every delivered object with SSE-KMS under `AWSKMSKeyARN`, overriding the bucket's default encryption: `HeadObject`/`GetObject` report `ServerSideEncryption: aws:kms` and that key ARN, and reads return the decrypted (still compressed, for GZIP or Snappy output) bytes. With `NoEncryptionConfig` or no `EncryptionConfiguration`, the bucket default applies. The key is captured when a batch detaches, so a later `UpdateDestination` does not change the key of a batch already awaiting delivery or retry. A key that cannot encrypt (unknown, disabled, or not usable by the bucket's account) is a delivery failure: the records were already acknowledged, and the batch is retained and retried as described under [Retries, persistence, and shutdown](#retries-persistence-and-shutdown). Firehose stream encryption configured by `StartDeliveryStreamEncryption` is separate from destination-requested object encryption. `KinesisStreamAsSource` remains a stored source configuration; there is no Kinesis-source polling.

Delivery does not assume `RoleARN` or evaluate IAM policies, bucket policies, or `--iam strict` for Firehose: the stream writes into the configured bucket of its own account as that bucket's owner would. CloudWatch Logs subscription filters and SES event destinations may name a delivery stream in another account, and their records are delivered into that account's stream. A delivery stream stored without an S3 destination (for example, created with an S3 configuration missing `RoleARN` or `BucketARN`, which AWS rejects) acknowledges records without writing them.

## Retries, persistence, and shutdown

An acknowledgment means accepted into process-local memory, not durably delivered. A malformed S3 bucket ARN is accepted by `CreateDeliveryStream`, `UpdateDestination`, and CloudFormation, but record admission rejects it with `InvalidArgumentException`. A well-formed ARN naming a missing bucket is accepted; failed delivery is retained and retried, so creating that bucket later unblocks delivery at the next retry. Encoding and storage failures also retain the batch. Each stream has a FIFO delivery queue and at most one in-flight write; a failed head blocks later batches in that stream, while other streams can continue. At most four S3 writes run at once across all streams, scheduled in rotation so no stream starves.

Runtime retries reuse the same captured key and output configuration. As AWS documents for S3 destinations, a failed head is retried every 5 seconds, with no backoff. Retries start on the worker's 1-second tick, so each actual delay can run up to one second longer. Each failed retry logs a warning with the attempt number and the next retry delay. As on AWS for `DirectPut` streams, retries stop 24 hours after a batch's oldest record arrived: the next failed attempt after that discards the batch with a warning naming its record and byte counts, and later batches in the stream proceed. The shutdown drain still makes its single final attempt regardless of age.

**Memory policy:** failed batches remain in memory for up to 24 hours while new valid records continue to be accepted, subject to any opt-in ingestion quotas. Those quotas are not backlog-aware backpressure: there is no memory limit or durable pending spool, so memory use can grow with everything accepted during that window of prolonged failure; a process crash loses pending data. Persistent mode stores stream configuration and completed S3 objects, not buffers or retry queues.

**Shutdown policy:** graceful shutdown first lets the HTTP server finish in-flight requests, so API callers never see the closed admission. It then closes admission, waits for in-flight writes, and makes one final drain of supported buffered data without waiting for normal size/time hints or the retry interval. If a final head fails, later batches in that stream are not attempted; shutdown reports undelivered record/decoded-byte counts and does not start another retry loop. Unsupported active data is also reported undelivered. Late internal handoffs are refused with `ServiceUnavailableException` (HTTP 503) and dropped. The final drain has no time limit: a large backlog or slow store prolongs shutdown, and a supervisor that kills the process first (for example, after Docker's default 10-second stop timeout) loses whatever was not yet written. With a healthy bucket and store, graceful restart drains buffered records into durable objects; failed final-drain data is not recovered after restart. No flush/admin endpoint is provided.

## Source

- [`crates/fakecloud-firehose`](https://github.com/faiscadev/fakecloud/tree/main/crates/fakecloud-firehose)
- [Amazon Data Firehose API reference](https://docs.aws.amazon.com/firehose/latest/APIReference/Welcome.html)
