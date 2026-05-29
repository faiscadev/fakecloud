+++
title = "Firehose"
description = "Amazon Data Firehose delivery streams (control plane), JSON 1.1 protocol."
weight = 32
+++

fakecloud implements Amazon Data Firehose's JSON 1.1 control plane: delivery stream CRUD + tagging. Records sent via `PutRecord` / `PutRecordBatch` are accepted and acked; data-plane fan-out to S3 / OpenSearch / Redshift / Splunk is not implemented (use the [S3](/docs/services/s3/) endpoints + a real Firehose if you need actual delivery).

**Status: control-plane parity. Data plane stops at acknowledgement — records are not written to destinations.**

## Supported today

- **Delivery streams** — `CreateDeliveryStream` / `DescribeDeliveryStream` / `ListDeliveryStreams` / `DeleteDeliveryStream` / `UpdateDestination`. Both `DirectPut` and `KinesisStreamAsSource` source types round-trip. Streams progress through `CREATING -> ACTIVE` on create and `DELETING -> deleted` on delete.
- **Destinations** — `ExtendedS3DestinationConfiguration`, `RedshiftDestinationConfiguration`, `ElasticsearchDestinationConfiguration`, `AmazonopensearchserviceDestinationConfiguration`, `SplunkDestinationConfiguration`, `HttpEndpointDestinationConfiguration`, `SnowflakeDestinationConfiguration`, `IcebergDestinationConfiguration`. Configuration round-trips verbatim through `UpdateDestination`.
- **Buffering hints** — `BufferingHints` (`SizeInMBs`, `IntervalInSeconds`) are **range-checked** on `CreateDeliveryStream` and `UpdateDestination`:
  - `SizeInMBs`: 1 - 128 MB.
  - `IntervalInSeconds`: `0` (disabled) or 60 - 900 s.

  Out-of-range values return `InvalidArgumentException` with the AWS-shaped message, matching real Firehose.
- **Records** — `PutRecord` / `PutRecordBatch` accept records, assign per-record `RecordId`s, and update `DeliveryStreamStatus` / `LastUpdateTimestamp`. Batches up to 500 records / 4 MB are honoured; over-limit batches return `ServiceUnavailableException`.
- **Tags** — `ListTagsForDeliveryStream` / `TagDeliveryStream` / `UntagDeliveryStream`. Keyed by stream ARN.
- **Server-side encryption** — `StartDeliveryStreamEncryption` / `StopDeliveryStreamEncryption`. Start sets the stream's `DeliveryStreamEncryptionConfiguration` to `Status=ENABLED` with the requested `KeyType` (defaults `AWS_OWNED_CMK`; stores the `KeyARN` for `CUSTOMER_MANAGED_CMK`); Stop flips it to `DISABLED`. The config is surfaced on `DescribeDeliveryStream`. Unknown streams return `ResourceNotFoundException`.

## Smoke test

```sh
fakecloud &

aws --endpoint-url http://localhost:4566 firehose create-delivery-stream \
  --delivery-stream-name events \
  --delivery-stream-type DirectPut \
  --extended-s3-destination-configuration '{
    "RoleARN": "arn:aws:iam::000000000000:role/firehose",
    "BucketARN": "arn:aws:s3:::my-bucket",
    "BufferingHints": {"SizeInMBs": 5, "IntervalInSeconds": 300}
  }'

aws --endpoint-url http://localhost:4566 firehose describe-delivery-stream \
  --delivery-stream-name events

aws --endpoint-url http://localhost:4566 firehose put-record \
  --delivery-stream-name events \
  --record Data=$(echo -n '{"id":"abc"}' | base64)

# Out-of-range BufferingHints is rejected to match real Firehose.
aws --endpoint-url http://localhost:4566 firehose update-destination \
  --delivery-stream-name events \
  --current-delivery-stream-version-id 1 \
  --destination-id destinationId-000000000001 \
  --extended-s3-destination-update 'BufferingHints={SizeInMBs=999,IntervalInSeconds=10}'
# -> InvalidArgumentException
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

Data delivery is not implemented. `PutRecord` returns a `RecordId` but the bytes are dropped — no S3 object is written, no Redshift COPY is issued, no OpenSearch document is indexed, no HTTP endpoint is hit. Buffering, format conversion (Parquet/ORC), and dynamic partitioning are all configuration-only.

This is enough to test IAM policy paths, SDK wiring, retry / batch logic, and BufferingHints validation. It is not enough to test downstream delivery semantics.

## Source

- [`crates/fakecloud-firehose`](https://github.com/faiscadev/fakecloud/tree/main/crates/fakecloud-firehose)
- [Amazon Data Firehose API reference](https://docs.aws.amazon.com/firehose/latest/APIReference/Welcome.html)
