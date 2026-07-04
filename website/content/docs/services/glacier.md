+++
title = "Amazon S3 Glacier"
description = "Amazon S3 Glacier (glacier) on fakecloud: a complete 33-operation implementation (100% conformance) with a real data plane — vaults, archive upload/delete with SHA-256 tree hashes, multipart uploads, retrieval and inventory jobs, vault lock, notifications, access policy, tags, data-retrieval policy, and provisioned capacity. restJson1."
weight = 47
+++

fakecloud implements **Amazon S3 Glacier** as a restJson1 service. All
**33 operations** ship with **100% conformance** against AWS's own Smithy
model, backed by account-partitioned state that persists across restarts in
persistent mode.

Unlike a control-plane-only mock, Glacier on fakecloud has a **real data
plane**: uploaded archive bytes are stored, a real SHA-256 tree hash is
computed, and retrieval jobs return the exact bytes you uploaded.

## Account-scoped paths

Glacier's REST API scopes every request under an account id
(`/{accountId}/vaults/{vaultName}/...`). fakecloud accepts the literal `-`
that AWS documents as "the caller's account" — and, permissively, any account
id — mapping it to the authenticated caller's account. Custom request headers
(`x-amz-archive-description`, `x-amz-sha256-tree-hash`, `x-amz-part-size`,
`Content-Range`, `Range`) are honoured, and ids are returned in response
headers (`x-amz-archive-id`, `Location`, `x-amz-multipart-upload-id`,
`x-amz-job-id`, `x-amz-lock-id`, `x-amz-capacity-id`) exactly as the model's
`@httpHeader` bindings declare.

## Archives round-trip end to end

`UploadArchive` stores the real request bytes and computes the archive's
**SHA-256 tree hash** — Glacier's binary hash tree over 1 MiB chunks, which
collapses to a plain SHA-256 for small archives. When the caller supplies an
`x-amz-sha256-tree-hash` header it is validated against the computed hash;
a mismatch returns `InvalidParameterValueException`.

A retrieval then returns the same bytes:

1. `InitiateJob` with `{"Type": "archive-retrieval", "ArchiveId": "..."}`
   creates a job (HTTP 202) and returns its id in `x-amz-job-id`.
2. The job settles to `Succeeded` on the first read (`DescribeJob`,
   `ListJobs`, or `GetJobOutput`), mirroring the eventual consistency of the
   real multi-hour retrieval — without the wait.
3. `GetJobOutput` streams the stored archive bytes back, honouring a `Range`
   header for partial retrieval (HTTP 206 with a `Content-Range`).

`DeleteArchive` removes the bytes; the vault's `NumberOfArchives` and
`SizeInBytes` update accordingly.

## Inventory jobs

`InitiateJob` with `{"Type": "inventory-retrieval"}` builds a JSON inventory of
the vault's archives (id, description, creation date, size, tree hash);
`GetJobOutput` returns it as `application/json`. `ListJobs` supports the
`statuscode` and `completed` filters plus `marker`/`limit` pagination.

## Multipart uploads

`InitiateMultipartUpload` (with an `x-amz-part-size` that must be 1 MiB times a
power of two) starts an upload; `UploadMultipartPart` stores each part keyed by
its `Content-Range`; `ListParts` and `ListMultipartUploads` page through the
in-flight state; and `CompleteMultipartUpload` assembles the parts in order
into a stored archive, validating the declared `x-amz-archive-size` and the
combined tree hash. `AbortMultipartUpload` discards the upload.

## Vault lock

The vault-lock state machine is real: `InitiateVaultLock` moves a vault from
unlocked to **`InProgress`** and returns a lock id with a 24-hour expiry;
`CompleteVaultLock` with that id transitions it to **`Locked`**; `GetVaultLock`
reports the current `State`, `Policy`, `CreationDate`, and `ExpirationDate`;
and `AbortVaultLock` cancels an in-progress lock (a completed `Locked` vault
cannot be aborted). A stale in-progress lock expires automatically after 24h.

## What else is covered

- **Vault notifications** — `SetVaultNotifications` / `GetVaultNotifications` /
  `DeleteVaultNotifications` persist the SNS topic and event list.
- **Vault access policy** — `SetVaultAccessPolicy` / `GetVaultAccessPolicy` /
  `DeleteVaultAccessPolicy` round-trip the policy document.
- **Tags** — `AddTagsToVault` / `RemoveTagsFromVault` / `ListTagsForVault`
  (the tags POST is discriminated by the `operation=add|remove` query param).
- **Data-retrieval policy** — `GetDataRetrievalPolicy` reports the account
  policy (defaulting to a `BytesPerHour` rule), `SetDataRetrievalPolicy`
  persists a custom one.
- **Provisioned capacity** — `PurchaseProvisionedCapacity` returns a capacity
  id; `ListProvisionedCapacity` lists the account's units.

## Errors

Glacier returns its own `{"code", "message", "type"}` error envelope with the
model's declared HTTP status: `ResourceNotFoundException` (404) for a missing
vault, archive, upload, or job; `InvalidParameterValueException` (400) for a
malformed parameter or tree-hash mismatch; and `MissingParameterValueException`
(400) for an omitted required parameter.
