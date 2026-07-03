+++
title = "AWS Backup"
description = "AWS Backup (backup) on fakecloud: a complete 109-operation control plane (100% conformance) — backup plans, vaults, recovery points, backup/copy/restore/scan jobs, frameworks, report plans, legal holds, restore testing, tiering. restJson1."
weight = 48
+++

fakecloud implements **AWS Backup** as a restJson1 control plane. All
**109 operations** ship with **100% conformance** against AWS's own Smithy
model, backed by account-partitioned state that persists across restarts in
persistent mode.

## Control-plane only

This is a control-plane emulation: no real backup engine runs, and no bytes are
moved (mirroring LocalStack Community, which treats AWS Backup as a control-plane
mock). CRUD is real — `CreateBackupPlan` / `CreateBackupVault` / `CreateFramework`
and friends persist state that their `Get` / `Describe` / `List` counterparts
reflect, `Update` persists, and `Delete` removes. Real ARNs are returned
(`arn:aws:backup:<region>:<account>:backup-vault:<name>`, `:backup-plan:<id>`,
recovery-point ARNs that reference the protected resource), and list filters
(`ByState`, `ByResourceType`, `ByBackupVaultName`, ...) apply.

## Jobs and recovery points

Backup, copy, restore, and scan **jobs** are recorded and progressed
synthetically to a terminal state, so `Describe*Job` / `List*Jobs` show completed
work without any real data movement:

- `StartBackupJob` returns a `BackupJobId` and a synthetic `RecoveryPointArn`,
  and records a recovery point in the target vault. `DescribeRecoveryPoint` then
  resolves that recovery point, and the resource becomes visible through
  `DescribeProtectedResource` / `ListProtectedResources` /
  `ListRecoveryPointsByResource`.
- `DescribeBackupJob` settles a `RUNNING` job to `COMPLETED` (with
  `PercentDone` `100.0` and a completion date), mirroring the eventual
  consistency of the real service.
- `StartCopyJob`, `StartRestoreJob`, and `StartScanJob` behave the same way;
  `StartRestoreJob` resolves the recovery point created by a prior backup and
  reports a synthesized `CreatedResourceArn` on completion.

## What is covered

The full surface persists and round-trips:

- **Backup plans** with versions (`ListBackupPlanVersions`) and **backup
  selections**, plus plan templates and JSON import/export.
- **Backup vaults** — standard, logically-air-gapped, and restore-access — with
  notifications, access policies, and vault-lock configuration.
- **Recovery points**, including lifecycle updates, index settings, restore
  metadata, and parent/child disassociation.
- **Frameworks** and **report plans** (with **report jobs**), which settle to a
  `COMPLETED` deployment status on describe.
- **Legal holds** (create / get / cancel), **restore-testing plans** and
  **selections**, and **tiering configurations**.
- Account-scoped **global settings** and **region (opt-in) settings**, resource
  tagging, and the supported-resource-type catalogue.

Validation mirrors the model's constraints: vault names must match
`^[a-zA-Z0-9\-\_]{2,50}$`, framework / report-plan names must match
`^[a-zA-Z][_a-zA-Z0-9]*$`, `MaxResults` is bounded to `1..=1000`, and enum-valued
filters are checked — each rejection returns the model's declared error
(`InvalidParameterValueException`, `MissingParameterValueException`,
`ResourceNotFoundException`, `AlreadyExistsException`, ...).
