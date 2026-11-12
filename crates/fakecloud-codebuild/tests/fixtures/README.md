# CodeBuild recordings

Recorded with the AWS CLI against a real account (us-east-1, 2026-11-11). The
account id is normalized to `123456789012` and the throwaway resource prefix to
`fc-cb-rec`; key ids, request ids and messages are as AWS returned them.

Setup: a role trusted by `codebuild.amazonaws.com` allowed CloudWatch Logs,
`secretsmanager:GetSecretValue`, `ssm:GetParameters` / `ssm:GetParameter` and
`kms:Decrypt`; a `NO_SOURCE` project (inline buildspec `echo hi`,
`aws/codebuild/amazonlinux-x86_64-standard:5.0`, `BUILD_GENERAL1_SMALL`); a
customer KMS key holding a secret (`fc-cb-rec-sec`) and a SecureString
parameter (`/fc-cb-rec/sec`), then disabled.

- `env-var-resolution-failures.json`: `BatchGetBuilds` (`buildStatus`,
  `currentPhase`, each phase's `phaseType` / `phaseStatus` / `contexts`) of
  `StartBuild` with one `environmentVariablesOverride` entry: a
  `SECRETS_MANAGER` variable naming a missing secret (with and without a
  `:json-key`), a `PARAMETER_STORE` variable naming a missing parameter, and
  each naming the value under the disabled key. Every build provisions, fails
  `DOWNLOAD_SOURCE` with the variable's error and still runs `FINALIZING`. A
  SecureString under a disabled key reads as "parameter does not exist".
- `env-var-resolution-direct-reads.txt`: the same values read directly:
  `GetSecretValue` (`DecryptionFailure` "Secrets Manager can't decrypt the
  secret value: <KMS message>"), `GetParameters` (the disabled-key SecureString
  listed under `InvalidParameters`, as a missing name is), `GetParameter`
  (`InvalidKeyId` relaying KMS) and `GetParameter` of
  `/aws/reference/secretsmanager/<secret>` for the disabled-key secret
  (`AccessDeniedException`) and a missing one (`ParameterNotFound`).

Every resource was deleted afterwards (project, role and inline policy,
secret, parameter) and its absence confirmed; the key was disabled and
scheduled for deletion (7 days).

Not recorded: a build batch (`StartBuildBatch`) with an unresolvable variable.
`CreateProject` with a `buildBatchConfig` (and without one) refused a freshly
created service role with `InvalidInputException` "Invalid service role" for
several minutes, so no batch ran; fakecloud fails such a batch in
`DOWNLOAD_BATCHSPEC` before any container starts, which is not checked
against AWS.
