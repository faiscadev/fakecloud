# Lambda function-error recordings

`aws-recordings.json` holds responses recorded against real AWS (us-east-1,
2026-11-10) with the AWS CLI and curl. They pin how Lambda reports function
errors and how Step Functions, API Gateway (REST and HTTP APIs, including
Lambda authorizers) and Cognito triggers react to them. `functions` describes
each recorded function; account IDs are replaced by `123456789012`.

Every resource was deleted after recording. Tests that assert these
behaviours cite the entry they reproduce.
