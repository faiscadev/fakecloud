+++
title = "Run an app unmodified: instance/task credentials"
description = "Resolve the AWS SDK default credential chain against fakecloud with no static keys and no code changes."
weight = 4
+++

Apps deployed on EC2 or ECS usually carry no static keys. The AWS SDK default credential chain pulls temporary credentials from the environment -- the ECS container-credentials endpoint (`AWS_CONTAINER_CREDENTIALS_FULL_URI`) or the EC2 instance metadata service (IMDS). Run that same app locally and the credential chain has nothing to talk to, so you either get a credentials error or inject static keys just for local runs -- a change to your app you have to remember to undo.

fakecloud serves a credentials endpoint so the default chain resolves with **no code change and no static keys**: set one environment variable and your real binary runs against fakecloud exactly as it runs in AWS.

## Container credentials (`AWS_CONTAINER_CREDENTIALS_FULL_URI`)

Point the AWS SDK's container-credentials provider at fakecloud:

```sh
export AWS_CONTAINER_CREDENTIALS_FULL_URI=http://localhost:4566/_fakecloud/credentials
# No AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY set.
aws sts get-caller-identity --endpoint-url http://localhost:4566
```

```json
{
  "UserId": "AROA...:fakecloud-local",
  "Account": "123456789012",
  "Arn": "arn:aws:sts::123456789012:assumed-role/fakecloud/fakecloud-local"
}
```

The endpoint returns credentials in the exact JSON shape the AWS SDK expects:

```json
{
  "AccessKeyId": "FSIA...",
  "SecretAccessKey": "...",
  "Token": "...",
  "Expiration": "2026-07-17T12:34:56Z",
  "RoleArn": "arn:aws:iam::123456789012:role/fakecloud"
}
```

These are not throwaway strings: each set is minted and registered in IAM state just like an `AssumeRole` session, so a request signed with them is accepted even when fakecloud runs with [`--verify-sigv4`](/docs/reference/security/). `GetCallerIdentity` reports the assumed-role principal.

### In docker-compose

Set the variable on the service container. Use the compose service name (not `localhost`) so the container can reach fakecloud:

```yaml
services:
  app:
    image: my-app
    environment:
      AWS_CONTAINER_CREDENTIALS_FULL_URI: http://fakecloud:4566/_fakecloud/credentials
      AWS_ENDPOINT_URL: http://fakecloud:4566
  fakecloud:
    image: fakecloud/fakecloud
```

The AWS SDKs only fetch container credentials over plain HTTP from loopback hosts (`127.0.0.1`, `localhost`) or when the host resolves to a loopback/link-local address; a compose service name backed by a private-network address is treated the same way real ECS treats `169.254.170.2`.

## Choosing the role

By default the vended credentials map to `arn:aws:iam::<account>:role/fakecloud`. Point them at the role your app actually assumes so `GetCallerIdentity` and any role-ARN assertions line up with production:

```sh
fakecloud --credentials-role-arn arn:aws:iam::123456789012:role/my-app-role
# or FAKECLOUD_CREDENTIALS_ROLE_ARN=...
```

## SDK helper

The first-party SDKs expose the endpoint directly for assertions -- e.g. in Rust:

```rust
let creds = fc.credentials().await?;
assert!(creds.access_key_id.starts_with("FSIA"));
```

Equivalent methods exist in every language SDK (`Credentials()` in Go, `credentials()` in Python/TypeScript/PHP/Java). See [SDK setup](/docs/getting-started/sdk-setup/).
