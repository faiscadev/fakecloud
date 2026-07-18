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

## Instance metadata (IMDS)

An app that reads credentials from the EC2 instance metadata service (IMDS at `http://169.254.169.254`) rather than through the container path works too. Point the SDK's IMDS client at fakecloud:

```sh
# Note the trailing slash: the AWS CLI appends `latest/...` to this base directly.
export AWS_EC2_METADATA_SERVICE_ENDPOINT=http://localhost:4566/
aws sts get-caller-identity --endpoint-url http://localhost:4566   # no static keys
```

fakecloud serves the `/latest/*` metadata surface, including both IMDSv1 (plain GET) and IMDSv2 (token-first):

```sh
# IMDSv2: fetch a token, then use it.
TOKEN=$(curl -sX PUT http://localhost:4566/latest/api/token \
  -H 'X-aws-ec2-metadata-token-ttl-seconds: 21600')
curl -s -H "X-aws-ec2-metadata-token: $TOKEN" \
  http://localhost:4566/latest/meta-data/iam/security-credentials/
# -> fakecloud   (the role name)
curl -s -H "X-aws-ec2-metadata-token: $TOKEN" \
  http://localhost:4566/latest/meta-data/iam/security-credentials/fakecloud
```

The credential JSON is the IMDS shape (`Code`, `LastUpdated`, `Type`, `AccessKeyId`, `SecretAccessKey`, `Token`, `Expiration`) and comes from the same registered-credential cache as `/_fakecloud/credentials`, so it verifies under `--verify-sigv4`. fakecloud does not enforce the IMDSv2 token — it hands one out and accepts requests with or without it — so both SDK modes work.

Other metadata paths served: `/latest/meta-data/instance-id`, `/latest/meta-data/placement/region`, `/latest/meta-data/placement/availability-zone`, `/latest/meta-data/iam/info`, and the instance identity document at `/latest/dynamic/instance-identity/document`. Set the reported instance ID with `--imds-instance-id` (default: a stable synthetic `i-…`).

### Advanced: apps that hardcode `169.254.169.254`

Some apps ignore `AWS_EC2_METADATA_SERVICE_ENDPOINT` and talk to the real IMDS IP `http://169.254.169.254` directly (and some ECS SDKs hardcode the `169.254.170.2` container-credentials base). Run fakecloud with `--imds-link-local` (or `FAKECLOUD_IMDS_LINK_LOCAL=1`) and it also binds those addresses on port 80: the full IMDS surface at `169.254.169.254`, and container credentials at `169.254.170.2/creds`. Set `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI=/creds` for apps that use the relative-URI base.

This needs privileged host setup that fakecloud does **not** perform for you: binding port 80 needs **root**, and the link-local addresses must already be assigned to the loopback interface. Assign them first, then run fakecloud as root:

```sh
# Linux
sudo ip addr add 169.254.169.254/32 dev lo
sudo ip addr add 169.254.170.2/32 dev lo
# macOS
sudo ifconfig lo0 alias 169.254.169.254
sudo ifconfig lo0 alias 169.254.170.2

sudo fakecloud --imds-link-local
```

fakecloud never creates or deletes these aliases itself, so it leaves no host-networking state behind: the alias is yours to add and to remove (`sudo ip addr del 169.254.169.254/32 dev lo` when you're done). If the address is not aliased, the bind simply fails: fakecloud logs the exact command above and keeps running, and the main listener (plus the `AWS_EC2_METADATA_SERVICE_ENDPOINT` / `AWS_CONTAINER_CREDENTIALS_FULL_URI` paths) is unaffected.

**Reaching it from a container.** A container hitting `169.254.169.254` reaches its own network namespace, not the host's loopback alias, so an app *inside* a container needs the address routed to fakecloud explicitly, e.g. a docker-compose `extra_hosts` entry or a route to the host. Prefer the `AWS_EC2_METADATA_SERVICE_ENDPOINT` / `AWS_CONTAINER_CREDENTIALS_FULL_URI` env-var approach for containerized apps; the link-local listener is aimed at apps running directly on the host.

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
