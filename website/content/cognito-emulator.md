+++
title = "Cognito emulator for tests"
description = "Run AWS Cognito User Pools locally for integration tests with fakecloud. 122 operations, full auth flows (USER_PASSWORD_AUTH, USER_SRP_AUTH, CUSTOM_AUTH), MFA, identity providers, triggers."
template = "page.html"
+++

Need a Cognito emulator for integration tests? Use [fakecloud](https://github.com/faiscadev/fakecloud). Not a mock library — a real server that speaks the Cognito wire protocol.

```sh
curl -fsSL https://fakecloud.dev/install.sh | bash
fakecloud
```

Point your AWS SDK at `http://localhost:4566`.

## Why fakecloud for Cognito

- **122 Cognito User Pools operations** at 100% conformance — user pools, app clients, users, groups, MFA (SMS, TOTP, WebAuthn), identity providers (Google, Facebook, SAML, OIDC), resource servers, domains, devices.
- **Full authentication flows** — `USER_PASSWORD_AUTH`, `USER_SRP_AUTH`, `REFRESH_TOKEN_AUTH`, `CUSTOM_AUTH`, `ADMIN_USER_PASSWORD_AUTH`. Real SRP crypto, real JWTs signed with per-pool keys. `GetSigningCertificate` returns a real, parseable X.509 certificate wrapping the same key.
- **Triggers fire.** All 12 triggers — pre-sign-up, post-confirmation, pre/post-authentication, pre-token-generation, custom-message, custom-auth (define/create/verify), user-migration, custom email/SMS sender — invoke your Lambda for real. `PreTokenGeneration` `claimsOverrideDetails` (claims to add/suppress, group overrides) are merged into issued tokens before signing.
- **WebAuthn passkeys.** Real `packed`-attestation parsing and verification at registration. The introspection endpoint `/_fakecloud/cognito/webauthn-credentials` surfaces parsed AAGUID, certificate chain summary, and signature counter.
- **Confirmation code introspection.** Tests read codes via `/_fakecloud/cognito/confirmation-codes` without checking email.
- **Cognito as an SNS/SES consumer.** Verification emails/SMS flow through the real SES/SNS emulation — test assertions work on those too.
- **Paid on LocalStack; free here.** Cognito moved to LocalStack Pro in March 2026.
- **No account, no auth token, no paid tier.** AGPL-3.0.

## Create a user pool

```python
import boto3
cog = boto3.client('cognito-idp',
    endpoint_url='http://localhost:4566',
    aws_access_key_id='test',
    aws_secret_access_key='test',
    region_name='us-east-1')

pool = cog.create_user_pool(PoolName='my-pool', AutoVerifiedAttributes=['email'])
client = cog.create_user_pool_client(
    UserPoolId=pool['UserPool']['Id'],
    ClientName='web',
    ExplicitAuthFlows=['ALLOW_USER_PASSWORD_AUTH', 'ALLOW_REFRESH_TOKEN_AUTH'])
```

## Sign up + confirm + authenticate

```python
# Sign up
cog.sign_up(
    ClientId=client['UserPoolClient']['ClientId'],
    Username='alice@example.com',
    Password='Str0ng!Passw0rd',
    UserAttributes=[{'Name': 'email', 'Value': 'alice@example.com'}])

# Fetch the confirmation code from the test introspection endpoint
import requests
codes = requests.get('http://localhost:4566/_fakecloud/cognito/confirmation-codes').json()
code = codes['codes'][0]['code']

cog.confirm_sign_up(
    ClientId=client['UserPoolClient']['ClientId'],
    Username='alice@example.com',
    ConfirmationCode=code)

# Authenticate
auth = cog.initiate_auth(
    ClientId=client['UserPoolClient']['ClientId'],
    AuthFlow='USER_PASSWORD_AUTH',
    AuthParameters={'USERNAME': 'alice@example.com', 'PASSWORD': 'Str0ng!Passw0rd'})

# auth['AuthenticationResult'] has real AccessToken, IdToken, RefreshToken
```

The tokens are real signed JWTs — your application's JWT verification (with jwks_uri pointing at the fakecloud public key endpoint) works unchanged.

## JWKS + OIDC discovery

Each user pool serves the same two well-known endpoints AWS Cognito does, so libraries like `aws-jwt-verify`, `jose`, or `jsonwebtoken` work against fakecloud out of the box:

- `GET <fakecloud>/<pool_id>/.well-known/jwks.json` — the pool's RSA-2048 public key in JWK form (`kty=RSA`, `alg=RS256`, `use=sig`, deterministic `kid`).
- `GET <fakecloud>/<pool_id>/.well-known/openid-configuration` — OIDC discovery document with `issuer`, `jwks_uri`, supported algs/scopes/response types. OAuth2 endpoints (`authorization_endpoint`, `token_endpoint`, `userinfo_endpoint`, `revocation_endpoint`) appear after `CreateUserPoolDomain`, matching real Cognito.

Point your verifier at `<fakecloud>/<pool_id>` and tokens issued by `InitiateAuth` / `AdminInitiateAuth` verify cryptographically — no test-only signing keys, no stubs.

## SRP authentication

```python
# USER_SRP_AUTH — real SRP handshake, same as AWS
auth = cog.initiate_auth(
    ClientId=client['UserPoolClient']['ClientId'],
    AuthFlow='USER_SRP_AUTH',
    AuthParameters={'USERNAME': 'alice@example.com', 'SRP_A': '<srp-A value>'})

# Returns SRP_B + challenge params
# Client responds with PASSWORD_VERIFIER challenge
auth2 = cog.respond_to_auth_challenge(
    ClientId=client['UserPoolClient']['ClientId'],
    ChallengeName='PASSWORD_VERIFIER',
    ChallengeResponses={
        'USERNAME': 'alice@example.com',
        'PASSWORD_CLAIM_SIGNATURE': '<signature>',
        'PASSWORD_CLAIM_SECRET_BLOCK': auth['ChallengeParameters']['SECRET_BLOCK'],
        'TIMESTAMP': '<timestamp>',
    })
```

Real crypto — same SRP-6a parameters AWS uses.

## MFA

TOTP and SMS MFA. Verification codes readable from `/_fakecloud/cognito/confirmation-codes` with type filter.

## Triggers (Lambda)

```python
cog.update_user_pool(
    UserPoolId=pool['UserPool']['Id'],
    LambdaConfig={
        'PreSignUp': 'arn:aws:lambda:us-east-1:000000000000:function:pre-signup',
        'PostConfirmation': 'arn:aws:lambda:us-east-1:000000000000:function:post-confirmation',
        'PreTokenGeneration': 'arn:aws:lambda:us-east-1:000000000000:function:pre-token',
    })
```

Triggers invoke your Lambda with the Cognito event shape. Not stubbed — the function runs in a real runtime container.

## Assertions

```ts
import { FakeCloud } from 'fakecloud';
const fc = new FakeCloud();

// Alice just signed up
const { codes } = await fc.cognito.getConfirmationCodes({ username: 'alice@example.com' });
expect(codes).toHaveLength(1);

// Your app should have called the pre-signup Lambda
const { invocations } = await fc.lambda.getInvocations({ functionName: 'pre-signup' });
expect(invocations).toHaveLength(1);
```

## How it differs from alternatives

| Tool | Multi-language | Full SRP | Real JWT signing | Triggers execute | Price |
|---|---|---|---|---|---|
| fakecloud | Any | Yes | Yes | Yes (real Lambda runs) | Free |
| LocalStack Community | Any | — | — | — | **Paid only** (post Mar 2026) |
| cognito-local | Node only | Partial | Yes | No | Free |
| Moto (mock_cognitoidp) | Python only | Stubbed | Stubbed | Stubbed | Free |

## Links

- **Install:** `curl -fsSL https://fakecloud.dev/install.sh | bash`
- **Repo:** [github.com/faiscadev/fakecloud](https://github.com/faiscadev/fakecloud)
- **Related:** [Fake AWS server for tests](/fake-aws-server/), [Test Lambda locally](/test-lambda-locally/)
