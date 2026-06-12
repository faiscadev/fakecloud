+++
title = "Cognito User Pools"
description = "User pools, app clients, MFA, identity providers, full authentication flows."
weight = 15
+++

fakecloud implements **126 of 126** Cognito User Pools operations at 100% Smithy conformance.

## Supported features

- **User pools** — CRUD, password policies, attribute configuration, account recovery, email/SMS configuration
- **App clients** — CRUD, OAuth flows, token validity, supported identity providers
- **Users** — admin create/delete/update, self-signup, group membership
- **Groups** — CRUD, user membership, precedence
- **MFA** — SMS, TOTP, software token setup/verification
- **WebAuthn** — passkey registration and assertion, with real `packed`-format attestation parsing (AAGUID, certificate chain summary, signature counter) and verification at registration
- **Identity providers** — SAML, OIDC, social
- **Resource servers** — CRUD, custom scopes
- **Domains** — user pool domains
- **Multi-region replicas** — `CreateUserPoolReplica` / `DeleteUserPoolReplica` / `ListUserPoolReplicas` / `UpdateUserPoolReplica`. The pool's own region is reported as the `PRIMARY` replica; secondary regions are recorded as control-plane entries (region + status) — fakecloud is single-region, so a replica is a record rather than a real second directory copy
- **Authentication flows** — USER_PASSWORD_AUTH, USER_SRP_AUTH, REFRESH_TOKEN_AUTH, CUSTOM_AUTH, ADMIN_USER_PASSWORD_AUTH
- **Password management** — ChangePassword, ForgotPassword, ConfirmForgotPassword
- **Confirmation codes** — email/SMS confirmation flows
- **Devices** — Confirm, update, forget, track
- **Tokens** — access, refresh, ID tokens with real JWT structure. The `PreTokenGeneration` trigger's `claimsOverrideDetails` (claims to add/suppress and group overrides) is merged into the issued access and ID tokens before signing
- **Compromised credentials** — `CompromisedCredentialsRiskConfiguration` with `Actions.EventAction = BLOCK` rejects `SignUp` / `AdminInitiateAuth` when the supplied password's SHA-256 hash is in the configured compromised set
- **Signing keys** — `GetSigningCertificate` returns a real, parseable X.509 certificate that wraps the pool's RSA-2048 signing key (same key served via JWKS), matching the AWS response shape
- **Auth events** — sign-up, sign-in, failures, password changes

## Protocol

JSON protocol. `X-Amz-Target` header, JSON body, JSON responses.

## OAuth2 / OIDC endpoints

- `GET /oauth2/authorize` — Hosted UI authorization endpoint. Supports `response_type=code` (Authorization Code grant, with optional PKCE `S256`/`plain`) and `response_type=token` (Implicit grant). Validates `client_id`, `redirect_uri` (must match the client's `CallbackURLs`), and `scope` (must be a subset of `AllowedOAuthScopes`). For scripted tests, accepts `username`/`password` query params in lieu of the real Cognito Hosted UI HTML form.
- `POST /oauth2/token` — token endpoint with `authorization_code`, `client_credentials`, and `refresh_token` grants. RFC 6749 §2.3.1 Basic auth supported. PKCE (S256/plain) verified for `authorization_code`. `PreTokenGeneration` `claimsOverrideDetails` are merged into the access and ID tokens before signing.
- `GET|POST /oauth2/userInfo` — RFC 7662 user info (bearer access token -> standard OIDC claims).
- `POST /oauth2/revoke` — RFC 7009 token revocation.
- `GET /<pool_id>/.well-known/jwks.json` — RS256 public key for token verification.
- `GET /<pool_id>/.well-known/openid-configuration` — OIDC discovery document.

## Introspection

- `GET /_fakecloud/cognito/confirmation-codes` — list all pending confirmation codes across pools
- `GET /_fakecloud/cognito/confirmation-codes/{pool_id}/{username}` — codes for a specific user
- `POST /_fakecloud/cognito/confirm-user` — force-confirm a user without the email/SMS flow
- `GET /_fakecloud/cognito/tokens` — list active tokens (without exposing strings)
- `POST /_fakecloud/cognito/expire-tokens` — expire tokens for a pool/user
- `GET /_fakecloud/cognito/auth-events` — list auth events (signup, signin, failures)
- `POST /_fakecloud/cognito/authorization-codes` — mint a single-use OAuth2 authorization code for the `authorization_code` grant (programmatic alternative to driving `/oauth2/authorize`)
- `POST /_fakecloud/cognito/compromised-passwords` — register plaintext passwords as compromised; each is SHA-256 hashed server-side and added to the per-account set checked by `CompromisedCredentialsRiskConfiguration` enforcement
- `GET /_fakecloud/cognito/webauthn-credentials` — list registered WebAuthn credentials with parsed `packed`-attestation info (AAGUID, certificate chain summary, signature counter)
- `GET /_fakecloud/cognito/pretokengen/invocations` — list PreTokenGeneration Lambda trigger invocations recorded by `InitiateAuth`, with full request/response payloads plus pre-parsed `claims_added`, `claims_overridden`, and `group_overrides` so tests can assert claim mutation flows without inspecting the issued JWT

## Cross-service delivery

- **Cognito -> Lambda** — All 12 triggers: PreSignUp, PostConfirmation, PreAuthentication, PostAuthentication, CustomMessage, PreTokenGeneration, UserMigration, DefineAuthChallenge, CreateAuthChallenge, VerifyAuthChallengeResponse, **CustomEmailSender**, **CustomSMSSender**
- **Cognito -> SES** — Verification emails generated by `SignUp` / `ResendConfirmationCode` / `ForgotPassword` / `GetUserAttributeVerificationCode` (email attribute) are dispatched through SES and visible at `/_fakecloud/ses/emails`
- **Cognito -> SNS** — SMS verification codes generated by `GetUserAttributeVerificationCode` (phone attribute) are dispatched through SNS and visible at `/_fakecloud/sns/sms`
- **CustomEmailSender / CustomSMSSender precedence** — When configured on the user pool, the Lambda is invoked instead of going through SES/SNS, matching AWS behavior

## Why this matters

LocalStack only offers Cognito behind a paid tier. fakecloud implements the full user pool surface free and open-source, with real JWT issuance, real auth flows, and introspection for the confirmation-code / token state that makes testing auth flows feasible end-to-end.

## Source

- [`crates/fakecloud-cognito`](https://github.com/faiscadev/fakecloud/tree/main/crates/fakecloud-cognito)
- [AWS Cognito Identity Provider API reference](https://docs.aws.amazon.com/cognito-user-identity-pools/latest/APIReference/Welcome.html)
