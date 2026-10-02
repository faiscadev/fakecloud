+++
title = "API Gateway v2"
description = "HTTP APIs, Lambda proxy integration, JWT and Lambda authorizers, CORS."
weight = 20
+++

fakecloud implements **103 of 103** API Gateway v2 operations at 100% conformance (HTTP APIs). REST APIs are covered separately on the [API Gateway v1](./apigateway.md) page.

## Supported features

- **HTTP APIs** — CreateApi, UpdateApi, DeleteApi, GetApis
- **WebSocket APIs** — CreateApi with `protocolType=WEBSOCKET`, live data plane upgrade endpoint, `$connect` / `$disconnect` / `$default` route dispatch to Lambda
- **Routes** — path parameters, wildcards, HTTP method routing; stage variable substitution in integration URIs and route keys
- **Integrations** — Lambda proxy (v2.0 format), HTTP proxy, Mock integration, AWS service integrations dispatched in-process
- **Stages** — CRUD, deployments, default stage; stage variables exposed to integrations and access logs
- **Authorizers** — JWT validated with real RS256 verification against the issuer's JWKS (audience, expiry, scope); REQUEST Lambda authorizers invoke real functions and cache by `identitySource`
- **Custom domain names + API mappings** — `ApiMapping` resolves host header to the right API + stage at request time
- **CORS** — configuration on routes and globally
- **Access logs** — per-stage `accessLogSettings` deliver formatted log events to the configured CloudWatch Logs log group
- **Request history** — every request served is recorded for introspection
- **Deployments** — CreateDeployment, GetDeployments
- **Management API** — `apigatewaymanagementapi` ops (PostToConnection, GetConnection, DeleteConnection) at `/@connections/{id}` and `/{stage}/@connections/{id}`

## Protocol

REST for management, path-based routing for the executed API. A deployed stage is reached at `{api_id}.execute-api.<region>.amazonaws.com/{stage}/{path}` (or LocalStack's `{api_id}.execute-api.localhost.localstack.cloud` host), at the host-less path-style URL `/_aws/execute-api/{api_id}/{stage}/{path}`, or at plain `/{stage}/{path}`. WebSocket APIs upgrade at `/_fakecloud/apigatewayv2/ws/{api_id}` and the management API is served on the same host:port — point the AWS SDK's `apigatewaymanagementapi` client at the fakecloud endpoint URL.

## Introspection

- `GET /_fakecloud/apigatewayv2/requests` — list all HTTP API requests received (method, path, headers, query params, status code, integration response)
- `GET /_fakecloud/apigatewayv2/connections` — list live WebSocket connections with `connectionId`, `apiId`, `stage`, `connectedAt`, `lastActiveAt`, `sourceIp`

## Cross-service delivery

- **API Gateway v2 -> Lambda** — HTTP API routes invoke Lambda functions with proxy integration v2.0 event format
- **API Gateway v2 -> Lambda (authorizers)** — REQUEST (Lambda) authorizers invoke real functions; policy + context returned drive the request decision
- **API Gateway v2 -> JWT issuers** — JWT authorizers fetch the configured issuer's JWKS and verify caller tokens (RS256 signature, expiry, audience, scope)
- **API Gateway v2 -> CloudWatch Logs** — stage access logs delivered to the configured log group with the formatted access log line
- **API Gateway v2 -> AWS services** — AWS service integrations dispatch to in-process service handlers

## Why this matters

LocalStack paywalls API Gateway v2. fakecloud implements the full HTTP API surface free, with real route matching, real Lambda proxy integration, and full request introspection. Webhook testing for event-driven applications is fully supported.

## Source

- [`crates/fakecloud-apigatewayv2`](https://github.com/faiscadev/fakecloud/tree/main/crates/fakecloud-apigatewayv2)
- [AWS API Gateway v2 API reference](https://docs.aws.amazon.com/apigatewayv2/latest/api-reference/api-reference.html)
