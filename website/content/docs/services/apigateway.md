+++
title = "API Gateway v1"
description = "REST APIs, resources/methods/integrations, deployments, stages, API keys, usage plans, authorizers."
weight = 19
+++

fakecloud implements **124 of 124** API Gateway v1 (REST APIs) operations at 100% conformance for the implemented surface. v1 is exposed under the same SigV4 service name (`apigateway`) as v2; URL prefix selects which handler runs.

REST APIs (v1) and HTTP APIs (v2) are independent AWS services. The v2 (HTTP APIs) page is at [API Gateway v2](./apigatewayv2.md).

## Supported features

- **REST APIs** — CreateRestApi, GetRestApi(s), UpdateRestApi, PutRestApi (OpenAPI overwrite/merge), ImportRestApi (an OpenAPI 3 / Swagger 2 definition, JSON or YAML, becomes the API's resources, methods, integrations, method/integration responses, authorizers and request validators), DeleteRestApi
- **Resources & methods** — full CRUD; nested paths; method requests/responses
- **Integrations** — `MOCK`, `HTTP`/`HTTP_PROXY`, `AWS_PROXY` (Lambda), AWS direct service integrations (`AWS`), `VPC_LINK`; request + response templates with VTL evaluation; integration responses
- **Deployments & stages** — CreateDeployment auto-creates a stage when `stageName` is set; cache flush operations; stage variables
- **Models & request validators** — schema management; validator CRUD; body/parameter validation enforced at the data plane
- **Authorizers** — TOKEN and REQUEST invoke real Lambda authorizer functions; COGNITO_USER_POOLS validates real RS256 JWTs against the user pool's JWKS (issuer, expiry, audience, signature)
- **API keys & usage plans** — CRUD + association via `usage_plan_keys`; data plane enforces `apiKeyRequired`, plan-level `throttle` (token-bucket per key+plan), method-level overrides under `apiStages[].throttle["{path}/{HTTP_METHOD}"]` (including the `/*/*` wildcard), and `quota` (DAY/WEEK/MONTH counters)
- **VPC links, domain names, base path mappings, client certificates** — full CRUD; custom domains route to APIs via base path mapping
- **Binary media types** — request/response bodies for configured `binaryMediaTypes` are base64-encoded into the Lambda proxy event and decoded from the integration response
- **Documentation parts/versions** — full CRUD
- **Gateway responses** — full CRUD
- **Tags** — TagResource/UntagResource/GetTags
- **Account & SDKs** — GetAccount/UpdateAccount; GetSdk(Type|Types); GetExport
- **Test invoke** — TestInvokeMethod / TestInvokeAuthorizer

## Data plane

A deployed stage is reachable at any of these URLs:

- `http://{api_id}.execute-api.<region>.amazonaws.com:4566/{stage}/{path}` or `http://{api_id}.execute-api.localhost.localstack.cloud:4566/{stage}/{path}`: the execute-api host names the API (point the hostname at fakecloud, or send it as the `Host` header).
- `http://localhost:4566/restapis/{api_id}/{stage}/_user_request_/{path}` and `http://localhost:4566/_aws/execute-api/{api_id}/{stage}/{path}`: path-style invocation URLs that need no special host.
- `http://localhost:4566/restapis/{api_id}/{stage}/{path}`: served by the data plane when it is not a control-plane route and `{stage}` is deployed on that API. Prefer `_user_request_` when a stage is named like a control-plane collection (`resources`, `stages`, ...).
- `http://localhost:4566/{stage}/{path}`: a plain-host request goes to the REST API that has that stage when no HTTP API (v2) defines a stage of the same name.

fakecloud walks the resource tree, picks the matching method/integration, and dispatches:

- `AWS_PROXY` — invokes the target Lambda via the same `DeliveryBus` used elsewhere; builds the v1.0 Lambda proxy event envelope (`event.version = "1.0"`, `requestContext.identity`, `multiValueHeaders`, `multiValueQueryStringParameters`, `pathParameters`, `stageVariables`, base64 body when the request content-type matches a configured `binaryMediaType`).
- `HTTP` / `HTTP_PROXY` — forwards via `reqwest` to the configured URI.
- `AWS` — direct AWS service integration; the URI is parsed for service/action and dispatched to the in-process service handler (no SigV4 round-trip).
- `VPC_LINK` — connection ID resolves to the configured VPC link target URL; request is forwarded the same way `HTTP` is, with the VPC link as the backend.
- `MOCK` — request template selects the integration response; response templates apply VTL substitution over `$input`, `$context`, and `$stageVariables` to build the body.

Request validators (`validateRequestBody`, `validateRequestParameters`) run before the integration: missing required headers/query parameters and JSON bodies that fail the attached model schema short-circuit with `400 BadRequestException`. Stage variables are exposed to integration URIs (`${stageVariables.name}`) and to VTL templates. TOKEN/REQUEST authorizers invoke the configured Lambda and cache the policy by `identitySource` for `authorizerResultTtlInSeconds`; COGNITO_USER_POOLS authorizers fetch the user pool's JWKS, verify the JWT's RS256 signature, issuer, expiry, and (if configured) audience, and expose claims under `requestContext.authorizer.claims`.

Before the integration runs, methods with `apiKeyRequired = true` go through the API key + usage plan gate: an `x-api-key` header is required, the value is matched against `state.api_keys` (must be enabled), and the first usage plan whose `apiStages[]` contains `(api_id, stage)` is selected. Throttle resolution prefers the matched apiStage's `throttle["{resource_path}/{HTTP_METHOD}"]` override (with `/*/*` honored as the catch-all) over the plan-level `throttle`; the chosen `(rateLimit, burstLimit)` drives a token-bucket whose meter key includes the override path so per-method buckets stay segregated. The plan's `quota` (DAY/WEEK/MONTH counter against `limit`, with `offset` shifting the period boundary) is enforced after throttle. Failures return `403 ForbiddenException` and `429 LimitExceededException` respectively, matching the AWS wire shape.

## Protocol

REST-style URL dispatch. fakecloud's facade routes `/restapis/...`, `/apikeys`, `/usageplans`, `/vpclinks`, `/domainnames`, `/clientcertificates`, `/sdktypes`, `/tags`, `/account` to the v1 service; `/v2/...` to v2; the data plane (deployed stage URLs, including the path-style forms above) is dispatched to whichever service owns the matching API. Wire format is HAL+JSON, matching the AWS SDK's expectation that list responses use the singular `item` key.

## Introspection

- `GET /_fakecloud/apigateway/requests` — list all data-plane requests served (method, path, headers, query params, status code, integration response)

## Cross-service delivery

- **API Gateway v1 -> Lambda** — REST API methods with `AWS_PROXY` integrations invoke Lambda functions with proxy event v1.0 format
- **API Gateway v1 -> Lambda (authorizers)** — TOKEN and REQUEST authorizers invoke real Lambda functions and honor the returned IAM policy + context
- **API Gateway v1 -> Cognito** — COGNITO_USER_POOLS authorizers fetch the configured user pool's JWKS and verify caller JWTs end-to-end
- **API Gateway v1 -> AWS services** — `AWS` integrations dispatch to in-process service handlers (SQS, SNS, DynamoDB, S3, ...) without leaving the process

## Why this matters

LocalStack paywalls API Gateway v1. fakecloud implements the full REST API surface free, with real route matching, real Lambda proxy integration, and full request introspection.

## Not yet implemented

- **DomainNameAccessAssociation** ops (4 ops): the AWS-side cross-account access association resource
- **CreateDocumentationPart** with the full property language: storage works, but property templating semantics are stubbed
- WAF wiring, VPC private endpoints, and edge-level canary deployments — these belong on the v1 follow-up roadmap

## Limitations

- Gateway response customization (per-`responseType` template overrides) renders the default response body; the AWS catalog of templated error shapes is not yet substituted.

## Source

- [`crates/fakecloud-apigateway`](https://github.com/faiscadev/fakecloud/tree/main/crates/fakecloud-apigateway)
- [AWS API Gateway v1 API reference](https://docs.aws.amazon.com/apigateway/latest/api/API_Operations.html)
