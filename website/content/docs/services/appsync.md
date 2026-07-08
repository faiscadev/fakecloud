+++
title = "AWS AppSync"
description = "AWS AppSync (appsync) on fakecloud: the complete 74-operation surface (100% conformance) — GraphQL APIs, data sources, resolvers, functions, schema types, API caches, domain names, Event APIs, and source-API associations. restJson1."
weight = 75
+++

fakecloud implements **AWS AppSync** as a restJson1 service. All **74 operations**
ship with **100% conformance** against AWS's own Smithy model, backed by
account-partitioned state that persists across restarts in persistent mode.
AppSync signs SigV4 with the `appsync` scope; every operation is a RESTful
`<METHOD> /v1|/v2/...` route with path labels (e.g. `POST /v1/apis`,
`GET /v1/apis/{apiId}`, `POST /v1/apis/{apiId}/types/{typeName}/resolvers`),
routed by its HTTP method and `@http` URI template.

This is a faithful **control plane + schema state**, not a set of stubs: every
resource is created, stored, and read back with the exact wire shapes AWS
returns, and every sub-resource operation validates its parent API.

## GraphQL APIs

- **`CreateGraphqlApi`** mints an `apiId`, an
  `arn:aws:appsync:<region>:<account>:apis/<apiId>` ARN, the GRAPHQL + REALTIME
  endpoint `uris` (and matching `dns`), and echoes the auth configuration for
  any of the five authentication types — `API_KEY`, `AWS_IAM`,
  `AMAZON_COGNITO_USER_POOLS`, `OPENID_CONNECT`, `AWS_LAMBDA` — including
  `additionalAuthenticationProviders`, `userPoolConfig`, `openIDConnectConfig`,
  `lambdaAuthorizerConfig`, `logConfig`, and the introspection / query-depth /
  resolver-count limits.
- **`GetGraphqlApi`** / **`ListGraphqlApis`** / **`UpdateGraphqlApi`** /
  **`DeleteGraphqlApi`** round-trip the stored API. Deleting an API cascades to
  its keys, data sources, resolvers, functions, types, cache, schema, and
  environment variables.

## Sub-resources

- **API keys** (`CreateApiKey` / `ListApiKeys` / `UpdateApiKey` /
  `DeleteApiKey`) with `expires` / `deletes` epoch bounds.
- **Data sources** (`CreateDataSource` / `Get` / `List` / `Update` / `Delete`)
  for every `DataSourceType` — `AWS_LAMBDA`, `AMAZON_DYNAMODB`,
  `AMAZON_OPENSEARCH_SERVICE`, `AMAZON_ELASTICSEARCH`, `HTTP`,
  `RELATIONAL_DATABASE`, `AMAZON_EVENTBRIDGE`, `AMAZON_BEDROCK_RUNTIME`,
  `NONE` — with the type-specific config (`dynamodbConfig`, `lambdaConfig`,
  `httpConfig`, ...) echoed verbatim.
- **Resolvers** (`CreateResolver` / `Get` / `List` / `ListResolversByFunction` /
  `Update` / `Delete`) attached to a `type.field`, `UNIT` or `PIPELINE` kind,
  with VTL request/response mapping templates or `APPSYNC_JS` `code` +
  `runtime`.
- **Functions** (`CreateFunction` / `Get` / `List` / `Update` / `Delete`).
- **Schema types** (`CreateType` / `Get` / `List` / `Update` / `Delete`), SDL or
  JSON `format`.
- **API caches** (`CreateApiCache` / `Get` / `Update` / `Delete` /
  `FlushApiCache`) and **environment variables**
  (`PutGraphqlApiEnvironmentVariables` / `GetGraphqlApiEnvironmentVariables`).
- **Custom domain names** (`CreateDomainName` / `Get` / `List` / `Update` /
  `Delete`) and **API associations** (`AssociateApi` / `GetApiAssociation` /
  `DisassociateApi`).
- **Event APIs** (`CreateApi` / `GetApi` / `ListApis` / `UpdateApi` /
  `DeleteApi`) and **channel namespaces** (`CreateChannelNamespace` and its
  Get/List/Update/Delete).
- **Source-API associations** (`AssociateSourceGraphqlApi` /
  `AssociateMergedGraphqlApi` / `GetSourceApiAssociation` /
  `ListSourceApiAssociations` / `UpdateSourceApiAssociation` /
  `DisassociateSourceGraphqlApi` / `DisassociateMergedGraphqlApi` /
  `StartSchemaMerge` / `ListTypesByAssociation`).

## Schema lifecycle

`StartSchemaCreation` ingests a base64-encoded SDL schema document and stores
it. `GetSchemaCreationStatus` settles the async lifecycle from `PROCESSING` to
`SUCCESS` on read (the same state-machine-on-read pattern other async fakecloud
operations use). `GetIntrospectionSchema` returns a representation derived from
the stored SDL: the SDL verbatim for `format=SDL`, or a deterministic JSON
projection listing the declared type names for `format=JSON`.

## Validation and errors

Model-derived validation rejects contract violations with the error codes each
operation declares — `BadRequestException` universally (missing required
members, out-of-range integers, invalid enums, over-length strings), plus
`NotFoundException` (a sub-resource op whose parent API — or the addressed
resource — does not exist), `ConcurrentModificationException`, and
`ConflictException` (a duplicate channel namespace). State is
account-partitioned and persists across restarts in persistent mode.

## Honest limitations

- **GraphQL query execution is not implemented.** fakecloud models the AppSync
  *control plane* and schema state; it does not resolve GraphQL queries or
  subscriptions against the configured data sources. This is the faithful
  control plane such an execution engine would build on, and it is a candidate
  for a later batch.
- **`EvaluateCode` / `EvaluateMappingTemplate` run a documented evaluation
  subset.** They validate that the code/template and the `context` are
  well-formed and deterministically render the context's `arguments` (the
  overwhelmingly common `$util.toJson($ctx.arguments)` shape), returning the
  model's `error` output member on a malformed context. They do **not** run a
  full VTL or `APPSYNC_JS` interpreter.
- **Data-source introspection** (`StartDataSourceIntrospection` /
  `GetDataSourceIntrospection`) returns job metadata; it does not introspect a
  live relational database.
