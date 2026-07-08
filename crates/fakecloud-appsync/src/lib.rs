//! AWS AppSync (`appsync`) restJson1 control plane + schema state for fakecloud.
//!
//! The full 74-operation AWS AppSync Smithy model. AppSync signs SigV4 with the
//! `appsync` scope and speaks restJson1; every operation is a RESTful
//! `<METHOD> /v1|/v2/...` route with path labels (e.g. `POST /v1/apis`,
//! `GET /v1/apis/{apiId}`, `POST /v1/apis/{apiId}/types/{typeName}/resolvers`),
//! so requests are routed by their HTTP method + `@http` URI template.
//!
//! This is real, persisted, account-partitioned control-plane + schema state,
//! not a set of stubs:
//!
//! * **GraphQL APIs.** `CreateGraphqlApi` mints an `apiId`, ARN, GRAPHQL +
//!   REALTIME endpoint `uris`, and echoes the auth config (`API_KEY`,
//!   `AWS_IAM`, `AMAZON_COGNITO_USER_POOLS`, `OPENID_CONNECT`, `AWS_LAMBDA`),
//!   round-tripping via Get/List/Update/Delete.
//! * **Sub-resources.** API keys (with expiry), data sources (all
//!   `DataSourceType`s with their config echoed), resolvers (UNIT/PIPELINE,
//!   VTL or `APPSYNC_JS` code, attached to `type.field`), functions, schema
//!   types, API caches, domain names + API associations, the newer Event-API
//!   surface (`CreateApi`/channel namespaces), and merged/source-API
//!   associations. Every sub-resource op validates its parent and returns the
//!   declared `NotFoundException` when the parent (or the resource) is absent.
//! * **Schema lifecycle.** `StartSchemaCreation` ingests an SDL schema blob and
//!   settles `GetSchemaCreationStatus` to `SUCCESS` on read (a state machine
//!   like other async ops). `GetIntrospectionSchema` returns a representation
//!   (SDL or JSON per the `format` query param) derived from the stored schema.
//! * **Evaluation.** `EvaluateCode` / `EvaluateMappingTemplate` validate the
//!   code/template + context are well-formed and return a deterministic,
//!   honestly-documented evaluated result (see [`evaluate`]); they do NOT run a
//!   full VTL/`APPSYNC_JS` interpreter.
//!
//! Model-driven validation rejects contract violations with the error codes
//! each operation declares (`BadRequestException` universally, plus
//! `NotFoundException`, `ConcurrentModificationException`, etc.).
//!
//! **Not yet implemented (documented gap, not stubbed):** the GraphQL query
//! *execution* data plane -- actually resolving GraphQL queries/subscriptions
//! against the configured data sources -- is a later batch. This crate is the
//! faithful control plane + schema state that such execution would build on.

pub mod evaluate;
pub mod persistence;
pub mod schema;
pub mod service;
pub mod state;
mod validate;

pub use service::{AppSyncService, APPSYNC_ACTIONS};
pub use state::{
    AppSyncData, AppSyncSnapshot, SharedAppSyncState, APPSYNC_SNAPSHOT_SCHEMA_VERSION,
};
