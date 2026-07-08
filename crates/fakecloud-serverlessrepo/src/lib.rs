//! AWS Serverless Application Repository (`serverlessrepo`) restJson1 control
//! plane for fakecloud.
//!
//! The full 14-operation AWS Serverless Application Repository Smithy model. SAR
//! signs SigV4 with the `serverlessrepo` scope and speaks restJson1; every
//! operation is a RESTful `<METHOD> /applications/...` route with path labels
//! (e.g. `POST /applications`, `GET /applications/{ApplicationId}`,
//! `PUT /applications/{ApplicationId}/versions/{SemanticVersion}`), so requests
//! are routed by their HTTP method + `@http` URI template.
//!
//! This is real, persisted, account-partitioned control-plane state, not a set
//! of stubs:
//!
//! * **Applications.** `CreateApplication` mints the application ARN (which is
//!   the `applicationId`), stores author/description/name/homePageUrl/labels/
//!   license/readme/spdxLicenseId/sourceCodeUrl, and -- when a
//!   `semanticVersion` + template is supplied -- seeds an initial version.
//!   `GetApplication` returns the application plus its `Version` block (with
//!   `parameterDefinitions` parsed from the SAM/CloudFormation template,
//!   `requiredCapabilities`, and `resourcesSupported`), optionally pinned to a
//!   `semanticVersion`. `ListApplications` paginates with a round-tripping
//!   `nextToken`; `UpdateApplication` patches the mutable metadata;
//!   `DeleteApplication` removes the application.
//! * **Versions.** `CreateApplicationVersion` (a `PUT` carrying the semantic
//!   version in the path) stores a version with its template, parses
//!   `parameterDefinitions` / `requiredCapabilities` / `resourcesSupported`
//!   from the template, and records `sourceCodeUrl` / `sourceCodeArchiveUrl`.
//!   `ListApplicationVersions` paginates the semantic versions.
//! * **Sharing policy.** `PutApplicationPolicy` stores the sharing statements
//!   (principals / actions / principalOrgIDs, each with a `statementId`);
//!   `GetApplicationPolicy` returns them; `UnshareApplication` removes an
//!   organisation share.
//! * **CloudFormation templates.** `CreateCloudFormationTemplate` mints a
//!   `templateId` + expiry and a `templateUrl` that points back at this
//!   fakecloud host, with status `PREPARING` that settles to `ACTIVE` on the
//!   first `GetCloudFormationTemplate`.
//! * **Dependencies.** `ListApplicationDependencies` returns the nested-
//!   application dependencies parsed from a template's
//!   `AWS::Serverless::Application` resources (an empty list when there are
//!   none), paginated.
//!
//! Model-driven validation rejects contract violations with the error codes SAR
//! declares (`BadRequestException`, `NotFoundException`, `ConflictException`,
//! `ForbiddenException`, `InternalServerErrorException`,
//! `TooManyRequestsException`).
//!
//! **Honest gaps (documented, not stubbed):**
//! * `CreateCloudFormationChangeSet` mints well-formed `changeSetId` / `stackId`
//!   identifiers but does not drive the CloudFormation service to materialise a
//!   real stack -- there is no clean in-process seam, and fabricating a stack
//!   the CFN service does not know about would be worse than an honest,
//!   correctly-shaped identifier.
//! * The `templateUrl` returned for a version / template is a well-formed,
//!   deterministic URL pointing back at this fakecloud host, but fakecloud does
//!   not currently serve the raw template bytes at that URL.

pub mod persistence;
pub mod service;
pub mod shared;
pub mod state;
mod validate;

pub use service::{ServerlessRepoService, SERVERLESSREPO_ACTIONS};
pub use state::{
    ServerlessRepoData, ServerlessRepoSnapshot, SharedServerlessRepoState,
    SERVERLESSREPO_SNAPSHOT_SCHEMA_VERSION,
};
