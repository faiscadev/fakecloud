//! AWS Amplify (`amplify`) restJson1 control plane for fakecloud.
//!
//! The full 37-operation AWS Amplify Smithy model: apps (`CreateApp` /
//! `GetApp` / `ListApps` / `UpdateApp` / `DeleteApp`), branches, domain
//! associations (with the `CREATING` -> `AVAILABLE` verification lifecycle),
//! webhooks, backend environments, the deterministic job/deployment lifecycle
//! (`StartJob` / `GetJob` / `ListJobs` / `StopJob` / `DeleteJob` +
//! `CreateDeployment` / `StartDeployment`), build artifacts
//! (`ListArtifacts` / `GetArtifactUrl`), access-log generation, and
//! ARN-keyed resource tagging (`TagResource` / `UntagResource` /
//! `ListTagsForResource`).
//!
//! Amplify signs SigV4 with the `amplify` scope and speaks restJson1: requests
//! are routed to an operation by HTTP method + `@http` URI path
//! (`POST /apps`, `GET /apps/{appId}`, `POST /apps/{appId}/branches`,
//! `DELETE /apps/{appId}/branches/{branchName}/jobs/{jobId}/stop`, ...); path
//! labels are captured positionally and percent-decoded (so an ARN label whose
//! slashes arrive percent-encoded survives intact), and query parameters are
//! read from the raw query string.
//!
//! Everything is real, persisted, account-partitioned state: every
//! `CreateApp` / `CreateBranch` / `CreateDomainAssociation` is reflected by its
//! `Get*` / `List*`, every `Delete*` deletes, and the async domain/job
//! lifecycles are modelled by advancing the stored status on the next read
//! (with in-flight transitions reconciled on restart). Amplify is a hosting
//! control plane; there is no separately-emulable data plane (the built app
//! artifacts are produced by a real CI/CD build farm), so job builds settle
//! deterministically rather than executing a customer's build.

pub mod persistence;
pub mod service;
pub mod shared;
pub mod state;
mod validate;

pub use service::{AmplifyService, AMPLIFY_ACTIONS};
pub use state::{
    AmplifyData, AmplifySnapshot, SharedAmplifyState, AMPLIFY_SNAPSHOT_SCHEMA_VERSION,
};
