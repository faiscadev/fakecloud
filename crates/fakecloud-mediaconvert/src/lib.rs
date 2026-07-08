//! AWS Elemental MediaConvert (`mediaconvert`) restJson1 control plane for
//! fakecloud.
//!
//! The full 34-operation AWS Elemental MediaConvert Smithy model. MediaConvert
//! signs SigV4 with the `mediaconvert` scope and speaks restJson1; every
//! operation is a RESTful `<METHOD> /2017-08-29/...` route with path labels
//! (e.g. `POST /2017-08-29/queues`, `GET /2017-08-29/jobs/{id}`), so requests
//! are routed by their HTTP method + `@http` URI template.
//!
//! This is real, persisted, account-partitioned control-plane state, not a set
//! of stubs:
//!
//! * **Queues.** `CreateQueue` mints an ARN and echoes the queue's pricing plan
//!   (`ON_DEMAND` / `RESERVED`, materialising a reservation plan for reserved
//!   queues), status (`ACTIVE` / `PAUSED`) and description, round-tripping via
//!   Get/List/Update/Delete. Every account is seeded with the `Default` SYSTEM
//!   queue, which cannot be deleted.
//! * **Presets and job templates.** Full CRUD of custom presets and job
//!   templates, storing their `settings` verbatim so `Get*` echoes exactly what
//!   `Create*` persisted.
//! * **Jobs.** `CreateJob` mints a job id + ARN and persists the settings, role,
//!   queue and priority. The job is created `SUBMITTED` and settles to
//!   `COMPLETE` on the next read (`GetJob` / `ListJobs`), attaching well-formed
//!   `outputGroupDetails` / `timing`. `CancelJob` moves a non-terminal job to
//!   `CANCELED`. `ListJobs` / `SearchJobs` filter by queue/status/input file.
//! * **Policy.** `PutPolicy` / `GetPolicy` / `DeletePolicy` manage the account
//!   input-restriction policy, defaulting each input class to `ALLOWED`.
//! * **Endpoints.** `DescribeEndpoints` echoes a deterministic account-specific
//!   endpoint URL that points back at this fakecloud host.
//! * **Tagging.** `TagResource` / `UntagResource` / `ListTagsForResource` over
//!   queue/preset/job-template/job ARNs, plus certificate association and
//!   resource sharing.
//! * **Jobs queries and engine versions.** `StartJobsQuery` /
//!   `GetJobsQueryResults` and `ListVersions`.
//!
//! Model-driven validation rejects contract violations with the error codes each
//! operation declares (`BadRequestException`, `NotFoundException`,
//! `ConflictException`).
//!
//! **Not implemented (documented gap, not stubbed):** the transcoding *data
//! plane*. fakecloud does not run a real video transcoder -- no media is read or
//! written. A job settles to `COMPLETE` with correctly-shaped but empty
//! `outputGroupDetails`, and `Probe` returns an empty `probeResults` list rather
//! than fabricating container metadata. This crate is the faithful control plane
//! that such execution would build on.

pub mod persistence;
pub mod service;
pub mod shared;
pub mod state;
mod validate;

pub use service::{MediaConvertService, MEDIACONVERT_ACTIONS};
pub use state::{
    MediaConvertData, MediaConvertSnapshot, SharedMediaConvertState,
    MEDIACONVERT_SNAPSHOT_SCHEMA_VERSION,
};
