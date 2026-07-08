//! Amazon Pinpoint (`pinpoint`) restJson1 control plane for fakecloud.
//!
//! The full 122-operation Amazon Pinpoint Smithy model. Pinpoint signs SigV4
//! with the `mobiletargeting` scope and speaks restJson1; every operation is a
//! RESTful `<METHOD> /v1/apps/...` (or `/v1/templates/...`, `/v1/recommenders`,
//! `/v1/tags/...`, `/v1/phone/number/validate`) route with `@http` URI path
//! labels, so requests are routed by their HTTP method + `@http` URI template.
//! Path labels are captured positionally (percent-decoded, so an ARN-shaped
//! `ResourceArn` label survives with its slashes/colons intact) and query
//! parameters are read from the raw query string.
//!
//! This is real, persisted, account-partitioned control-plane state, not a set
//! of stubs:
//!
//! * **Apps (projects).** `CreateApp` mints a 32-hex application id + ARN
//!   (`arn:aws:mobiletargeting:<region>:<account>:apps/<id>`) and stores the
//!   name / tags / creation date. `GetApp` / `DeleteApp` project the
//!   `ApplicationResponse`; `GetApps` paginates. `GetApplicationSettings` /
//!   `UpdateApplicationSettings` store and return the campaign-hook / limits /
//!   quiet-time settings resource.
//! * **Campaigns & Segments.** Versioned: each `Update` bumps `Version` and
//!   appends a new version record, and `GetCampaignVersions` /
//!   `GetSegmentVersions` (plus the by-version reads) list them.
//!   `CreateCampaign` / `CreateSegment` mint an id + ARN, derive `SegmentType`
//!   (`IMPORT` when an import definition is supplied, else `DIMENSIONAL`), and
//!   persist the write request.
//! * **Endpoints & Users.** Endpoints are stored per `(appId, endpointId)` with
//!   their address / channel / attributes / user / opt-out;
//!   `UpdateEndpointsBatch` upserts many; `GetUserEndpoints` /
//!   `DeleteUserEndpoints` operate over the endpoints sharing a `User.UserId`;
//!   `RemoveAttributes` clears an attribute set.
//! * **Channels.** Each platform channel (ADM, APNS + sandbox/VoIP variants,
//!   Baidu, Email, GCM, SMS, Voice) stores its credentials + `Enabled` flag and
//!   echoes them back on `Get`/`Update`, with the platform-specific `Platform`
//!   value. `GetChannels` returns the configured channel map.
//! * **Journeys.** `CreateJourney` mints an id and starts in the `DRAFT` state;
//!   `UpdateJourneyState` drives the state machine (`DRAFT` -> `ACTIVE` -> ...);
//!   `GetJourneyRuns` paginates the (empty) run list.
//! * **Templates.** Email / Push / SMS / Voice / InApp templates are versioned;
//!   `Create`/`Update` append a version, `UpdateTemplateActiveVersion` pins the
//!   active one, and `ListTemplates` / `ListTemplateVersions` paginate.
//! * **Import / Export jobs & Event streams.** Jobs mint a `JobId` and settle
//!   `CREATED` -> `COMPLETED` on read. `PutEventStream` / `GetEventStream` /
//!   `DeleteEventStream` store the Kinesis/Firehose destination ARN + role.
//! * **Recommender configurations & Tags.** Recommenders are a global,
//!   persisted resource family; tags are ARN-keyed
//!   (`TagResource` / `UntagResource` / `ListTagsForResource`).
//!
//! Model-driven validation rejects contract violations with the error codes
//! Pinpoint declares (`BadRequestException`, `NotFoundException`,
//! `ForbiddenException`, `TooManyRequestsException`,
//! `InternalServerErrorException`, `PayloadTooLargeException`,
//! `MethodNotAllowedException`, `ConflictException`).
//!
//! **Honest gaps (documented, not stubbed):**
//! * There is no real message **delivery**: `SendMessages`, `SendUsersMessages`,
//!   `SendOTPMessage`, `PutEvents` and `VerifyOTPMessage` validate their input
//!   and return a structurally-correct `MessageResponse` /
//!   `SendUsersMessageResponse` with a per-address/per-endpoint delivery-status
//!   entry, but do not actually transmit email / SMS / push to any provider.
//! * The **analytics / KPI** operations (`GetApplicationDateRangeKpi`,
//!   `GetCampaignDateRangeKpi`, `GetJourneyDateRangeKpi`, the journey
//!   execution-metrics reads) return well-formed responses with an empty /
//!   zeroed metric result set rather than fabricated numbers — there is no
//!   analytics engine behind Pinpoint in fakecloud.
//! * Import / export jobs do not read or write S3; they mint a job id and
//!   settle to `COMPLETED` so the read path is realistic, but no CSV/JSON is
//!   actually processed.

pub mod persistence;
pub mod service;
pub mod shared;
pub mod state;
mod validate;

pub use service::{PinpointService, PINPOINT_ACTIONS};
pub use state::{
    PinpointData, PinpointSnapshot, SharedPinpointState, PINPOINT_SNAPSHOT_SCHEMA_VERSION,
};
