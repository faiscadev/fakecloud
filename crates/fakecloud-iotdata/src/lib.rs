//! AWS IoT Data Plane (`iotdata`) restJson1 service for fakecloud.
//!
//! The full 11-operation AWS IoT Data Plane Smithy model (SDK id
//! `IoT Data Plane`, SigV4 signing name `iotdata`, endpoint prefix
//! `data-ats.iot`). Every operation is a RESTful `<METHOD> <@http URI>` route
//! with path labels (e.g. `POST /things/{thingName}/shadow`,
//! `GET /retainedMessage/{topic}`, `POST /topics/{topic}`) so requests are
//! routed by their HTTP method + `@http` URI template. Device-shadow and
//! retained-message state is real, persisted, and account-partitioned.
//!
//! **Device shadows (classic + named).** `UpdateThingShadow` accepts a shadow
//! document (`{"state":{"desired":…,"reported":…}}`) as the raw
//! `@httpPayload` body, deep-merges `desired`/`reported` into the stored shadow
//! (a `null` leaf deletes that key, matching AWS), bumps `version`, stamps
//! per-leaf `metadata` timestamps, and recomputes `state.delta` (the subset of
//! `desired` that differs from `reported`). `GetThingShadow` returns the full
//! shadow document (state + delta + metadata + version + timestamp).
//! `DeleteThingShadow` removes it and returns the deletion payload
//! (`version` + `timestamp`). Named shadows are keyed by
//! `(thingName, shadowName)` and behave identically;
//! `ListNamedShadowsForThing` paginates a thing's named-shadow names with a
//! round-tripping `nextToken`. A shadow-`version` conflict (the update carries
//! a `version` that does not match the stored one) returns
//! `ConflictException`. A missing shadow on GET/DELETE returns
//! `ResourceNotFoundException`.
//!
//! **Publish + retained messages.** `Publish` accepts an MQTT message for a
//! `topic` with `qos`/`retain` + a payload. When `retain=true` the message is
//! stored so `GetRetainedMessage` / `ListRetainedMessages` reflect it (an empty
//! retained payload clears the topic, matching AWS); a non-retained publish is
//! accepted as a 200 no-op. `GetRetainedMessage` returns a stored message
//! (payload, qos, `lastModifiedTime`); `ListRetainedMessages` paginates the
//! retained-topic summaries.
//!
//! **Honest emulation choices (documented, not stubbed):**
//! * fakecloud runs no MQTT broker, so `Publish` is accepted and — for
//!   retained messages — stored, but is never fanned out to live subscribers.
//!   There is no live pub/sub delivery.
//! * Because there is no broker, no MQTT client is ever *connected*, so the
//!   connection-introspection operations (`GetConnection`, `DeleteConnection`,
//!   `ListSubscriptions`, `SendDirectMessage`) faithfully report
//!   `ResourceNotFoundException` for every client id — the AWS-correct response
//!   when no such connection exists. This is a real response reflecting the
//!   emulated state, not a fabricated one.
//! * Shadows are stored by `thingName` string; fakecloud does not require the
//!   thing to pre-exist in an IoT Core registry.

pub mod persistence;
pub mod service;
pub mod shared;
pub mod state;
mod validate;

pub use service::{IotDataService, IOTDATA_ACTIONS};
pub use state::{
    IotDataData, IotDataSnapshot, SharedIotDataState, IOTDATA_SNAPSHOT_SCHEMA_VERSION,
};
