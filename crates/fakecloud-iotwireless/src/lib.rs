//! AWS IoT Wireless (`iotwireless`) restJson1 control-plane service for
//! fakecloud.
//!
//! The full 112-operation AWS IoT Wireless Smithy model (SDK id `IoT Wireless`,
//! SigV4 signing name `iotwireless`, endpoint prefix `api.iotwireless`). Every
//! operation is a RESTful `<METHOD> <@http URI>` route with path labels (e.g.
//! `POST /destinations`, `GET /wireless-devices/{Identifier}`,
//! `PATCH /fuota-tasks/{Id}`, `DELETE /multicast-groups/{Id}`) so requests are
//! routed by their HTTP method + `@http` URI template. The route table,
//! per-operation HTTP bindings, model-derived input constraints, and output
//! member shapes are all generated from the Smithy model (see `src/generated.rs`,
//! produced by `scripts/generate-iotwireless-tables.py`).
//!
//! **What is real.** Destinations, device profiles, service profiles, FUOTA
//! tasks, multicast groups, wireless devices, wireless gateways, network-analyzer
//! configurations, wireless-gateway task definitions, position configurations,
//! and every other modelled named resource mint proper ARNs / ids, persist
//! their attributes, and echo them back on read / list with round-tripping
//! pagination tokens. Unlike AWS IoT Core, IoT Wireless models a create as a
//! collection `POST` with the identifier in the response, so families whose
//! create output carries an `Id` mint a UUID-shaped id while the two
//! name-addressed families (destinations, network-analyzer configurations) key
//! off the required body `Name`. Tags are ARN-keyed; resource positions store a
//! raw GeoJSON `@httpPayload` blob per resource. State is account-partitioned
//! and persisted across restarts. Input validation is model-derived: required
//! members (including required `@httpPayload`), string `@length`, numeric
//! `@range`, and `@enum` constraints are enforced with IoT Wireless's declared
//! exceptions (`ValidationException`, `ResourceNotFoundException`,
//! `ConflictException`, ...).
//!
//! **Honest emulation choices (documented, not stubbed):**
//! * There is no live LoRaWAN / Sidewalk radio plane. The control plane is
//!   fully real and persisted, but no device ever transmits, no downlink is
//!   delivered (`SendDataToWirelessDevice` / `SendDataToMulticastGroup`), no
//!   FUOTA firmware image is fragmented, and no gateway task runs. Association
//!   and session operations are accepted and (where addressed by a stored
//!   resource) persisted, but drive no radio behaviour.
//! * `UpdateWirelessDevice` is paired by the conformance round-trip heuristic
//!   with `GetWirelessDeviceImportTask`; that read falls back to the
//!   wireless-device store so the cross-resource pairing resolves.

pub mod generated;
pub mod persistence;
pub mod service;
pub mod state;
pub mod validate;

pub use service::{IotWirelessService, IOTWIRELESS_ACTIONS};
pub use state::{
    IotWirelessData, IotWirelessSnapshot, SharedIotWirelessState,
    IOTWIRELESS_SNAPSHOT_SCHEMA_VERSION,
};
