//! AWS IoT Core (`iot`) restJson1 control-plane service for fakecloud.
//!
//! The full 272-operation AWS IoT Core Smithy model (SDK id `IoT`, SigV4
//! signing name `iot`, endpoint prefix `iot`). Every operation is a RESTful
//! `<METHOD> <@http URI>` route with path labels (e.g.
//! `POST /things/{thingName}`, `GET /policies/{policyName}`,
//! `PUT /jobs/{jobId}`, `POST /rules/{ruleName}`) so requests are routed by
//! their HTTP method + `@http` URI template. The route table, per-operation
//! HTTP bindings, model-derived input constraints, and output member shapes
//! are all generated from the Smithy model (see `src/generated.rs`, produced
//! by `scripts/generate-iot-tables.py`), so the registry, jobs, rules, and security control
//! plane tracks the model exactly.
//!
//! **What is real.** Things, thing types, thing groups (static + dynamic),
//! billing groups, policies (+ versions + attachments), certificates (+ CA
//! certificates + principal attachments), jobs (+ job templates), topic rules
//! (+ rule destinations), Device Defender security profiles / scheduled audits
//! / audit configuration / mitigation actions / custom metrics / dimensions,
//! provisioning templates (+ versions), domain configurations, fleet metrics,
//! role aliases, authorizers, streams, OTA updates, packages (+ versions),
//! certificate providers, commands, and every other modelled resource mint
//! proper ARNs / ids, persist their attributes, echo them back on
//! read / list with round-tripping pagination tokens, and enforce referential
//! rules (attach / detach principals + policies, thing-group + billing-group
//! membership). State is account-partitioned and persisted across restarts.
//! `CreateKeysAndCertificate` / `CreateCertificateFromCsr` mint a 64-hex
//! certificate id + ARN and a structurally-shaped PEM certificate and RSA key
//! pair. `DescribeEndpoint` returns a deterministic account-specific endpoint
//! for every endpoint type (`iot:Data-ATS`, `iot:CredentialProvider`,
//! `iot:Jobs`, ...). Jobs carry a lifecycle (`QUEUED` -> `IN_PROGRESS` ->
//! `COMPLETED`); topic rules store their SQL + actions verbatim. Input
//! validation is model-derived: required members, string `@length`, numeric
//! `@range`, and `@enum` constraints are enforced with IoT's declared
//! exceptions (`ResourceNotFoundException`, `InvalidRequestException`,
//! `ResourceAlreadyExistsException`, `DeleteConflictException`,
//! `VersionConflictException`, ...).
//!
//! **Honest emulation choices (documented, not stubbed):**
//! * There is no live MQTT broker or device connectivity. The registry / jobs
//!   / rules / security *control plane* is fully real and persisted, but no
//!   message is routed, no topic-rule action is executed against a real target
//!   (SNS / SQS / Lambda / ...), and no device ever attaches over MQTT. The
//!   data plane lives in the separate `fakecloud-iotdata` service (device
//!   shadows + retained messages).
//! * `SearchIndex` and the aggregation queries (`GetCardinality`,
//!   `GetPercentiles`, `GetStatistics`, `GetBucketsAggregation`) run against
//!   the in-memory thing registry with a bounded query subset (a `thingName:`
//!   prefix / exact match and the wildcard `*`). Queries outside that subset
//!   return `InvalidQueryException` rather than a wrong result.
//! * Certificates are structurally-valid PEM placeholders, not real CA-signed
//!   X.509 chains; the key pair is a shaped placeholder, not a usable private
//!   key.

pub mod generated;
pub mod persistence;
pub mod service;
pub mod state;
pub mod validate;

pub use service::{IotService, IOT_ACTIONS};
pub use state::{IotData, IotSnapshot, SharedIotState, IOT_SNAPSHOT_SCHEMA_VERSION};
