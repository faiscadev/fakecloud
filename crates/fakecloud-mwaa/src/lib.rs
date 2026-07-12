//! Amazon Managed Workflows for Apache Airflow (`mwaa`) restJson1 control plane
//! for fakecloud.
//!
//! The full 12-operation Amazon MWAA Smithy model: environments (with the async
//! `CREATING` -> `AVAILABLE` lifecycle, `UPDATING` update that applies the
//! requested changes and records a `SUCCESS` `LastUpdate`, and `DELETING`
//! teardown), the short-lived `CreateCliToken` / `CreateWebLoginToken` access
//! tokens, the `InvokeRestApi` passthrough to the Airflow web server, the
//! internal `PublishMetrics` sink, and ARN-keyed resource tagging
//! (`TagResource` / `UntagResource` / `ListTagsForResource`).
//!
//! MWAA signs SigV4 with the `airflow` scope (`arn:aws:airflow:...`); the
//! server aliases that scope to this `mwaa` registry entry. Requests are routed
//! to an operation by HTTP method + `@http` URI path (`PUT /environments/{Name}`,
//! `GET /environments`, `PATCH /environments/{Name}`, `POST /clitoken/{Name}`,
//! `GET /tags/{ResourceArn}`, ...); path labels are captured positionally and
//! percent-decoded, and query parameters are read from the raw query string so
//! repeated multi-value keys (`tagKeys=a&tagKeys=b`) survive intact.
//!
//! Everything is real, persisted, account-partitioned state: every
//! `CreateEnvironment` / `UpdateEnvironment` is reflected by its
//! `GetEnvironment` / `ListEnvironments`, every `DeleteEnvironment` deletes, and
//! AWS's async environment lifecycle is modelled by returning the transient
//! state and settling on the next read (with in-flight transitions reconciled
//! on restart). A later batch attaches a real Docker-backed Airflow web server /
//! DAG runtime behind this same control plane.

pub mod persistence;
pub mod service;
pub mod shared;
pub mod state;
mod validate;

pub use service::{MwaaService, MWAA_ACTIONS};
pub use state::{MwaaData, MwaaSnapshot, SharedMwaaState, MWAA_SNAPSHOT_SCHEMA_VERSION};
