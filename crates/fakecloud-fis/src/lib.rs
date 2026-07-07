//! AWS Fault Injection Simulator (`fis`) restJson1 control plane for fakecloud.
//!
//! The full 26-operation AWS FIS Smithy model: experiment templates
//! (`CreateExperimentTemplate` / `UpdateExperimentTemplate` /
//! `GetExperimentTemplate` / `ListExperimentTemplates` /
//! `DeleteExperimentTemplate`), the experiment lifecycle
//! (`StartExperiment` -> `initiating` -> `running` -> `completed`,
//! `StopExperiment` -> `stopping` -> `stopped`, settled deterministically on the
//! next read like the MWAA environment state machine), the AWS-provided static
//! `actions` catalog (`ListActions` / `GetAction`) and target-resource-type
//! catalog (`ListTargetResourceTypes` / `GetTargetResourceType`), per-template
//! and per-experiment multi-account target-account configurations
//! (`CreateTargetAccountConfiguration` / `UpdateTargetAccountConfiguration` /
//! `DeleteTargetAccountConfiguration` / `GetTargetAccountConfiguration` /
//! `ListTargetAccountConfigurations` and their experiment-scoped read
//! counterparts), resolved-target listing (`ListExperimentResolvedTargets`),
//! account-level safety levers (`GetSafetyLever` / `UpdateSafetyLeverState`),
//! and ARN-keyed resource tagging (`TagResource` / `UntagResource` /
//! `ListTagsForResource`).
//!
//! FIS signs SigV4 with the `fis` scope, so no signing-name alias is required.
//! Requests are routed to an operation by HTTP method + `@http` URI path
//! (`POST /experimentTemplates`, `GET /experimentTemplates/{id}`,
//! `PATCH /experimentTemplates/{id}`, `POST /experiments`,
//! `DELETE /experiments/{id}`, `GET /actions/{id}`,
//! `GET /tags/{resourceArn}`, ...); path labels are captured positionally and
//! percent-decoded, and query parameters are read from the raw query string so
//! repeated multi-value keys (`tagKeys=a&tagKeys=b`) survive intact. restJson1
//! member names are camelCase (FIS declares no `jsonName` overrides).
//!
//! Everything is real, persisted, account-partitioned state: every
//! `CreateExperimentTemplate` / `UpdateExperimentTemplate` is reflected by its
//! `GetExperimentTemplate` / `ListExperimentTemplates`, every
//! `DeleteExperimentTemplate` deletes, every `StartExperiment` is durably
//! recorded and its deterministic lifecycle settles on the next read (and is
//! reconciled on restart). The control plane is modelled faithfully; the actual
//! Docker-backed fault injection into other services (stopping EC2 instances,
//! draining ECS tasks, ...) is a later batch, and until it is attached the
//! experiment progresses through its states without perturbing real resources.

pub mod catalog;
pub mod persistence;
pub mod service;
pub mod shared;
pub mod state;
mod validate;

pub use service::{FisService, FIS_ACTIONS};
pub use state::{FisData, FisSnapshot, SharedFisState, FIS_SNAPSHOT_SCHEMA_VERSION};
